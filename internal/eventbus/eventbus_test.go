package eventbus

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type allowPolicy struct{}

func (allowPolicy) AllowPublish(string, string) bool        { return true }
func (allowPolicy) AllowSubscribe(string, string) bool      { return true }
func (allowPolicy) AllowInvoke(string, string, string) bool { return true }
func (allowPolicy) AllowProvide(string, string) bool        { return true }

func TestPublishUsesPriorityThenRegistrationOrder(t *testing.T) {
	bus := newTestBus(t, time.Second, nil)
	var mu sync.Mutex
	var order []string
	add := func(name string) contract.EventHandler {
		return func(context.Context, contract.Event) error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, name)
			return nil
		}
	}

	scoped := bus.ForPlugin("subscriber")
	_, _ = scoped.Subscribe("topic", add("low"), contract.WithPriority(1))
	_, _ = scoped.Subscribe("topic", add("high-first"), contract.WithPriority(10))
	_, _ = scoped.Subscribe("topic", add("high-second"), contract.WithPriority(10))

	if err := bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{Topic: "topic"}); err != nil {
		t.Fatal(err)
	}
	want := []string{"high-first", "high-second", "low"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("投递顺序 = %v，期望 %v", order, want)
	}
}

func TestDifferentOwnersRunConcurrently(t *testing.T) {
	bus := newTestBus(t, time.Second, nil)
	entered := make(chan string, 2)
	release := make(chan struct{})
	handler := func(owner string) contract.EventHandler {
		return func(context.Context, contract.Event) error {
			entered <- owner
			<-release
			return nil
		}
	}
	_, _ = bus.ForPlugin("owner.a").Subscribe("topic", handler("owner.a"))
	_, _ = bus.ForPlugin("owner.b").Subscribe("topic", handler("owner.b"))

	completed := make(chan error, 1)
	go func() {
		completed <- bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{Topic: "topic"})
	}()

	seen := make(map[string]bool)
	for range 2 {
		select {
		case owner := <-entered:
			seen[owner] = true
		case <-time.After(200 * time.Millisecond):
			close(release)
			t.Fatal("不同插件的处理器应并发进入")
		}
	}
	close(release)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	if !seen["owner.a"] || !seen["owner.b"] {
		t.Fatalf("进入的插件 = %v", seen)
	}
}

func TestHandlerFailureDoesNotBlockOthers(t *testing.T) {
	sink := &MemoryDeadLetterSink{}
	bus := newTestBus(t, time.Second, sink)
	var called bool

	_, _ = bus.ForPlugin("bad").Subscribe("topic", func(context.Context, contract.Event) error {
		panic("boom")
	})
	_, _ = bus.ForPlugin("good").Subscribe("topic", func(context.Context, contract.Event) error {
		called = true
		return nil
	})

	if err := bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{Topic: "topic"}); err == nil {
		t.Fatal("处理器 panic 应使 Publish 返回聚合错误")
	}
	if !called {
		t.Fatal("失败处理器不应阻塞后续订阅者")
	}
	if len(sink.Entries()) != 1 {
		t.Fatalf("死信数量 = %d，期望 1", len(sink.Entries()))
	}
}

func TestHandlerTimeout(t *testing.T) {
	sink := &MemoryDeadLetterSink{}
	bus := newTestBus(t, 20*time.Millisecond, sink)
	_, _ = bus.ForPlugin("slow").Subscribe("topic", func(ctx context.Context, _ contract.Event) error {
		<-ctx.Done()
		return ctx.Err()
	})

	if err := bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{Topic: "topic"}); err == nil {
		t.Fatal("处理器超时应返回错误")
	}
	if len(sink.Entries()) != 1 {
		t.Fatalf("死信数量 = %d，期望 1", len(sink.Entries()))
	}
}

func newTestBus(t *testing.T, timeout time.Duration, sink DeadLetterSink) *Bus {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bus, err := New(timeout, allowPolicy{}, logger, sink)
	if err != nil {
		t.Fatal(err)
	}
	return bus
}
