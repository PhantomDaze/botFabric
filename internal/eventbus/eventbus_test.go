package eventbus

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
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

// TestMemoryDeadLetterSinkBounded 验证死信存储按容量上限淘汰最旧记录。
func TestMemoryDeadLetterSinkBounded(t *testing.T) {
	sink := NewMemoryDeadLetterSink(3)
	for i := 0; i < 5; i++ {
		sink.Record(DeadLetter{SubscriberID: fmt.Sprintf("owner-%d", i)})
	}
	entries := sink.Entries()
	if len(entries) != 3 {
		t.Fatalf("死信数量 = %d，期望上限 3", len(entries))
	}
	if entries[0].SubscriberID != "owner-2" || entries[2].SubscriberID != "owner-4" {
		t.Fatalf("应保留最近 3 条死信，实际 = %#v", entries)
	}
	if dropped := sink.Dropped(); dropped != 2 {
		t.Fatalf("淘汰数量 = %d，期望 2", dropped)
	}
}

// TestMemoryDeadLetterSinkDefaultBounded 验证零值存储同样使用默认上限。
func TestMemoryDeadLetterSinkDefaultBounded(t *testing.T) {
	sink := &MemoryDeadLetterSink{}
	for i := 0; i < defaultDeadLetterLimit+10; i++ {
		sink.Record(DeadLetter{})
	}
	if entries := sink.Entries(); len(entries) != defaultDeadLetterLimit {
		t.Fatalf("死信数量 = %d，期望默认上限 %d", len(entries), defaultDeadLetterLimit)
	}
}

// TestMemoryDeadLetterSinkTrimsPayloadAndBoundsBytes 验证死信会裁剪载荷并按字节上限淘汰。
func TestMemoryDeadLetterSinkTrimsPayloadAndBoundsBytes(t *testing.T) {
	sink := NewMemoryDeadLetterSinkWithLimits(100, 2048)
	sink.Record(DeadLetter{
		Event: contract.Event{
			ID:      "big",
			Payload: make([]byte, 8192),
			Headers: map[string]string{"secret": "value", "trace_id": "trace-1"},
		},
		SubscriberID: "owner",
		Error:        strings.Repeat("e", 4096),
	})
	entries := sink.Entries()
	if len(entries) != 1 {
		t.Fatalf("死信数量 = %d，期望 1", len(entries))
	}
	if got := len(entries[0].Event.Payload); got != maxDeadLetterPayload {
		t.Fatalf("载荷长度 = %d，期望截断到 %d", got, maxDeadLetterPayload)
	}
	if _, exists := entries[0].Event.Headers["secret"]; exists {
		t.Fatal("死信不应保留非安全事件头")
	}
	if entries[0].Event.Headers["trace_id"] != "trace-1" {
		t.Fatal("死信应保留 trace_id")
	}
	if got := len(entries[0].Error); got != maxDeadLetterError {
		t.Fatalf("错误摘要长度 = %d，期望 %d", got, maxDeadLetterError)
	}

	for i := range 10 {
		sink.Record(DeadLetter{Event: contract.Event{ID: fmt.Sprintf("e-%d", i), Payload: make([]byte, 1024)}})
	}
	if sink.Dropped() == 0 {
		t.Fatal("超过字节上限时应淘汰最旧死信")
	}
	total := 0
	for _, entry := range sink.Entries() {
		total += len(entry.Event.Payload)
	}
	if total > 2048 {
		t.Fatalf("保留载荷总字节 = %d，超过上限 2048", total)
	}
}

// TestMemoryDeadLetterSinkPagination 验证分页读取不复制整个存储且边界安全。
func TestMemoryDeadLetterSinkPagination(t *testing.T) {
	sink := NewMemoryDeadLetterSink(10)
	for i := range 5 {
		sink.Record(DeadLetter{Event: contract.Event{ID: fmt.Sprintf("e-%d", i)}})
	}
	page := sink.EntriesPage(1, 2)
	if len(page) != 2 || page[0].Event.ID != "e-1" || page[1].Event.ID != "e-2" {
		t.Fatalf("分页结果 = %#v", page)
	}
	if all := sink.EntriesPage(0, 0); len(all) != 5 {
		t.Fatalf("不分页结果数 = %d，期望 5", len(all))
	}
	if empty := sink.EntriesPage(10, 2); len(empty) != 0 {
		t.Fatalf("越界分页应返回空，实际 %d", len(empty))
	}
}
