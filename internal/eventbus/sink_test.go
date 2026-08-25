package eventbus

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type recordingSink struct {
	mu      sync.Mutex
	ids     []string
	events  []contract.Event
	targets [][]string
}

func (s *recordingSink) Candidates() []string { return []string{"app-1", "app-2"} }
func (s *recordingSink) Deliver(_ context.Context, event contract.Event, targets []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	s.targets = append(s.targets, append([]string(nil), targets...))
	return nil
}

func TestEventSinkReceivesRoutedEvent(t *testing.T) {
	bus, err := New(time.Second, allowPolicy{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	sink := &recordingSink{}
	if err := bus.RegisterSink(sink); err != nil {
		t.Fatal(err)
	}
	if err := bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{Topic: "topic"}); err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 1 || sink.events[0].Source != "publisher" {
		t.Fatalf("sink 未收到标准事件: %#v", sink.events)
	}
}
