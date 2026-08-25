package eventbus

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/dedup"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func TestDedupDisabledKeepsBroadcastBehavior(t *testing.T) {
	bus, err := New(time.Second, allowPolicy{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	for _, owner := range []string{"a", "b"} {
		if _, err := bus.ForPlugin(owner).Subscribe("topic", func(context.Context, contract.Event) error {
			calls.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	message, _ := contract.EncodeJSON(contract.GroupMessage{GroupID: "1", UserID: "2", Text: "hello"})
	event := contract.Event{Topic: "topic", Payload: message}
	if err := bus.ForPlugin("publisher").Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("默认广播调用次数 = %d，期望 2", calls.Load())
	}
}

func TestDedupDropSkipsDuplicate(t *testing.T) {
	bus, err := New(time.Second, allowPolicy{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	deduplicator, err := dedup.New(dedup.NewMemoryStore(), contract.DedupSpec{
		Enabled:       true,
		Strategy:      contract.DedupFingerprint,
		Window:        time.Second,
		DuplicateMode: contract.DuplicateDrop,
	})
	if err != nil {
		t.Fatal(err)
	}
	bus.SetDeduplicator(deduplicator)
	var calls atomic.Int32
	if _, err := bus.ForPlugin("consumer").Subscribe("topic", func(context.Context, contract.Event) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	message, _ := contract.EncodeJSON(contract.GroupMessage{GroupID: "1", UserID: "2", Text: "hello"})
	for range 2 {
		if err := bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{Topic: "topic", Payload: message}); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("drop 模式调用次数 = %d，期望 1", calls.Load())
	}
}

func TestDedupForwardMarksDuplicate(t *testing.T) {
	bus, err := New(time.Second, allowPolicy{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	deduplicator, err := dedup.New(dedup.NewMemoryStore(), contract.DedupSpec{
		Enabled:       true,
		Strategy:      contract.DedupFingerprint,
		Window:        time.Second,
		DuplicateMode: contract.DuplicateForward,
	})
	if err != nil {
		t.Fatal(err)
	}
	bus.SetDeduplicator(deduplicator)
	var duplicates atomic.Int32
	if _, err := bus.ForPlugin("consumer").Subscribe("topic", func(_ context.Context, event contract.Event) error {
		if event.Headers["botfabric_duplicate"] == "true" {
			duplicates.Add(1)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	message, _ := contract.EncodeJSON(contract.GroupMessage{GroupID: "1", UserID: "2", Text: "hello"})
	for range 2 {
		if err := bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{Topic: "topic", Payload: message}); err != nil {
			t.Fatal(err)
		}
	}
	if duplicates.Load() != 1 {
		t.Fatalf("forward 模式重复标记次数 = %d，期望 1", duplicates.Load())
	}
}
