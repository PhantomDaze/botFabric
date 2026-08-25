package activity

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTrackerRejectsNewTasksAndWaitsForExisting(t *testing.T) {
	tracker := NewTracker()
	tracker.Activate("plugin")
	done, accepted := tracker.Begin("plugin")
	if !accepted {
		t.Fatal("已激活所有者应接受任务")
	}
	tracker.Deactivate("plugin")
	if _, accepted := tracker.Begin("plugin"); accepted {
		t.Fatal("停用后不应接受新任务")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := tracker.Wait(ctx, "plugin"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("等待错误 = %v，期望 deadline exceeded", err)
	}
	done()
	if err := tracker.Wait(context.Background(), "plugin"); err != nil {
		t.Fatal(err)
	}
}

func TestTrackerCanReactivateDrainedOwner(t *testing.T) {
	tracker := NewTracker()
	tracker.Deactivate("plugin")
	tracker.Activate("plugin")
	done, accepted := tracker.Begin("plugin")
	if !accepted {
		t.Fatal("排空后的所有者应能重新激活")
	}
	done()
}
