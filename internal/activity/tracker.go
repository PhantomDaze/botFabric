package activity

import (
	"context"
	"sync"
)

// Tracker 管理按所有者隔离的任务准入和在途计数。
type Tracker struct {
	mu     sync.Mutex
	owners map[string]*ownerState
}

type ownerState struct {
	accepting bool
	inFlight  int
	drained   chan struct{}
	closed    bool
}

// NewTracker 创建空的任务跟踪器。
func NewTracker() *Tracker {
	return &Tracker{owners: make(map[string]*ownerState)}
}

// Activate 允许所有者接收新任务。
func (t *Tracker) Activate(owner string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, exists := t.owners[owner]
	if !exists || (state.closed && state.inFlight == 0) {
		t.owners[owner] = &ownerState{accepting: true, drained: make(chan struct{})}
		return
	}
	state.accepting = true
}

// Begin 尝试登记一个新任务，并返回幂等完成函数。
func (t *Tracker) Begin(owner string) (done func(), accepted bool) {
	t.mu.Lock()
	state, exists := t.owners[owner]
	if !exists {
		state = &ownerState{accepting: true, drained: make(chan struct{})}
		t.owners[owner] = state
	}
	if !state.accepting {
		t.mu.Unlock()
		return func() {}, false
	}
	state.inFlight++
	t.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() { t.finish(owner, state) })
	}, true
}

// Deactivate 拒绝所有者的新任务，并在现有任务归零时关闭等待通道。
func (t *Tracker) Deactivate(owner string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, exists := t.owners[owner]
	if !exists {
		state = &ownerState{drained: make(chan struct{})}
		t.owners[owner] = state
	}
	state.accepting = false
	if state.inFlight == 0 && !state.closed {
		close(state.drained)
		state.closed = true
	}
}

// Wait 等待所有者的在途任务结束。
func (t *Tracker) Wait(ctx context.Context, owner string) error {
	t.mu.Lock()
	state, exists := t.owners[owner]
	if !exists {
		t.mu.Unlock()
		return nil
	}
	drained := state.drained
	if state.inFlight == 0 {
		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()

	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *Tracker) finish(owner string, expected *ownerState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, exists := t.owners[owner]
	if !exists || state != expected || state.inFlight == 0 {
		return
	}
	state.inFlight--
	if !state.accepting && state.inFlight == 0 && !state.closed {
		close(state.drained)
		state.closed = true
	}
}
