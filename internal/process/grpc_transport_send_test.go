package process

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	botfabricv1 "github.com/PhantomDaze/botfabric/gen/botfabric/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

// fakeSessionStream 是进程内 grpcSessionStream 测试替身，实现
// ProcessPlugin_SessionServer 的三个必需方法（Send/Recv/Context）。
type fakeSessionStream struct {
	mu         sync.Mutex
	closed     chan struct{}
	closedOnce sync.Once
	blocked    chan struct{} // 由 Send 阻塞路径关闭
	release    chan struct{} // 测试控制：关闭后 Send 才返回
	ctx        context.Context
}

func newFakeSessionStream() *fakeSessionStream {
	return &fakeSessionStream{
		closed:  make(chan struct{}),
		blocked: make(chan struct{}),
		release: make(chan struct{}),
		ctx:     context.Background(),
	}
}

func (s *fakeSessionStream) Send(*structpb.Struct) error {
	close(s.blocked)
	select {
	case <-s.release:
		return nil
	case <-s.closed:
		return errSessionClosed
	}
}

func (s *fakeSessionStream) Recv() (*structpb.Struct, error) {
	<-s.closed
	return nil, io.EOF
}

func (s *fakeSessionStream) Context() context.Context { return s.ctx }

func (s *fakeSessionStream) RecvMsg(any) error { return io.EOF }

func (s *fakeSessionStream) SendMsg(any) error { return nil }

func (s *fakeSessionStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeSessionStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeSessionStream) SetTrailer(metadata.MD)       {}

func (s *fakeSessionStream) close() {
	s.closedOnce.Do(func() { close(s.closed) })
}

// waitForBlocked 等待 Send 真正进入阻塞状态。
func (s *fakeSessionStream) waitForBlocked(t *testing.T) {
	t.Helper()
	select {
	case <-s.blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("等待 Send 进入阻塞超时")
	}
}

var _ botfabricv1.ProcessPlugin_SessionServer = (*fakeSessionStream)(nil)

// TestSendCancelDuringQueue 验证发送请求在排队等待期间被 context 取消时立即返回错误，
// 不会永久阻塞，也不会向底层流写入。
func TestSendCancelDuringQueue(t *testing.T) {
	raw := newFakeSessionStream()
	stream := newGRPCSessionStream(raw)
	defer stream.close()

	ctx, cancel := context.WithCancel(context.Background())
	// 先让队列里积压一个阻塞中的请求，新请求只能排队。
	go func() {
		_ = stream.send(context.Background(), wireFrame{Type: "event", RequestID: "blocked-1"})
	}()

	done := make(chan error, 1)
	go func() {
		done <- stream.send(ctx, wireFrame{Type: "event", RequestID: "queued-1"})
	}()

	select {
	case <-time.After(100 * time.Millisecond):
	case err := <-done:
		t.Fatalf("排队请求提前返回: %v", err)
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("排队取消错误 = %v，期望 context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("排队请求取消后未返回")
	}
}

// TestSendBlockedThenClose 验证底层 Send 长期阻塞时，close() 能让发送立即返回
// errSessionClosed，这是 cleanup/server.Stop 打断阻塞发送的路径。
func TestSendBlockedThenClose(t *testing.T) {
	raw := newFakeSessionStream()
	stream := newGRPCSessionStream(raw)
	defer stream.close()

	done := make(chan error, 1)
	go func() {
		done <- stream.send(context.Background(), wireFrame{Type: "health", RequestID: "blocked-2"})
	}()

	raw.waitForBlocked(t)
	stream.close()

	select {
	case err := <-done:
		if !errors.Is(err, errSessionClosed) {
			t.Fatalf("阻塞发送被 close 后错误 = %v，期望 errSessionClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("阻塞发送在 close 后未返回")
	}
}

// TestStaleResponseDiscarded 验证迟到响应被安全丢弃：
// 已被 context 取消移出 pending 的请求，后续收到响应不会 panic 或误配对。
func TestStaleResponseDiscarded(t *testing.T) {
	transport := &GRPCTransport{
		pending: make(map[string]chan wireFrame),
	}

	// 已取消的请求在写入 pending 前就返回，不会留下 waitter。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := transport.requestFrame(ctx, "stale-1", wireFrame{Type: "health", RequestID: "stale-1"}, "health_response")
	if err == nil {
		t.Fatal("已取消的请求应返回错误")
	}

	// 迟到响应落到不存在的 pending 上必须安全丢弃；空 request_id 同样丢弃。
	transport.resolve(wireFrame{Type: "health_response", RequestID: "missing"})
	transport.resolve(wireFrame{Type: "health_response", RequestID: ""})

	// 真实的 waitter 只被 resolve 一次，第二次 resolve（迟到重复响应）静默丢弃。
	transport.pendingMu.Lock()
	waiter := make(chan wireFrame, 1)
	transport.pending["dup-1"] = waiter
	transport.pendingMu.Unlock()
	transport.resolve(wireFrame{Type: "health_response", RequestID: "dup-1"})
	select {
	case <-waiter:
	default:
		t.Fatal("首次 resolve 应唤醒等待者")
	}
	transport.resolve(wireFrame{Type: "health_response", RequestID: "dup-1"})
	select {
	case frame := <-waiter:
		t.Fatalf("重复响应不应再次投递: %#v", frame)
	default:
	}
}
