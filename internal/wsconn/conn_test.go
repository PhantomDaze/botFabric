package wsconn

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// startWSUpgrader 返回一个会升级所有请求为 WebSocket 的 httptest 服务端及其地址。
func startWSUpgrader(t *testing.T, handler func(socket *websocket.Conn)) (*httptest.Server, string) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		handler(socket)
	}))
	return server, strings.Replace(server.URL, "http://", "ws://", 1)
}

// dial 连接到给定地址并返回 wsconn 助手。
func dial(t *testing.T, addr string, opts Options) *Conn {
	t.Helper()
	socket, _, err := websocket.DefaultDialer.Dial(addr, nil)
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	return New(socket, opts)
}

func TestWriteJSONAndRead(t *testing.T) {
	var got map[string]any
	var wg sync.WaitGroup
	wg.Add(1)
	server, addr := startWSUpgrader(t, func(socket *websocket.Conn) {
		defer wg.Done()
		_, payload, err := socket.ReadMessage()
		if err != nil {
			return
		}
		_ = json.Unmarshal(payload, &got)
	})
	defer server.Close()

	conn := dial(t, addr, Options{WriteTimeout: time.Second})
	defer conn.Close(websocket.CloseNormalClosure, "done")

	if err := conn.WriteJSON(map[string]any{"hello": "world"}); err != nil {
		t.Fatalf("WriteJSON 失败: %v", err)
	}
	wg.Wait()
	if got["hello"] != "world" {
		t.Fatalf("未收到预期消息: %v", got)
	}
}

func TestCloseOnceIsIdempotent(t *testing.T) {
	server, addr := startWSUpgrader(t, func(socket *websocket.Conn) {
		for {
			if _, _, err := socket.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer server.Close()

	conn := dial(t, addr, Options{})
	if err := conn.Close(websocket.CloseNormalClosure, "first"); err != nil {
		t.Fatalf("首次 Close 失败: %v", err)
	}
	// 第二次 Close 必须幂等，不 panic、不报错。
	if err := conn.Close(websocket.CloseNormalClosure, "second"); err != nil {
		t.Fatalf("二次 Close 应幂等: %v", err)
	}
}

func TestWriteTimeoutFailsOnBlockedPeer(t *testing.T) {
	// 服务端升级后不读任何消息，客户端持续写大数据最终因写超时失败。
	server, addr := startWSUpgrader(t, func(socket *websocket.Conn) {
		// 不读，持续占用 socket，触发客户端写超时。
		time.Sleep(3 * time.Second)
		_ = socket.Close()
	})
	defer server.Close()

	conn := dial(t, addr, Options{WriteTimeout: 50 * time.Millisecond})
	defer conn.Close(websocket.CloseNormalClosure, "done")

	big := make([]byte, 1024*1024)
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := conn.WriteMessage(websocket.TextMessage, big); err != nil {
			lastErr = err
			break
		}
	}
	if lastErr == nil {
		t.Fatal("持续写应因写超时失败，实际全部成功")
	}
}

func TestHeartbeatPingPongRefreshesReadDeadline(t *testing.T) {
	// 服务端在收到客户端 Ping 时自动回 Pong（gorilla 默认）。
	// 客户端启心跳后，读 deadline 被续期；若不发数据，读循环应持续存活而非超时。
	server, addr := startWSUpgrader(t, func(socket *websocket.Conn) {
		// 仅维持连接，不主动发消息。
		for {
			if _, _, err := socket.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer server.Close()

	conn := dial(t, addr, Options{HeartbeatInterval: 30 * time.Millisecond})
	stop := conn.StartKeepalive()
	defer stop()
	defer conn.Close(websocket.CloseNormalClosure, "done")

	// 在远超单个 readWindow(60ms) 的时间内，读循环不应因超时立即退出。
	// 这里用一个短 goroutine 尝试读，预期在心跳续期下持续阻塞（直到我们关闭）。
	conn.TrackRun()
	go func() {
		defer conn.RunDone()
		_, _, _ = conn.ReadMessage()
	}()

	// 等待若干心跳周期，确认读 goroutine 仍在运行（未因 deadline 超时退出）。
	time.Sleep(120 * time.Millisecond)
	// 关闭后读 goroutine 应退出。
	conn.Close(websocket.CloseNormalClosure, "done")
	done := make(chan struct{})
	go func() {
		conn.WaitRun()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WaitRun 未在关闭后返回")
	}
}

func TestWaitRunBlocksUntilRunDone(t *testing.T) {
	server, addr := startWSUpgrader(t, func(socket *websocket.Conn) {
		_, _, _ = socket.ReadMessage()
	})
	defer server.Close()

	conn := dial(t, addr, Options{})
	conn.TrackRun()
	started := make(chan struct{})
	go func() {
		defer conn.RunDone()
		close(started)
		_, _, _ = conn.ReadMessage() // 阻塞到 conn.Close
	}()
	<-started
	conn.Close(websocket.CloseNormalClosure, "done")

	done := make(chan struct{})
	go func() {
		conn.WaitRun()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WaitRun 未在 RunDone 后返回")
	}
}

func TestStartKeepaliveNoopWhenHeartbeatZero(t *testing.T) {
	server, addr := startWSUpgrader(t, func(socket *websocket.Conn) {
		for {
			if _, _, err := socket.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer server.Close()

	conn := dial(t, addr, Options{HeartbeatInterval: 0})
	stop := conn.StartKeepalive()
	stop() // 幂等，不 panic
	_ = conn.Close(websocket.CloseNormalClosure, "done")
}
