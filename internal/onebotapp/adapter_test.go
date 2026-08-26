package onebotapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/endpoint"
	"github.com/PhantomDaze/botfabric/internal/response"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/gorilla/websocket"
)

type submitter struct {
	replies chan contract.CandidateReply
}

func (s *submitter) Submit(_ context.Context, reply contract.CandidateReply) (response.SubmitResult, error) {
	s.replies <- reply
	return response.SubmitResult{Status: "accepted", Winner: true}, nil
}

func testConfig() Config {
	return Config{Enabled: true, ListenAddress: "127.0.0.1:0", PathV11: "/v11", PathV12: "/v12", AccessToken: "test-token", ReadTimeout: time.Second, WriteTimeout: time.Second}
}

func TestV11EventRoundTripToCandidateReply(t *testing.T) {
	testRoundTrip(t, VersionV11)
}

func TestV12EventRoundTripToCandidateReply(t *testing.T) {
	testRoundTrip(t, VersionV12)
}

func testRoundTrip(t *testing.T, version string) {
	submitter := &submitter{replies: make(chan contract.CandidateReply, 1)}
	adapter, err := New(testConfig(), endpoint.New(), submitter)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(); err != nil {
		t.Fatal(err)
	}
	defer adapter.Shutdown(context.Background())

	u, err := url.Parse("ws://" + adapter.Addr() + map[string]string{VersionV11: "/v11", VersionV12: "/v12"}[version])
	if err != nil {
		t.Fatal(err)
	}
	header := http.Header{}
	header.Set("Authorization", "Bearer test-token")
	socket, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()

	eventPayload, _ := json.Marshal(contract.GroupMessage{MessageID: "msg-1", GroupID: "group-1", UserID: "user-1", Text: "你好"})
	connection := waitConnection(t, adapter)
	event := contract.Event{ID: "event-1", Timestamp: time.Unix(1700000000, 0).UTC(), BotID: "bot-1", Payload: eventPayload}
	if err := connection.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	_, payload, err := socket.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope["__botfabric_event_id"] != "event-1" {
		t.Fatalf("推送事件缺少关联 ID: %#v", envelope)
	}
	params := map[string]any{"group_id": "group-1", "message": "回复"}
	action := map[string]any{"action": "send_group_msg", "params": params, "echo": "event-1"}
	if version == VersionV12 {
		action["action"] = "send_message"
		action["params"] = map[string]any{"group_id": "group-1", "message": []map[string]any{{"type": "text", "data": map[string]string{"text": "回复"}}}}
	}
	if err := socket.WriteJSON(action); err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-submitter.replies:
		if reply.EventID != "event-1" || reply.ApplicationID != connection.id || reply.GroupID != "group-1" || reply.Text != "回复" {
			t.Fatalf("候选回复错误: %#v", reply)
		}
	case <-time.After(time.Second):
		t.Fatal("候选回复提交超时")
	}
}

func waitConnection(t *testing.T, adapter *Adapter) *connection {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		adapter.connMu.RLock()
		for _, conn := range adapter.conns {
			adapter.connMu.RUnlock()
			return conn
		}
		adapter.connMu.RUnlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("应用端连接未注册")
	return nil
}

func TestRejectsWrongToken(t *testing.T) {
	adapter, err := New(testConfig(), endpoint.New(), &submitter{replies: make(chan contract.CandidateReply, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(); err != nil {
		t.Fatal(err)
	}
	defer adapter.Shutdown(context.Background())
	u := "ws://" + adapter.Addr() + "/v11"
	header := http.Header{}
	header.Set("Authorization", "Bearer wrong")
	_, response, err := websocket.DefaultDialer.Dial(u, header)
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("错误 token 握手结果 response=%v err=%v", response, err)
	}
}

// TestDeliverNoConnectionRecordsDeadLetter 验证事件路由到无连接的应用端 sink 时
// 返回哨兵错误（使 EventBus 记死信），而非静默丢弃。
func TestDeliverNoConnectionReturnsError(t *testing.T) {
	adapter, err := New(testConfig(), endpoint.New(), &submitter{replies: make(chan contract.CandidateReply, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(); err != nil {
		t.Fatal(err)
	}
	defer adapter.Shutdown(context.Background())
	// 不连入任何应用端；广播投递应返回哨兵错误。
	payload, _ := json.Marshal(contract.GroupMessage{GroupID: "g", UserID: "u", Text: "x"})
	event := contract.Event{ID: "event-no-conn", Topic: GroupTopic, Payload: payload}
	if err := adapter.Deliver(context.Background(), event, nil); err == nil {
		t.Fatal("无连接时 Deliver 应返回错误而非 nil（静默丢失）")
	}
}

// TestHeartbeatKeepsConnectionAlive 验证应用端启用心跳后，
// 无数据连接在多个心跳周期后仍存活（读 deadline 被续期，不超时退出）。
func TestHeartbeatKeepsConnectionAlive(t *testing.T) {
	cfg := testConfig()
	cfg.HeartbeatInterval = 30 * time.Millisecond
	submitter := &submitter{replies: make(chan contract.CandidateReply, 1)}
	adapter, err := New(cfg, endpoint.New(), submitter)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(); err != nil {
		t.Fatal(err)
	}
	defer adapter.Shutdown(context.Background())
	u, _ := url.Parse("ws://" + adapter.Addr() + "/v11")
	header := http.Header{}
	header.Set("Authorization", "Bearer test-token")
	socket, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	// 客户端需读循环以处理服务端 Ping 并回 Pong，否则服务端读 deadline 到期会断开。
	go func() {
		for {
			if _, _, err := socket.ReadMessage(); err != nil {
				return
			}
		}
	}()
	conn := waitConnection(t, adapter)
	// 等待远超单个 readWindow(60ms)，连接应仍注册存活。
	time.Sleep(150 * time.Millisecond)
	adapter.connMu.RLock()
	_, alive := adapter.conns[conn.id]
	adapter.connMu.RUnlock()
	if !alive {
		t.Fatal("心跳续期下连接不应超时退出")
	}
}

// TestClientReconnectAfterDrop 验证 client 模式配置重连后，
// 远端服务端重启后中间层自动重新连接。
func TestClientReconnectAfterDrop(t *testing.T) {
	// 远端应用端服务端：首条连接建立后关闭，模拟断线；第二次升级后保持。
	connCount := 0
	var remoteMu sync.Mutex
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteMu.Lock()
		connCount++
		n := connCount
		remoteMu.Unlock()
		socket, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if n == 1 {
			_ = socket.Close() // 首次连接立即断开
			return
		}
		// 第二次保持，读循环占位。
		for {
			if _, _, err := socket.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	addr := strings.Replace(server.URL, "http://", "", 1)

	cfg := Config{
		Enabled:              true,
		Mode:                 "client",
		AccessToken:          "test-token",
		ReadTimeout:          time.Second,
		WriteTimeout:         time.Second,
		HeartbeatInterval:    50 * time.Millisecond,
		ReconnectInterval:    30 * time.Millisecond,
		ReconnectMaxInterval: 120 * time.Millisecond,
		RemoteAddress:        addr,
		RemotePath:           "/",
		RemoteVersion:        VersionV11,
	}
	adapter, err := New(cfg, endpoint.New(), &submitter{replies: make(chan contract.CandidateReply, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Connect(context.Background(), addr, "/", VersionV11); err != nil {
		t.Fatal(err)
	}
	defer adapter.Shutdown(context.Background())

	// 等待重连成功：候选端点数量恢复为 1。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		adapter.connMu.RLock()
		n := len(adapter.conns)
		adapter.connMu.RUnlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	adapter.connMu.RLock()
	n := len(adapter.conns)
	adapter.connMu.RUnlock()
	if n == 0 {
		t.Fatal("client 未在断线后重连")
	}
}

// TestClientReconnectGivesUpAfterMaxTries 验证配置最大重试次数后停止重连。
func TestClientReconnectGivesUpAfterMaxTries(t *testing.T) {
	// 远端始终拒绝（返回 401），client 重连达到上限后应停止。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()
	addr := strings.Replace(server.URL, "http://", "", 1)

	cfg := Config{
		Enabled:              true,
		Mode:                 "client",
		AccessToken:          "test-token",
		ReadTimeout:          time.Second,
		WriteTimeout:         time.Second,
		ReconnectInterval:    10 * time.Millisecond,
		ReconnectMaxInterval: 20 * time.Millisecond,
		ReconnectMaxTries:    3,
		RemoteAddress:        addr,
		RemotePath:           "/",
		RemoteVersion:        VersionV11,
	}
	adapter, err := New(cfg, endpoint.New(), &submitter{replies: make(chan contract.CandidateReply, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Connect(context.Background(), addr, "/", VersionV11); err != nil {
		t.Fatal(err)
	}
	// 重连循环应在上限后退出，reconnWG 解除。
	done := make(chan struct{})
	go func() {
		adapter.reconnWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("重连循环未在达到上限次数后退出")
	}
	_ = adapter.Shutdown(context.Background())
}

// TestServerIdentityPreservedAcrossReconnect 验证 server 模式下应用端声明
// 稳定身份（__botfabric_endpoint_id）后，重连端点 ID 复用同一值。
func TestServerIdentityPreservedAcrossReconnect(t *testing.T) {
	adapter, err := New(testConfig(), endpoint.New(), &submitter{replies: make(chan contract.CandidateReply, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(); err != nil {
		t.Fatal(err)
	}
	defer adapter.Shutdown(context.Background())
	u, _ := url.Parse("ws://" + adapter.Addr() + "/v11")
	header := http.Header{}
	header.Set("Authorization", "Bearer test-token")

	// 首次连入并声明稳定身份。
	socket1, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		t.Fatal(err)
	}
	_ = socket1.WriteJSON(map[string]any{"action": "send_group_msg", "params": map[string]any{"group_id": "g", "message": "x"}, "__botfabric_endpoint_id": "stable-app-1"})
	// 等待身份采用。
	waitForConnID(t, adapter, "stable-app-1")

	// 断开第一次连接。
	_ = socket1.Close()
	waitForConnGone(t, adapter, "stable-app-1")

	// 第二次连入并声明同一身份。
	socket2, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		t.Fatal(err)
	}
	defer socket2.Close()
	_ = socket2.WriteJSON(map[string]any{"action": "send_group_msg", "params": map[string]any{"group_id": "g", "message": "y"}, "__botfabric_endpoint_id": "stable-app-1"})
	waitForConnID(t, adapter, "stable-app-1")

	// 端点注册表应包含该稳定 ID。
	info, ok := adapter.endpoints.Get("stable-app-1")
	if !ok {
		t.Fatal("重连后端点未以稳定 ID 注册")
	}
	if info.Owner != PluginID {
		t.Fatalf("端点 owner 错误: %s", info.Owner)
	}
}

func waitForConnID(t *testing.T, adapter *Adapter, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		adapter.connMu.RLock()
		_, ok := adapter.conns[id]
		adapter.connMu.RUnlock()
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("连接 %s 未在超时内出现", id)
}

func waitForConnGone(t *testing.T, adapter *Adapter, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		adapter.connMu.RLock()
		_, ok := adapter.conns[id]
		adapter.connMu.RUnlock()
		if !ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("连接 %s 未在超时内断开", id)
}
