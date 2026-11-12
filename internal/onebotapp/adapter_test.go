package onebotapp

import (
	"context"
	"encoding/json"
	"fmt"
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
		if reply.DispatchSubject != response.ApplicationDispatchSubject {
			t.Fatalf("动作调用主体 = %q，期望固定主体 %q", reply.DispatchSubject, response.ApplicationDispatchSubject)
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
	_ = socket1.WriteJSON(map[string]any{"action": "send_group_msg", "params": map[string]any{"group_id": "g", "message": "x"}, "__botfabric_endpoint_id": "app:stable-app-1"})
	// 等待身份采用。
	waitForConnID(t, adapter, "app:stable-app-1")

	// 断开第一次连接。
	_ = socket1.Close()
	waitForConnGone(t, adapter, "app:stable-app-1")

	// 第二次连入并声明同一身份。
	socket2, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		t.Fatal(err)
	}
	defer socket2.Close()
	_ = socket2.WriteJSON(map[string]any{"action": "send_group_msg", "params": map[string]any{"group_id": "g", "message": "y"}, "__botfabric_endpoint_id": "app:stable-app-1"})
	waitForConnID(t, adapter, "app:stable-app-1")

	// 端点注册表应包含该稳定 ID。
	info, ok := adapter.endpoints.Get("app:stable-app-1")
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

// deliverEvent 推送一个标准群事件到应用端，返回送达的事件 ID。
func deliverEvent(t *testing.T, adapter *Adapter, eventID, groupID string) {
	t.Helper()
	payload, _ := json.Marshal(contract.GroupMessage{MessageID: "m-" + eventID, GroupID: groupID, UserID: "u", Text: "x"})
	event := contract.Event{ID: eventID, Topic: GroupTopic, Payload: payload}
	if err := adapter.Deliver(context.Background(), event, nil); err != nil {
		t.Fatalf("推送事件失败: %v", err)
	}
}

// readReply 读取服务端对一条动作帧的响应，断言其状态字段。
func readReply(t *testing.T, socket *websocket.Conn, wantStatus string) map[string]any {
	t.Helper()
	_ = socket.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := socket.ReadMessage()
	if err != nil {
		t.Fatalf("读取动作响应失败: %v", err)
	}
	var reply map[string]any
	if err := json.Unmarshal(payload, &reply); err != nil {
		t.Fatalf("解析动作响应失败: %v", err)
	}
	if reply["status"] != wantStatus {
		t.Fatalf("期望响应状态 %s，实际 %#v", wantStatus, reply)
	}
	return reply
}

// TestFallbackSameGroupResolvesToRecentUndelivered 验证应用端回复缺少 __botfabric_event_id/echo 时，
// 适配器按同群最近未决事件 fallback 关联：推送同群两个事件后，无关联回复关联到最近一个。
func TestFallbackSameGroupResolvesToRecentUndelivered(t *testing.T) {
	submitter := &submitter{replies: make(chan contract.CandidateReply, 2)}
	adapter, err := New(testConfig(), endpoint.New(), submitter)
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
	conn := waitConnection(t, adapter)
	deliverEvent(t, adapter, "evt-old", "group-A")
	deliverEvent(t, adapter, "evt-new", "group-A")
	// 消费推送的两个事件信封，使读指针越过它们，避免与动作响应混读。
	_ = socket.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 2; i++ {
		if _, _, err := socket.ReadMessage(); err != nil {
			t.Fatal(err)
		}
	}
	// 应用端回复不带 __botfabric_event_id/echo，仅带 group_id。
	reply := map[string]any{"action": "send_group_msg", "params": map[string]any{"group_id": "group-A", "message": "回复"}}
	if err := socket.WriteJSON(reply); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-submitter.replies:
		// fallback 应关联到同群最近未决事件 evt-new。
		if got.EventID != "evt-new" || got.GroupID != "group-A" || got.ApplicationID != conn.id || got.Text != "回复" {
			t.Fatalf("fallback 关联错误: %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("fallback 关联候选回复提交超时")
	}
	readReply(t, socket, "ok")
}

// TestFallbackDoesNotCrossGroups 验证 fallback 严格按群过滤：群 A、群 B 各推一事件后，
// 群 B 缺关联回复关联到群 B 事件，而非群 A 事件。
func TestFallbackDoesNotCrossGroups(t *testing.T) {
	submitter := &submitter{replies: make(chan contract.CandidateReply, 2)}
	adapter, err := New(testConfig(), endpoint.New(), submitter)
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
	waitConnection(t, adapter)
	deliverEvent(t, adapter, "evt-A", "group-A")
	deliverEvent(t, adapter, "evt-B", "group-B")
	// 消费推送的两个事件信封，使读指针越过它们，避免与动作响应混读。
	_ = socket.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 2; i++ {
		if _, _, err := socket.ReadMessage(); err != nil {
			t.Fatal(err)
		}
	}
	// 群 B 缺关联回复，应关联到群 B 事件，而非更早的群 A 事件。
	reply := map[string]any{"action": "send_group_msg", "params": map[string]any{"group_id": "group-B", "message": "回复B"}}
	if err := socket.WriteJSON(reply); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-submitter.replies:
		if got.EventID != "evt-B" || got.GroupID != "group-B" {
			t.Fatalf("跨群 fallback 误关联: %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("fallback 候选回复提交超时")
	}
}

// TestFallbackNoUndeliveredReturnsError 验证无任何同群未决事件可关联时，
// 缺关联回复返回 failed，应用端收到明确错误而非静默丢弃。
func TestFallbackNoUndeliveredReturnsError(t *testing.T) {
	submitter := &submitter{replies: make(chan contract.CandidateReply, 1)}
	adapter, err := New(testConfig(), endpoint.New(), submitter)
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
	waitConnection(t, adapter)
	// 不推送任何事件，直接缺关联回复。
	reply := map[string]any{"action": "send_group_msg", "params": map[string]any{"group_id": "group-A", "message": "回复"}}
	if err := socket.WriteJSON(reply); err != nil {
		t.Fatal(err)
	}
	readReply(t, socket, "failed")
	select {
	case <-submitter.replies:
		t.Fatal("无未决事件时不应提交候选回复")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestNumericGroupIDPreservedInCandidateReply 验证应用端以数字形式提交大群号时，
// 候选回复保留精确群号而非科学计数法（回归：float64 的 %g 输出 1.23456789e+08）。
func TestNumericGroupIDPreservedInCandidateReply(t *testing.T) {
	submitter := &submitter{replies: make(chan contract.CandidateReply, 1)}
	adapter, err := New(testConfig(), endpoint.New(), submitter)
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
	waitConnection(t, adapter)
	deliverEvent(t, adapter, "evt-numeric", "123456789")
	_ = socket.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := socket.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	action := map[string]any{"action": "send_group_msg", "params": map[string]any{
		"group_id":             123456789,
		"message":              "回复",
		"__botfabric_event_id": "evt-numeric",
	}}
	if err := socket.WriteJSON(action); err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-submitter.replies:
		if reply.GroupID != "123456789" || reply.EventID != "evt-numeric" {
			t.Fatalf("数字群号未精确保留: %#v", reply)
		}
	case <-time.After(time.Second):
		t.Fatal("候选回复提交超时")
	}
}

// TestNumericGroupIDFallbackMatchesEvent 验证数字群号参与的 fallback 同群关联：
// 推送事件的群号是字符串 "123456789"，应用端以数字提交时必须命中。
func TestNumericGroupIDFallbackMatchesEvent(t *testing.T) {
	submitter := &submitter{replies: make(chan contract.CandidateReply, 1)}
	adapter, err := New(testConfig(), endpoint.New(), submitter)
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
	waitConnection(t, adapter)
	deliverEvent(t, adapter, "evt-numeric-fallback", "123456789")
	_ = socket.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := socket.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	// 不带 event_id/echo，仅以数字群号走同群 fallback。
	action := map[string]any{"action": "send_group_msg", "params": map[string]any{
		"group_id": 123456789,
		"message":  "回复",
	}}
	if err := socket.WriteJSON(action); err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-submitter.replies:
		if reply.EventID != "evt-numeric-fallback" || reply.GroupID != "123456789" {
			t.Fatalf("数字群号 fallback 关联错误: %#v", reply)
		}
	case <-time.After(time.Second):
		t.Fatal("fallback 候选回复提交超时")
	}
}

// TestPendingBoundedByMaxPending 验证未回复事件不会超过容量上限。
func TestPendingBoundedByMaxPending(t *testing.T) {
	cfg := testConfig()
	cfg.MaxPending = 3
	cfg.PendingTTL = time.Minute
	adapter, err := New(cfg, endpoint.New(), &submitter{replies: make(chan contract.CandidateReply, 1)})
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
	// 持续消费推送信封，避免写缓冲阻塞。
	go func() {
		for {
			if _, _, err := socket.ReadMessage(); err != nil {
				return
			}
		}
	}()
	conn := waitConnection(t, adapter)
	for i := 0; i < 10; i++ {
		deliverEvent(t, adapter, fmt.Sprintf("evt-cap-%d", i), "group-cap")
	}
	conn.pendingMu.Lock()
	defer conn.pendingMu.Unlock()
	if len(conn.pending) != cfg.MaxPending {
		t.Fatalf("pending 数量 = %d，期望上限 %d", len(conn.pending), cfg.MaxPending)
	}
	// 应保留最近的 3 条，最早的被淘汰。
	for i := 0; i < 7; i++ {
		if _, ok := conn.pending[fmt.Sprintf("evt-cap-%d", i)]; ok {
			t.Fatalf("最早的事件 %d 未被淘汰", i)
		}
	}
	for i := 7; i < 10; i++ {
		if _, ok := conn.pending[fmt.Sprintf("evt-cap-%d", i)]; !ok {
			t.Fatalf("最近的事件 %d 不应被淘汰", i)
		}
	}
	// recent 与 pending 同步收敛，保证 fallback 扫描不会随历史事件无界增长。
	if len(conn.recent) != cfg.MaxPending {
		t.Fatalf("recent 数量 = %d，期望上限 %d", len(conn.recent), cfg.MaxPending)
	}
}

// TestPendingExpiresAfterTTL 验证长时间无回复的事件会按 TTL 清理。
func TestPendingExpiresAfterTTL(t *testing.T) {
	cfg := testConfig()
	cfg.PendingTTL = 40 * time.Millisecond
	cfg.MaxPending = 100
	adapter, err := New(cfg, endpoint.New(), &submitter{replies: make(chan contract.CandidateReply, 1)})
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
	go func() {
		for {
			if _, _, err := socket.ReadMessage(); err != nil {
				return
			}
		}
	}()
	conn := waitConnection(t, adapter)
	deliverEvent(t, adapter, "evt-ttl", "group-ttl")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn.pendingMu.Lock()
		count := len(conn.pending)
		recentCount := len(conn.recent)
		conn.pendingMu.Unlock()
		if count == 0 {
			if recentCount != 0 {
				t.Fatalf("TTL 清理后 recent 数量 = %d，期望 0", recentCount)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("超时后 pending 仍未被 TTL 清理")
}

// TestDeliverWriteFailureRollsBackPending 验证推送写入失败的事件不会留在 pending 中。
func TestDeliverWriteFailureRollsBackPending(t *testing.T) {
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
	socket, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		t.Fatal(err)
	}
	conn := waitConnection(t, adapter)
	_ = socket.Close()
	select {
	case <-conn.dead:
	case <-time.After(2 * time.Second):
		t.Fatal("连接未在超时内关闭")
	}
	payload, _ := json.Marshal(contract.GroupMessage{MessageID: "m-fail", GroupID: "group-fail", UserID: "u", Text: "x"})
	event := contract.Event{ID: "evt-write-fail", Topic: GroupTopic, Payload: payload}
	if err := conn.Deliver(context.Background(), event); err == nil {
		t.Fatal("连接关闭后 Deliver 应返回错误")
	}
	conn.pendingMu.Lock()
	_, ok := conn.pending["evt-write-fail"]
	conn.pendingMu.Unlock()
	if ok {
		t.Fatal("写入失败的事件不应保留在 pending")
	}
}

// TestMaxMessageSizeDefault 验证未配置时应用端连接采用默认读上限。
func TestMaxMessageSizeDefault(t *testing.T) {
	adapter, err := New(testConfig(), endpoint.New(), &submitter{replies: make(chan contract.CandidateReply, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.config.MaxMessageSize != defaultMaxMessageSize {
		t.Fatalf("MaxMessageSize = %d，期望默认值 %d", adapter.config.MaxMessageSize, defaultMaxMessageSize)
	}
}

// TestRejectsOversizedApplicationFrame 验证超过读上限的应用端帧会导致连接关闭，
// 防止超大帧持续扩容撑爆内存。
func TestRejectsOversizedApplicationFrame(t *testing.T) {
	cfg := testConfig()
	cfg.MaxMessageSize = 1024
	adapter, err := New(cfg, endpoint.New(), &submitter{replies: make(chan contract.CandidateReply, 1)})
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
	waitConnection(t, adapter)
	frame := []byte(`{"__botfabric_endpoint_id":"oversized","pad":"` + strings.Repeat("x", 4096) + `"}`)
	if err := socket.WriteMessage(websocket.TextMessage, frame); err != nil {
		t.Fatalf("写入超大帧失败: %v", err)
	}
	_ = socket.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, _, err := socket.ReadMessage()
		if err == nil {
			continue
		}
		if !websocket.IsCloseError(err, websocket.CloseMessageTooBig) && !strings.Contains(err.Error(), "read limit") {
			t.Fatalf("超大帧应触发 1009/read limit，实际错误: %v", err)
		}
		return
	}
}

// TestAcceptsFrameAtMaxMessageSize 验证恰好等于读上限的帧仍可正常处理（上限为严格大于才拒绝）。
func TestAcceptsFrameAtMaxMessageSize(t *testing.T) {
	cfg := testConfig()
	cfg.MaxMessageSize = 512
	adapter, err := New(cfg, endpoint.New(), &submitter{replies: make(chan contract.CandidateReply, 1)})
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
	waitConnection(t, adapter)
	base := `{"__botfabric_endpoint_id":"app:exact-limit","pad":""}`
	padding := int(cfg.MaxMessageSize) - len(base)
	if padding < 0 {
		t.Fatalf("测试帧基础长度 %d 超过上限 %d", len(base), cfg.MaxMessageSize)
	}
	frame := []byte(`{"__botfabric_endpoint_id":"app:exact-limit","pad":"` + strings.Repeat("x", padding) + `"}`)
	if int64(len(frame)) != cfg.MaxMessageSize {
		t.Fatalf("测试帧长度 = %d，期望 %d", len(frame), cfg.MaxMessageSize)
	}
	if err := socket.WriteMessage(websocket.TextMessage, frame); err != nil {
		t.Fatalf("写入上限大小帧失败: %v", err)
	}
	readReply(t, socket, "ok")
	waitForConnID(t, adapter, "app:exact-limit")
}

// TestDeliverConcurrentWithIdentitySwitch 验证交替声明稳定身份与并发投递之间不存在数据竞争。
// 该回归需配合 go test -race 运行。
func TestDeliverConcurrentWithIdentitySwitch(t *testing.T) {
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
	socket, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	// 持续消费推送信封与身份确认，避免服务端写阻塞。
	go func() {
		for {
			if _, _, err := socket.ReadMessage(); err != nil {
				return
			}
		}
	}()
	waitConnection(t, adapter)

	const rounds = 200
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		payload, _ := json.Marshal(contract.GroupMessage{MessageID: "m-race", GroupID: "group-race", UserID: "u", Text: "x"})
		for i := 0; i < rounds; i++ {
			event := contract.Event{ID: fmt.Sprintf("race-evt-%d", i), Topic: GroupTopic, Payload: payload}
			_ = adapter.Deliver(context.Background(), event, nil)
		}
	}()
	for i := 0; i < rounds; i++ {
		id := "app:race-stable-a"
		if i%2 == 1 {
			id = "app:race-stable-b"
		}
		if err := socket.WriteJSON(map[string]any{"__botfabric_endpoint_id": id}); err != nil {
			t.Fatalf("声明身份帧写入失败: %v", err)
		}
	}
	wg.Wait()
}

// TestRejectsStableIdentityOutsideNamespace 验证客户端不能声明插件 ID 等受信主体。
func TestRejectsStableIdentityOutsideNamespace(t *testing.T) {
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
	socket, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	connection := waitConnection(t, adapter)
	dynamicID := connection.endpointID()
	if err := socket.WriteJSON(map[string]any{"__botfabric_endpoint_id": "example.reply"}); err != nil {
		t.Fatal(err)
	}
	// 读取身份确认帧，确保该帧已被处理。
	_ = socket.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := socket.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	if _, exists := adapter.endpoints.Get("example.reply"); exists {
		t.Fatal("未命名空间化的身份不应注册为端点")
	}
	if got := connection.endpointID(); got != dynamicID {
		t.Fatalf("连接 ID 被意外改为 %q", got)
	}
}

// TestRejectsStableIdentityOwnedByOtherOwner 验证其他 owner 的端点 ID 不可被冒用或覆盖。
func TestRejectsStableIdentityOwnedByOtherOwner(t *testing.T) {
	endpoints := endpoint.New()
	if err := endpoints.Register(contract.EndpointInfo{ID: "app:conflict", Owner: "plugin.other", Kind: contract.EndpointProtocol}); err != nil {
		t.Fatal(err)
	}
	adapter, err := New(testConfig(), endpoints, &submitter{replies: make(chan contract.CandidateReply, 1)})
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
	connection := waitConnection(t, adapter)
	dynamicID := connection.endpointID()
	if err := socket.WriteJSON(map[string]any{"__botfabric_endpoint_id": "app:conflict"}); err != nil {
		t.Fatal(err)
	}
	_ = socket.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := socket.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	info, exists := endpoints.Get("app:conflict")
	if !exists || info.Owner != "plugin.other" {
		t.Fatalf("其他 owner 的端点被覆盖: %#v, exists=%v", info, exists)
	}
	if got := connection.endpointID(); got != dynamicID {
		t.Fatalf("连接 ID 被意外改为 %q", got)
	}
}
