package e2e_test

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

	"github.com/PhantomDaze/botfabric/internal/onebotapp"
	"github.com/gorilla/websocket"
)

// 本文件提供可复用的 OneBot 仿真端：
//   - simQQClient：扮演 QQ 协议端，主动连接 BotFabric，推送标准事件并自动回写动作 echo；
//   - simAppClient：扮演应用端，主动连接 BotFabric 应用端 WebSocket，接收事件并提交候选回复；
//   - simRemoteApp：扮演“接受 BotFabric 主动连接”的远端应用服务端，用于 client/both 模式。
//
// 三者都走真实 WebSocket 与真实 Runtime，不伪造内部事件/服务调用。

const (
	simProtocolAccessToken = "sim-protocol-token"
	simApplicationToken    = "sim-application-token"
)

// simQQAction 是仿真 QQ 协议端收到的一条 OneBot 动作请求。
type simQQAction struct {
	Action  string
	Params  map[string]any
	Echo    string
	Version string
}

// Text 返回动作中的文本内容，兼容 v11 字符串消息与 v12 消息段。
func (a simQQAction) Text() string {
	return simMessageText(a.Params["message"], "")
}

// GroupID 返回动作中的群号（JSON 数字或字符串均可）。
func (a simQQAction) GroupID() string {
	return simAnyString(a.Params["group_id"])
}

// simQQClient 是连接 BotFabric OneBot 协议端适配器的仿真 QQ 协议端。
// 读循环自动确认动作 echo，使响应协调器的动作调用能够完成。
type simQQClient struct {
	t       *testing.T
	botID   string
	version string
	socket  *websocket.Conn

	writeMu sync.Mutex
	mu      sync.Mutex
	actions []simQQAction
	readErr error

	notify    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// dialSimQQClient 连接真实的 OneBot 协议端适配器。
func dialSimQQClient(t *testing.T, address, path, botID, version string) *simQQClient {
	t.Helper()
	target := url.URL{Scheme: "ws", Host: address, Path: path}
	header := http.Header{"Authorization": []string{"Bearer " + simProtocolAccessToken}}
	socket, _, err := websocket.DefaultDialer.Dial(target.String(), header)
	if err != nil {
		t.Fatalf("连接仿真 QQ 协议端 %s: %v", target.String(), err)
	}
	client := &simQQClient{
		t:       t,
		botID:   botID,
		version: version,
		socket:  socket,
		notify:  make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	go client.readLoop()
	t.Cleanup(client.Close)
	return client
}

// SendGroupMessage 按协议版本推送一条群消息事件。
func (c *simQQClient) SendGroupMessage(messageID, groupID, userID, text string) error {
	timestamp := time.Now().Unix()
	var frame map[string]any
	if c.version == onebotapp.VersionV11 {
		frame = map[string]any{
			"time":         timestamp,
			"self_id":      c.botID,
			"post_type":    "message",
			"message_type": "group",
			"message_id":   messageID,
			"group_id":     groupID,
			"user_id":      userID,
			"message":      text,
			"raw_message":  text,
		}
	} else {
		frame = map[string]any{
			"time":        timestamp,
			"self_id":     c.botID,
			"type":        "message",
			"detail_type": "group",
			"id":          messageID,
			"group_id":    groupID,
			"user_id":     userID,
			"message":     []map[string]any{{"type": "text", "data": map[string]string{"text": text}}},
			"alt_message": text,
		}
	}
	return c.writeJSON(frame)
}

// Actions 返回已收到动作的快照。
func (c *simQQClient) Actions() []simQQAction {
	c.mu.Lock()
	defer c.mu.Unlock()
	actions := make([]simQQAction, len(c.actions))
	copy(actions, c.actions)
	return actions
}

// ActionCount 返回已收到动作数量。
func (c *simQQClient) ActionCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.actions)
}

// WaitActions 等待至少 n 个动作到达。
func (c *simQQClient) WaitActions(n int, timeout time.Duration) []simQQAction {
	c.t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if actions := c.Actions(); len(actions) >= n {
			return actions
		}
		select {
		case <-c.notify:
		case <-c.done:
			if actions := c.Actions(); len(actions) >= n {
				return actions
			}
			c.t.Fatalf("仿真 QQ 协议端连接已关闭，收到动作 %d，期望至少 %d：%v", c.ActionCount(), n, c.ReadError())
		case <-deadline.C:
			c.t.Fatalf("等待仿真 QQ 协议端动作超时（%s）：收到 %d，期望至少 %d", timeout, c.ActionCount(), n)
		}
	}
}

// WaitIdle 在给定时间内等待动作数量稳定为 want。
func (c *simQQClient) WaitIdle(want int, window time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := c.ActionCount(); got != want {
		c.t.Fatalf("仿真 QQ 协议端动作数 = %d，期望稳定为 %d：%#v", got, want, c.Actions())
	}
}

// ReadError 返回读循环错误；正常关闭时为 CloseError。
func (c *simQQClient) ReadError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readErr
}

// Close 关闭底层 WebSocket 并等待读循环退出。
func (c *simQQClient) Close() {
	c.closeOnce.Do(func() { _ = c.socket.Close() })
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
	}
}

func (c *simQQClient) readLoop() {
	defer close(c.done)
	for {
		_, payload, err := c.socket.ReadMessage()
		if err != nil {
			c.setReadErr(err)
			return
		}
		var frame struct {
			Action string          `json:"action"`
			Params map[string]any  `json:"params"`
			Echo   json.RawMessage `json:"echo"`
		}
		if err := json.Unmarshal(payload, &frame); err != nil {
			c.setReadErr(fmt.Errorf("解码 OneBot 动作请求: %w", err))
			return
		}
		if frame.Action == "" {
			continue
		}
		action := simQQAction{Action: frame.Action, Params: frame.Params, Echo: simRawString(frame.Echo), Version: c.version}
		c.mu.Lock()
		c.actions = append(c.actions, action)
		sequence := len(c.actions)
		c.mu.Unlock()
		c.signal()
		if action.Echo == "" {
			continue
		}
		// 真实 QQ 协议端会以 echo 关联动作结果；仿真端立即确认，避免动作调用超时。
		ack := map[string]any{
			"status":  "ok",
			"retcode": 0,
			"data":    map[string]any{"message_id": fmt.Sprintf("%s-%s-%d", c.botID, c.version, sequence)},
			"echo":    action.Echo,
		}
		if err := c.writeJSON(ack); err != nil {
			c.setReadErr(fmt.Errorf("回写动作 echo: %w", err))
			return
		}
	}
}

func (c *simQQClient) writeJSON(value any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.socket.SetWriteDeadline(time.Now().Add(2 * time.Second))
	return c.socket.WriteJSON(value)
}

func (c *simQQClient) setReadErr(err error) {
	c.mu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}
	c.mu.Unlock()
	c.signal()
}

func (c *simQQClient) signal() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// simAppEvent 是仿真应用端从 BotFabric 收到的一条标准事件。
type simAppEvent struct {
	EventID   string
	MessageID string
	GroupID   string
	UserID    string
	Text      string
}

// simAppReply 是仿真应用端对一条事件生成的候选回复。
type simAppReply struct {
	Text  string
	Delay time.Duration
}

// simAppBehavior 根据事件决定是否回复；返回 nil 表示不回复。
type simAppBehavior func(simAppEvent) *simAppReply

type simAppEnvelope struct {
	EventID    string          `json:"__botfabric_event_id"`
	GroupID    json.RawMessage `json:"group_id"`
	UserID     json.RawMessage `json:"user_id"`
	MessageID  json.RawMessage `json:"message_id"`
	Message    json.RawMessage `json:"message"`
	AltMessage string          `json:"alt_message"`
	Status     string          `json:"status"`
	Echo       json.RawMessage `json:"echo"`
}

func (e simAppEnvelope) toEvent() simAppEvent {
	return simAppEvent{
		EventID:   e.EventID,
		MessageID: simRawString(e.MessageID),
		GroupID:   simRawString(e.GroupID),
		UserID:    simRawString(e.UserID),
		Text:      simJSONText(e.Message, e.AltMessage),
	}
}

// simAppClient 是连接 BotFabric 应用端 WebSocket 的仿真应用端。
type simAppClient struct {
	t        *testing.T
	name     string
	version  string
	socket   *websocket.Conn
	behavior simAppBehavior

	writeMu sync.Mutex
	mu      sync.Mutex
	events  []simAppEvent
	acks    int
	replies int
	readErr error

	notify    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// dialSimAppClient 连接真实应用端适配器；stableID 非空时先声明稳定身份，
// 使路由规则可以按可预测的应用端 ID 选择目标。
func dialSimAppClient(t *testing.T, address, path, name, version, stableID string, behavior simAppBehavior) *simAppClient {
	t.Helper()
	target := url.URL{Scheme: "ws", Host: address, Path: path}
	header := http.Header{"Authorization": []string{"Bearer " + simApplicationToken}}
	socket, _, err := websocket.DefaultDialer.Dial(target.String(), header)
	if err != nil {
		t.Fatalf("连接仿真应用端 %s: %v", target.String(), err)
	}
	client := &simAppClient{
		t:        t,
		name:     name,
		version:  version,
		socket:   socket,
		behavior: behavior,
		notify:   make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	go client.readLoop()
	t.Cleanup(client.Close)
	if stableID != "" {
		if err := client.writeJSON(map[string]any{"__botfabric_endpoint_id": stableID}); err != nil {
			t.Fatalf("声明应用端稳定身份 %q: %v", stableID, err)
		}
	}
	return client
}

// Events 返回已收到事件快照。
func (c *simAppClient) Events() []simAppEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	events := make([]simAppEvent, len(c.events))
	copy(events, c.events)
	return events
}

// EventCount 返回已收到事件数量。
func (c *simAppClient) EventCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events)
}

// AckCount 返回已收到候选回复受理确认数量。
func (c *simAppClient) AckCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acks
}

// WaitEvents 等待至少 n 条事件到达。
func (c *simAppClient) WaitEvents(n int, timeout time.Duration) []simAppEvent {
	c.t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if events := c.Events(); len(events) >= n {
			return events
		}
		select {
		case <-c.notify:
		case <-c.done:
			if events := c.Events(); len(events) >= n {
				return events
			}
			c.t.Fatalf("仿真应用端 %s 连接已关闭，收到事件 %d，期望至少 %d：%v", c.name, c.EventCount(), n, c.ReadError())
		case <-deadline.C:
			c.t.Fatalf("等待仿真应用端 %s 事件超时（%s）：收到 %d，期望至少 %d", c.name, timeout, c.EventCount(), n)
		}
	}
}

// WaitMessageID 等待指定协议消息 ID 对应的事件到达。
func (c *simAppClient) WaitMessageID(messageID string, timeout time.Duration) simAppEvent {
	c.t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		for _, event := range c.Events() {
			if event.MessageID == messageID {
				return event
			}
		}
		select {
		case <-c.notify:
		case <-c.done:
			c.t.Fatalf("仿真应用端 %s 连接已关闭，未收到消息 %q：%v", c.name, messageID, c.ReadError())
		case <-deadline.C:
			c.t.Fatalf("等待仿真应用端 %s 消息 %q 超时（%s），已收到 %#v", c.name, messageID, timeout, c.Events())
		}
	}
}

// WaitAcks 等待至少 n 个候选回复受理确认。
func (c *simAppClient) WaitAcks(n int, timeout time.Duration) {
	c.t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if c.AckCount() >= n {
			return
		}
		select {
		case <-c.notify:
		case <-c.done:
			if c.AckCount() >= n {
				return
			}
			c.t.Fatalf("仿真应用端 %s 连接已关闭，收到确认 %d，期望至少 %d：%v", c.name, c.AckCount(), n, c.ReadError())
		case <-deadline.C:
			c.t.Fatalf("等待仿真应用端 %s 候选回复确认超时（%s）：收到 %d，期望至少 %d", c.name, timeout, c.AckCount(), n)
		}
	}
}

// ReadError 返回读循环错误。
func (c *simAppClient) ReadError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readErr
}

// Close 关闭底层 WebSocket 并等待读循环退出。
func (c *simAppClient) Close() {
	c.closeOnce.Do(func() { _ = c.socket.Close() })
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
	}
}

func (c *simAppClient) readLoop() {
	defer close(c.done)
	for {
		_, payload, err := c.socket.ReadMessage()
		if err != nil {
			c.setReadErr(err)
			return
		}
		var envelope simAppEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			c.setReadErr(fmt.Errorf("解码应用端事件: %w", err))
			return
		}
		if envelope.EventID != "" {
			event := envelope.toEvent()
			c.mu.Lock()
			c.events = append(c.events, event)
			c.mu.Unlock()
			c.signal()
			if c.behavior == nil {
				continue
			}
			reply := c.behavior(event)
			if reply == nil {
				continue
			}
			if reply.Delay > 0 {
				time.Sleep(reply.Delay)
			}
			if err := c.sendReply(event, reply.Text); err != nil {
				c.setReadErr(fmt.Errorf("提交仿真应用端回复: %w", err))
				return
			}
			continue
		}
		// 带状态且 echo 属于本客户端的帧是候选回复受理确认；身份声明确认没有 echo，忽略。
		if envelope.Status != "" && strings.HasPrefix(simRawString(envelope.Echo), c.name+"-reply-") {
			c.mu.Lock()
			c.acks++
			c.mu.Unlock()
			c.signal()
		}
	}
}

func (c *simAppClient) sendReply(event simAppEvent, text string) error {
	c.mu.Lock()
	c.replies++
	sequence := c.replies
	c.mu.Unlock()
	echo := fmt.Sprintf("%s-reply-%d", c.name, sequence)
	return c.writeJSON(simAppReplyFrame(c.version, event, text, echo))
}

func (c *simAppClient) writeJSON(value any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.socket.SetWriteDeadline(time.Now().Add(2 * time.Second))
	return c.socket.WriteJSON(value)
}

func (c *simAppClient) setReadErr(err error) {
	c.mu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}
	c.mu.Unlock()
	c.signal()
}

func (c *simAppClient) signal() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// simRemoteApp 是 client/both 模式下的远端应用服务端。
// BotFabric 主动连接它并推送事件；它读取事件后可直接回写候选回复。
type simRemoteApp struct {
	t        *testing.T
	version  string
	behavior simAppBehavior
	host     string
	path     string
	server   *httptest.Server

	mu      sync.Mutex
	events  []simAppEvent
	acks    int
	readErr error

	notify chan struct{}
}

// newSimRemoteApp 启动本地 WebSocket 应用服务端（httptest，仅回环）。
func newSimRemoteApp(t *testing.T, version string, behavior simAppBehavior) *simRemoteApp {
	t.Helper()
	remote := &simRemoteApp{
		t:        t,
		version:  version,
		behavior: behavior,
		path:     "/onebot",
		notify:   make(chan struct{}, 1),
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	remote.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		socket, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer socket.Close()
		for {
			_, payload, err := socket.ReadMessage()
			if err != nil {
				remote.setReadErr(err)
				return
			}
			var envelope simAppEnvelope
			if err := json.Unmarshal(payload, &envelope); err != nil {
				remote.setReadErr(fmt.Errorf("解码远端应用事件: %w", err))
				return
			}
			if envelope.EventID != "" {
				event := envelope.toEvent()
				remote.mu.Lock()
				remote.events = append(remote.events, event)
				remote.mu.Unlock()
				remote.signal()
				if remote.behavior == nil {
					continue
				}
				reply := remote.behavior(event)
				if reply == nil {
					continue
				}
				if reply.Delay > 0 {
					time.Sleep(reply.Delay)
				}
				if err := socket.WriteJSON(simAppReplyFrame(remote.version, event, reply.Text, t.Name()+"-reply")); err != nil {
					remote.setReadErr(fmt.Errorf("远端应用提交回复: %w", err))
					return
				}
				continue
			}
			if envelope.Status != "" {
				remote.mu.Lock()
				remote.acks++
				remote.mu.Unlock()
				remote.signal()
			}
		}
	}))
	t.Cleanup(remote.server.Close)

	parsed, err := url.Parse(remote.server.URL)
	if err != nil {
		t.Fatalf("解析远端应用地址: %v", err)
	}
	remote.host = parsed.Host
	return remote
}

// Events 返回远端应用收到的事件快照。
func (r *simRemoteApp) Events() []simAppEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	events := make([]simAppEvent, len(r.events))
	copy(events, r.events)
	return events
}

// EventCount 返回远端应用收到的事件数量。
func (r *simRemoteApp) EventCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// AckCount 返回远端应用收到的候选回复确认数量。
func (r *simRemoteApp) AckCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acks
}

// WaitEvents 等待至少 n 条事件。
func (r *simRemoteApp) WaitEvents(n int, timeout time.Duration) []simAppEvent {
	r.t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if events := r.Events(); len(events) >= n {
			return events
		}
		select {
		case <-r.notify:
		case <-deadline.C:
			r.t.Fatalf("等待远端应用事件超时（%s）：收到 %d，期望至少 %d", timeout, r.EventCount(), n)
		}
	}
}

// WaitAcks 等待至少 n 个候选回复确认。
func (r *simRemoteApp) WaitAcks(n int, timeout time.Duration) {
	r.t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if r.AckCount() >= n {
			return
		}
		select {
		case <-r.notify:
		case <-deadline.C:
			r.t.Fatalf("等待远端应用候选回复确认超时（%s）：收到 %d，期望至少 %d", timeout, r.AckCount(), n)
		}
	}
}

func (r *simRemoteApp) setReadErr(err error) {
	r.mu.Lock()
	if r.readErr == nil {
		r.readErr = err
	}
	r.mu.Unlock()
	r.signal()
}

func (r *simRemoteApp) signal() {
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

// startSimOneBotApplication 启动真实应用端适配器并注册为事件 sink。
// mode 取 server/client/both；remote 仅用于 client/both。
func startSimOneBotApplication(t *testing.T, env *simEnv, mode string, remote *simRemoteApp) *onebotapp.Adapter {
	t.Helper()
	config := onebotapp.Config{
		Enabled:           true,
		Mode:              mode,
		ListenAddress:     "127.0.0.1:0",
		PathV11:           "/app/v11",
		PathV12:           "/app/v12",
		AccessToken:       simApplicationToken,
		ReadTimeout:       2 * time.Second,
		WriteTimeout:      2 * time.Second,
		HeartbeatInterval: time.Second,
	}
	if remote != nil {
		config.RemoteAddress = remote.host
		config.RemotePath = remote.path
		config.RemoteVersion = remote.version
	}
	adapter, err := onebotapp.New(config, env.Endpoints, env.Manager.ResponseCoordinator())
	if err != nil {
		t.Fatalf("创建仿真应用端适配器: %v", err)
	}
	if err := adapter.Start(); err != nil {
		t.Fatalf("启动仿真应用端适配器: %v", err)
	}
	if err := env.Bus.RegisterSink(adapter); err != nil {
		t.Fatalf("注册仿真应用端 sink: %v", err)
	}
	t.Cleanup(func() {
		env.Bus.UnregisterSink(adapter)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := adapter.Shutdown(ctx); err != nil {
			t.Errorf("关闭仿真应用端适配器: %v", err)
		}
	})
	if mode == "client" || mode == "both" {
		if remote == nil {
			t.Fatalf("%s 模式缺少远端应用", mode)
		}
		if err := adapter.Connect(context.Background(), remote.host, remote.path, remote.version); err != nil {
			t.Fatalf("连接远端仿真应用: %v", err)
		}
	}
	return adapter
}

// waitForSimCandidates 等待应用端连接注册完成。
func waitForSimCandidates(t *testing.T, adapter *onebotapp.Adapter, ids ...string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		current := make(map[string]struct{})
		for _, id := range adapter.Candidates() {
			current[id] = struct{}{}
		}
		missing := false
		for _, id := range ids {
			if _, ok := current[id]; !ok {
				missing = true
				break
			}
		}
		if !missing {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待应用端候选 %v 超时，当前候选 %v", ids, adapter.Candidates())
		}
		time.Sleep(time.Millisecond)
	}
}

// waitForSimCandidateCount 等待应用端候选连接数达到 n。
func waitForSimCandidateCount(t *testing.T, adapter *onebotapp.Adapter, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(adapter.Candidates()) >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待应用端候选连接数 %d 超时，当前候选 %v", n, adapter.Candidates())
		}
		time.Sleep(time.Millisecond)
	}
}

// waitForSimEndpoint 等待指定端点注册完成。
func waitForSimEndpoint(env *simEnv, id string) {
	env.waitFor(2*time.Second, "端点 "+id+" 注册", func() bool {
		_, ok := env.Endpoints.Get(id)
		return ok
	})
}

// waitForSimBotEndpoint 等待指定 BotID 的协议端点注册完成。
func waitForSimBotEndpoint(env *simEnv, botID string) {
	env.waitFor(2*time.Second, "BotID "+botID+" 的协议端点", func() bool {
		for _, info := range env.Endpoints.ByBotID(botID) {
			if info.Kind == "protocol" {
				return true
			}
		}
		return false
	})
}

// simAppReplyFrame 构造应用端候选回复帧，兼容 v11 send_group_msg 与 v12 send_message。
func simAppReplyFrame(version string, event simAppEvent, text, echo string) map[string]any {
	if version == onebotapp.VersionV11 {
		return map[string]any{
			"action": "send_group_msg",
			"params": map[string]any{
				"group_id":             event.GroupID,
				"message":              text,
				"__botfabric_event_id": event.EventID,
			},
			"echo": echo,
		}
	}
	return map[string]any{
		"action": "send_message",
		"params": map[string]any{
			"detail_type":          "group",
			"group_id":             event.GroupID,
			"message":              []map[string]any{{"type": "text", "data": map[string]string{"text": text}}},
			"__botfabric_event_id": event.EventID,
		},
		"echo": echo,
	}
}

// simRawString 解析 OneBot 中可能为字符串或数字的 ID 字段。
func simRawString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		return number.String()
	}
	return strings.Trim(string(raw), `"`)
}

// simAnyString 把 JSON 解码后的通用值转换为字符串。
func simAnyString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return fmt.Sprintf("%.0f", typed)
	default:
		return fmt.Sprint(typed)
	}
}

// simJSONText 解析消息原文（字符串或消息段）。
func simJSONText(raw json.RawMessage, fallback string) string {
	if len(raw) == 0 {
		return fallback
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return fallback
	}
	return simMessageText(value, fallback)
}

// simMessageText 从 OneBot 消息字符串或消息段中提取文本。
func simMessageText(value any, fallback string) string {
	switch typed := value.(type) {
	case string:
		if typed == "" {
			return fallback
		}
		return typed
	case []any:
		var builder strings.Builder
		for _, item := range typed {
			segment, ok := item.(map[string]any)
			if !ok || segment["type"] != "text" {
				continue
			}
			data, _ := segment["data"].(map[string]any)
			if text, ok := data["text"].(string); ok {
				builder.WriteString(text)
			}
		}
		if builder.Len() == 0 {
			return fallback
		}
		return builder.String()
	default:
		return fallback
	}
}
