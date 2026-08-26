// Package onebotapp 提供 OneBot 应用端接入适配器。
// 该适配器与 protocol/onebot 相反：应用端连接到 BotFabric，
// BotFabric 向应用端推送标准事件，并接收应用端的回复动作。
package onebotapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/PhantomDaze/botfabric/internal/endpoint"
	"github.com/PhantomDaze/botfabric/internal/response"
	"github.com/PhantomDaze/botfabric/internal/wsconn"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/gorilla/websocket"
)

const (
	// PluginID 是应用端 OneBot 适配器插件 ID。
	PluginID = "application.onebot"
	// GroupTopic 是应用端接收的标准群消息 Topic。
	GroupTopic = "bot.message.group"
	// GroupSchema 是应用端接收的标准群消息 Schema。
	GroupSchema = "bot.message@v1"
	// VersionV11 与 protocol/onebot 保持相同版本标识。
	VersionV11 = "v11"
	// VersionV12 与 protocol/onebot 保持相同版本标识。
	VersionV12 = "v12"
)

// errNoApplicationConnection 表示事件被路由到应用端 sink 但无任何可用连接。
// EventBus 据此记录死信，而非静默丢弃。
var errNoApplicationConnection = errors.New("无可用应用端连接")

// Config 控制应用端 OneBot 接入服务端。
type Config struct {
	Enabled              bool
	Mode                 string
	ListenAddress        string
	PathV11              string
	PathV12              string
	AccessToken          string
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	HeartbeatInterval    time.Duration
	RemoteAddress        string
	RemotePath           string
	RemoteVersion        string
	ReconnectInterval    time.Duration // client/both 重连基础间隔；0 表示不重连（失败即返回）
	ReconnectMaxInterval time.Duration // 指数退避上限；0 表示缺省=ReconnectInterval*8
	ReconnectMaxTries    int           // 最大重连次数；0 表示无限
}

// Validate 校验应用端 OneBot 配置。
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Mode == "" {
		c.Mode = "server"
	}
	if c.Mode != "server" && c.Mode != "client" && c.Mode != "both" {
		return errors.New("应用端 OneBot mode 仅支持 server、client 或 both")
	}
	if c.Mode == "server" || c.Mode == "both" {
		if c.ListenAddress == "" || c.PathV11 == "" || c.PathV12 == "" {
			return errors.New("应用端 OneBot 地址和路径不能为空")
		}
		if c.PathV11 == c.PathV12 {
			return errors.New("应用端 OneBot v11/v12 路径不能相同")
		}
	}
	if c.Mode == "client" || c.Mode == "both" {
		if c.RemoteAddress == "" || c.RemotePath == "" || (c.RemoteVersion != VersionV11 && c.RemoteVersion != VersionV12) {
			return errors.New("应用端 OneBot 远端地址、路径和版本不能为空")
		}
	}
	if len(c.AccessToken) < 8 {
		return errors.New("应用端 OneBot access token 长度不能少于 8")
	}
	if c.ReadTimeout <= 0 || c.WriteTimeout <= 0 {
		return errors.New("应用端 OneBot 读写超时必须大于零")
	}
	return nil
}

// CandidateReplySubmitter 接收应用端候选回复。
type CandidateReplySubmitter interface {
	Submit(ctx context.Context, reply contract.CandidateReply) (response.SubmitResult, error)
}

// Adapter 是应用端 OneBot WebSocket 服务端。
type Adapter struct {
	mu        sync.RWMutex
	config    Config
	endpoints *endpoint.Registry
	submitter CandidateReplySubmitter
	server    *http.Server
	listener  net.Listener
	started   bool
	connMu    sync.RWMutex
	conns     map[string]*connection
	stopMu    sync.Mutex
	stopped   bool
	reconnWG  sync.WaitGroup // 跟踪 client 重连 goroutine
}

// New 创建应用端 OneBot 适配器。
func New(config Config, endpoints *endpoint.Registry, submitter CandidateReplySubmitter) (*Adapter, error) {
	if endpoints == nil || submitter == nil {
		return nil, errors.New("应用端 OneBot 端点注册表和回复提交器不能为空")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Adapter{config: config, endpoints: endpoints, submitter: submitter, conns: make(map[string]*connection)}, nil
}

// Connect 主动连接一个作为 OneBot 服务端的应用端。
// 该连接与应用端主动连入的连接共用端点、事件推送和 CandidateReply 流程。
// 若配置了 ReconnectInterval>0，进入异步重连循环并立即返回 nil；
// 否则同步拨号一次并返回首次拨号错误（保持旧行为）。
func (a *Adapter) Connect(ctx context.Context, address, path, version string) error {
	if address == "" || path == "" || (version != VersionV11 && version != VersionV12) {
		return errors.New("应用端 OneBot 主动连接参数无效")
	}
	if a.config.ReconnectInterval > 0 {
		a.reconnWG.Add(1)
		go a.connectLoop(ctx, address, path, version)
		return nil
	}
	_, err := a.dialOnce(ctx, address, path, version)
	return err
}

// dialOnce 执行一次拨号并启动连接 run 循环；成功返回连接（用于断线监听），失败返回 nil+error。
func (a *Adapter) dialOnce(ctx context.Context, address, path, version string) (*connection, error) {
	header := http.Header{}
	if a.config.AccessToken != "" {
		header.Set("Authorization", "Bearer "+a.config.AccessToken)
	}
	socket, _, err := websocket.DefaultDialer.DialContext(ctx, "ws://"+address+path, header)
	if err != nil {
		return nil, err
	}
	conn := newConnection(a, version, socket)
	a.connMu.Lock()
	a.conns[conn.id] = conn
	a.connMu.Unlock()
	conn.ws.TrackRun()
	go conn.run()
	return conn, nil
}

// connectLoop 负责 client/both 模式的拨号与断线重连。
// 首次拨号失败不退出循环；断线后按指数退避重试，直到成功或达到次数上限或停止。
func (a *Adapter) connectLoop(ctx context.Context, address, path, version string) {
	defer a.reconnWG.Done()
	base := a.config.ReconnectInterval
	if base <= 0 {
		// 未配置重连：仅尝试一次拨号。
		_, _ = a.dialOnce(ctx, address, path, version)
		return
	}
	maxInterval := a.config.ReconnectMaxInterval
	if maxInterval <= 0 {
		maxInterval = base * 8
	}
	maxTries := a.config.ReconnectMaxTries
	try := 0
	for {
		if a.isStopped() {
			return
		}
		conn, err := a.dialOnce(ctx, address, path, version)
		if err == nil {
			// 拨号成功，阻塞等待该连接断线，再决定是否重连。
			a.waitConnDead(ctx, conn)
			if a.isStopped() {
				return
			}
			try = 0
			continue
		}
		try++
		if maxTries > 0 && try > maxTries {
			return
		}
		backoff := base * time.Duration(1<<uint(try-1))
		if backoff > maxInterval {
			backoff = maxInterval
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
	}
}

// waitConnDead 阻塞直到指定连接的 run 循环退出（断线）或停止/上下文取消。
func (a *Adapter) waitConnDead(ctx context.Context, conn *connection) {
	if conn == nil {
		return
	}
	select {
	case <-conn.dead:
	case <-ctx.Done():
	}
}

func (a *Adapter) isStopped() bool {
	a.stopMu.Lock()
	defer a.stopMu.Unlock()
	return a.stopped
}

// Start 按配置启动应用端 OneBot 服务端；client 模式由 Connect 调用方主动连接。
func (a *Adapter) Start() error {
	if err := a.config.Validate(); err != nil {
		return err
	}
	if a.config.Mode == "client" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle(a.config.PathV11, a.upgradeHandler(VersionV11))
	mux.Handle(a.config.PathV12, a.upgradeHandler(VersionV12))
	server := &http.Server{Addr: a.config.ListenAddress, Handler: mux, ReadHeaderTimeout: a.config.ReadTimeout}
	listener, err := net.Listen("tcp", a.config.ListenAddress)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.server = server
	a.listener = listener
	a.started = true
	a.mu.Unlock()
	go func() { _ = server.Serve(listener) }()
	return nil
}

// Addr 返回实际监听地址。
func (a *Adapter) Addr() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.listener == nil {
		return ""
	}
	return a.listener.Addr().String()
}

// Shutdown 关闭应用端接入服务和所有连接，并停止 client 重连循环。
func (a *Adapter) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	server := a.server
	a.server = nil
	a.started = false
	a.mu.Unlock()
	// 置停止标志，使 client 重连循环退出。
	a.stopMu.Lock()
	a.stopped = true
	a.stopMu.Unlock()
	if server != nil {
		if err := server.Shutdown(ctx); err != nil {
			return err
		}
	}
	a.connMu.Lock()
	connections := make([]*connection, 0, len(a.conns))
	for id, conn := range a.conns {
		connections = append(connections, conn)
		delete(a.conns, id)
	}
	a.connMu.Unlock()
	for _, conn := range connections {
		_ = conn.close(websocket.CloseNormalClosure, "shutdown")
	}
	for _, conn := range connections {
		conn.ws.WaitRun()
	}
	// 等待 client 重连 goroutine 退出。
	a.reconnWG.Wait()
	return nil
}

// Candidates 返回当前已连接应用端端点 ID。
func (a *Adapter) Candidates() []string {
	a.connMu.RLock()
	defer a.connMu.RUnlock()
	result := make([]string, 0, len(a.conns))
	for id := range a.conns {
		result = append(result, id)
	}
	return result
}

// Deliver 将事件投递给匹配的应用端连接。
func (a *Adapter) Deliver(ctx context.Context, event contract.Event, targets []string) error {
	a.connMu.RLock()
	connections := make([]*connection, 0, len(a.conns))
	for _, conn := range a.conns {
		connections = append(connections, conn)
	}
	a.connMu.RUnlock()
	selected := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		selected[target] = struct{}{}
	}
	delivered := 0
	var failures []error
	for _, conn := range connections {
		if len(selected) > 0 {
			if _, ok := selected[conn.id]; !ok {
				continue
			}
		}
		if err := conn.Deliver(ctx, event); err != nil {
			failures = append(failures, err)
			continue
		}
		delivered++
	}
	// 无任何连接被投递时返回哨兵错误，使 EventBus 记死信而非静默丢弃。
	// 这覆盖路由到已断开应用端的场景。
	if delivered == 0 && len(failures) == 0 {
		return errNoApplicationConnection
	}
	return errors.Join(failures...)
}

// Health 返回应用端连接健康状态。
func (a *Adapter) Health() contract.HealthStatus {
	a.mu.RLock()
	started := a.started || a.config.Mode == "client" && len(a.Candidates()) > 0
	a.mu.RUnlock()
	a.connMu.RLock()
	count := len(a.conns)
	a.connMu.RUnlock()
	state := contract.HealthHealthy
	message := "应用端 OneBot 适配器运行中"
	if !started {
		state = contract.HealthUnhealthy
		message = "应用端 OneBot 适配器未启动"
	}
	return contract.HealthStatus{State: state, Message: message, CheckedAt: time.Now().UTC(), Details: map[string]string{"connections": fmt.Sprint(count)}}
}

func (a *Adapter) upgradeHandler(version string) http.Handler {
	upgrader := websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096, CheckOrigin: func(*http.Request) bool { return true }}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !validBearer(request.Header.Get("Authorization"), a.config.AccessToken) {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		socket, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		conn := newConnection(a, version, socket)
		a.connMu.Lock()
		a.conns[conn.id] = conn
		a.connMu.Unlock()
		conn.ws.TrackRun()
		go conn.run()
	})
}

func validBearer(value, expected string) bool {
	const prefix = "Bearer "
	return strings.HasPrefix(value, prefix) && strings.TrimPrefix(value, prefix) == expected && expected != ""
}

// pendingEntry 记录一个待回复事件及其所属群，用于缺关联回复的同群 fallback 关联。
type pendingEntry struct {
	event   contract.Event
	groupID string
}

type connection struct {
	adapter   *Adapter
	version   string
	id        string
	ws        *wsconn.Conn
	dead      chan struct{}
	pendingMu sync.Mutex
	pending   map[string]pendingEntry // eventID -> entry（显式关联 O(1) 查）
	recent    []string                // eventID 按推送顺序，仅用于 fallback 逆序取最近未决
}

func newConnection(adapter *Adapter, version string, socket *websocket.Conn) *connection {
	ws := wsconn.New(socket, wsconn.Options{
		HeartbeatInterval: adapter.config.HeartbeatInterval,
		WriteTimeout:      adapter.config.WriteTimeout,
	})
	return &connection{adapter: adapter, version: version, id: fmt.Sprintf("%s-%d", version, time.Now().UnixNano()), ws: ws, dead: make(chan struct{}), pending: make(map[string]pendingEntry)}
}

func (c *connection) run() {
	c.registerEndpoint()
	defer func() {
		c.adapter.connMu.Lock()
		delete(c.adapter.conns, c.id)
		c.adapter.connMu.Unlock()
		_ = c.adapter.endpoints.Unregister(c.id)
		_ = c.ws.Close(websocket.CloseNormalClosure, "closed")
		c.ws.RunDone()
		close(c.dead)
	}()
	stopKeepalive := c.ws.StartKeepalive()
	defer stopKeepalive()
	for {
		_, payload, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		if err := c.handle(payload); err != nil {
			_ = c.writeJSON(map[string]any{"status": "failed", "retcode": 1, "message": err.Error()})
		}
	}
}

// registerEndpoint 把连接注册为 application 端点。
// 若应用端声明了稳定身份（__botfabric_endpoint_id 或 self_id），复用该 ID：
// 已存在则用 Update 刷新元数据，使重连后路由与响应状态可关联。
func (c *connection) registerEndpoint() {
	info := contract.EndpointInfo{ID: c.id, Owner: PluginID, Kind: contract.EndpointApplication, Protocol: "onebot", Version: c.version, ConnectionID: c.id, Capabilities: []string{"receive_event", "submit_reply"}}
	if err := c.adapter.endpoints.Register(info); err == nil {
		return
	}
	// ID 已存在（重连复用稳定身份）：刷新元数据。
	_ = c.adapter.endpoints.Update(info)
}

// adoptIdentity 采用应用端声明的稳定身份：注销旧端点、迁移连接表键、注册新端点。
// 仅在身份与当前 ID 不同时执行；幂等。
func (c *connection) adoptIdentity(stableID string) {
	if stableID == "" || stableID == c.id {
		return
	}
	oldID := c.id
	c.adapter.connMu.Lock()
	if _, exists := c.adapter.conns[stableID]; exists && stableID != oldID {
		// 稳定 ID 已被占用且非自身：放弃采用，避免冲突。
		c.adapter.connMu.Unlock()
		return
	}
	c.adapter.conns[stableID] = c
	delete(c.adapter.conns, oldID)
	c.adapter.connMu.Unlock()
	_ = c.adapter.endpoints.Unregister(oldID)
	c.id = stableID
	c.registerEndpoint()
}

func (c *connection) handle(payload []byte) error {
	var envelope struct {
		Action string          `json:"action"`
		Params map[string]any  `json:"params"`
		Echo   json.RawMessage `json:"echo"`
		// EndpointID 是应用端可选声明的稳定身份；重连后复用，便于路由与状态关联。
		EndpointID string `json:"__botfabric_endpoint_id"`
		SelfID     string `json:"self_id"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("解析应用端 OneBot 消息: %w", err)
	}
	// 可选身份声明：应用端可在任意帧携带 __botfabric_endpoint_id/self_id 声明稳定身份。
	if stableID := envelope.EndpointID; stableID != "" {
		c.adoptIdentity(stableID)
	} else if stableID := envelope.SelfID; stableID != "" {
		c.adoptIdentity(stableID)
	}
	if envelope.Action == "" {
		// 纯身份声明帧（无 action）合法，确认身份已采用。
		if envelope.EndpointID != "" || envelope.SelfID != "" {
			return c.writeJSON(map[string]any{"status": "ok", "retcode": 0})
		}
		return errors.New("应用端 OneBot action 不能为空")
	}
	if envelope.Action != "send_group_msg" && envelope.Action != "send_message" {
		return fmt.Errorf("不支持应用端 action %s", envelope.Action)
	}
	groupID := stringParam(envelope.Params, "group_id")
	text := stringParam(envelope.Params, "message")
	if envelope.Action == "send_message" {
		text = extractV12Text(envelope.Params["message"])
	}
	if groupID == "" || text == "" {
		return errors.New("应用端回复缺少 group_id 或 message")
	}
	eventID := stringParam(envelope.Params, "__botfabric_event_id")
	if eventID == "" {
		eventID = stringValue(envelope.Echo)
	}
	fromFallback := false
	if eventID == "" {
		// 缺关联信息：按同群最近未决事件 fallback 关联，使无法携带关联信息的简易应用端也能参与响应仲裁。
		eventID = c.recentByGroup(groupID)
		fromFallback = eventID != ""
	}
	c.pendingMu.Lock()
	entry, ok := c.pending[eventID]
	if ok {
		delete(c.pending, eventID)
		c.removeRecent(eventID)
	}
	c.pendingMu.Unlock()
	if eventID == "" || !ok || entry.event.ID == "" {
		return errors.New("应用端回复缺少有效的 event_id/echo 关联，且无可用的最近未决事件")
	}
	result, err := c.adapter.submitter.Submit(context.Background(), contract.CandidateReply{EventID: entry.event.ID, ApplicationID: c.id, GroupID: groupID, Text: text})
	if err != nil {
		// fallback 命中但事件已被协调器过期释放：清除残留 entry，避免后续同群 fallback 反复命中陈旧事件。
		if fromFallback {
			var serviceErr *contract.ServiceError
			if errors.As(err, &serviceErr) && serviceErr.Code == contract.ErrorUnavailable {
				c.pendingMu.Lock()
				if _, stillPending := c.pending[eventID]; stillPending {
					delete(c.pending, eventID)
					c.removeRecent(eventID)
				}
				c.pendingMu.Unlock()
			}
		}
		return err
	}
	return c.writeJSON(map[string]any{"status": "ok", "retcode": 0, "data": result, "echo": stringValue(envelope.Echo)})
}

// recentByGroup 逆序遍历最近推送的事件，返回首个仍待回复且属于指定群的事件 ID。
// 不命中返回空串。遍历中跳过已不在 pending 的 eventID（被显式关联消费或清理）。
func (c *connection) recentByGroup(groupID string) string {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for i := len(c.recent) - 1; i >= 0; i-- {
		id := c.recent[i]
		entry, ok := c.pending[id]
		if !ok {
			continue
		}
		if entry.groupID == groupID {
			return id
		}
	}
	return ""
}

// removeRecent 从 recent 逆序删除首个匹配的 eventID。调用方须持有 pendingMu。
func (c *connection) removeRecent(eventID string) {
	for i := len(c.recent) - 1; i >= 0; i-- {
		if c.recent[i] == eventID {
			c.recent = append(c.recent[:i], c.recent[i+1:]...)
			return
		}
	}
}

// Deliver 将标准事件推送为 OneBot v11/v12 事件，并登记回复关联。
func (c *connection) Deliver(_ context.Context, event contract.Event) error {
	var message contract.GroupMessage
	if err := json.Unmarshal(event.Payload, &message); err != nil {
		return err
	}
	c.pendingMu.Lock()
	c.pending[event.ID] = pendingEntry{event: event, groupID: message.GroupID}
	c.recent = append(c.recent, event.ID)
	c.pendingMu.Unlock()
	var envelope any
	if c.version == VersionV11 {
		envelope = map[string]any{"time": event.Timestamp.Unix(), "self_id": event.BotID, "post_type": "message", "message_type": "group", "message_id": message.MessageID, "group_id": message.GroupID, "user_id": message.UserID, "message": message.Text, "raw_message": message.Text, "__botfabric_event_id": event.ID}
	} else {
		envelope = map[string]any{"time": event.Timestamp.Unix(), "type": "message", "detail_type": "group", "id": message.MessageID, "group_id": message.GroupID, "user_id": message.UserID, "message": []map[string]any{{"type": "text", "data": map[string]string{"text": message.Text}}}, "alt_message": message.Text, "__botfabric_event_id": event.ID}
	}
	return c.writeJSON(envelope)
}

func (c *connection) writeJSON(value any) error {
	return c.ws.WriteJSON(value)
}

func (c *connection) close(code int, text string) error {
	return c.ws.Close(code, text)
}

func stringParam(params map[string]any, key string) string {
	value, ok := params[key]
	if !ok {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func stringValue(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	return strings.Trim(string(raw), `"`)
}

func extractV12Text(value any) string {
	segments, ok := value.([]any)
	if !ok {
		if text, ok := value.(string); ok {
			return text
		}
		return ""
	}
	var builder strings.Builder
	for _, item := range segments {
		segment, ok := item.(map[string]any)
		if !ok || segment["type"] != "text" {
			continue
		}
		data, _ := segment["data"].(map[string]any)
		if text, ok := data["text"].(string); ok {
			builder.WriteString(text)
		}
	}
	return builder.String()
}
