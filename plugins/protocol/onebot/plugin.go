package onebot

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/PhantomDaze/botfabric/internal/config"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/gorilla/websocket"
)

const (
	PluginID    = "protocol.onebot"
	ServiceName = "protocol.qq.action"
	SendMessage = "SendMessage"
	GroupTopic  = "bot.message.group"
	GroupSchema = "bot.message@v1"
	VersionV11  = "v11"
	VersionV12  = "v12"
)

// Plugin 是同时支持 OneBot v11/v12 的反向 WebSocket 适配器。
type Plugin struct {
	mu        sync.RWMutex
	config    config.OneBotConfig
	host      contract.Host
	server    *http.Server
	started   bool
	stopOnce  sync.Once
	registrar contract.EndpointRegistrar
	connMu    sync.RWMutex
	conns     map[string]*connection
}

// New 创建 OneBot 协议插件。
func New(oneBotConfig config.OneBotConfig) *Plugin {
	return &Plugin{config: oneBotConfig, conns: make(map[string]*connection)}
}

// Manifest 返回协议插件清单。
func (p *Plugin) Manifest() contract.Manifest {
	return contract.Manifest{
		ID:      PluginID,
		Name:    "OneBot v11/v12 Protocol",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   "builtin.onebot",
		Provides: contract.Provides{
			Services: []contract.ServiceDescriptor{{Name: ServiceName, Methods: []string{SendMessage}}},
		},
		Limits: contract.Limits{Timeout: 5 * time.Second},
	}
}

// Start 注册动作服务并监听双版本 WebSocket 路径。
func (p *Plugin) Start(_ context.Context, host contract.Host) error {
	if host == nil {
		return errors.New("宿主不能为空")
	}
	if err := p.config.Validate(); err != nil {
		return err
	}
	if err := host.Gateway().Register(
		contract.ServiceDescriptor{Name: ServiceName, Methods: []string{SendMessage}},
		p.handleAction,
	); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle(p.config.PathV11, p.upgradeHandler(VersionV11))
	mux.Handle(p.config.PathV12, p.upgradeHandler(VersionV12))
	server := &http.Server{Addr: p.config.ListenAddress, Handler: mux, ReadHeaderTimeout: p.config.ConnectTimeout.Duration}
	listener, err := net.Listen("tcp", p.config.ListenAddress)
	if err != nil {
		return fmt.Errorf("监听 OneBot 地址: %w", err)
	}
	p.mu.Lock()
	p.host = host
	p.server = server
	p.registrar, _ = host.(contract.EndpointRegistrar)
	p.started = true
	p.mu.Unlock()

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			host.Logger().Error("OneBot WebSocket 服务停止", "error", err)
		}
	}()
	return nil
}

// Stop 关闭监听器与所有连接。
func (p *Plugin) Stop(ctx context.Context) error {
	var stopErr error
	p.stopOnce.Do(func() {
		p.mu.Lock()
		server := p.server
		p.server = nil
		p.started = false
		p.host = nil
		p.mu.Unlock()
		if server != nil {
			stopErr = server.Shutdown(ctx)
		}
		p.connMu.Lock()
		connections := make([]*connection, 0, len(p.conns))
		for id, conn := range p.conns {
			connections = append(connections, conn)
			delete(p.conns, id)
		}
		p.connMu.Unlock()
		for _, conn := range connections {
			_ = conn.close(websocket.CloseNormalClosure, "shutdown")
		}
	})
	return stopErr
}

// Health 返回连接与监听健康状态。
func (p *Plugin) Health(context.Context) contract.HealthStatus {
	p.mu.RLock()
	started := p.started
	p.mu.RUnlock()
	p.connMu.RLock()
	connections := len(p.conns)
	p.connMu.RUnlock()
	state := contract.HealthHealthy
	message := "OneBot 适配器运行中"
	if !started {
		state = contract.HealthUnhealthy
		message = "OneBot 适配器未启动"
	}
	return contract.HealthStatus{State: state, Message: message, CheckedAt: time.Now().UTC(), Details: map[string]string{"connections": fmt.Sprint(connections)}}
}

func (p *Plugin) upgradeHandler(version string) http.Handler {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     func(*http.Request) bool { return true },
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !validBearer(request.Header.Get("Authorization"), p.config.AccessToken) {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		socket, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		conn := newConnection(p, version, socket)
		p.mu.RLock()
		conn.registrar = p.registrar
		p.mu.RUnlock()
		p.connMu.Lock()
		p.conns[conn.id] = conn
		p.connMu.Unlock()
		go conn.run()
	})
}

func validBearer(value, expected string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(value, prefix) || len(expected) == 0 {
		return false
	}
	provided := []byte(strings.TrimPrefix(value, prefix))
	wanted := []byte(expected)
	if len(provided) != len(wanted) {
		return false
	}
	return subtle.ConstantTimeCompare(provided, wanted) == 1
}

type connection struct {
	plugin    *Plugin
	version   string
	id        string
	mu        sync.Mutex
	botID     string
	registrar contract.EndpointRegistrar
	createdAt time.Time
	socket    *websocket.Conn
	writeMu   sync.Mutex
	closeOnce sync.Once
	echoMu    sync.Mutex
	echoes    map[string]chan actionResponse
}

func newConnection(plugin *Plugin, version string, socket *websocket.Conn) *connection {
	return &connection{plugin: plugin, version: version, id: fmt.Sprintf("%s-%d", version, time.Now().UnixNano()), createdAt: time.Now(), socket: socket, echoes: make(map[string]chan actionResponse)}
}

func (c *connection) setBotID(botID string) {
	if botID == "" {
		return
	}
	c.mu.Lock()
	if c.botID == botID {
		c.mu.Unlock()
		return
	}
	c.botID = botID
	registrar := c.registrar
	c.mu.Unlock()
	if registrar != nil {
		_ = registrar.RegisterEndpoint(contract.EndpointInfo{
			ID:           c.id,
			Owner:        PluginID,
			Kind:         contract.EndpointProtocol,
			Protocol:     "onebot",
			Version:      c.version,
			BotID:        botID,
			ConnectionID: c.id,
			Capabilities: []string{SendMessage},
		})
	}
}

func (c *connection) run() {
	defer func() {
		c.plugin.connMu.Lock()
		delete(c.plugin.conns, c.id)
		c.plugin.connMu.Unlock()
		c.mu.Lock()
		registrar := c.registrar
		c.mu.Unlock()
		if registrar != nil {
			_ = registrar.UnregisterEndpoint(c.id)
		}
		_ = c.close(websocket.CloseNormalClosure, "closed")
	}()
	c.socket.SetReadLimit(int64(c.plugin.config.MaxMessageSize))
	_ = c.socket.SetReadDeadline(time.Now().Add(c.plugin.config.HeartbeatInterval.Duration * 2))
	c.socket.SetPongHandler(func(string) error {
		return c.socket.SetReadDeadline(time.Now().Add(c.plugin.config.HeartbeatInterval.Duration * 2))
	})
	go c.heartbeat()
	for {
		_, payload, err := c.socket.ReadMessage()
		if err != nil {
			return
		}
		if err := c.handle(payload); err != nil {
			_ = c.writeJSON(map[string]any{"status": "failed", "retcode": 1, "message": err.Error()})
		}
	}
}

func (c *connection) handle(payload []byte) error {
	if echo, response, ok := responseFromPayload(payload); ok {
		c.echoMu.Lock()
		responseCh := c.echoes[echo]
		c.echoMu.Unlock()
		if responseCh != nil {
			select {
			case responseCh <- response:
			default:
			}
			return nil
		}
	}
	if c.version == VersionV11 {
		return c.plugin.handleV11(c, payload)
	}
	return c.plugin.handleV12(c, payload)
}

func (c *connection) heartbeat() {
	ticker := time.NewTicker(c.plugin.config.HeartbeatInterval.Duration)
	defer ticker.Stop()
	for range ticker.C {
		c.writeMu.Lock()
		err := c.socket.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second))
		c.writeMu.Unlock()
		if err != nil {
			return
		}
	}
}

func (c *connection) registerEcho(echo string, response chan actionResponse) {
	c.echoMu.Lock()
	c.echoes[echo] = response
	c.echoMu.Unlock()
}

func (c *connection) unregisterEcho(echo string) {
	c.echoMu.Lock()
	delete(c.echoes, echo)
	c.echoMu.Unlock()
}

func (c *connection) writeJSON(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.socket.WriteMessage(websocket.TextMessage, payload)
}

func (c *connection) close(code int, text string) error {
	var err error
	c.closeOnce.Do(func() {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		_ = c.socket.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, text), time.Now().Add(time.Second))
		err = c.socket.Close()
	})
	return err
}
