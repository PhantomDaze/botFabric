package mockonebot

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

const (
	PluginID           = "protocol.mockonebot"
	ServiceName        = "protocol.qq.action"
	SendMessageMethod  = "SendMessage"
	GroupMessageTopic  = "bot.message.group"
	GroupMessageSchema = "bot.message@v1"
)

// RawGroupMessage 模拟 OneBot 协议端收到的群消息。
type RawGroupMessage struct {
	MessageID string `json:"message_id"`
	GroupID   string `json:"group_id"`
	UserID    string `json:"user_id"`
	Text      string `json:"text"`
}

// GroupMessage 保留 mock 插件的兼容名称，实际使用公共契约模型。
type GroupMessage = contract.GroupMessage

// SendMessageRequest 保留 mock 插件的兼容名称，实际使用公共契约模型。
type SendMessageRequest = contract.SendMessageRequest

// SendMessageResult 保留 mock 插件的兼容名称，实际使用公共契约模型。
type SendMessageResult = contract.SendMessageResult

// SentMessage 是模拟协议端实际收到的动作记录。
type SentMessage struct {
	GroupID   string
	Text      string
	MessageID string
	Caller    string
	TraceID   string
}

// Plugin 是不连接真实 QQ 的协议适配示例。
type Plugin struct {
	mu         sync.RWMutex
	host       contract.Host
	registrar  contract.EndpointRegistrar
	endpointID string
	started    bool
	sent       []SentMessage
}

// New 创建模拟协议插件。
func New() *Plugin {
	return &Plugin{}
}

// Manifest 返回内置插件清单。
func (p *Plugin) Manifest() contract.Manifest {
	return contract.Manifest{
		ID:      PluginID,
		Name:    "Mock OneBot Protocol",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   "builtin.mockonebot",
		Provides: contract.Provides{
			Services: []contract.ServiceDescriptor{{Name: ServiceName, Methods: []string{SendMessageMethod}}},
		},
		Limits: contract.Limits{Timeout: 2 * time.Second},
	}
}

// Start 注册 QQ 动作服务。
func (p *Plugin) Start(_ context.Context, host contract.Host) error {
	if host == nil {
		return errors.New("宿主不能为空")
	}
	p.mu.RLock()
	started := p.started
	p.mu.RUnlock()
	if started {
		return errors.New("模拟协议插件已经启动")
	}
	if err := host.Gateway().Register(
		contract.ServiceDescriptor{Name: ServiceName, Methods: []string{SendMessageMethod}},
		p.handleAction,
	); err != nil {
		return err
	}
	registrar, _ := host.(contract.EndpointRegistrar)
	endpointID := PluginID + ".default"
	if registrar != nil {
		if err := registrar.RegisterEndpoint(contract.EndpointInfo{
			ID:           endpointID,
			Owner:        PluginID,
			Kind:         contract.EndpointProtocol,
			Protocol:     "onebot",
			Version:      "mock-v1",
			BotID:        host.BotID(),
			ConnectionID: endpointID,
			Capabilities: []string{SendMessageMethod},
		}); err != nil {
			return err
		}
	}
	p.mu.Lock()
	p.host = host
	p.registrar = registrar
	p.endpointID = endpointID
	p.started = true
	p.mu.Unlock()
	return nil
}

// Stop 停止接收模拟事件；运行时负责回收服务注册。
func (p *Plugin) Stop(context.Context) error {
	p.mu.Lock()
	registrar := p.registrar
	endpointID := p.endpointID
	p.started = false
	p.host = nil
	p.registrar = nil
	p.endpointID = ""
	p.mu.Unlock()
	if registrar != nil {
		return registrar.UnregisterEndpoint(endpointID)
	}
	return nil
}

// Health 返回当前插件健康状态。
func (p *Plugin) Health(context.Context) contract.HealthStatus {
	p.mu.RLock()
	started := p.started
	p.mu.RUnlock()
	state := contract.HealthHealthy
	message := "模拟协议插件运行中"
	if !started {
		state = contract.HealthUnhealthy
		message = "模拟协议插件未启动"
	}
	return contract.HealthStatus{State: state, Message: message, CheckedAt: time.Now().UTC()}
}

// EmitGroupMessage 将模拟 OneBot 消息归一化并发布到内部事件总线。
func (p *Plugin) EmitGroupMessage(ctx context.Context, raw RawGroupMessage) error {
	p.mu.RLock()
	host := p.host
	started := p.started
	p.mu.RUnlock()
	if !started || host == nil {
		return errors.New("模拟协议插件未启动")
	}
	if raw.GroupID == "" || raw.UserID == "" || raw.Text == "" {
		return errors.New("群号、用户号和文本不能为空")
	}
	payload, err := json.Marshal(GroupMessage{
		MessageID: raw.MessageID,
		GroupID:   raw.GroupID,
		UserID:    raw.UserID,
		Text:      raw.Text,
		Protocol:  "onebot",
		Version:   "mock-v1",
	})
	if err != nil {
		return err
	}
	return host.EventBus().Publish(ctx, contract.Event{
		Topic:      GroupMessageTopic,
		Schema:     GroupMessageSchema,
		BotID:      host.BotID(),
		EndpointID: p.endpointID,
		Payload:    payload,
	})
}

// SentMessages 返回模拟协议端收到的动作快照。
func (p *Plugin) SentMessages() []SentMessage {
	p.mu.RLock()
	defer p.mu.RUnlock()
	messages := make([]SentMessage, len(p.sent))
	copy(messages, p.sent)
	return messages
}

func (p *Plugin) handleAction(_ context.Context, req contract.Request) (contract.Response, error) {
	if req.Method != SendMessageMethod {
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorMethodNotFound, Message: "不支持的动作方法"}
	}
	var action SendMessageRequest
	if err := json.Unmarshal(req.Payload, &action); err != nil {
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "发送消息载荷无效"}
	}
	if action.GroupID == "" || action.Text == "" {
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "群号和文本不能为空"}
	}

	messageID := "mock-" + time.Now().UTC().Format("20060102150405.000000000")
	p.mu.Lock()
	p.sent = append(p.sent, SentMessage{
		GroupID:   action.GroupID,
		Text:      action.Text,
		MessageID: messageID,
		Caller:    req.Caller,
		TraceID:   req.TraceID,
	})
	p.mu.Unlock()
	payload, err := json.Marshal(SendMessageResult{MessageID: messageID})
	if err != nil {
		return contract.Response{}, err
	}
	return contract.Response{Payload: payload}, nil
}
