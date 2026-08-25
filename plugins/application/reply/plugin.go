package reply

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

const (
	PluginID          = "example.reply"
	groupMessageTopic = "bot.message.group"
	responseService   = "botfabric.response"
	actionService     = "protocol.qq.action"
	submitMethod      = "Submit"
)

// Plugin 是收到群消息后通过动作服务回复的示例业务插件。
type Plugin struct {
	mu           sync.RWMutex
	host         contract.Host
	subscription contract.Subscription
	started      bool
}

// New 创建示例回复插件。
func New() *Plugin {
	return &Plugin{}
}

// Manifest 返回内置插件清单。
func (p *Plugin) Manifest() contract.Manifest {
	return contract.Manifest{
		ID:      PluginID,
		Name:    "Example Reply",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   "builtin.reply",
		Provides: contract.Provides{
			Subscriptions: []string{groupMessageTopic},
		},
		Requires: contract.Requires{
			Services:    []string{responseService, actionService},
			Permissions: []string{"event.read.message", "action.send_message"},
		},
		Limits: contract.Limits{Timeout: 2 * time.Second},
	}
}

// Start 订阅标准群消息事件。
func (p *Plugin) Start(_ context.Context, host contract.Host) error {
	if host == nil {
		return errors.New("宿主不能为空")
	}
	p.mu.Lock()
	if p.started || p.host != nil {
		p.mu.Unlock()
		return errors.New("示例回复插件已经启动")
	}
	p.host = host
	p.mu.Unlock()

	subscription, err := host.EventBus().Subscribe(
		groupMessageTopic,
		p.handleGroupMessage,
		contract.WithTimeout(2*time.Second),
	)
	if err != nil {
		p.mu.Lock()
		p.host = nil
		p.mu.Unlock()
		return err
	}
	p.mu.Lock()
	p.subscription = subscription
	p.started = true
	p.mu.Unlock()
	return nil
}

// Stop 主动取消事件订阅。
func (p *Plugin) Stop(context.Context) error {
	p.mu.Lock()
	subscription := p.subscription
	p.subscription = nil
	p.host = nil
	p.started = false
	p.mu.Unlock()
	if subscription != nil {
		return subscription.Unsubscribe()
	}
	return nil
}

// Health 返回当前插件健康状态。
func (p *Plugin) Health(context.Context) contract.HealthStatus {
	p.mu.RLock()
	started := p.started
	p.mu.RUnlock()
	state := contract.HealthHealthy
	message := "示例回复插件运行中"
	if !started {
		state = contract.HealthUnhealthy
		message = "示例回复插件未启动"
	}
	return contract.HealthStatus{State: state, Message: message, CheckedAt: time.Now().UTC()}
}

func (p *Plugin) handleGroupMessage(ctx context.Context, event contract.Event) error {
	var message contract.GroupMessage
	if err := json.Unmarshal(event.Payload, &message); err != nil {
		return &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "群消息事件载荷无效"}
	}
	if message.GroupID == "" || message.Text == "" {
		return &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "群消息缺少群号或文本"}
	}

	p.mu.RLock()
	host := p.host
	p.mu.RUnlock()
	if host == nil {
		return errors.New("插件宿主不可用")
	}
	payload, err := json.Marshal(contract.CandidateReply{
		EventID:       event.ID,
		ApplicationID: PluginID,
		GroupID:       message.GroupID,
		Text:          "收到：" + message.Text,
		TargetBotID:   event.RequiredReplyBot,
	})
	if err != nil {
		return err
	}
	response, err := host.Gateway().Invoke(ctx, contract.Request{
		Service:       responseService,
		Method:        submitMethod,
		TraceID:       event.TraceID,
		CorrelationID: event.CorrelationID,
		Payload:       payload,
	})
	if err != nil {
		return err
	}
	if response.Error != nil {
		return response.Error
	}
	return nil
}
