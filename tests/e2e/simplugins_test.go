package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/PhantomDaze/botfabric/internal/policy"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// simEndpoint 描述模拟协议端暴露的一个协议端点。
type simEndpoint struct {
	ID       string
	BotID    string
	Protocol string
	Version  string
}

// simProtocolConfig 配置一个模拟协议端插件。
type simProtocolConfig struct {
	ID        string
	Entry     string
	Endpoints []simEndpoint
}

// SimSent 是模拟协议端接到的发送动作记录。
type SimSent struct {
	GroupID        string
	Text           string
	BotID          string
	EndpointID     string
	Caller         string
	TraceID        string
	EventID        string
	IdempotencyKey string
}

// simProtocol 是不连接真实 QQ 的可配置协议端插件，可承载多个协议端点。
type simProtocol struct {
	manifest  contract.Manifest
	endpoints []simEndpoint

	mu      sync.RWMutex
	host    contract.Host
	started bool
	sent    []SimSent
}

// SimMessage 是模拟协议端收到的一条原始群消息。
type SimMessage struct {
	MessageID string
	EventID   string
	GroupID   string
	UserID    string
	Text      string
}

// addProtocol 声明一个模拟协议端插件并返回其句柄。
func (h *simHarness) addProtocol(cfg simProtocolConfig) *simProtocol {
	h.t.Helper()
	if cfg.ID == "" {
		h.t.Fatal("模拟协议端缺少 ID")
	}
	if cfg.Entry == "" {
		cfg.Entry = cfg.ID + ".entry"
	}
	endpoints := cfg.Endpoints
	if len(endpoints) == 0 {
		endpoints = []simEndpoint{{ID: cfg.ID + ".default", BotID: h.botID, Protocol: "onebot", Version: "v11"}}
	}
	plugin := &simProtocol{
		manifest: contract.Manifest{
			ID:       cfg.ID,
			Name:     cfg.ID,
			Version:  "0.1.0",
			Runtime:  contract.RuntimeInProcess,
			Entry:    cfg.Entry,
			Provides: contract.Provides{Services: []contract.ServiceDescriptor{{Name: actionService, Methods: []string{actionSend}}}},
			Limits:   contract.Limits{Timeout: 2 * time.Second},
		},
		endpoints: endpoints,
	}
	h.declare(plugin.manifest, plugin, simRules{
		publish: []string{groupTopic},
		provide: []string{actionService},
	})
	return plugin
}

func (p *simProtocol) Manifest() contract.Manifest { return p.manifest }

func (p *simProtocol) Start(_ context.Context, host contract.Host) error {
	if host == nil {
		return errors.New("宿主不能为空")
	}
	if err := host.Gateway().Register(
		contract.ServiceDescriptor{Name: actionService, Methods: []string{actionSend}},
		p.handleAction,
	); err != nil {
		return err
	}
	registrar, ok := host.(contract.EndpointRegistrar)
	if !ok {
		return errors.New("宿主缺少端点注册能力")
	}
	for _, ep := range p.endpoints {
		if err := registrar.RegisterEndpoint(contract.EndpointInfo{
			ID:           ep.ID,
			Owner:        p.manifest.ID,
			Kind:         contract.EndpointProtocol,
			Protocol:     ep.Protocol,
			Version:      ep.Version,
			BotID:        ep.BotID,
			ConnectionID: ep.ID,
			Capabilities: []string{actionSend},
		}); err != nil {
			return err
		}
	}
	p.mu.Lock()
	p.host = host
	p.started = true
	p.mu.Unlock()
	return nil
}

func (p *simProtocol) Stop(context.Context) error {
	p.mu.Lock()
	p.started = false
	p.host = nil
	p.mu.Unlock()
	return nil
}

func (p *simProtocol) Health(context.Context) contract.HealthStatus {
	p.mu.RLock()
	started := p.started
	p.mu.RUnlock()
	if !started {
		return contract.HealthStatus{State: contract.HealthUnhealthy, Message: "模拟协议端未启动", CheckedAt: time.Now().UTC()}
	}
	return contract.HealthStatus{State: contract.HealthHealthy, Message: "模拟协议端运行中", CheckedAt: time.Now().UTC()}
}

// Emit 从指定端点发布一条归一化群消息事件。endpointID 为空时使用首个端点。
func (p *simProtocol) Emit(ctx context.Context, endpointID string, raw SimMessage) error {
	p.mu.RLock()
	host := p.host
	started := p.started
	p.mu.RUnlock()
	if !started || host == nil {
		return errors.New("模拟协议端未启动")
	}
	if raw.GroupID == "" || raw.UserID == "" || raw.Text == "" {
		return errors.New("群号、用户号和文本不能为空")
	}
	ep, ok := p.endpoint(endpointID)
	if !ok {
		return fmt.Errorf("未知协议端点 %q", endpointID)
	}
	payload, err := json.Marshal(contract.GroupMessage{
		MessageID: raw.MessageID,
		GroupID:   raw.GroupID,
		UserID:    raw.UserID,
		Text:      raw.Text,
		Protocol:  ep.Protocol,
		Version:   ep.Version,
	})
	if err != nil {
		return err
	}
	return host.EventBus().Publish(ctx, contract.Event{
		ID:         raw.EventID,
		Topic:      groupTopic,
		Schema:     groupSchema,
		BotID:      ep.BotID,
		EndpointID: ep.ID,
		Payload:    payload,
	})
}

func (p *simProtocol) endpoint(id string) (simEndpoint, bool) {
	if id == "" {
		return p.endpoints[0], true
	}
	for _, ep := range p.endpoints {
		if ep.ID == id {
			return ep, true
		}
	}
	return simEndpoint{}, false
}

// Sent 返回已接收动作的快照。
func (p *simProtocol) Sent() []SimSent {
	p.mu.RLock()
	defer p.mu.RUnlock()
	sent := make([]SimSent, len(p.sent))
	copy(sent, p.sent)
	return sent
}

// SentCount 返回已接收动作数量。
func (p *simProtocol) SentCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.sent)
}

func (p *simProtocol) handleAction(_ context.Context, req contract.Request) (contract.Response, error) {
	if req.Method != actionSend {
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorMethodNotFound, Message: "不支持的动作方法"}
	}
	var action contract.SendMessageRequest
	if err := json.Unmarshal(req.Payload, &action); err != nil {
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "发送消息载荷无效"}
	}
	if action.GroupID == "" || action.Text == "" {
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "群号和文本不能为空"}
	}
	p.mu.Lock()
	p.sent = append(p.sent, SimSent{
		GroupID:        action.GroupID,
		Text:           action.Text,
		BotID:          action.BotID,
		EndpointID:     action.EndpointID,
		Caller:         req.Caller,
		TraceID:        req.TraceID,
		EventID:        action.EventID,
		IdempotencyKey: action.IdempotencyKey,
	})
	p.mu.Unlock()
	payload, err := json.Marshal(contract.SendMessageResult{MessageID: fmt.Sprintf("sim-%d", p.SentCount())})
	if err != nil {
		return contract.Response{}, err
	}
	return contract.Response{Payload: payload}, nil
}

// simAppConfig 配置一个模拟应用端插件。
type simAppConfig struct {
	ID     string
	Entry  string
	Topics []string
	// Delay 在提交候选回复前引入延时，用于构造到达顺序和超时场景。
	Delay time.Duration
	// SubscriptionTimeout 覆盖事件处理器的订阅超时；未设置时使用默认值。
	SubscriptionTimeout time.Duration
	// Reply 依据事件生成候选回复；返回 nil 表示该应用端不回复。
	Reply func(event contract.Event, message contract.GroupMessage) *contract.CandidateReply
	// RepeatSubmitCount > 1 时对同一事件重复提交相同候选（默认 1 只提交一次）。
	RepeatSubmitCount int
	// RequiresActions 为 false 时不声明协议动作依赖，用于构造越权场景。
	SkipActionDependency bool
	// DenySubscription 为 true 时，清单仍声明订阅 Topic，但权限策略不放行该订阅，
	// 用于验证 deny-by-default 在清单守卫之外独立生效。
	DenySubscription bool
	// DenyActionPolicy 为 true 时，Manifest 仍声明动作服务依赖，但静态策略不授权调用。
	DenyActionPolicy bool
	// HandlerErr 非 nil 时，事件处理器直接返回该错误（不提交候选），用于验证死信路径。
	HandlerErr error
}

// simApp 是行为可配置的应用端插件，可模拟竞争、延迟与不回复。
type simApp struct {
	manifest contract.Manifest
	config   simAppConfig

	mu        sync.RWMutex
	host      contract.Host
	subs      []contract.Subscription
	started   bool
	received  []contract.Event
	submitted []contract.CandidateReply
	errs      []error
}

// addApp 声明一个模拟应用端插件并返回其句柄。
func (h *simHarness) addApp(cfg simAppConfig) *simApp {
	h.t.Helper()
	if cfg.ID == "" {
		h.t.Fatal("模拟应用端缺少 ID")
	}
	if cfg.Entry == "" {
		cfg.Entry = cfg.ID + ".entry"
	}
	topics := cfg.Topics
	if len(topics) == 0 {
		topics = []string{groupTopic}
	}
	requires := []string{responseService}
	if !cfg.SkipActionDependency {
		requires = append(requires, actionService)
	}
	plugin := &simApp{
		manifest: contract.Manifest{
			ID:       cfg.ID,
			Name:     cfg.ID,
			Version:  "0.1.0",
			Runtime:  contract.RuntimeInProcess,
			Entry:    cfg.Entry,
			Provides: contract.Provides{Subscriptions: topics},
			Requires: contract.Requires{
				Services:    requires,
				Permissions: []string{"event.read.message", "action.send_message"},
			},
			Limits: contract.Limits{Timeout: 2 * time.Second},
		},
		config: cfg,
	}
	rules := simRules{
		subscribe: topics,
		invoke: []policy.InvokeRule{
			{Service: responseService, Methods: []string{responseSubmit, responseCancel}},
		},
	}
	if !cfg.SkipActionDependency && !cfg.DenyActionPolicy {
		rules.invoke = append(rules.invoke, policy.InvokeRule{Service: actionService, Methods: []string{actionSend}})
	}
	if cfg.DenySubscription {
		rules.subscribe = nil // 清单声明订阅，但策略不放行
	}
	h.declare(plugin.manifest, plugin, rules)
	return plugin
}

func (p *simApp) Manifest() contract.Manifest { return p.manifest }

func (p *simApp) Start(_ context.Context, host contract.Host) error {
	if host == nil {
		return errors.New("宿主不能为空")
	}
	p.mu.Lock()
	p.host = host
	p.mu.Unlock()

	topics := p.config.Topics
	if len(topics) == 0 {
		topics = []string{groupTopic}
	}
	subs := make([]contract.Subscription, 0, len(topics))
	for _, topic := range topics {
		timeout := p.config.SubscriptionTimeout
		if timeout <= 0 {
			timeout = 2 * time.Second
		}
		sub, err := host.EventBus().Subscribe(topic, p.handle, contract.WithTimeout(timeout))
		if err != nil {
			for _, existing := range subs {
				_ = existing.Unsubscribe()
			}
			return err
		}
		subs = append(subs, sub)
	}
	p.mu.Lock()
	p.subs = subs
	p.started = true
	p.mu.Unlock()
	return nil
}

func (p *simApp) Stop(context.Context) error {
	p.mu.Lock()
	subs := p.subs
	p.subs = nil
	p.host = nil
	p.started = false
	p.mu.Unlock()
	for _, sub := range subs {
		if err := sub.Unsubscribe(); err != nil {
			return err
		}
	}
	return nil
}

func (p *simApp) Health(context.Context) contract.HealthStatus {
	p.mu.RLock()
	started := p.started
	p.mu.RUnlock()
	if !started {
		return contract.HealthStatus{State: contract.HealthUnhealthy, Message: "模拟应用端未启动", CheckedAt: time.Now().UTC()}
	}
	return contract.HealthStatus{State: contract.HealthHealthy, Message: "模拟应用端运行中", CheckedAt: time.Now().UTC()}
}

func (p *simApp) handle(ctx context.Context, event contract.Event) error {
	var message contract.GroupMessage
	if err := json.Unmarshal(event.Payload, &message); err != nil {
		return &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "群消息载荷无效"}
	}
	p.mu.Lock()
	p.received = append(p.received, event)
	p.mu.Unlock()

	if p.config.HandlerErr != nil {
		p.recordError(p.config.HandlerErr)
		return p.config.HandlerErr
	}
	if p.config.Reply == nil {
		return nil
	}
	if p.config.Delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.config.Delay):
		}
	}
	reply := p.config.Reply(event, message)
	if reply == nil {
		return nil
	}
	if reply.EventID == "" {
		reply.EventID = event.ID
	}
	if reply.ApplicationID == "" {
		reply.ApplicationID = p.manifest.ID
	}
	if reply.GroupID == "" {
		reply.GroupID = message.GroupID
	}
	if reply.TargetBotID == "" {
		reply.TargetBotID = event.RequiredReplyBot
	}

	p.mu.RLock()
	host := p.host
	repeats := p.config.RepeatSubmitCount
	p.mu.RUnlock()
	if host == nil {
		return errors.New("插件宿主不可用")
	}
	if repeats <= 0 {
		repeats = 1
	}
	for attempt := 0; attempt < repeats; attempt++ {
		payload, err := json.Marshal(*reply)
		if err != nil {
			return err
		}
		response, err := host.Gateway().Invoke(ctx, contract.Request{
			Service:       responseService,
			Method:        responseSubmit,
			TraceID:       event.TraceID,
			CorrelationID: event.CorrelationID,
			Payload:       payload,
		})
		if err != nil {
			p.recordError(err)
			return err
		}
		if response.Error != nil {
			p.recordError(response.Error)
			return response.Error
		}
	}
	p.mu.Lock()
	p.submitted = append(p.submitted, *reply)
	p.mu.Unlock()
	return nil
}

func (p *simApp) recordError(err error) {
	p.mu.Lock()
	p.errs = append(p.errs, err)
	p.mu.Unlock()
}

// ReceivedCount 返回已接收事件数量。
func (p *simApp) ReceivedCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.received)
}

// ReceivedGroupIDs 返回已接收事件中的群号集合（按事件负载解析）。
func (p *simApp) ReceivedGroupIDs() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ids := make([]string, 0, len(p.received))
	for _, event := range p.received {
		var message contract.GroupMessage
		if json.Unmarshal(event.Payload, &message) == nil {
			ids = append(ids, message.GroupID)
		}
	}
	return ids
}

// Received 返回已接收事件快照。
func (p *simApp) Received() []contract.Event {
	p.mu.RLock()
	defer p.mu.RUnlock()
	events := make([]contract.Event, len(p.received))
	copy(events, p.received)
	return events
}

// SubmittedCount 返回已成功提交的候选回复数量。
func (p *simApp) SubmittedCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.submitted)
}

// Errors 返回处理过程中记录的错误。
func (p *simApp) Errors() []error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	errs := make([]error, len(p.errs))
	copy(errs, p.errs)
	return errs
}
