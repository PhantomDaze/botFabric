package eventbus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PhantomDaze/botfabric/internal/activity"
	"github.com/PhantomDaze/botfabric/internal/dedup"
	"github.com/PhantomDaze/botfabric/internal/observability"
	"github.com/PhantomDaze/botfabric/internal/policy"
	"github.com/PhantomDaze/botfabric/internal/routing"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// DeadLetter 记录一次未成功完成的事件投递。
type DeadLetter struct {
	Event        contract.Event
	SubscriberID string
	Error        string
	FailedAt     time.Time
}

// DeadLetterSink 接收事件投递失败记录。
type DeadLetterSink interface {
	Record(deadLetter DeadLetter)
}

// MemoryDeadLetterSink 是供 MVP 和测试使用的内存死信存储。
type MemoryDeadLetterSink struct {
	mu      sync.RWMutex
	entries []DeadLetter
}

// Record 保存一条死信记录。
func (s *MemoryDeadLetterSink) Record(deadLetter DeadLetter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, deadLetter)
}

// Entries 返回死信记录的副本。
func (s *MemoryDeadLetterSink) Entries() []DeadLetter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]DeadLetter, len(s.entries))
	copy(entries, s.entries)
	return entries
}

type subscription struct {
	id       uint64
	owner    string
	topic    string
	priority int
	timeout  time.Duration
	handler  contract.EventHandler
}

// Bus 是精确 Topic、同步等待投递完成的事件总线。
type Bus struct {
	mu             sync.RWMutex
	subscriptions  map[string][]subscription
	nextID         atomic.Uint64
	defaultTimeout time.Duration
	policy         policy.Checker
	logger         *slog.Logger
	deadLetters    DeadLetterSink
	activity       *activity.Tracker
	deduplicator   *dedup.Deduplicator
	router         *routing.Router
	responseHook   func(contract.Event, contract.ResponsePolicy)
	sinks          []contract.EventSink
}

// New 创建事件总线。
func New(defaultTimeout time.Duration, checker policy.Checker, logger *slog.Logger, deadLetters DeadLetterSink) (*Bus, error) {
	if defaultTimeout <= 0 {
		return nil, errors.New("默认事件处理器超时必须大于零")
	}
	if checker == nil {
		return nil, errors.New("权限检查器不能为空")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if deadLetters == nil {
		deadLetters = &MemoryDeadLetterSink{}
	}
	return &Bus{
		subscriptions:  make(map[string][]subscription),
		defaultTimeout: defaultTimeout,
		policy:         checker,
		logger:         logger,
		deadLetters:    deadLetters,
		activity:       activity.NewTracker(),
	}, nil
}

// SetDeduplicator 配置可选的跨协议端事件去重器。
// nil 表示关闭去重，保持默认全广播行为。
func (b *Bus) SetDeduplicator(value *dedup.Deduplicator) {
	b.mu.Lock()
	b.deduplicator = value
	b.mu.Unlock()
}

// Deduplicator 返回当前去重器；未配置时返回 nil。
func (b *Bus) Deduplicator() *dedup.Deduplicator {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.deduplicator
}

// RegisterSink 注册一个外部应用端事件投递面。
func (b *Bus) RegisterSink(sink contract.EventSink) error {
	if sink == nil {
		return errors.New("事件投递面不能为空")
	}
	b.mu.Lock()
	b.sinks = append(b.sinks, sink)
	b.mu.Unlock()
	return nil
}

// UnregisterSink 移除一个外部应用端事件投递面。
func (b *Bus) UnregisterSink(sink contract.EventSink) {
	b.mu.Lock()
	defer b.mu.Unlock()
	filtered := b.sinks[:0]
	for _, candidate := range b.sinks {
		if candidate != sink {
			filtered = append(filtered, candidate)
		}
	}
	b.sinks = filtered
}

// SetResponseHook 注册事件投递前的响应策略钩子。
// 钩子由 Runtime 注入响应协调器；nil 表示不启用响应仲裁。
func (b *Bus) SetResponseHook(hook func(contract.Event, contract.ResponsePolicy)) {
	b.mu.Lock()
	b.responseHook = hook
	b.mu.Unlock()
}

// SetRouter 配置可选路由器；nil 表示默认向所有订阅者广播。
func (b *Bus) SetRouter(value *routing.Router) {
	b.mu.Lock()
	b.router = value
	b.mu.Unlock()
}

// Router 返回当前路由器；未配置时返回 nil。
func (b *Bus) Router() *routing.Router {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.router
}

// ForPlugin 返回绑定可信插件身份的事件总线视图。
func (b *Bus) ForPlugin(pluginID string) contract.EventBus {
	b.activity.Activate(pluginID)
	return b.ViewForPlugin(pluginID)
}

// ViewForPlugin 返回不改变 Activity 状态的插件事件总线视图。
func (b *Bus) ViewForPlugin(pluginID string) contract.EventBus {
	return &scopedBus{bus: b, pluginID: pluginID}
}

// DeactivateOwner 停止指定插件接收新事件。
func (b *Bus) DeactivateOwner(pluginID string) {
	b.activity.Deactivate(pluginID)
}

// WaitOwner 等待指定插件的在途事件处理结束。
func (b *Bus) WaitOwner(ctx context.Context, pluginID string) error {
	return b.activity.Wait(ctx, pluginID)
}

// DeactivateAndWaitOwner 停止接收新事件并等待在途处理结束。
func (b *Bus) DeactivateAndWaitOwner(ctx context.Context, pluginID string) error {
	b.DeactivateOwner(pluginID)
	return b.WaitOwner(ctx, pluginID)
}

// ActivateOwner 允许指定插件接收事件处理。
func (b *Bus) ActivateOwner(pluginID string) {
	b.activity.Activate(pluginID)
}

// UnsubscribeOwner 移除指定插件的所有订阅。
func (b *Bus) UnsubscribeOwner(pluginID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for topic, subscriptions := range b.subscriptions {
		filtered := subscriptions[:0]
		for _, candidate := range subscriptions {
			if candidate.owner != pluginID {
				filtered = append(filtered, candidate)
			}
		}
		if len(filtered) == 0 {
			delete(b.subscriptions, topic)
		} else {
			b.subscriptions[topic] = filtered
		}
	}
}

type scopedBus struct {
	bus      *Bus
	pluginID string
}

func (s *scopedBus) Publish(ctx context.Context, event contract.Event) error {
	return s.bus.publish(ctx, s.pluginID, event)
}

func (s *scopedBus) Subscribe(topic string, handler contract.EventHandler, opts ...contract.SubOption) (contract.Subscription, error) {
	return s.bus.subscribe(s.pluginID, topic, handler, opts...)
}

func (b *Bus) publish(ctx context.Context, publisher string, event contract.Event) error {
	if publisher == "" {
		return &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "发布者插件 ID 不能为空"}
	}
	if event.Topic == "" {
		return &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "事件 Topic 不能为空"}
	}
	if !b.policy.AllowPublish(publisher, event.Topic) {
		b.logger.Warn("拒绝未授权事件发布", "plugin_id", publisher, "topic", event.Topic)
		return &contract.ServiceError{Code: contract.ErrorPermissionDenied, Message: "无权发布 Topic " + event.Topic}
	}

	ctx, traceID := observability.EnsureTraceID(ctx)
	if event.TraceID == "" {
		event.TraceID = traceID
	} else {
		ctx = observability.ContextWithTraceID(ctx, event.TraceID)
	}
	if event.ID == "" {
		event.ID = observability.NewTraceID()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	event.Source = publisher

	b.mu.RLock()
	router := b.router
	deduplicator := b.deduplicator
	b.mu.RUnlock()
	if deduplicator != nil {
		_, duplicate, key, err := deduplicator.Check(event)
		if err != nil {
			return fmt.Errorf("事件去重失败: %w", err)
		}
		if duplicate && deduplicator.ShouldDrop(true) {
			b.logger.Debug("丢弃重复事件", "event_id", event.ID, "dedup_key", key, "topic", event.Topic)
			return nil
		}
		if duplicate {
			if event.Headers == nil {
				event.Headers = make(map[string]string)
			}
			event.Headers["botfabric_duplicate"] = "true"
			event.Headers["botfabric_dedup_key"] = key
		}
	}

	subscriptions := b.snapshot(event.Topic)
	candidateOwners := make([]string, 0)
	seenOwners := make(map[string]struct{})
	for _, subscription := range subscriptions {
		if _, seen := seenOwners[subscription.owner]; !seen {
			seenOwners[subscription.owner] = struct{}{}
			candidateOwners = append(candidateOwners, subscription.owner)
		}
	}
	b.mu.RLock()
	sinks := append([]contract.EventSink(nil), b.sinks...)
	b.mu.RUnlock()
	for _, sink := range sinks {
		for _, candidate := range sink.Candidates() {
			if _, seen := seenOwners[candidate]; !seen {
				seenOwners[candidate] = struct{}{}
				candidateOwners = append(candidateOwners, candidate)
			}
		}
	}
	plan := contract.DispatchPlan{Mode: contract.DispatchBroadcast, Targets: append([]string(nil), candidateOwners...)}
	if router != nil {
		plan = router.Plan(ctx, event, candidateOwners)
		if plan.Drop {
			b.logger.Debug("路由规则丢弃事件", "event_id", event.ID, "topic", event.Topic)
			return nil
		}
		if plan.Policy.RequiredReplyBot != "" {
			event.RequiredReplyBot = plan.Policy.RequiredReplyBot
		}
		event.ResponsePolicy = plan.Policy
	}

	b.mu.RLock()
	hook := b.responseHook
	sinkTargets := append([]contract.EventSink(nil), b.sinks...)
	b.mu.RUnlock()
	if hook != nil {
		hook(cloneEvent(event), plan.Policy)
	}
	for _, sink := range sinkTargets {
		if err := sink.Deliver(ctx, cloneEvent(event), plan.Targets); err != nil {
			b.deadLetters.Record(DeadLetter{Event: cloneEvent(event), SubscriberID: "event-sink", Error: err.Error(), FailedAt: time.Now().UTC()})
		}
	}

	groups := make(map[string][]subscription)
	owners := make([]string, 0, len(candidateOwners))
	targets := make(map[string]struct{}, len(plan.Targets))
	for _, target := range plan.Targets {
		targets[target] = struct{}{}
	}
	for _, candidate := range subscriptions {
		if plan.Mode == contract.DispatchSelective {
			if _, selected := targets[candidate.owner]; !selected {
				continue
			}
		}
		if _, exists := groups[candidate.owner]; !exists {
			owners = append(owners, candidate.owner)
		}
		groups[candidate.owner] = append(groups[candidate.owner], candidate)
	}

	type deliveryFailure struct {
		candidate subscription
		err       error
	}
	failures := make(chan deliveryFailure, len(subscriptions))
	var wait sync.WaitGroup
	for _, owner := range owners {
		ownerSubscriptions := groups[owner]
		wait.Add(1)
		go func(subscriptions []subscription) {
			defer wait.Done()
			for _, candidate := range subscriptions {
				if err := b.deliver(ctx, candidate, event); err != nil {
					failures <- deliveryFailure{candidate: candidate, err: err}
				}
			}
		}(ownerSubscriptions)
	}
	wait.Wait()
	close(failures)

	var joined []error
	for failure := range failures {
		joined = append(joined, fmt.Errorf("订阅者 %s: %w", failure.candidate.owner, failure.err))
		b.deadLetters.Record(DeadLetter{
			Event:        cloneEvent(event),
			SubscriberID: failure.candidate.owner,
			Error:        failure.err.Error(),
			FailedAt:     time.Now().UTC(),
		})
	}
	return errors.Join(joined...)
}

func (b *Bus) snapshot(topic string) []subscription {
	b.mu.RLock()
	defer b.mu.RUnlock()
	subscriptions := make([]subscription, len(b.subscriptions[topic]))
	copy(subscriptions, b.subscriptions[topic])
	return subscriptions
}

func (b *Bus) deliver(parent context.Context, candidate subscription, event contract.Event) (err error) {
	done, accepted := b.activity.Begin(candidate.owner)
	if !accepted {
		return &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "订阅者已停止接收新事件"}
	}
	ctx, cancel := context.WithTimeout(parent, candidate.timeout)
	defer cancel()

	completed := make(chan error, 1)
	go func() {
		defer done()
		defer func() {
			if recovered := recover(); recovered != nil {
				completed <- fmt.Errorf("处理器 panic: %v", recovered)
			}
		}()
		completed <- candidate.handler(ctx, cloneEvent(event))
	}()

	select {
	case err = <-completed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Bus) subscribe(owner, topic string, handler contract.EventHandler, opts ...contract.SubOption) (contract.Subscription, error) {
	if owner == "" || topic == "" || handler == nil {
		return nil, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "订阅者、Topic 和处理器不能为空"}
	}
	if !b.policy.AllowSubscribe(owner, topic) {
		b.logger.Warn("拒绝未授权事件订阅", "plugin_id", owner, "topic", topic)
		return nil, &contract.ServiceError{Code: contract.ErrorPermissionDenied, Message: "无权订阅 Topic " + topic}
	}
	config := contract.SubscriptionConfig{Timeout: b.defaultTimeout}
	for _, option := range opts {
		if option != nil {
			option(&config)
		}
	}
	if config.Timeout <= 0 {
		return nil, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "处理器超时必须大于零"}
	}

	candidate := subscription{
		id:       b.nextID.Add(1),
		owner:    owner,
		topic:    topic,
		priority: config.Priority,
		timeout:  config.Timeout,
		handler:  handler,
	}
	b.mu.Lock()
	b.subscriptions[topic] = append(b.subscriptions[topic], candidate)
	sort.SliceStable(b.subscriptions[topic], func(i, j int) bool {
		return b.subscriptions[topic][i].priority > b.subscriptions[topic][j].priority
	})
	b.mu.Unlock()

	return &subscriptionHandle{bus: b, topic: topic, id: candidate.id}, nil
}

type subscriptionHandle struct {
	bus   *Bus
	topic string
	id    uint64
	once  sync.Once
}

func (s *subscriptionHandle) Unsubscribe() error {
	s.once.Do(func() {
		s.bus.mu.Lock()
		defer s.bus.mu.Unlock()
		subscriptions := s.bus.subscriptions[s.topic]
		for index, candidate := range subscriptions {
			if candidate.id == s.id {
				subscriptions = append(subscriptions[:index], subscriptions[index+1:]...)
				break
			}
		}
		if len(subscriptions) == 0 {
			delete(s.bus.subscriptions, s.topic)
		} else {
			s.bus.subscriptions[s.topic] = subscriptions
		}
	})
	return nil
}

func cloneEvent(event contract.Event) contract.Event {
	cloned := event
	cloned.Payload = append([]byte(nil), event.Payload...)
	if event.Headers != nil {
		cloned.Headers = make(map[string]string, len(event.Headers))
		for key, value := range event.Headers {
			cloned.Headers[key] = value
		}
	}
	return cloned
}
