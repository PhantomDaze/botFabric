// Package response 提供多个应用端候选回复的竞争采纳与目标协议端选择。
package response

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PhantomDaze/botfabric/internal/endpoint"
	"github.com/PhantomDaze/botfabric/internal/observability"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

const (
	// ServiceName 是响应协调服务名称。
	ServiceName = "botfabric.response"
	// SubmitMethod 提交一个候选回复。
	SubmitMethod = "Submit"
	// CancelMethod 取消一个未决事件的候选回复。
	CancelMethod = "Cancel"
)

// ApplicationDispatchSubject 是应用端适配器发起协议动作调用时使用的固定内部主体。
// 该主体只由服务端写入，不从客户端载荷派生，便于策略中做稳定授权。
const ApplicationDispatchSubject = "application.onebot"

// ActionInvoker 以指定应用端身份调用协议动作服务。
type ActionInvoker func(ctx context.Context, caller string, request contract.Request) (contract.Response, error)

// CandidateStore 是响应状态的可插拔存储接口。
// 分布式部署可用 Redis 等实现替换本轮内存实现。
type CandidateStore interface {
	Claim(eventID, key string) bool
	TryWin(eventID, key string) bool
	Winner(eventID string) bool
	ReleaseWinner(eventID string)
	Release(eventID string)
}

type memoryStore struct {
	mu     sync.Mutex
	claims map[string]map[string]struct{}
	winner map[string]string
}

func newMemoryStore() *memoryStore {
	return &memoryStore{claims: make(map[string]map[string]struct{}), winner: make(map[string]string)}
}
func (s *memoryStore) Claim(eventID, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.winner[eventID] != "" {
		return false
	}
	if _, ok := s.claims[eventID]; !ok {
		s.claims[eventID] = make(map[string]struct{})
	}
	if _, exists := s.claims[eventID][key]; exists {
		return false
	}
	s.claims[eventID][key] = struct{}{}
	return true
}
func (s *memoryStore) Winner(eventID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.winner[eventID] != ""
}
func (s *memoryStore) TryWin(eventID, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.winner[eventID] != "" {
		return false
	}
	s.winner[eventID] = key
	return true
}
func (s *memoryStore) ReleaseWinner(eventID string) {
	s.mu.Lock()
	delete(s.winner, eventID)
	s.mu.Unlock()
}
func (s *memoryStore) Release(eventID string) {
	s.mu.Lock()
	delete(s.claims, eventID)
	delete(s.winner, eventID)
	s.mu.Unlock()
}

// Coordinator 管理一条事件的候选回复生命周期。
type Coordinator struct {
	mu        sync.Mutex
	endpoints *endpoint.Registry
	invoke    ActionInvoker
	store     CandidateStore
	events    map[string]contract.Event
	policies  map[string]contract.ResponsePolicy
	pending   map[string][]contract.CandidateReply
	timers    map[string]*time.Timer
	expires   map[string]*time.Timer
	metrics   atomic.Pointer[observability.Metrics]
}

// New 创建响应协调器。
func New(endpoints *endpoint.Registry, invoke ActionInvoker) (*Coordinator, error) {
	if endpoints == nil || invoke == nil {
		return nil, errors.New("端点注册表和动作调用器不能为空")
	}
	return &Coordinator{
		endpoints: endpoints,
		invoke:    invoke,
		store:     newMemoryStore(),
		events:    make(map[string]contract.Event),
		policies:  make(map[string]contract.ResponsePolicy),
		pending:   make(map[string][]contract.CandidateReply),
		timers:    make(map[string]*time.Timer),
		expires:   make(map[string]*time.Timer),
	}, nil
}

// SetStore 替换响应状态存储；应在并发使用前调用。
func (c *Coordinator) SetStore(store CandidateStore) error {
	if store == nil {
		return errors.New("响应状态存储不能为空")
	}
	c.mu.Lock()
	c.store = store
	c.mu.Unlock()
	return nil
}

// SetMetrics 注入可选的运行时指标注册表。
func (c *Coordinator) SetMetrics(metrics *observability.Metrics) {
	c.metrics.Store(metrics)
}

// RegisterEvent 保存待回复事件及其响应策略。
func (c *Coordinator) RegisterEvent(event contract.Event, policy contract.ResponsePolicy) {
	c.mu.Lock()
	if old := c.expires[event.ID]; old != nil {
		old.Stop()
	}
	c.events[event.ID] = cloneEvent(event)
	c.policies[event.ID] = clonePolicy(policy)
	ttl := policy.Timeout
	if policy.Mode == contract.ResponsePriorityOverride {
		ttl += time.Minute
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	c.expires[event.ID] = time.AfterFunc(ttl, func() { c.expire(event.ID) })
	c.mu.Unlock()
}

func (c *Coordinator) expire(eventID string) {
	c.mu.Lock()
	delete(c.events, eventID)
	delete(c.policies, eventID)
	delete(c.pending, eventID)
	delete(c.timers, eventID)
	delete(c.expires, eventID)
	c.mu.Unlock()
	c.store.Release(eventID)
}

// SubmitResult 是 Submit 的业务结果。
type SubmitResult struct {
	Status    string `json:"status"`
	MessageID string `json:"message_id,omitempty"`
	Winner    bool   `json:"winner,omitempty"`
}

// Submit 验证并处理一个候选回复。
func (c *Coordinator) Submit(ctx context.Context, reply contract.CandidateReply) (SubmitResult, error) {
	if reply.EventID == "" || reply.ApplicationID == "" || reply.GroupID == "" || reply.Text == "" {
		return SubmitResult{}, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "候选回复缺少 event_id、application_id、group_id 或 text"}
	}
	if metrics := c.metrics.Load(); metrics != nil {
		metrics.ResponseCandidate()
	}
	key := reply.IdempotencyKey
	if key == "" {
		key = reply.ApplicationID + "\x00" + reply.EventID
	}
	if !c.store.Claim(reply.EventID, key) {
		if metrics := c.metrics.Load(); metrics != nil {
			metrics.ResponseDiscarded()
		}
		return SubmitResult{Status: "discarded"}, nil
	}

	c.mu.Lock()
	policy, exists := c.policies[reply.EventID]
	event := c.events[reply.EventID]
	c.mu.Unlock()
	if !exists {
		c.store.Release(reply.EventID)
		return SubmitResult{}, &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "事件不存在或已过期", Retryable: true}
	}
	if policy.Mode == "" || policy.Mode == contract.ResponseNone {
		return c.send(ctx, event, policy, reply, false)
	}
	if policy.Mode == contract.ResponseFirstWins {
		if !c.store.TryWin(reply.EventID, key) {
			if metrics := c.metrics.Load(); metrics != nil {
				metrics.ResponseDiscarded()
			}
			return SubmitResult{Status: "discarded"}, nil
		}
		result, err := c.send(ctx, event, policy, reply, true)
		if err != nil {
			c.store.ReleaseWinner(reply.EventID)
		}
		return result, err
	}
	if policy.Mode == contract.ResponsePriorityOverride {
		c.mu.Lock()
		c.pending[reply.EventID] = append(c.pending[reply.EventID], reply)
		if _, running := c.timers[reply.EventID]; !running {
			timeout := policy.Timeout
			if timeout <= 0 {
				timeout = time.Second
			}
			c.timers[reply.EventID] = time.AfterFunc(timeout, func() { c.flush(reply.EventID) })
		}
		c.mu.Unlock()
		return SubmitResult{Status: "pending"}, nil
	}
	return SubmitResult{}, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "未知响应模式"}
}

func (c *Coordinator) flush(eventID string) {
	c.mu.Lock()
	replies := append([]contract.CandidateReply(nil), c.pending[eventID]...)
	delete(c.pending, eventID)
	delete(c.timers, eventID)
	event := c.events[eventID]
	policy := c.policies[eventID]
	c.mu.Unlock()
	if len(replies) == 0 || !c.store.TryWin(eventID, "priority_override") {
		return
	}
	best := replies[0]
	for _, reply := range replies[1:] {
		if reply.Priority > best.Priority {
			best = reply
		}
	}
	if _, err := c.send(context.Background(), event, policy, best, true); err != nil {
		c.store.ReleaseWinner(eventID)
	}
}

func (c *Coordinator) send(ctx context.Context, event contract.Event, policy contract.ResponsePolicy, reply contract.CandidateReply, alreadyWon bool) (SubmitResult, error) {
	if !alreadyWon && c.store.Winner(event.ID) {
		if metrics := c.metrics.Load(); metrics != nil {
			metrics.ResponseDiscarded()
		}
		return SubmitResult{Status: "discarded"}, nil
	}
	botID := reply.TargetBotID
	if botID == "" {
		botID = policy.RequiredReplyBot
	}
	endpointID := event.EndpointID
	if botID != "" {
		endpoints := c.endpoints.ByBotID(botID)
		for _, candidate := range endpoints {
			if candidate.Kind == contract.EndpointProtocol {
				endpointID = candidate.ID
				break
			}
		}
		if endpointID == event.EndpointID && len(endpoints) == 0 {
			for _, fallbackBot := range policy.FallbackBots {
				for _, candidate := range c.endpoints.ByBotID(fallbackBot) {
					if candidate.Kind == contract.EndpointProtocol {
						botID = fallbackBot
						endpointID = candidate.ID
						break
					}
				}
				if endpointID != event.EndpointID {
					break
				}
			}
		}
	}
	if endpointID == "" && botID == "" {
		return SubmitResult{}, &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "没有可用的回复协议端", Retryable: true}
	}
	payload, err := json.Marshal(contract.SendMessageRequest{
		GroupID:        reply.GroupID,
		Text:           reply.Text,
		BotID:          botID,
		EndpointID:     endpointID,
		EventID:        event.ID,
		IdempotencyKey: reply.IdempotencyKey,
	})
	if err != nil {
		return SubmitResult{}, err
	}
	// 优先使用服务端内部主体，避免以客户端可控的应用端 ID 作为策略主体。
	subject := reply.DispatchSubject
	if subject == "" {
		subject = reply.ApplicationID
	}
	response, err := c.invoke(ctx, subject, contract.Request{
		Service:       "protocol.qq.action",
		Method:        "SendMessage",
		TraceID:       event.TraceID,
		CorrelationID: event.CorrelationID,
		Payload:       payload,
	})
	if err != nil {
		return SubmitResult{}, err
	}
	if response.Error != nil {
		return SubmitResult{}, response.Error
	}
	var result contract.SendMessageResult
	if len(response.Payload) > 0 {
		if err := json.Unmarshal(response.Payload, &result); err != nil {
			return SubmitResult{}, fmt.Errorf("解析回复动作结果: %w", err)
		}
	}
	if metrics := c.metrics.Load(); metrics != nil {
		metrics.ResponseAccepted()
	}
	return SubmitResult{Status: "accepted", MessageID: result.MessageID, Winner: true}, nil
}

// HandleServiceRequest 将公共服务请求转换为 Submit/Cancel 操作。
func (c *Coordinator) HandleServiceRequest(ctx context.Context, request contract.Request) (contract.Response, error) {
	if request.Method != SubmitMethod && request.Method != CancelMethod {
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorMethodNotFound, Message: "不支持的响应协调方法"}
	}
	if request.Method == CancelMethod {
		var value struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(request.Payload, &value); err != nil || value.EventID == "" {
			return contract.Response{}, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "Cancel 载荷无效"}
		}
		c.Cancel(value.EventID)
		return contract.Response{Payload: []byte(`{"status":"cancelled"}`)}, nil
	}
	var reply contract.CandidateReply
	if err := json.Unmarshal(request.Payload, &reply); err != nil {
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "Submit 载荷无效"}
	}
	// 服务入口一律以调用方身份执行，忽略载荷中的 application_id，
	// 防止插件伪造他人身份越权调用动作服务。
	reply.ApplicationID = request.Caller
	reply.DispatchSubject = request.Caller
	result, err := c.Submit(ctx, reply)
	if err != nil {
		return contract.Response{}, err
	}
	payload, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return contract.Response{}, marshalErr
	}
	return contract.Response{Payload: payload}, nil
}

// Cancel 取消指定事件的未决候选。
func (c *Coordinator) Cancel(eventID string) {
	c.mu.Lock()
	if timer := c.timers[eventID]; timer != nil {
		timer.Stop()
	}
	delete(c.timers, eventID)
	if expiry := c.expires[eventID]; expiry != nil {
		expiry.Stop()
	}
	delete(c.expires, eventID)
	delete(c.pending, eventID)
	delete(c.events, eventID)
	delete(c.policies, eventID)
	c.mu.Unlock()
	c.store.Release(eventID)
}

func cloneEvent(event contract.Event) contract.Event {
	event.Payload = append([]byte(nil), event.Payload...)
	if event.Headers != nil {
		event.Headers = make(map[string]string, len(event.Headers))
		for key, value := range event.Headers {
			event.Headers[key] = value
		}
	}
	return event
}

func clonePolicy(policy contract.ResponsePolicy) contract.ResponsePolicy {
	policy.FallbackBots = append([]string(nil), policy.FallbackBots...)
	return policy
}
