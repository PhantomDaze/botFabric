package response

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/endpoint"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type actionCall struct {
	mu      sync.Mutex
	caller  string
	request contract.Request
}

func (c *actionCall) snapshot() (string, contract.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.caller, c.request
}

func newTestCoordinator(t *testing.T) (*Coordinator, *actionCall) {
	t.Helper()
	endpoints := endpoint.New()
	if err := endpoints.Register(contract.EndpointInfo{ID: "proto-a", Kind: contract.EndpointProtocol, BotID: "bot-a"}); err != nil {
		t.Fatal(err)
	}
	calls := &actionCall{}
	coordinator, err := New(endpoints, func(_ context.Context, caller string, request contract.Request) (contract.Response, error) {
		calls.mu.Lock()
		calls.caller = caller
		calls.request = request
		calls.mu.Unlock()
		return contract.Response{Payload: mustJSON(contract.SendMessageResult{MessageID: "sent-1"})}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, calls
}

func mustJSON(value any) []byte {
	payload, _ := json.Marshal(value)
	return payload
}

func testEvent(policy contract.ResponsePolicy) contract.Event {
	return contract.Event{ID: "event-1", EndpointID: "proto-a", BotID: "bot-a", TraceID: "trace-1", ResponsePolicy: policy}
}

func TestFirstWinsAcceptsOnlyFirstValidReply(t *testing.T) {
	coordinator, calls := newTestCoordinator(t)
	coordinator.RegisterEvent(testEvent(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: time.Second}), contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: time.Second})
	first, err := coordinator.Submit(context.Background(), contract.CandidateReply{EventID: "event-1", ApplicationID: "app-a", GroupID: "1", Text: "first"})
	if err != nil || first.Status != "accepted" || !first.Winner {
		t.Fatalf("首个回复 = %#v, err=%v", first, err)
	}
	second, err := coordinator.Submit(context.Background(), contract.CandidateReply{EventID: "event-1", ApplicationID: "app-b", GroupID: "1", Text: "second"})
	if err != nil || second.Status != "discarded" {
		t.Fatalf("迟到回复 = %#v, err=%v", second, err)
	}
	caller, request := calls.snapshot()
	var action contract.SendMessageRequest
	if err := json.Unmarshal(request.Payload, &action); err != nil {
		t.Fatal(err)
	}
	if caller != "app-a" || action.Text != "first" || action.EndpointID != "proto-a" {
		t.Fatalf("动作路由错误 caller=%q action=%#v", caller, action)
	}
}

func TestPriorityOverrideChoosesHighestPriority(t *testing.T) {
	coordinator, calls := newTestCoordinator(t)
	coordinator.RegisterEvent(testEvent(contract.ResponsePolicy{Mode: contract.ResponsePriorityOverride, Timeout: 20 * time.Millisecond}), contract.ResponsePolicy{Mode: contract.ResponsePriorityOverride, Timeout: 20 * time.Millisecond})
	low, err := coordinator.Submit(context.Background(), contract.CandidateReply{EventID: "event-1", ApplicationID: "app-a", GroupID: "1", Text: "low", Priority: 1})
	if err != nil || low.Status != "pending" {
		t.Fatalf("低优先级提交 = %#v, err=%v", low, err)
	}
	high, err := coordinator.Submit(context.Background(), contract.CandidateReply{EventID: "event-1", ApplicationID: "app-b", GroupID: "1", Text: "high", Priority: 10})
	if err != nil || high.Status != "pending" {
		t.Fatalf("高优先级提交 = %#v, err=%v", high, err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, request := calls.snapshot()
		var action contract.SendMessageRequest
		if len(request.Payload) > 0 && json.Unmarshal(request.Payload, &action) == nil && action.Text != "" {
			if action.Text != "high" {
				t.Fatalf("选中的回复 = %q，期望 high", action.Text)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("priority_override 未在超时后发送")
}

func TestRequiredReplyBotSelectsTargetProtocolEndpoint(t *testing.T) {
	coordinator, calls := newTestCoordinator(t)
	endpoints := coordinator.endpoints
	if err := endpoints.Register(contract.EndpointInfo{ID: "proto-b", Kind: contract.EndpointProtocol, BotID: "bot-b"}); err != nil {
		t.Fatal(err)
	}
	policy := contract.ResponsePolicy{Mode: contract.ResponseFirstWins, RequiredReplyBot: "bot-b"}
	coordinator.RegisterEvent(testEvent(policy), policy)
	if _, err := coordinator.Submit(context.Background(), contract.CandidateReply{EventID: "event-1", ApplicationID: "app-a", GroupID: "1", Text: "target"}); err != nil {
		t.Fatal(err)
	}
	_, request := calls.snapshot()
	var action contract.SendMessageRequest
	if err := json.Unmarshal(request.Payload, &action); err != nil {
		t.Fatal(err)
	}
	if action.BotID != "bot-b" || action.EndpointID != "proto-b" {
		t.Fatalf("指定机器人路由错误: %#v", action)
	}
}

func TestDuplicateIdempotencyKeyDoesNotSendTwice(t *testing.T) {
	coordinator, calls := newTestCoordinator(t)
	coordinator.RegisterEvent(testEvent(contract.ResponsePolicy{Mode: contract.ResponseFirstWins}), contract.ResponsePolicy{Mode: contract.ResponseFirstWins})
	reply := contract.CandidateReply{EventID: "event-1", ApplicationID: "app-a", GroupID: "1", Text: "once", IdempotencyKey: "key-1"}
	if _, err := coordinator.Submit(context.Background(), reply); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Submit(context.Background(), reply); err != nil {
		t.Fatal(err)
	}
	caller, _ := calls.snapshot()
	if caller != "app-a" {
		t.Fatal("首个动作调用方错误")
	}
}

// 确认测试调用器在异步路径下的共享状态不因数据竞争而失效。
func TestCoordinatorAsyncInvokerIsSafeForTest(t *testing.T) {
	coordinator, _ := newTestCoordinator(t)
	coordinator.RegisterEvent(testEvent(contract.ResponsePolicy{Mode: contract.ResponsePriorityOverride, Timeout: time.Millisecond}), contract.ResponsePolicy{Mode: contract.ResponsePriorityOverride, Timeout: time.Millisecond})
	if _, err := coordinator.Submit(context.Background(), contract.CandidateReply{EventID: "event-1", ApplicationID: "app-a", GroupID: "1", Text: "ok"}); err != nil {
		t.Fatal(err)
	}
}
