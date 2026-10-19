package e2e_test

import (
	"context"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// A1：单个协议端发布的事件应广播到所有已订阅应用端。
func TestScenarioBroadcastReachesAllApplications(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	beta := h.addApp(simAppConfig{ID: "app.beta"})
	gamma := h.addApp(simAppConfig{ID: "app.gamma"})
	env := h.start(context.Background())

	ctx := context.Background()
	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hello"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}

	apps := map[string]*simApp{"app.alpha": alpha, "app.beta": beta, "app.gamma": gamma}
	for id, app := range apps {
		if app.ReceivedCount() != 1 {
			t.Fatalf("%s 收到事件数 = %d，期望 1", id, app.ReceivedCount())
		}
	}
	if alpha.Received()[0].EndpointID != "sim.protocol.default" {
		t.Fatalf("事件 EndpointID = %q", alpha.Received()[0].EndpointID)
	}
	if got := proto.SentCount(); got != 0 {
		t.Fatalf("无回复应用端不应触发动作，实际 %d", got)
	}
	if env.DeadLetterCount() != 0 {
		t.Fatalf("广播不应产生死信: %#v", env.Dead.Entries())
	}
}

// A2：多个协议端点与多个应用端构成 N:N，事件保留来源端点标识。
func TestScenarioNNRoutingAcrossProtocolEndpoints(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{
		ID: "sim.protocol",
		Endpoints: []simEndpoint{
			{ID: "ep-a", BotID: "bot-a", Protocol: "onebot", Version: "v11"},
			{ID: "ep-b", BotID: "bot-b", Protocol: "onebot", Version: "v12"},
		},
	})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	beta := h.addApp(simAppConfig{ID: "app.beta"})
	h.start(context.Background())

	ctx := context.Background()
	if err := proto.Emit(ctx, "ep-a", SimMessage{MessageID: "a-1", GroupID: "100", UserID: "7", Text: "from-a"}); err != nil {
		t.Fatalf("从 ep-a 发布: %v", err)
	}
	if err := proto.Emit(ctx, "ep-b", SimMessage{MessageID: "b-1", GroupID: "200", UserID: "8", Text: "from-b"}); err != nil {
		t.Fatalf("从 ep-b 发布: %v", err)
	}

	for id, app := range map[string]*simApp{"app.alpha": alpha, "app.beta": beta} {
		if app.ReceivedCount() != 2 {
			t.Fatalf("%s 收到事件数 = %d，期望 2", id, app.ReceivedCount())
		}
		first, second := app.Received()[0], app.Received()[1]
		if first.EndpointID != "ep-a" || first.BotID != "bot-a" {
			t.Fatalf("%s 首个事件来源 = %s/%s，期望 ep-a/bot-a", id, first.EndpointID, first.BotID)
		}
		if second.EndpointID != "ep-b" || second.BotID != "bot-b" {
			t.Fatalf("%s 第二个事件来源 = %s/%s，期望 ep-b/bot-b", id, second.EndpointID, second.BotID)
		}
		if groups := app.ReceivedGroupIDs(); groups[0] != "100" || groups[1] != "200" {
			t.Fatalf("%s 收到的群号序列 = %v，期望 [100 200]", id, groups)
		}
	}
}

// A3：selective 规则应只把事件投递给指定目标应用端。
func TestScenarioSelectiveRoutingTargetsSingleApplication(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	beta := h.addApp(simAppConfig{ID: "app.beta"})
	h.addRouting("sim.routing.selective", &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:       "only-alpha",
		Priority: 100,
		Match:    contract.RuleMatch{Topic: groupTopic},
		Action:   contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app.alpha"}},
	}}})
	h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}

	if alpha.ReceivedCount() != 1 {
		t.Fatalf("目标应用端收到事件数 = %d，期望 1", alpha.ReceivedCount())
	}
	if beta.ReceivedCount() != 0 {
		t.Fatalf("非目标应用端收到事件数 = %d，期望 0", beta.ReceivedCount())
	}
}

// A4：drop 规则应丢弃事件，任何应用端都不接收。
func TestScenarioDropRuleSuppressesEvent(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	h.addRouting("sim.routing.drop", &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:       "drop-all",
		Priority: 100,
		Match:    contract.RuleMatch{Topic: groupTopic},
		Action:   contract.RuleAction{Mode: contract.DispatchDrop},
	}}})
	env := h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("被丢弃的事件发布不应报错: %v", err)
	}
	if alpha.ReceivedCount() != 0 {
		t.Fatalf("drop 规则下应用端收到事件数 = %d，期望 0", alpha.ReceivedCount())
	}
	if env.DeadLetterCount() != 0 {
		t.Fatalf("drop 不应产生死信: %#v", env.Dead.Entries())
	}
}

// A5：高优先级规则命中时覆盖低优先级规则；未命中时回退到 fallback 规则。
func TestScenarioPriorityAndFallbackRules(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	beta := h.addApp(simAppConfig{ID: "app.beta"})
	h.addRouting("sim.routing.priority", &contract.RoutingSpec{Rules: []contract.RoutingRule{
		{
			ID:       "primary",
			Priority: 200,
			Match:    contract.RuleMatch{Topic: groupTopic, TextRegex: `^/special`},
			Action:   contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app.alpha"}},
		},
		{
			ID:       "fallback",
			Priority: 100,
			Match:    contract.RuleMatch{Topic: groupTopic},
			Action:   contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app.beta"}, Fallback: true},
		},
	}})
	h.start(context.Background())

	ctx := context.Background()
	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "/special run"}); err != nil {
		t.Fatalf("发布命中事件: %v", err)
	}
	if alpha.ReceivedCount() != 1 || beta.ReceivedCount() != 0 {
		t.Fatalf("命中主规则时 alpha=%d beta=%d，期望 1/0", alpha.ReceivedCount(), beta.ReceivedCount())
	}

	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-2", GroupID: "100", UserID: "7", Text: "ordinary"}); err != nil {
		t.Fatalf("发布回退事件: %v", err)
	}
	if alpha.ReceivedCount() != 1 || beta.ReceivedCount() != 1 {
		t.Fatalf("回退规则生效时 alpha=%d beta=%d，期望 1/1", alpha.ReceivedCount(), beta.ReceivedCount())
	}
}
