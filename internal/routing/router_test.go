package routing

import (
	"context"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func eventWithMessage(message contract.GroupMessage) contract.Event {
	payload, _ := contract.EncodeJSON(message)
	return contract.Event{Topic: "bot.message.group", Schema: "bot.message@v1", BotID: "bot-a", Payload: payload, Headers: map[string]string{"tenant": "one"}}
}

func TestDefaultPlanBroadcastsAllCandidates(t *testing.T) {
	router := New()
	plan := router.Plan(context.Background(), eventWithMessage(contract.GroupMessage{Text: "hello"}), []string{"app-a", "app-b"})
	if plan.Mode != contract.DispatchBroadcast || plan.Drop || len(plan.Targets) != 2 {
		t.Fatalf("默认计划 = %#v", plan)
	}
}

func TestRulePriorityAndSelectiveTargets(t *testing.T) {
	router := New()
	spec := &contract.RoutingSpec{Rules: []contract.RoutingRule{
		{
			ID:       "low",
			Priority: 1,
			Match:    contract.RuleMatch{Topic: "bot.message.group"},
			Action:   contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"low-app"}},
		},
		{
			ID:       "command",
			Priority: 10,
			Match:    contract.RuleMatch{TextRegex: `^/help`},
			Action:   contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"help-app"}},
		},
	}}
	if err := router.RegisterRules("routing", spec); err != nil {
		t.Fatal(err)
	}
	plan := router.Plan(context.Background(), eventWithMessage(contract.GroupMessage{Text: "/help now"}), []string{"app-a", "app-b"})
	if plan.Mode != contract.DispatchSelective || len(plan.Targets) != 1 || plan.Targets[0] != "help-app" {
		t.Fatalf("高优先级选择规则未生效: %#v", plan)
	}
}

func TestFallbackRuleUsedWhenNoPrimaryMatch(t *testing.T) {
	router := New()
	if err := router.RegisterRules("routing", &contract.RoutingSpec{Rules: []contract.RoutingRule{
		{ID: "fallback", Priority: 1, Action: contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"default-app"}, Fallback: true}},
	}}); err != nil {
		t.Fatal(err)
	}
	plan := router.Plan(context.Background(), eventWithMessage(contract.GroupMessage{Text: "ordinary"}), []string{"app-a"})
	if plan.Mode != contract.DispatchSelective || len(plan.Targets) != 1 || plan.Targets[0] != "default-app" {
		t.Fatalf("fallback 计划错误: %#v", plan)
	}
}

func TestDropRuleAndHeaders(t *testing.T) {
	router := New()
	if err := router.RegisterRules("routing", &contract.RoutingSpec{Rules: []contract.RoutingRule{
		{ID: "tenant", Priority: 10, Match: contract.RuleMatch{Headers: map[string]string{"tenant": "blocked"}}, Action: contract.RuleAction{Mode: contract.DispatchDrop}},
	}}); err != nil {
		t.Fatal(err)
	}
	blocked := eventWithMessage(contract.GroupMessage{Text: "hello"})
	blocked.Headers["tenant"] = "blocked"
	plan := router.Plan(context.Background(), blocked, []string{"app-a"})
	if !plan.Drop || plan.Mode != contract.DispatchDrop {
		t.Fatalf("drop 计划错误: %#v", plan)
	}
	allowed := router.Plan(context.Background(), eventWithMessage(contract.GroupMessage{Text: "hello"}), []string{"app-a"})
	if allowed.Drop {
		t.Fatal("不匹配 Header 不应丢弃")
	}
}

type classifierFunc func(context.Context, contract.Event, []string) (contract.DispatchPlan, error)

func (f classifierFunc) Classify(ctx context.Context, event contract.Event, candidates []string) (contract.DispatchPlan, error) {
	return f(ctx, event, candidates)
}

func TestClassifierAndTimeoutFallback(t *testing.T) {
	router := New()
	if err := router.SetClassifierTimeout(10 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := router.RegisterClassifier("mock", classifierFunc(func(context.Context, contract.Event, []string) (contract.DispatchPlan, error) {
		return contract.DispatchPlan{Mode: contract.DispatchSelective, Targets: []string{"llm-app"}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := router.RegisterRules("routing", &contract.RoutingSpec{Rules: []contract.RoutingRule{
		{ID: "classify", Priority: 10, Action: contract.RuleAction{Mode: contract.DispatchBroadcast, Classifier: "mock"}},
	}}); err != nil {
		t.Fatal(err)
	}
	plan := router.Plan(context.Background(), eventWithMessage(contract.GroupMessage{Text: "hello"}), []string{"app-a"})
	if plan.Mode != contract.DispatchSelective || len(plan.Targets) != 1 || plan.Targets[0] != "llm-app" {
		t.Fatalf("分类器计划错误: %#v", plan)
	}
}

func TestUnregisterRulesRestoresDefault(t *testing.T) {
	router := New()
	_ = router.RegisterRules("routing", &contract.RoutingSpec{Rules: []contract.RoutingRule{{ID: "drop", Action: contract.RuleAction{Mode: contract.DispatchDrop}}}})
	router.UnregisterRules("routing")
	plan := router.Plan(context.Background(), eventWithMessage(contract.GroupMessage{Text: "hello"}), []string{"app-a"})
	if plan.Drop || plan.Mode != contract.DispatchBroadcast {
		t.Fatalf("删除规则后未恢复默认广播: %#v", plan)
	}
}
