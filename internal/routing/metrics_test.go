package routing

import (
	"context"
	"strings"
	"testing"

	"github.com/PhantomDaze/botfabric/internal/observability"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func TestMetricsRecordPlanModesAndClassifierFallback(t *testing.T) {
	router := New()
	metrics := observability.NewMetrics()
	router.SetMetrics(metrics)
	event := eventWithMessage(contract.GroupMessage{Text: "hello"})
	ctx := context.Background()
	candidates := []string{"app-a"}

	plan := router.Plan(ctx, event, candidates)
	if plan.Mode != contract.DispatchBroadcast {
		t.Fatalf("默认计划 = %#v", plan)
	}

	if err := router.RegisterRules("routing", &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:       "selective",
		Priority: 10,
		Match:    contract.RuleMatch{Topic: "bot.message.group"},
		Action:   contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app-a"}},
	}}}); err != nil {
		t.Fatal(err)
	}
	if plan := router.Plan(ctx, event, candidates); plan.Mode != contract.DispatchSelective {
		t.Fatalf("选择计划 = %#v", plan)
	}

	if err := router.RegisterRules("routing", &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:       "drop",
		Priority: 10,
		Match:    contract.RuleMatch{Topic: "bot.message.group"},
		Action:   contract.RuleAction{Mode: contract.DispatchDrop},
	}}}); err != nil {
		t.Fatal(err)
	}
	if plan := router.Plan(ctx, event, candidates); !plan.Drop {
		t.Fatalf("丢弃计划 = %#v", plan)
	}

	if err := router.RegisterRules("routing", &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:       "classifier",
		Priority: 10,
		Match:    contract.RuleMatch{Topic: "bot.message.group"},
		Action:   contract.RuleAction{Classifier: "missing", Mode: contract.DispatchBroadcast},
	}}}); err != nil {
		t.Fatal(err)
	}
	if plan := router.Plan(ctx, event, candidates); plan.Mode != contract.DispatchBroadcast || len(plan.Targets) != 1 {
		t.Fatalf("分类器回退计划 = %#v", plan)
	}

	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, expected := range []string{
		"botfabric_dispatch_plans_total{mode=\"broadcast\"} 2",
		"botfabric_dispatch_plans_total{mode=\"selective\"} 1",
		"botfabric_dispatch_plans_total{mode=\"drop\"} 1",
		"botfabric_classifier_fallbacks_total 1",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("指标输出缺少 %q:\n%s", expected, text)
		}
	}
}
