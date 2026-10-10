package routing

import (
	"context"
	"fmt"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

var benchmarkDispatchPlan contract.DispatchPlan

func BenchmarkRouterPlanDefaultBroadcast(b *testing.B) {
	for _, candidateCount := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("candidates=%d", candidateCount), func(b *testing.B) {
			router := New()
			event := eventWithMessage(contract.GroupMessage{Text: "hello"})
			candidates := benchmarkCandidates(candidateCount)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkDispatchPlan = router.Plan(ctx, event, candidates)
			}
		})
	}
}

func BenchmarkRouterPlanRules(b *testing.B) {
	benchmarks := []struct {
		name  string
		rules []contract.RoutingRule
		text  string
	}{
		{
			name: "one-topic-rule",
			rules: []contract.RoutingRule{{
				ID:       "group-message",
				Priority: 1,
				Match:    contract.RuleMatch{Topic: "bot.message.group"},
				Action:   contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app-0"}},
			}},
			text: "hello",
		},
		{
			name:  "32-priority-regex-rules",
			rules: benchmarkRegexRules(32),
			text:  "/command-31",
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			router := New()
			if err := router.RegisterRules("benchmark", &contract.RoutingSpec{Rules: benchmark.rules}); err != nil {
				b.Fatal(err)
			}
			event := eventWithMessage(contract.GroupMessage{Text: benchmark.text})
			candidates := benchmarkCandidates(16)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkDispatchPlan = router.Plan(ctx, event, candidates)
			}
		})
	}
}

func benchmarkCandidates(count int) []string {
	candidates := make([]string, count)
	for i := range candidates {
		candidates[i] = fmt.Sprintf("app-%d", i)
	}
	return candidates
}

func benchmarkRegexRules(count int) []contract.RoutingRule {
	rules := make([]contract.RoutingRule, count)
	for i := range rules {
		rules[i] = contract.RoutingRule{
			ID:       fmt.Sprintf("command-%d", i),
			Priority: count - i,
			Match: contract.RuleMatch{
				Topic:     "bot.message.group",
				TextRegex: fmt.Sprintf(`^/command-%d$`, i),
			},
			Action: contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app-0"}},
		}
	}
	return rules
}
