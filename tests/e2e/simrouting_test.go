package e2e_test

import (
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/PhantomDaze/botfabric/plugins/classifier"
	"github.com/PhantomDaze/botfabric/plugins/routing"
)

// addRouting 声明一个只含路由规则的仿真插件（无服务、无订阅）。
func (h *simHarness) addRouting(id string, spec *contract.RoutingSpec) {
	h.t.Helper()
	manifest := contract.Manifest{
		ID:      id,
		Name:    id,
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   id + ".entry",
		Limits:  contract.Limits{Timeout: 2 * time.Second},
		Routing: spec,
	}
	h.declare(manifest, routing.New(manifest), simRules{})
}

// addClassifierRouting 声明一个把群消息交给真实 LLM 分类器决策的路由规则插件。
// 分类器本身由 addRealLLMClassifier 另行装载并注册到路由器。
func (h *simHarness) addClassifierRouting() {
	h.t.Helper()
	manifest := contract.Manifest{
		ID:      "sim.routing.classifier",
		Name:    "sim.routing.classifier",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   "sim.routing.classifier.entry",
		Limits:  contract.Limits{Timeout: 2 * time.Second},
		Routing: &contract.RoutingSpec{Rules: []contract.RoutingRule{{
			ID:       "classify-group-messages",
			Priority: 100,
			Match:    contract.RuleMatch{Topic: groupTopic},
			Action:   contract.RuleAction{Mode: contract.DispatchBroadcast, Classifier: classifier.RealLLMID},
		}}},
	}
	h.declare(manifest, routing.New(manifest), simRules{})
}

// addRealLLMClassifier 装载真实 LLM 分类器插件；配置来自 newSimLLM 注入的环境变量。
func (h *simHarness) addRealLLMClassifier() {
	h.t.Helper()
	h.declare(classifier.NewRealLLMPlugin().Manifest(), classifier.NewRealLLMPlugin(), simRules{})
}
