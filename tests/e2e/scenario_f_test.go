package e2e_test

import (
	"context"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/PhantomDaze/botfabric/plugins/routing"
)

// F1：应用端运行中依赖协议端动作服务时，卸载协议端应被拒绝；
// 先卸载依赖者后，才能卸载被依赖的协议端。
func TestScenarioUnloadRejectedWhileDependentRunning(t *testing.T) {
	h := newSim(t)
	h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	h.addApp(simAppConfig{ID: "app.alpha"})
	env := h.start(context.Background())
	ctx := context.Background()

	// 协议端被运行中的应用端依赖，直接卸载应失败。
	if err := env.unloadPlugin(ctx, "sim.protocol"); err == nil {
		t.Fatal("被运行中依赖的协议端不应能卸载")
	}

	// 卸载依赖者后，协议端可安全卸载。
	if err := env.unloadPlugin(ctx, "app.alpha"); err != nil {
		t.Fatalf("卸载依赖应用端: %v", err)
	}
	if err := env.unloadPlugin(ctx, "sim.protocol"); err != nil {
		t.Fatalf("解除依赖后卸载协议端: %v", err)
	}
	if _, ok := env.Endpoints.Get("sim.protocol.default"); ok {
		t.Fatal("卸载协议端后其端点应被清理")
	}
}

// F2：停止应用端插件后取消订阅，不再接收后续事件。
func TestScenarioStoppedApplicationStopsReceiving(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	env := h.start(context.Background())
	ctx := context.Background()

	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "first"}); err != nil {
		t.Fatalf("发布首条: %v", err)
	}
	if alpha.ReceivedCount() != 1 {
		t.Fatalf("停止前收到 = %d，期望 1", alpha.ReceivedCount())
	}

	if err := env.stopPlugin(ctx, "app.alpha"); err != nil {
		t.Fatalf("停止应用端: %v", err)
	}
	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-2", GroupID: "100", UserID: "7", Text: "second"}); err != nil {
		t.Fatalf("停止后发布: %v", err)
	}
	if alpha.ReceivedCount() != 1 {
		t.Fatalf("停止应用端后仍收到新事件（%d 条），期望保持 1", alpha.ReceivedCount())
	}
}

// F3：路由规则随其声明插件卸载而失效，流量回到默认广播。
func TestScenarioUnloadRoutingRuleRestoresBroadcast(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	beta := h.addApp(simAppConfig{ID: "app.beta"})
	h.addRouting("sim.routing.selective", &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID: "only-alpha", Priority: 100, Match: contract.RuleMatch{Topic: groupTopic},
		Action: contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app.alpha"}},
	}}})
	env := h.start(context.Background())
	ctx := context.Background()

	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "before"}); err != nil {
		t.Fatalf("规则生效期发布: %v", err)
	}
	if alpha.ReceivedCount() != 1 || beta.ReceivedCount() != 0 {
		t.Fatalf("selective 规则下 alpha=%d beta=%d，期望 1/0", alpha.ReceivedCount(), beta.ReceivedCount())
	}
	if err := env.unloadPlugin(ctx, "sim.routing.selective"); err != nil {
		t.Fatalf("卸载路由插件: %v", err)
	}
	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-2", GroupID: "100", UserID: "7", Text: "after"}); err != nil {
		t.Fatalf("卸载规则后发布: %v", err)
	}
	if alpha.ReceivedCount() != 2 || beta.ReceivedCount() != 1 {
		t.Fatalf("路由卸载后默认广播 alpha=%d beta=%d，期望 2/1", alpha.ReceivedCount(), beta.ReceivedCount())
	}
}

// F4：路由插件运行中热重载新规则，下一条消息立即按新规则投递。
func TestScenarioReloadRoutingSwitchesDispatchTarget(t *testing.T) {
	firstSpec := &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID: "only-alpha", Priority: 100, Match: contract.RuleMatch{Topic: groupTopic},
		Action: contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app.alpha"}},
	}}}
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	beta := h.addApp(simAppConfig{ID: "app.beta"})
	h.addRouting("sim.routing.reload", firstSpec)

	// Reload 使用相同 entry 装载新的不可变路由清单；factory 依据当前版本创建实例。
	spec := h.specs[len(h.specs)-1]
	manifestV2 := spec.manifest
	manifestV2.Routing = &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID: "only-beta", Priority: 100, Match: contract.RuleMatch{Topic: groupTopic},
		Action: contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app.beta"}},
	}}}
	factoryCalls := 0
	h.specs[len(h.specs)-1].factory = func() contract.Plugin {
		if factoryCalls == 0 {
			factoryCalls++
			return routing.New(spec.manifest)
		}
		factoryCalls++
		return routing.New(manifestV2)
	}
	env := h.start(context.Background())

	ctx := context.Background()
	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "before"}); err != nil {
		t.Fatalf("重载前发布: %v", err)
	}
	if alpha.ReceivedCount() != 1 || beta.ReceivedCount() != 0 {
		t.Fatalf("重载前投递 alpha=%d beta=%d，期望 1/0", alpha.ReceivedCount(), beta.ReceivedCount())
	}

	if err := env.Manager.Reload(ctx, manifestV2); err != nil {
		t.Fatalf("热重载路由插件: %v", err)
	}
	if state, ok := env.Manager.State("sim.routing.reload"); !ok || state != contract.PluginRunning {
		t.Fatalf("重载后路由插件状态 = %s, exists=%v，期望 running", state, ok)
	}
	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-2", GroupID: "100", UserID: "7", Text: "after"}); err != nil {
		t.Fatalf("重载后发布: %v", err)
	}
	if alpha.ReceivedCount() != 1 || beta.ReceivedCount() != 1 {
		t.Fatalf("重载后投递 alpha=%d beta=%d，期望 1/1", alpha.ReceivedCount(), beta.ReceivedCount())
	}
}
