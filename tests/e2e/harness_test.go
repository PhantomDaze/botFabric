package e2e_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/dedup"
	"github.com/PhantomDaze/botfabric/internal/endpoint"
	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/gateway"
	"github.com/PhantomDaze/botfabric/internal/policy"
	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/internal/runtime"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// 仿真公共常量：主题、模式与核心服务名，保持与生产契约一致。
const (
	groupTopic      = "bot.message.group"
	groupSchema     = "bot.message@v1"
	responseService = "botfabric.response"
	responseSubmit  = "Submit"
	responseCancel  = "Cancel"
	actionService   = "protocol.qq.action"
	actionSend      = "SendMessage"
)

// simRules 是仿真插件在静态权限策略中获得的授权集合。
// 渲染为静态策略后可同时验证清单守卫与策略边界（deny-by-default）。
type simRules struct {
	publish   []string
	subscribe []string
	invoke    []policy.InvokeRule
	provide   []string
}

type simSpec struct {
	manifest contract.Manifest
	factory  contract.PluginFactory
	rules    simRules
}

type simPolicyGrant struct {
	id    string
	rules simRules
}

// simHarness 先收集仿真插件声明与策略授权，再一次性渲染策略并构建运行时。
// 由于静态策略必须在 EventBus/Gateway 构造前确定，插件需先声明后启动。
type simHarness struct {
	t           *testing.T
	botID       string
	endpoints   *endpoint.Registry
	store       dedup.Store
	logger      *slog.Logger
	specs       []simSpec
	policyExtra []simPolicyGrant
	allowAll    bool
}

// newSim 创建仿真环境构建器。
func newSim(t *testing.T) *simHarness {
	t.Helper()
	return &simHarness{
		t:         t,
		botID:     "sim-bot",
		endpoints: endpoint.New(),
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// withBotID 覆盖运行时 BotID。
func (h *simHarness) withBotID(id string) *simHarness { h.botID = id; return h }

// withEndpoints 复用共享端点注册表（用于跨 Runtime 实例场景）。
func (h *simHarness) withEndpoints(reg *endpoint.Registry) *simHarness {
	h.endpoints = reg
	return h
}

// withStore 注入共享去重存储（通常为 Redis 实现）。
func (h *simHarness) withStore(store dedup.Store) *simHarness { h.store = store; return h }

// withLogger 覆盖日志器；默认丢弃，避免测试输出噪声。
func (h *simHarness) withLogger(logger *slog.Logger) *simHarness { h.logger = logger; return h }

// declare 登记一个仿真插件（清单 + 实例 + 策略授权），等待 start 统一装载。
func (h *simHarness) declare(manifest contract.Manifest, plugin contract.Plugin, rules simRules) {
	h.t.Helper()
	if manifest.Entry == "" {
		h.t.Fatalf("仿真插件 %s 缺少入口", manifest.ID)
	}
	h.specs = append(h.specs, simSpec{
		manifest: manifest,
		factory:  func() contract.Plugin { return plugin },
		rules:    rules,
	})
}

// grantPolicy 为未由仿真插件工厂管理的外部身份登记权限策略。
func (h *simHarness) grantPolicy(id string, rules simRules) {
	h.policyExtra = append(h.policyExtra, simPolicyGrant{id: id, rules: rules})
}

// withAllowAllPolicy 供真实 OneBot 应用端回连使用：该适配器不具备插件 Host 身份，
// 由调用方控制策略边界的集成测试使用 allow 策略。
func (h *simHarness) withAllowAllPolicy() *simHarness {
	h.allowAll = true
	return h
}

// start 渲染权限策略、构建运行时、装载并启动全部已声明插件。
func (h *simHarness) start(ctx context.Context) *simEnv {
	h.t.Helper()
	env, err := h.startE(ctx)
	if err != nil {
		h.t.Fatalf("启动仿真运行时: %v", err)
	}
	return env
}

// startE 与 start 相同，但把装载/启动错误返回给调用方，用于断言失败路径。
func (h *simHarness) startE(ctx context.Context) (*simEnv, error) {
	h.t.Helper()
	policyYAML := h.renderPolicy()
	if h.allowAll {
		policyYAML = "default: allow\nplugins: {}\n"
	}
	checker, err := policy.Decode(strings.NewReader(policyYAML))
	if err != nil {
		return nil, fmt.Errorf("构建仿真权限策略: %w", err)
	}

	reg := registry.New()
	dead := &eventbus.MemoryDeadLetterSink{}
	bus, err := eventbus.New(time.Second, checker, h.logger, dead)
	if err != nil {
		return nil, fmt.Errorf("创建事件总线: %w", err)
	}
	svcGateway, err := gateway.New(reg, checker, h.logger, time.Second)
	if err != nil {
		return nil, fmt.Errorf("创建服务网关: %w", err)
	}
	manager, err := runtime.NewManagerWithEndpoints(reg, bus, svcGateway, h.endpoints, h.logger, h.botID, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("创建运行时: %w", err)
	}
	if h.store != nil {
		manager.SetDedupStore(h.store)
	}

	loader := runtime.NewBuiltinLoader()
	for _, spec := range h.specs {
		if err := loader.RegisterFactory(spec.manifest.Entry, spec.factory); err != nil {
			return nil, fmt.Errorf("注册工厂 %s: %w", spec.manifest.Entry, err)
		}
	}
	if err := manager.RegisterLoader(loader); err != nil {
		return nil, fmt.Errorf("注册加载器: %w", err)
	}
	for _, spec := range h.specs {
		if err := manager.Load(spec.manifest); err != nil {
			return nil, fmt.Errorf("装载插件 %s: %w", spec.manifest.ID, err)
		}
	}
	if err := manager.StartAll(ctx); err != nil {
		return nil, fmt.Errorf("启动运行时: %w", err)
	}

	env := &simEnv{
		t:         h.t,
		Manager:   manager,
		Endpoints: h.endpoints,
		Bus:       bus,
		Gateway:   svcGateway,
		Dead:      dead,
		loader:    loader,
		registry:  reg,
	}
	h.t.Cleanup(func() {
		if err := manager.StopAll(context.Background()); err != nil {
			h.t.Errorf("停止运行时: %v", err)
		}
	})
	return env, nil
}

// renderPolicy 依据已声明插件的授权渲染一份静态权限策略。
func (h *simHarness) renderPolicy() string {
	var b strings.Builder
	b.WriteString("default: deny\nplugins:\n")
	for _, spec := range h.specs {
		writePolicyPlugin(&b, spec.manifest.ID, spec.rules)
	}
	for _, grant := range h.policyExtra {
		writePolicyPlugin(&b, grant.id, grant.rules)
	}
	return b.String()
}

func writePolicyPlugin(b *strings.Builder, id string, rules simRules) {
	fmt.Fprintf(b, "  %s:\n", id)
	writeYAMLList(b, "publish", rules.publish)
	writeYAMLList(b, "subscribe", rules.subscribe)
	writeYAMLList(b, "provide", rules.provide)
	if len(rules.invoke) == 0 {
		b.WriteString("    invoke: []\n")
		return
	}
	b.WriteString("    invoke:\n")
	for _, rule := range rules.invoke {
		fmt.Fprintf(b, "      - service: %s\n", rule.Service)
		b.WriteString("        methods:\n")
		for _, method := range rule.Methods {
			fmt.Fprintf(b, "          - %s\n", method)
		}
	}
}

func writeYAMLList(b *strings.Builder, key string, values []string) {
	if len(values) == 0 {
		fmt.Fprintf(b, "    %s: []\n", key)
		return
	}
	fmt.Fprintf(b, "    %s:\n", key)
	for _, value := range values {
		fmt.Fprintf(b, "      - %s\n", value)
	}
}

// simEnv 是一次仿真运行的可操作句柄。
type simEnv struct {
	t         *testing.T
	Manager   *runtime.Manager
	Endpoints *endpoint.Registry
	Bus       *eventbus.Bus
	Gateway   *gateway.Gateway
	Dead      *eventbus.MemoryDeadLetterSink
	loader    *runtime.BuiltinLoader
	registry  *registry.Registry
}

// DeadLetterCount 返回当前死信数量。
func (e *simEnv) DeadLetterCount() int { return len(e.Dead.Entries()) }

// stopPlugin 停止一个已运行插件。
func (e *simEnv) stopPlugin(ctx context.Context, id string) error { return e.Manager.Stop(ctx, id) }

// unloadPlugin 卸载一个已停止插件。
func (e *simEnv) unloadPlugin(ctx context.Context, id string) error {
	return e.Manager.Unload(ctx, id)
}

// waitFor 轮询等待条件成立，超时则测试失败。
func (e *simEnv) waitFor(timeout time.Duration, description string, condition func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	e.t.Fatalf("等待超时（%s）：%s", timeout, description)
}

// waitForCount 等待取值函数达到期望计数。
func (e *simEnv) waitForCount(timeout time.Duration, description string, count func() int, want int) {
	e.t.Helper()
	e.waitFor(timeout, description, func() bool { return count() == want })
}
