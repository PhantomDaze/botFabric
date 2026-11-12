package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type reloadPlugin struct {
	manifest contract.Manifest
	version  string
	fail     bool
	started  atomic.Bool
	host     contract.Host
}

func (p *reloadPlugin) Manifest() contract.Manifest { return p.manifest }
func (p *reloadPlugin) Start(_ context.Context, host contract.Host) error {
	p.host = host
	if p.fail {
		return errors.New("新实例启动失败")
	}
	if err := host.Gateway().Register(p.manifest.Provides.Services[0], func(context.Context, contract.Request) (contract.Response, error) {
		payload, _ := json.Marshal(map[string]string{"version": p.version})
		return contract.Response{Payload: payload}, nil
	}); err != nil {
		return err
	}
	p.started.Store(true)
	return nil
}
func (p *reloadPlugin) Stop(context.Context) error { p.started.Store(false); return nil }
func (p *reloadPlugin) Health(context.Context) contract.HealthStatus {
	return contract.HealthStatus{State: contract.HealthHealthy}
}

func TestReloadFailureKeepsOldPlugin(t *testing.T) {
	manager, reg := newTestManager(t)
	manifest := reloadManifest()
	old := &reloadPlugin{manifest: manifest, version: "old"}
	loader := NewBuiltinLoader()
	_ = loader.RegisterFactory(manifest.Entry, func() contract.Plugin { return old })
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), manifest.ID); err != nil {
		t.Fatal(err)
	}

	newManifest := manifest
	newPlugin := &reloadPlugin{manifest: newManifest, version: "new", fail: true}
	loader2 := NewBuiltinLoader()
	_ = loader2.RegisterFactory(manifest.Entry, func() contract.Plugin { return newPlugin })
	manager.loaders[contract.RuntimeInProcess] = loader2
	if err := manager.Reload(context.Background(), newManifest); err == nil {
		t.Fatal("失败重载应返回错误")
	}
	if !old.started.Load() {
		t.Fatal("重载失败后旧插件应保持运行")
	}
	entry, exists := reg.Service("reload.service")
	if !exists || entry.Owner != manifest.ID {
		t.Fatal("旧服务应保留")
	}
	response, err := entry.Handler(context.Background(), contract.Request{})
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]string
	_ = json.Unmarshal(response.Payload, &value)
	if value["version"] != "old" {
		t.Fatalf("服务版本 = %q", value["version"])
	}
}

func TestReloadSuccessSwitchesService(t *testing.T) {
	manager, reg := newTestManager(t)
	manifest := reloadManifest()
	old := &reloadPlugin{manifest: manifest, version: "old"}
	loader := NewBuiltinLoader()
	_ = loader.RegisterFactory(manifest.Entry, func() contract.Plugin { return old })
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), manifest.ID); err != nil {
		t.Fatal(err)
	}

	newPlugin := &reloadPlugin{manifest: manifest, version: "new"}
	loader2 := NewBuiltinLoader()
	_ = loader2.RegisterFactory(manifest.Entry, func() contract.Plugin { return newPlugin })
	manager.loaders[contract.RuntimeInProcess] = loader2
	if err := manager.Reload(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	if old.started.Load() {
		t.Fatal("成功重载后旧插件应停止")
	}
	if !newPlugin.started.Load() {
		t.Fatal("成功重载后新插件应运行")
	}
	entry, _ := reg.Service("reload.service")
	response, err := entry.Handler(context.Background(), contract.Request{})
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]string
	_ = json.Unmarshal(response.Payload, &value)
	if value["version"] != "new" {
		t.Fatalf("服务版本 = %q", value["version"])
	}
}

func TestConcurrentReloadsAreSerialized(t *testing.T) {
	manager, _ := newTestManager(t)
	manifest := reloadManifest()
	old := &reloadPlugin{manifest: manifest, version: "old"}
	loader := NewBuiltinLoader()
	if err := loader.RegisterFactory(manifest.Entry, func() contract.Plugin { return old }); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), manifest.ID); err != nil {
		t.Fatal(err)
	}

	var factoryCalls atomic.Int32
	loader2 := NewBuiltinLoader()
	if err := loader2.RegisterFactory(manifest.Entry, func() contract.Plugin {
		version := factoryCalls.Add(1)
		return &reloadPlugin{manifest: manifest, version: fmt.Sprintf("new-%d", version)}
	}); err != nil {
		t.Fatal(err)
	}
	manager.loaders[contract.RuntimeInProcess] = loader2

	var group sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- manager.Reload(context.Background(), manifest)
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := factoryCalls.Load(); got != 2 {
		t.Fatalf("重载工厂调用次数 = %d，期望 2", got)
	}
	if state, exists := manager.State(manifest.ID); !exists || state != contract.PluginRunning {
		t.Fatalf("并发重载后的插件状态 = %s, exists=%v", state, exists)
	}
}

func TestUnloadRejectsRunningDependents(t *testing.T) {
	manager, _ := newTestManager(t)
	providerManifest := reloadManifest()
	provider := &reloadPlugin{manifest: providerManifest, version: "provider"}
	consumerManifest := testManifest("consumer.reload", "consumer.reload.entry")
	consumerManifest.Requires.Services = []string{"reload.service"}
	consumer := &trackingPlugin{manifest: consumerManifest}
	loader := NewBuiltinLoader()
	_ = loader.RegisterFactory(providerManifest.Entry, func() contract.Plugin { return provider })
	_ = loader.RegisterFactory(consumerManifest.Entry, func() contract.Plugin { return consumer })
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(providerManifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(consumerManifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.StartAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Unload(context.Background(), providerManifest.ID); err == nil {
		t.Fatal("有运行中依赖时应拒绝卸载")
	}
}

func TestUnloadRemovesStoppedPlugin(t *testing.T) {
	manager, reg := newTestManager(t)
	manifest := reloadManifest()
	plugin := &reloadPlugin{manifest: manifest, version: "old"}
	loader := NewBuiltinLoader()
	_ = loader.RegisterFactory(manifest.Entry, func() contract.Plugin { return plugin })
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), manifest.ID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Unload(context.Background(), manifest.ID); err != nil {
		t.Fatal(err)
	}
	if _, exists := reg.Service("reload.service"); exists {
		t.Fatal("卸载后服务应移除")
	}
	if _, exists := manager.State(manifest.ID); exists {
		t.Fatal("卸载后插件应从 Runtime 移除")
	}
}

func reloadManifest() contract.Manifest {
	return contract.Manifest{ID: "reload.plugin", Name: "Reload Plugin", Version: "0.1.0", Runtime: contract.RuntimeInProcess, Entry: "reload.entry", Provides: contract.Provides{Services: []contract.ServiceDescriptor{{Name: "reload.service", Methods: []string{"Run"}}}}, Limits: contract.Limits{Timeout: time.Second}}
}

// reloadEndpointPlugin 在 Start 注册固定端点，并在 Stop 中主动注销，
// 用于验证重载切换后旧实例的清理不会删除新实例接管的同名端点。
type reloadEndpointPlugin struct {
	manifest  contract.Manifest
	endpoint  string
	registrar contract.EndpointRegistrar
	started   atomic.Bool
}

func (p *reloadEndpointPlugin) Manifest() contract.Manifest { return p.manifest }
func (p *reloadEndpointPlugin) Start(_ context.Context, host contract.Host) error {
	registrar, ok := host.(contract.EndpointRegistrar)
	if !ok {
		return errors.New("宿主不支持端点注册")
	}
	p.registrar = registrar
	if err := registrar.RegisterEndpoint(contract.EndpointInfo{ID: p.endpoint, Kind: contract.EndpointApplication, Protocol: "botfabric"}); err != nil {
		return err
	}
	p.started.Store(true)
	return nil
}
func (p *reloadEndpointPlugin) Stop(context.Context) error {
	p.started.Store(false)
	if p.registrar != nil {
		return p.registrar.UnregisterEndpoint(p.endpoint)
	}
	return nil
}
func (*reloadEndpointPlugin) Health(context.Context) contract.HealthStatus {
	return contract.HealthStatus{State: contract.HealthHealthy}
}

// fixedPlanClassifier 按预设计划返回路由结果，便于断言重载后分类器是否生效。
type fixedPlanClassifier struct {
	targets []string
}

func (c *fixedPlanClassifier) Classify(context.Context, contract.Event, []string) (contract.DispatchPlan, error) {
	return contract.DispatchPlan{Mode: contract.DispatchSelective, Targets: append([]string(nil), c.targets...)}, nil
}

// reloadClassifierPlugin 在 Start 注册分类器，清单路由规则引用该分类器。
type reloadClassifierPlugin struct {
	manifest contract.Manifest
	targets  []string
	started  atomic.Bool
}

func (p *reloadClassifierPlugin) Manifest() contract.Manifest { return p.manifest }
func (p *reloadClassifierPlugin) Start(_ context.Context, host contract.Host) error {
	registrar, ok := host.(contract.RouteClassifierRegistrar)
	if !ok {
		return errors.New("宿主不支持分类器注册")
	}
	if err := registrar.RegisterRouteClassifier("reload.classifier", &fixedPlanClassifier{targets: p.targets}); err != nil {
		return err
	}
	p.started.Store(true)
	return nil
}
func (p *reloadClassifierPlugin) Stop(context.Context) error { p.started.Store(false); return nil }
func (*reloadClassifierPlugin) Health(context.Context) contract.HealthStatus {
	return contract.HealthStatus{State: contract.HealthHealthy}
}

func classifierReloadManifest() contract.Manifest {
	return contract.Manifest{
		ID:      "reload.classifier.plugin",
		Name:    "Reload Classifier Plugin",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   "reload.classifier.entry",
		Routing: &contract.RoutingSpec{Rules: []contract.RoutingRule{{
			ID:       "reload-rule",
			Priority: 10,
			Match:    contract.RuleMatch{Topic: "bot.message.group"},
			Action:   contract.RuleAction{Mode: contract.DispatchBroadcast, Classifier: "reload.classifier"},
		}}},
		Limits: contract.Limits{Timeout: time.Second},
	}
}

// TestReloadEndpointPluginReusesStableID 回归：存在固定端点 ID 的插件可重载，
// 新实例不再因 ErrEndpointExists 失败，且旧实例 Stop 不会删除新端点。
func TestReloadEndpointPluginReusesStableID(t *testing.T) {
	manager, _ := newTestManager(t)
	manifest := testManifest("reload.endpoint.plugin", "reload.endpoint.entry")
	const endpointID = "protocol.mockonebot.default"
	old := &reloadEndpointPlugin{manifest: manifest, endpoint: endpointID}
	loader := NewBuiltinLoader()
	if err := loader.RegisterFactory(manifest.Entry, func() contract.Plugin { return old }); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), manifest.ID); err != nil {
		t.Fatal(err)
	}

	newPlugin := &reloadEndpointPlugin{manifest: manifest, endpoint: endpointID}
	loader2 := NewBuiltinLoader()
	if err := loader2.RegisterFactory(manifest.Entry, func() contract.Plugin { return newPlugin }); err != nil {
		t.Fatal(err)
	}
	manager.loaders[contract.RuntimeInProcess] = loader2
	if err := manager.Reload(context.Background(), manifest); err != nil {
		t.Fatalf("带固定端点的插件应可重载: %v", err)
	}
	if !newPlugin.started.Load() {
		t.Fatal("重载后新实例应运行")
	}
	info, exists := manager.Endpoints().Get(endpointID)
	if !exists {
		t.Fatal("重载后端点应存在")
	}
	if info.Owner != manifest.ID {
		t.Fatalf("端点 owner = %q，期望 %q", info.Owner, manifest.ID)
	}
}

// TestReloadClassifierPluginTakesEffect 回归：分类器插件重载成功，新分类器立即生效。
func TestReloadClassifierPluginTakesEffect(t *testing.T) {
	manager, _ := newTestManager(t)
	manifest := classifierReloadManifest()
	old := &reloadClassifierPlugin{manifest: manifest, targets: []string{"old-target"}}
	loader := NewBuiltinLoader()
	if err := loader.RegisterFactory(manifest.Entry, func() contract.Plugin { return old }); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), manifest.ID); err != nil {
		t.Fatal(err)
	}

	newPlugin := &reloadClassifierPlugin{manifest: manifest, targets: []string{"new-target"}}
	loader2 := NewBuiltinLoader()
	if err := loader2.RegisterFactory(manifest.Entry, func() contract.Plugin { return newPlugin }); err != nil {
		t.Fatal(err)
	}
	manager.loaders[contract.RuntimeInProcess] = loader2
	if err := manager.Reload(context.Background(), manifest); err != nil {
		t.Fatalf("分类器插件应可重载: %v", err)
	}
	if !newPlugin.started.Load() {
		t.Fatal("重载后新实例应运行")
	}
	plan := manager.Router().Plan(context.Background(), contract.Event{Topic: "bot.message.group"}, []string{"candidate"})
	if len(plan.Targets) != 1 || plan.Targets[0] != "new-target" {
		t.Fatalf("重载后分类器计划 = %#v，期望 new-target", plan)
	}
}

// TestReloadFailureKeepsPreviousRouting 回归：新实例启动失败时不得替换线上路由规则。
func TestReloadFailureKeepsPreviousRouting(t *testing.T) {
	manager, _ := newTestManager(t)
	manifest := reloadManifest()
	manifest.Routing = &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:       "old-rule",
		Priority: 10,
		Match:    contract.RuleMatch{Topic: "bot.message.group"},
		Action:   contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app.alpha"}},
	}}}
	old := &reloadPlugin{manifest: manifest, version: "old"}
	loader := NewBuiltinLoader()
	if err := loader.RegisterFactory(manifest.Entry, func() contract.Plugin { return old }); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), manifest.ID); err != nil {
		t.Fatal(err)
	}

	newManifest := manifest
	newManifest.Routing = &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:       "new-rule",
		Priority: 10,
		Match:    contract.RuleMatch{Topic: "bot.message.group"},
		Action:   contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app.beta"}},
	}}}
	newPlugin := &reloadPlugin{manifest: newManifest, version: "new", fail: true}
	loader2 := NewBuiltinLoader()
	if err := loader2.RegisterFactory(manifest.Entry, func() contract.Plugin { return newPlugin }); err != nil {
		t.Fatal(err)
	}
	manager.loaders[contract.RuntimeInProcess] = loader2
	if err := manager.Reload(context.Background(), newManifest); err == nil {
		t.Fatal("失败重载应返回错误")
	}
	if !old.started.Load() {
		t.Fatal("重载失败后旧插件应保持运行")
	}
	plan := manager.Router().Plan(context.Background(), contract.Event{Topic: "bot.message.group"}, []string{"candidate"})
	if len(plan.Targets) != 1 || plan.Targets[0] != "app.alpha" {
		t.Fatalf("失败重载后路由计划 = %#v，期望仍为 app.alpha", plan)
	}
}

// blockingStopPlugin 的 Stop 只在上下文截止后返回，用于验证重载失败清理有超时保护。
type blockingStopPlugin struct {
	reloadPlugin
}

func (p *blockingStopPlugin) Stop(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestReloadFailureCleanupHonorsTimeout 回归：暂存实例 Stop 卡住时 Reload 不应永久阻塞。
func TestReloadFailureCleanupHonorsTimeout(t *testing.T) {
	manager, _ := newTestManager(t)
	manager.shutdownTimeout = 50 * time.Millisecond
	manifest := reloadManifest()
	old := &reloadPlugin{manifest: manifest, version: "old"}
	loader := NewBuiltinLoader()
	if err := loader.RegisterFactory(manifest.Entry, func() contract.Plugin { return old }); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), manifest.ID); err != nil {
		t.Fatal(err)
	}

	staged := &blockingStopPlugin{reloadPlugin: reloadPlugin{manifest: manifest, fail: true}}
	loader2 := NewBuiltinLoader()
	if err := loader2.RegisterFactory(manifest.Entry, func() contract.Plugin { return staged }); err != nil {
		t.Fatal(err)
	}
	manager.loaders[contract.RuntimeInProcess] = loader2
	started := time.Now()
	if err := manager.Reload(context.Background(), manifest); err == nil {
		t.Fatal("失败重载应返回错误")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("重载清理耗时 %v，应受 shutdownTimeout 限制", elapsed)
	}
}
