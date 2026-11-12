package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/gateway"
	"github.com/PhantomDaze/botfabric/internal/observability"
	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type runtimeAllowPolicy struct{}

func (runtimeAllowPolicy) AllowPublish(string, string) bool        { return true }
func (runtimeAllowPolicy) AllowSubscribe(string, string) bool      { return true }
func (runtimeAllowPolicy) AllowInvoke(string, string, string) bool { return true }
func (runtimeAllowPolicy) AllowProvide(string, string) bool        { return true }

type failingPlugin struct {
	manifest contract.Manifest
}

func (p *failingPlugin) Manifest() contract.Manifest { return p.manifest }
func (p *failingPlugin) Start(_ context.Context, host contract.Host) error {
	_, _ = host.EventBus().Subscribe("topic", func(context.Context, contract.Event) error { return nil })
	_ = host.Gateway().Register(contract.ServiceDescriptor{Name: "temporary.service", Methods: []string{"Run"}}, func(context.Context, contract.Request) (contract.Response, error) {
		return contract.Response{}, nil
	})
	return errors.New("启动失败")
}
func (p *failingPlugin) Stop(context.Context) error { return nil }
func (p *failingPlugin) Health(context.Context) contract.HealthStatus {
	return contract.HealthStatus{State: contract.HealthUnhealthy}
}

func TestStartFailureRollsBackServices(t *testing.T) {
	manager, registry := newTestManager(t)
	metrics := observability.NewMetrics()
	manager.SetMetrics(metrics)
	manifest := contract.Manifest{
		ID:       "failing.plugin",
		Name:     "Failing Plugin",
		Version:  "0.1.0",
		Runtime:  contract.RuntimeInProcess,
		Entry:    "failing",
		Provides: contract.Provides{Services: []contract.ServiceDescriptor{{Name: "temporary.service", Methods: []string{"Run"}}}},
		Limits:   contract.Limits{Timeout: time.Second},
	}
	loader := NewBuiltinLoader()
	if err := loader.RegisterFactory("failing", func() contract.Plugin { return &failingPlugin{manifest: manifest} }); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), manifest.ID); err == nil {
		t.Fatal("启动应失败")
	}
	if _, exists := registry.Service("temporary.service"); exists {
		t.Fatal("启动失败后服务应被回滚")
	}
	if state, _ := manager.State(manifest.ID); state != contract.PluginFailed {
		t.Fatalf("状态 = %s，期望 failed", state)
	}
	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "botfabric_plugin_start_failures_total 1") {
		t.Fatalf("启动失败未计入指标:\n%s", output.String())
	}
}

func newTestManager(t *testing.T) (*Manager, *registry.Registry) {
	t.Helper()
	reg := registry.New()
	manager, bus := newTestManagerWithRegistry(t, reg)
	_ = bus
	return manager, reg
}

func newTestManagerWithRegistry(t *testing.T, reg *registry.Registry) (*Manager, *eventbus.Bus) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bus, err := eventbus.New(time.Second, runtimeAllowPolicy{}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := gateway.New(reg, runtimeAllowPolicy{}, logger, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(reg, bus, gw, logger, "test-bot", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return manager, bus
}

// TestNotifyPluginExitRollsBackResources 验证外部插件进程异常退出后，
// Runtime 注销其服务/订阅、移出启动列表并把手握状态置为 failed。
func TestNotifyPluginExitRollsBackResources(t *testing.T) {
	manager, reg := newTestManager(t)
	manifest := testManifest("crashed.plugin", "crashed.entry")
	manifest.Provides = contract.Provides{
		Services:      []contract.ServiceDescriptor{{Name: "crashed.service", Methods: []string{"Run"}}},
		Subscriptions: []string{"crashed.topic"},
	}
	plugin := &trackingPlugin{manifest: manifest}
	plugin.start = func(_ context.Context, host contract.Host) error {
		if err := host.Gateway().Register(
			contract.ServiceDescriptor{Name: "crashed.service", Methods: []string{"Run"}},
			func(context.Context, contract.Request) (contract.Response, error) { return contract.Response{}, nil },
		); err != nil {
			return err
		}
		_, err := host.EventBus().Subscribe("crashed.topic", func(context.Context, contract.Event) error { return nil })
		return err
	}
	loader := NewBuiltinLoader()
	if err := loader.RegisterFactory("crashed.entry", func() contract.Plugin { return plugin }); err != nil {
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
	if _, exists := reg.Service("crashed.service"); !exists {
		t.Fatal("启动后服务应已注册")
	}
	if subs := manager.eventBus.SnapshotOwner(manifest.ID); len(subs) != 1 {
		t.Fatalf("启动后订阅数量 = %d，期望 1", len(subs))
	}

	manager.NotifyPluginExit(manifest.ID, errors.New("进程崩溃"))

	if state, _ := manager.State(manifest.ID); state != contract.PluginFailed {
		t.Fatalf("插件状态 = %s，期望 failed", state)
	}
	if _, exists := reg.Service("crashed.service"); exists {
		t.Fatal("崩溃后服务应被注销")
	}
	if subs := manager.eventBus.SnapshotOwner(manifest.ID); len(subs) != 0 {
		t.Fatalf("崩溃后订阅数量 = %d，期望 0", len(subs))
	}
	manager.mu.Lock()
	started := len(manager.started)
	manager.mu.Unlock()
	if started != 0 {
		t.Fatalf("崩溃后已启动列表 = %d，期望 0", started)
	}

	// 重复通知应幂等，不改变已迁移的 failed 状态。
	manager.NotifyPluginExit(manifest.ID, errors.New("重复通知"))
	if state, _ := manager.State(manifest.ID); state != contract.PluginFailed {
		t.Fatalf("重复通知后插件状态 = %s，期望 failed", state)
	}
	if err := manager.Unload(context.Background(), manifest.ID); err != nil {
		t.Fatal(err)
	}
}
