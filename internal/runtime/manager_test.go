package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/gateway"
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
