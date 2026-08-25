package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type trackingPlugin struct {
	manifest contract.Manifest
	start    func(context.Context, contract.Host) error
	stop     func(context.Context) error
}

func (p *trackingPlugin) Manifest() contract.Manifest { return p.manifest }
func (p *trackingPlugin) Start(ctx context.Context, host contract.Host) error {
	if p.start != nil {
		return p.start(ctx, host)
	}
	return nil
}
func (p *trackingPlugin) Stop(ctx context.Context) error {
	if p.stop != nil {
		return p.stop(ctx)
	}
	return nil
}
func (*trackingPlugin) Health(context.Context) contract.HealthStatus {
	return contract.HealthStatus{State: contract.HealthHealthy}
}

type forceStopPlugin struct {
	*trackingPlugin
	forced atomic.Bool
}

func (p *forceStopPlugin) ForceStop() error {
	p.forced.Store(true)
	return nil
}

func TestStartAllUsesServiceDependencyOrder(t *testing.T) {
	manager, _ := newTestManager(t)
	var mu sync.Mutex
	var order []string
	provider := &trackingPlugin{manifest: testManifest("provider", "provider.entry")}
	provider.manifest.Provides.Services = []contract.ServiceDescriptor{{Name: "required.service", Methods: []string{"Run"}}}
	provider.start = func(_ context.Context, host contract.Host) error {
		if err := host.Gateway().Register(
			contract.ServiceDescriptor{Name: "required.service", Methods: []string{"Run"}},
			func(context.Context, contract.Request) (contract.Response, error) { return contract.Response{}, nil },
		); err != nil {
			return err
		}
		mu.Lock()
		order = append(order, "provider")
		mu.Unlock()
		return nil
	}
	consumer := &trackingPlugin{manifest: testManifest("consumer", "consumer.entry")}
	consumer.manifest.Requires.Services = []string{"required.service"}
	consumer.start = func(context.Context, contract.Host) error {
		mu.Lock()
		order = append(order, "consumer")
		mu.Unlock()
		return nil
	}

	loader := NewBuiltinLoader()
	_ = loader.RegisterFactory("provider.entry", func() contract.Plugin { return provider })
	_ = loader.RegisterFactory("consumer.entry", func() contract.Plugin { return consumer })
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(consumer.manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(provider.manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.StartAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.StopAll(context.Background()) })

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "provider" || order[1] != "consumer" {
		t.Fatalf("启动顺序 = %v，期望 [provider consumer]", order)
	}
}

func TestStartTimeoutRevokesHost(t *testing.T) {
	manager, reg := newTestManager(t)
	manifest := testManifest("slow", "slow.entry")
	manifest.Limits.Timeout = 20 * time.Millisecond
	manifest.Provides.Services = []contract.ServiceDescriptor{{Name: "late.service", Methods: []string{"Run"}}}
	lateRegistration := make(chan error, 1)
	plugin := &trackingPlugin{manifest: manifest}
	plugin.start = func(ctx context.Context, host contract.Host) error {
		<-ctx.Done()
		time.Sleep(10 * time.Millisecond)
		lateRegistration <- host.Gateway().Register(
			contract.ServiceDescriptor{Name: "late.service", Methods: []string{"Run"}},
			func(context.Context, contract.Request) (contract.Response, error) { return contract.Response{}, nil },
		)
		return ctx.Err()
	}
	loader := NewBuiltinLoader()
	_ = loader.RegisterFactory(manifest.Entry, func() contract.Plugin { return plugin })
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := manager.Start(context.Background(), manifest.ID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("启动错误 = %v，期望 deadline exceeded", err)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("Start 超时预算未生效")
	}
	select {
	case err := <-lateRegistration:
		var serviceError *contract.ServiceError
		if !errors.As(err, &serviceError) || serviceError.Code != contract.ErrorUnavailable {
			t.Fatalf("撤销后的注册错误 = %v，期望 unavailable", err)
		}
	case <-time.After(time.Second):
		t.Fatal("超时插件未完成晚到注册尝试")
	}
	if _, exists := reg.Service("late.service"); exists {
		t.Fatal("Start 超时后不应留下服务注册")
	}
}

func TestStopTimeoutForcesExternalPlugin(t *testing.T) {
	manager, _ := newTestManager(t)
	manager.shutdownTimeout = 20 * time.Millisecond
	manifest := testManifest("force", "force.entry")
	plugin := &forceStopPlugin{trackingPlugin: &trackingPlugin{manifest: manifest}}
	plugin.stop = func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		return ctx.Err()
	}
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
	if err := manager.Stop(context.Background(), manifest.ID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("停止错误 = %v，期望 deadline exceeded", err)
	}
	if !plugin.forced.Load() {
		t.Fatal("Stop 超时后应调用 ForceStop")
	}
	if state, _ := manager.State(manifest.ID); state != contract.PluginFailed {
		t.Fatalf("超时停止后的状态 = %s，期望 failed", state)
	}
}

func testManifest(id, entry string) contract.Manifest {
	return contract.Manifest{
		ID:      id,
		Name:    id,
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   entry,
		Limits:  contract.Limits{Timeout: time.Second},
	}
}
