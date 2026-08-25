package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func TestHostEnforcesManifestCapabilities(t *testing.T) {
	manager, _ := newTestManager(t)
	manifest := testManifest("scoped", "scoped.entry")
	manifest.Provides.Subscriptions = []string{"allowed.topic"}
	manifest.Provides.Services = []contract.ServiceDescriptor{{Name: "allowed.service", Methods: []string{"Run"}}}
	manifest.Requires.Services = []string{"dependency.service"}
	plugin := &trackingPlugin{manifest: manifest}
	plugin.start = func(_ context.Context, host contract.Host) error {
		if _, err := host.EventBus().Subscribe("forbidden.topic", func(context.Context, contract.Event) error { return nil }); err == nil {
			return errors.New("越权订阅未被拒绝")
		}
		if err := host.Gateway().Register(contract.ServiceDescriptor{Name: "forbidden.service", Methods: []string{"Run"}}, func(context.Context, contract.Request) (contract.Response, error) { return contract.Response{}, nil }); err == nil {
			return errors.New("越权提供服务未被拒绝")
		}
		return errors.New("测试插件不应成功启动")
	}
	loader := NewBuiltinLoader()
	_ = loader.RegisterFactory(manifest.Entry, func() contract.Plugin { return plugin })
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), manifest.ID); err == nil {
		t.Fatal("测试插件应返回启动失败")
	}
}

func TestStopWaitsForInFlightEventHandler(t *testing.T) {
	manager, bus := newTestManagerWithRegistry(t, registry.New())
	manifest := testManifest("subscriber", "subscriber.entry")
	manifest.Provides.Subscriptions = []string{"topic"}
	entered := make(chan struct{})
	release := make(chan struct{})
	plugin := &trackingPlugin{manifest: manifest}
	plugin.start = func(_ context.Context, host contract.Host) error {
		_, err := host.EventBus().Subscribe("topic", func(context.Context, contract.Event) error {
			close(entered)
			<-release
			return nil
		})
		return err
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

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{Topic: "topic"})
	}()
	<-entered

	manager.shutdownTimeout = time.Second
	stopDone := make(chan error, 1)
	go func() { stopDone <- manager.Stop(context.Background(), manifest.ID) }()
	select {
	case err := <-stopDone:
		t.Fatalf("处理器未释放前 Stop 不应完成: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-publishDone; err != nil {
		t.Fatal(err)
	}
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
}
