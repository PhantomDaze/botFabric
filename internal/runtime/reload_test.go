package runtime

import (
	"context"
	"encoding/json"
	"errors"
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
