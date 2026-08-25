package runtime

import (
	"context"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type endpointPlugin struct {
	manifest contract.Manifest
	endpoint string
}

func (p *endpointPlugin) Manifest() contract.Manifest { return p.manifest }
func (p *endpointPlugin) Start(_ context.Context, host contract.Host) error {
	registrar, ok := host.(contract.EndpointRegistrar)
	if !ok {
		return &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "端点注册能力缺失"}
	}
	return registrar.RegisterEndpoint(contract.EndpointInfo{ID: p.endpoint, Kind: contract.EndpointApplication, Protocol: "botfabric"})
}
func (p *endpointPlugin) Stop(context.Context) error { return nil }
func (p *endpointPlugin) Health(context.Context) contract.HealthStatus {
	return contract.HealthStatus{State: contract.HealthHealthy}
}

func TestPluginEndpointLifecycle(t *testing.T) {
	manager, _ := newTestManager(t)
	manifest := testManifest("endpoint.plugin", "endpoint.entry")
	plugin := &endpointPlugin{manifest: manifest, endpoint: "app-1"}
	loader := NewBuiltinLoader()
	if err := loader.RegisterFactory(manifest.Entry, func() contract.Plugin { return plugin }); err != nil {
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
	if _, ok := manager.Endpoints().Get("app-1"); !ok {
		t.Fatal("启动后端点应注册")
	}
	if err := manager.Stop(context.Background(), manifest.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.Endpoints().Get("app-1"); ok {
		t.Fatal("停止后端点应清理")
	}
}
