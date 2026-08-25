package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func TestRegistryRejectsDuplicateService(t *testing.T) {
	r := New()
	descriptor := contract.ServiceDescriptor{Name: "example.service", Methods: []string{"Run"}}
	handler := func(context.Context, contract.Request) (contract.Response, error) {
		return contract.Response{}, nil
	}

	if err := r.RegisterService("plugin.a", descriptor, handler); err != nil {
		t.Fatalf("首次注册服务失败: %v", err)
	}
	if err := r.RegisterService("plugin.b", descriptor, handler); !errors.Is(err, ErrServiceExists) {
		t.Fatalf("重复注册返回 %v，期望 %v", err, ErrServiceExists)
	}
}

func TestRegistryClonesPluginManifest(t *testing.T) {
	r := New()
	manifest := contract.Manifest{
		ID: "plugin.a",
		Provides: contract.Provides{
			Subscriptions: []string{"topic.a"},
		},
	}
	if err := r.RegisterPlugin(PluginEntry{Manifest: manifest}); err != nil {
		t.Fatal(err)
	}
	manifest.Provides.Subscriptions[0] = "forged"
	entries := r.Plugins()
	entries[0].Manifest.Provides.Subscriptions[0] = "mutated"
	entry, _ := r.Plugin("plugin.a")
	if entry.Manifest.Provides.Subscriptions[0] != "topic.a" {
		t.Fatalf("插件 Manifest 被外部修改: %#v", entry.Manifest)
	}
}

func TestRegistryClonesServiceDescriptor(t *testing.T) {
	r := New()
	descriptor := contract.ServiceDescriptor{Name: "example.service", Methods: []string{"Run"}}
	if err := r.RegisterService("plugin.a", descriptor, nil); err != nil {
		t.Fatal(err)
	}
	descriptor.Methods[0] = "Forged"
	entry, exists := r.Service("example.service")
	if !exists || entry.Descriptor.Methods[0] != "Run" {
		t.Fatalf("注册表描述符被外部修改: %#v", entry.Descriptor)
	}
	entry.Descriptor.Methods[0] = "Mutated"
	again, _ := r.Service("example.service")
	if again.Descriptor.Methods[0] != "Run" {
		t.Fatalf("读取结果不应修改注册表内部状态: %#v", again.Descriptor)
	}
}

func TestRegistryProtectsServiceOwner(t *testing.T) {
	r := New()
	descriptor := contract.ServiceDescriptor{Name: "example.service", Methods: []string{"Run"}}
	if err := r.RegisterService("plugin.a", descriptor, nil); err != nil {
		t.Fatal(err)
	}

	if err := r.UnregisterService("plugin.b", descriptor.Name); err == nil {
		t.Fatal("其他插件不应能取消服务")
	}
	if _, exists := r.Service(descriptor.Name); !exists {
		t.Fatal("所有者校验失败后服务不应被删除")
	}
}
