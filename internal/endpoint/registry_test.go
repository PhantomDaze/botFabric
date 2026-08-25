package endpoint

import (
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func TestRegisterAndUnregister(t *testing.T) {
	reg := New()
	info := contract.EndpointInfo{ID: "ep-1", Kind: contract.EndpointProtocol, BotID: "10001"}
	if err := reg.Register(info); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(info); err != ErrEndpointExists {
		t.Fatalf("重复注册应报错，got %v", err)
	}
	if err := reg.Register(contract.EndpointInfo{ID: ""}); err == nil {
		t.Fatal("空 ID 应拒绝")
	}

	got, ok := reg.Get("ep-1")
	if !ok || got.BotID != "10001" {
		t.Fatalf("Get 返回错误: %#v", got)
	}
	if err := reg.Unregister("ep-1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("ep-1"); ok {
		t.Fatal("注销后不应存在")
	}
	if err := reg.Unregister("ep-1"); err != ErrEndpointNotFound {
		t.Fatalf("重复注销应报错，got %v", err)
	}
}

func TestByBotIDPrefersProtocol(t *testing.T) {
	reg := New()
	_ = reg.Register(contract.EndpointInfo{ID: "app-1", Kind: contract.EndpointApplication, BotID: "10001"})
	_ = reg.Register(contract.EndpointInfo{ID: "proto-1", Kind: contract.EndpointProtocol, BotID: "10001"})
	endpoints := reg.ByBotID("10001")
	if len(endpoints) != 2 {
		t.Fatalf("ByBotID 数量 = %d，期望 2", len(endpoints))
	}
	proto := reg.ByKind(contract.EndpointProtocol)
	if len(proto) != 1 || proto[0].ID != "proto-1" {
		t.Fatalf("ByKind(protocol) 错误: %#v", proto)
	}
}

func TestCloneIsolation(t *testing.T) {
	reg := New()
	info := contract.EndpointInfo{ID: "ep-1", Capabilities: []string{"send"}}
	_ = reg.Register(info)
	got, _ := reg.Get("ep-1")
	got.Capabilities[0] = "tampered"
	again, _ := reg.Get("ep-1")
	if again.Capabilities[0] != "send" {
		t.Fatal("外部修改不应影响注册表内部副本")
	}
}
