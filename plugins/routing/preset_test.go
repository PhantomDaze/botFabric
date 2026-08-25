package routing_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/gateway"
	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/internal/runtime"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/PhantomDaze/botfabric/plugins/application/reply"
	"github.com/PhantomDaze/botfabric/plugins/protocol/mockonebot"
	routingplugin "github.com/PhantomDaze/botfabric/plugins/routing"
)

type allowPolicy struct{}

func (allowPolicy) AllowPublish(string, string) bool        { return true }
func (allowPolicy) AllowSubscribe(string, string) bool      { return true }
func (allowPolicy) AllowInvoke(string, string, string) bool { return true }
func (allowPolicy) AllowProvide(string, string) bool        { return true }

func TestRegexPresetSelectsMatchingApplication(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := registry.New()
	bus, err := eventbus.New(time.Second, allowPolicy{}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := gateway.New(reg, allowPolicy{}, logger, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runtime.NewManager(reg, bus, gateway, logger, "test-bot", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	loader := runtime.NewBuiltinLoader()
	protocol := mockonebot.New()
	application := reply.New()
	preset := routingplugin.RegexManifest()
	if err := loader.RegisterFactory("builtin.reply", func() contract.Plugin { return application }); err != nil {
		t.Fatal(err)
	}
	if err := loader.RegisterFactory("builtin.mockonebot", func() contract.Plugin { return protocol }); err != nil {
		t.Fatal(err)
	}
	if err := loader.RegisterFactory(routingplugin.RegexEntry, func() contract.Plugin { return routingplugin.New(preset) }); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(protocol.Manifest()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(application.Manifest()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(preset); err != nil {
		t.Fatal(err)
	}
	if err := manager.StartAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.StopAll(context.Background()) })

	if err := protocol.EmitGroupMessage(context.Background(), mockonebot.RawGroupMessage{GroupID: "1", UserID: "2", Text: "/help"}); err != nil {
		t.Fatal(err)
	}
	messages := protocol.SentMessages()
	if len(messages) != 1 || messages[0].Text != "收到：/help" {
		t.Fatalf("正则预设未触发回复: %#v", messages)
	}
}

func TestRequiredBotPresetHasForwardDedupAndTarget(t *testing.T) {
	manifest := routingplugin.RequiredBotManifest()
	if manifest.Routing == nil || !manifest.Routing.Dedup.Enabled || manifest.Routing.Dedup.DuplicateMode != contract.DuplicateForward {
		t.Fatalf("指定 Bot 预设去重配置错误: %#v", manifest.Routing)
	}
	if len(manifest.Routing.Rules) != 1 {
		t.Fatalf("指定 Bot 预设规则数量错误: %#v", manifest.Routing)
	}
	policy := manifest.Routing.Rules[0].Action.Response
	if policy.RequiredReplyBot != "bot-b" || policy.Mode != contract.ResponseFirstWins {
		t.Fatalf("指定 Bot 预设响应策略错误: %#v", policy)
	}
}
