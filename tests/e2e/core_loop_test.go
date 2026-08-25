package e2e_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/gateway"
	"github.com/PhantomDaze/botfabric/internal/policy"
	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/internal/runtime"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/PhantomDaze/botfabric/plugins/application/reply"
	"github.com/PhantomDaze/botfabric/plugins/protocol/mockonebot"
)

func TestCoreMessageReplyLoop(t *testing.T) {
	checker, err := policy.Load("../../config/policy.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	reg := registry.New()
	deadLetters := &eventbus.MemoryDeadLetterSink{}
	bus, err := eventbus.New(time.Second, checker, logger, deadLetters)
	if err != nil {
		t.Fatal(err)
	}
	serviceGateway, err := gateway.New(reg, checker, logger, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runtime.NewManager(reg, bus, serviceGateway, logger, "test-bot", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	protocolPlugin := mockonebot.New()
	replyPlugin := reply.New()
	loader := runtime.NewBuiltinLoader()
	if err := loader.RegisterFactory("builtin.mockonebot", func() contract.Plugin { return protocolPlugin }); err != nil {
		t.Fatal(err)
	}
	if err := loader.RegisterFactory("builtin.reply", func() contract.Plugin { return replyPlugin }); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(replyPlugin.Manifest()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Load(protocolPlugin.Manifest()); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := manager.StartAll(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.StopAll(context.Background()); err != nil {
			t.Errorf("停止运行时: %v", err)
		}
	})

	if err := protocolPlugin.EmitGroupMessage(ctx, mockonebot.RawGroupMessage{
		MessageID: "incoming-1",
		GroupID:   "9223372036854775807",
		UserID:    "10001",
		Text:      "你好",
	}); err != nil {
		t.Fatal(err)
	}

	messages := protocolPlugin.SentMessages()
	if len(messages) != 1 {
		t.Fatalf("发送动作数量 = %d，期望 1；死信 = %#v", len(messages), deadLetters.Entries())
	}
	if messages[0].GroupID != "9223372036854775807" {
		t.Fatalf("群号 = %q，期望保持字符串精度", messages[0].GroupID)
	}
	if messages[0].Text != "收到：你好" {
		t.Fatalf("回复文本 = %q", messages[0].Text)
	}
	if messages[0].Caller != reply.PluginID {
		t.Fatalf("动作调用方 = %q，期望 %q", messages[0].Caller, reply.PluginID)
	}
	if messages[0].TraceID == "" {
		t.Fatal("动作链路应携带 TraceID")
	}
	if len(deadLetters.Entries()) != 0 {
		t.Fatalf("成功链路不应产生死信: %#v", deadLetters.Entries())
	}
}
