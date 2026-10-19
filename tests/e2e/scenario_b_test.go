package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/dedup"
	"github.com/PhantomDaze/botfabric/internal/response"
	"github.com/PhantomDaze/botfabric/internal/store"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/alicebob/miniredis/v2"
)

// newSimRedis 启动 miniredis 并装配共享去重与响应存储。
// miniredis 是纯 Go 嵌入式 Redis：SET NX EX 等命令足以覆盖两个子系统。
func newSimRedis(t *testing.T) (dedup.Store, response.CandidateStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	client, err := store.New(context.Background(), store.Config{
		Address:   mr.Addr(),
		KeyPrefix: "sim:",
	})
	if err != nil {
		t.Fatalf("连接 miniredis: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	dedupStore, err := dedup.NewRedisStore(client)
	if err != nil {
		t.Fatalf("创建 Redis 去重存储: %v", err)
	}
	respStore, err := response.NewRedisStore(client, 0)
	if err != nil {
		t.Fatalf("创建 Redis 响应存储: %v", err)
	}
	return dedupStore, respStore
}

// dedupSpec 构造启用去重的路由配置。
func dedupSpec(strategy contract.DedupStrategy, mode contract.DuplicateMode, window time.Duration) *contract.RoutingSpec {
	return &contract.RoutingSpec{
		Dedup: contract.DedupSpec{
			Enabled:       true,
			Strategy:      strategy,
			Window:        window,
			DuplicateMode: mode,
		},
	}
}

// B1：message_id 策略下，同 ID 重复事件被丢弃，只投递首条。
func TestScenarioDedupMessageIDDrop(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{
		ID: "sim.protocol",
		Endpoints: []simEndpoint{
			{ID: "ep-a", BotID: "bot-a", Protocol: "onebot", Version: "v11"},
			{ID: "ep-b", BotID: "bot-b", Protocol: "onebot", Version: "v11"},
		},
	})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	h.addRouting("sim.routing.dedup", dedupSpec(contract.DedupMessageID, contract.DuplicateDrop, 5*time.Second))
	h.start(context.Background())

	ctx := context.Background()
	raw := SimMessage{MessageID: "dup-1", GroupID: "100", UserID: "7", Text: "same message"}
	// 两个协议端收到同一条消息：第二个事件应被去重丢弃。
	if err := proto.Emit(ctx, "ep-a", raw); err != nil {
		t.Fatalf("首协议端发布: %v", err)
	}
	if err := proto.Emit(ctx, "ep-b", raw); err != nil {
		t.Fatalf("重复事件发布不应报错: %v", err)
	}

	if alpha.ReceivedCount() != 1 {
		t.Fatalf("应用端收到事件数 = %d，期望 1（重复事件被 drop）", alpha.ReceivedCount())
	}
	if first := alpha.Received()[0]; first.EndpointID != "ep-a" {
		t.Fatalf("主事件应来自首个协议端，实际 %q", first.EndpointID)
	}
}

// B2：fingerprint 策略按群号+用户+归一化文本识别重复，message_id 不同也拦截。
func TestScenarioDedupFingerprint(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	h.addRouting("sim.routing.dedup", dedupSpec(contract.DedupFingerprint, contract.DuplicateDrop, 5*time.Second))
	h.start(context.Background())

	ctx := context.Background()
	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hello  world"}); err != nil {
		t.Fatalf("发布首条: %v", err)
	}
	// message_id 不同，但群号/用户/文本指纹一致（文本归一化空白）。
	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-2", GroupID: "100", UserID: "7", Text: "hello world"}); err != nil {
		t.Fatalf("发布指纹重复: %v", err)
	}
	// 不同用户不构成重复。
	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-3", GroupID: "100", UserID: "8", Text: "hello world"}); err != nil {
		t.Fatalf("发布他人消息: %v", err)
	}

	if alpha.ReceivedCount() != 2 {
		t.Fatalf("应用端收到事件数 = %d，期望 2（指纹重复被 drop）", alpha.ReceivedCount())
	}
}

// B3：auto 策略在有 message_id 时按 ID 去重。
func TestScenarioDedupAutoUsesMessageID(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	h.addRouting("sim.routing.dedup", dedupSpec(contract.DedupAuto, contract.DuplicateDrop, 5*time.Second))
	h.start(context.Background())

	ctx := context.Background()
	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hello"}); err != nil {
		t.Fatalf("发布首条: %v", err)
	}
	if err := proto.Emit(ctx, "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hello"}); err != nil {
		t.Fatalf("发布重复 ID: %v", err)
	}

	if alpha.ReceivedCount() != 1 {
		t.Fatalf("应用端收到事件数 = %d，期望 1", alpha.ReceivedCount())
	}
}

// B4：forward 模式重复事件仍投递，但带 botfabric_duplicate 标记头。
func TestScenarioDedupForwardMarksDuplicate(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	h.addRouting("sim.routing.dedup", dedupSpec(contract.DedupMessageID, contract.DuplicateForward, 5*time.Second))
	h.start(context.Background())

	ctx := context.Background()
	raw := SimMessage{MessageID: "dup-1", GroupID: "100", UserID: "7", Text: "same"}
	if err := proto.Emit(ctx, "", raw); err != nil {
		t.Fatalf("发布首条: %v", err)
	}
	if err := proto.Emit(ctx, "", raw); err != nil {
		t.Fatalf("发布重复: %v", err)
	}

	if alpha.ReceivedCount() != 2 {
		t.Fatalf("forward 模式应用端收到事件数 = %d，期望 2", alpha.ReceivedCount())
	}
	first, second := alpha.Received()[0], alpha.Received()[1]
	if first.Headers["botfabric_duplicate"] != "" {
		t.Fatalf("首条事件不应带重复标记: %v", first.Headers)
	}
	if second.Headers["botfabric_duplicate"] != "true" {
		t.Fatalf("重复事件应带 botfabric_duplicate=true，实际 %v", second.Headers)
	}
	if second.Headers["botfabric_dedup_key"] == "" {
		t.Fatal("重复事件应携带去重键")
	}
}

// B5：去重窗口过后，相同消息重新获得主事件资格。
func TestScenarioDedupWindowExpiry(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	h.addRouting("sim.routing.dedup", dedupSpec(contract.DedupMessageID, contract.DuplicateDrop, 30*time.Millisecond))
	h.start(context.Background())

	ctx := context.Background()
	raw := SimMessage{MessageID: "dup-1", GroupID: "100", UserID: "7", Text: "same"}
	if err := proto.Emit(ctx, "", raw); err != nil {
		t.Fatalf("发布首条: %v", err)
	}
	if err := proto.Emit(ctx, "", raw); err != nil {
		t.Fatalf("窗口内重复: %v", err)
	}
	if alpha.ReceivedCount() != 1 {
		t.Fatalf("窗口内收到事件数 = %d，期望 1", alpha.ReceivedCount())
	}
	time.Sleep(60 * time.Millisecond)
	if err := proto.Emit(ctx, "", raw); err != nil {
		t.Fatalf("窗口后发布: %v", err)
	}
	if alpha.ReceivedCount() != 2 {
		t.Fatalf("窗口过期后收到事件数 = %d，期望 2", alpha.ReceivedCount())
	}
}

// B6：miniredis 共享去重存储——两个 Runtime 实例（视为两个中间层进程）共享同一存储，
// 第二个实例上的重复事件同样被拦截。
func TestScenarioDedupRedisCrossInstance(t *testing.T) {
	dedupStore, _ := newSimRedis(t)

	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	h.addRouting("sim.routing.dedup", dedupSpec(contract.DedupMessageID, contract.DuplicateDrop, 30*time.Second))
	h.withStore(dedupStore).start(context.Background())

	// 第二个实例：不同 Bot 运行时，但共享 Redis 去重存储与端点注册表。
	h2 := newSim(t)
	h2.withBotID("sim-bot-2")
	h2.withStore(dedupStore)
	proto2 := h2.addProtocol(simProtocolConfig{
		ID:        "sim.protocol2",
		Endpoints: []simEndpoint{{ID: "ep-2", BotID: "bot-2", Protocol: "onebot", Version: "v11"}},
	})
	beta := h2.addApp(simAppConfig{ID: "app.beta"})
	h2.addRouting("sim.routing.dedup2", dedupSpec(contract.DedupMessageID, contract.DuplicateDrop, 30*time.Second))
	h2.start(context.Background())

	raw := SimMessage{MessageID: "shared-1", GroupID: "100", UserID: "7", Text: "one"}

	// 实例一先发布：主事件。
	if err := proto.Emit(context.Background(), "", raw); err != nil {
		t.Fatalf("实例一发布: %v", err)
	}
	// 实例二发布同一条消息：跨实例重复，应被 drop。
	if err := proto2.Emit(context.Background(), "", raw); err != nil {
		t.Fatalf("实例二发布重复: %v", err)
	}

	if alpha.ReceivedCount() != 1 {
		t.Fatalf("实例一应用端收到 = %d，期望 1", alpha.ReceivedCount())
	}
	if beta.ReceivedCount() != 0 {
		t.Fatalf("实例二应用端收到 = %d，期望 0（跨实例重复被 drop）", beta.ReceivedCount())
	}
}
