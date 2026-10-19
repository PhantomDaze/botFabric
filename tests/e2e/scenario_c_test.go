package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// arbRouting 构造带响应策略的路由配置。
func arbRouting(policy contract.ResponsePolicy) *contract.RoutingSpec {
	return &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:       "arbitration",
		Priority: 100,
		Match:    contract.RuleMatch{Topic: groupTopic},
		Action:   contract.RuleAction{Mode: contract.DispatchBroadcast, Response: policy},
	}}}
}

// textReply 返回一个把固定文本作为候选回复的应用端行为。
func textReply(text string) func(contract.Event, contract.GroupMessage) *contract.CandidateReply {
	return func(contract.Event, contract.GroupMessage) *contract.CandidateReply {
		return &contract.CandidateReply{Text: text}
	}
}

// sentTexts 汇总模拟协议端已发出的文本。
func sentTexts(sent []SimSent) []string {
	texts := make([]string, 0, len(sent))
	for _, item := range sent {
		texts = append(texts, item.Text)
	}
	return texts
}

// C1：first-wins 模式下先到的有效回复立即发出，后到的被丢弃。
// EventBus.Publish 同步等待全部处理器返回，因此 Emit 返回时仲裁结果已确定。
func TestScenarioFirstWinsPicksFirstValidReply(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	slow := h.addApp(simAppConfig{ID: "app.slow", Delay: 40 * time.Millisecond, Reply: textReply("slow")})
	fast := h.addApp(simAppConfig{ID: "app.fast", Delay: 5 * time.Millisecond, Reply: textReply("fast")})
	h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second}))
	h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}

	if slow.SubmittedCount() != 1 || fast.SubmittedCount() != 1 {
		t.Fatalf("两个应用端都应提交候选：slow=%d fast=%d", slow.SubmittedCount(), fast.SubmittedCount())
	}
	sent := proto.Sent()
	if len(sent) != 1 {
		t.Fatalf("发送动作数量 = %d，期望 1；文本 = %v", len(sent), sentTexts(sent))
	}
	if sent[0].Text != "fast" {
		t.Fatalf("胜出回复 = %q，期望 fast", sent[0].Text)
	}
	if sent[0].Caller != "app.fast" {
		t.Fatalf("发出者 = %q，期望 app.fast", sent[0].Caller)
	}
	if sent[0].EventID == "" || sent[0].TraceID == "" {
		t.Fatal("发出的动作应关联原事件与链路 ID")
	}
}

// C2：priority_override 在窗口内收集候选，超时后取最高优先级发出。
// flush 由 time.AfterFunc 触发，需等待异步结果。
func TestScenarioPriorityOverrideChoosesHighestPriority(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	low := h.addApp(simAppConfig{ID: "app.low", Reply: func(contract.Event, contract.GroupMessage) *contract.CandidateReply {
		return &contract.CandidateReply{Text: "low", Priority: 1}
	}})
	high := h.addApp(simAppConfig{ID: "app.high", Reply: func(contract.Event, contract.GroupMessage) *contract.CandidateReply {
		return &contract.CandidateReply{Text: "high", Priority: 10}
	}})
	h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{Mode: contract.ResponsePriorityOverride, Timeout: 20 * time.Millisecond}))
	env := h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}

	env.waitForCount(2*time.Second, "priority_override 发送最高优先级回复", proto.SentCount, 1)
	sent := proto.Sent()
	if sent[0].Text != "high" {
		t.Fatalf("选中的回复 = %q，期望 high", sent[0].Text)
	}
	if low.SubmittedCount() != 1 || high.SubmittedCount() != 1 {
		t.Fatalf("两个候选都应被收集：low=%d high=%d", low.SubmittedCount(), high.SubmittedCount())
	}
}

// C3：同一事件上以相同幂等键重复提交，只发送一次。
func TestScenarioIdempotencyKeyDoesNotSendTwice(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	h.addApp(simAppConfig{
		ID:                "app.dup",
		RepeatSubmitCount: 3,
		Reply:             textReplyWithKey("once", "stable-key"),
	})
	h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second}))
	h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}

	if sent := proto.Sent(); len(sent) != 1 {
		t.Fatalf("幂等重复提交应只发送一次，实际 %d 次：%v", len(sent), sentTexts(sent))
	}
}

// textReplyWithKey 返回带固定幂等键的候选回复行为。
func textReplyWithKey(text, key string) func(contract.Event, contract.GroupMessage) *contract.CandidateReply {
	return func(contract.Event, contract.GroupMessage) *contract.CandidateReply {
		return &contract.CandidateReply{Text: text, IdempotencyKey: key}
	}
}

// C4：required_reply_bot 指定目标协议端点；目标 Bot 不存在时回退到 fallback bots。
func TestScenarioRequiredReplyBotTargetsProtocolEndpoint(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{
		ID: "sim.protocol",
		Endpoints: []simEndpoint{
			{ID: "ep-a", BotID: "bot-a", Protocol: "onebot", Version: "v11"},
			{ID: "ep-b", BotID: "bot-b", Protocol: "onebot", Version: "v12"},
		},
	})
	h.addApp(simAppConfig{ID: "app.reply", Reply: textReply("target")})
	h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{
		Mode:             contract.ResponseFirstWins,
		Timeout:          2 * time.Second,
		RequiredReplyBot: "bot-b",
	}))
	h.start(context.Background())

	// 事件来自 ep-a(bot-a)，但回复必须落到 bot-b 的端点。
	if err := proto.Emit(context.Background(), "ep-a", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}

	sent := proto.Sent()
	if len(sent) != 1 {
		t.Fatalf("发送动作数量 = %d，期望 1", len(sent))
	}
	if sent[0].BotID != "bot-b" || sent[0].EndpointID != "ep-b" {
		t.Fatalf("回复端点 = %s/%s，期望 bot-b/ep-b", sent[0].BotID, sent[0].EndpointID)
	}
}

// C4b：required_reply_bot 的 Bot 无协议端点时回退到 fallback bots。
func TestScenarioRequiredReplyBotFallsBackWhenTargetMissing(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{
		ID:        "sim.protocol",
		Endpoints: []simEndpoint{{ID: "ep-a", BotID: "bot-a", Protocol: "onebot", Version: "v11"}},
	})
	h.addApp(simAppConfig{ID: "app.reply", Reply: textReply("fallback")})
	h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{
		Mode:             contract.ResponseFirstWins,
		Timeout:          2 * time.Second,
		RequiredReplyBot: "bot-missing",
		FallbackBots:     []string{"bot-a"},
	}))
	h.start(context.Background())

	if err := proto.Emit(context.Background(), "ep-a", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}

	sent := proto.Sent()
	if len(sent) != 1 {
		t.Fatalf("发送动作数量 = %d，期望 1（回退到 bot-a）", len(sent))
	}
	if sent[0].BotID != "bot-a" || sent[0].EndpointID != "ep-a" {
		t.Fatalf("回退端点 = %s/%s，期望 bot-a/ep-a", sent[0].BotID, sent[0].EndpointID)
	}
}

// C5：没有应用端回复时，仲裁器不发出任何动作。
func TestScenarioNoReplySendsNothing(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	h.addApp(simAppConfig{ID: "app.silent"}) // Reply 为 nil：不回复
	h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 50 * time.Millisecond}))
	env := h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}

	if sent := proto.Sent(); len(sent) != 0 {
		t.Fatalf("无回复不应发送动作，实际 %d 次：%v", len(sent), sentTexts(sent))
	}
	if env.DeadLetterCount() != 0 {
		t.Fatalf("无回复不应产生死信: %#v", env.Dead.Entries())
	}
}

// C6：miniredis 共享响应存储下，两个 Runtime 同时收到同一 eventID，
// first-wins 必须通过 Redis 原子仲裁，只能有一个实例发出动作。
func TestScenarioArbitrationRedisCrossInstance(t *testing.T) {
	dedupStore, respStore := newSimRedis(t)

	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{
		ID:        "sim.protocol",
		Endpoints: []simEndpoint{{ID: "ep-a", BotID: "bot-a", Protocol: "onebot", Version: "v11"}},
	})
	h.addApp(simAppConfig{ID: "app.one", Reply: textReply("from-instance-one")})
	h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second}))
	env := h.withStore(dedupStore).start(context.Background())
	if err := env.Manager.ResponseCoordinator().SetStore(respStore); err != nil {
		t.Fatalf("注入实例一响应存储: %v", err)
	}

	h2 := newSim(t)
	h2.withBotID("sim-bot-2")
	h2.withStore(dedupStore)
	proto2 := h2.addProtocol(simProtocolConfig{
		ID:        "sim.protocol2",
		Endpoints: []simEndpoint{{ID: "ep-b", BotID: "bot-b", Protocol: "onebot", Version: "v11"}},
	})
	h2.addApp(simAppConfig{ID: "app.two", Reply: textReply("from-instance-two")})
	h2.addRouting("sim.routing.arb2", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second}))
	env2 := h2.start(context.Background())
	if err := env2.Manager.ResponseCoordinator().SetStore(respStore); err != nil {
		t.Fatalf("注入实例二响应存储: %v", err)
	}

	const sharedEventID = "shared-event-id"
	results := make(chan error, 2)
	go func() {
		results <- proto.Emit(context.Background(), "ep-a", SimMessage{
			EventID: sharedEventID, MessageID: "m-a", GroupID: "100", UserID: "7", Text: "same event",
		})
	}()
	go func() {
		results <- proto2.Emit(context.Background(), "ep-b", SimMessage{
			EventID: sharedEventID, MessageID: "m-b", GroupID: "100", UserID: "7", Text: "same event",
		})
	}()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("并发发布同一事件: %v", err)
		}
	}

	totalSent := proto.SentCount() + proto2.SentCount()
	if totalSent != 1 {
		t.Fatalf("两个 Runtime 对同一 eventID 的总发送数 = %d，期望 Redis first-wins 只发送 1 次；实例一=%v，实例二=%v",
			totalSent, sentTexts(proto.Sent()), sentTexts(proto2.Sent()))
	}
	winner := append(proto.Sent(), proto2.Sent()...)
	if winner[0].EventID != sharedEventID {
		t.Fatalf("胜出动作关联 eventID = %q，期望 %q", winner[0].EventID, sharedEventID)
	}
}
