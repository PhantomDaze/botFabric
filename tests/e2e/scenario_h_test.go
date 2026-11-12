package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/onebotapp"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// H 组：以真实 OneBot WebSocket 仿真协议端/应用端覆盖多连接与多模式组合。
// 与 A-G 组使用进程内模拟插件不同，本组所有事件、动作、候选回复都经过真实
// WebSocket 编解码与 Echo 关联，验证跨连接、跨版本、跨模式时的可观察行为。

// simReplyOnGroup 返回只在指定群回复的仿真应用端行为；groupID 为空表示所有群。
func simReplyOnGroup(text string, delay time.Duration, groupID string) simAppBehavior {
	return func(event simAppEvent) *simAppReply {
		if groupID != "" && event.GroupID != groupID {
			return nil
		}
		return &simAppReply{Text: text, Delay: delay}
	}
}

// H1：两个真实协议连接（v11/bot-a、v12/bot-b）× 两个真实应用连接（v11/v12）
// 组成 N:N；广播后两个应用端都收到事件，first-wins 只允许一个动作回到事件来源连接。
func TestScenarioRealOneBotMultiEndpointFirstWins(t *testing.T) {
	h := newSim(t).withAllowAllPolicy()
	protocol := h.addOneBotProtocol()
	h.addRouting("sim.routing.h1", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second}))
	env := h.start(context.Background())

	adapter := startSimOneBotApplication(t, env, "server", nil)
	alpha := dialSimAppClient(t, adapter.Addr(), "/app/v11", "app:app.alpha", onebotapp.VersionV11, "app:app.alpha", simReplyOnGroup("alpha", 5*time.Millisecond, "100"))
	beta := dialSimAppClient(t, adapter.Addr(), "/app/v12", "app:app.beta", onebotapp.VersionV12, "app:app.beta", simReplyOnGroup("beta", 40*time.Millisecond, "100"))
	waitForSimCandidates(t, adapter, "app:app.alpha", "app:app.beta")

	a := dialSimQQClient(t, protocol.Addr(), "/onebot/v11", "bot-a", onebotapp.VersionV11)
	b := dialSimQQClient(t, protocol.Addr(), "/onebot/v12", "bot-b", onebotapp.VersionV12)

	// 先由 bot-b 推送一条预热消息：OneBot 连接只有收到事件后才声明 BotID/协议端点。
	if err := b.SendGroupMessage("m-b-warm", "200", "8", "warmup"); err != nil {
		t.Fatalf("bot-b 推送预热事件: %v", err)
	}
	waitForSimBotEndpoint(env, "bot-b")
	alpha.WaitEvents(1, 2*time.Second)
	beta.WaitEvents(1, 2*time.Second)

	if err := a.SendGroupMessage("m-a", "100", "7", "hello"); err != nil {
		t.Fatalf("bot-a 推送群消息: %v", err)
	}
	waitForSimBotEndpoint(env, "bot-a")
	alpha.WaitEvents(2, 2*time.Second)
	beta.WaitEvents(2, 2*time.Second)

	a.WaitActions(1, 2*time.Second)
	beta.WaitAcks(1, 2*time.Second)
	if got := a.ActionCount(); got != 1 {
		t.Fatalf("事件来源连接收到动作 = %d，期望 1：%#v", got, a.Actions())
	}
	if got := b.ActionCount(); got != 0 {
		t.Fatalf("非来源连接收到动作 = %d，期望 0：%#v", got, b.Actions())
	}
	action := a.Actions()[0]
	if action.Action != "send_group_msg" || action.Text() != "alpha" || action.GroupID() != "100" {
		t.Fatalf("v11 动作 = %#v，期望 send_group_msg/alpha/100", action)
	}
	if action.Echo == "" {
		t.Fatal("协议动作应携带 echo")
	}
	if env.DeadLetterCount() != 0 {
		t.Fatalf("完整闭环不应产生死信: %#v", env.Dead.Entries())
	}
}

// H2：required_reply_bot 在真实双协议连接间定向：事件来自 bot-a，
// 回复必须编码为 v12 动作并由 bot-b 连接接收。
func TestScenarioRealOneBotRequiredBotRoutesToOtherConnection(t *testing.T) {
	h := newSim(t).withAllowAllPolicy()
	protocol := h.addOneBotProtocol()
	h.addRouting("sim.routing.h2", arbRouting(contract.ResponsePolicy{
		Mode:             contract.ResponseFirstWins,
		Timeout:          2 * time.Second,
		RequiredReplyBot: "bot-b",
	}))
	env := h.start(context.Background())

	adapter := startSimOneBotApplication(t, env, "server", nil)
	alpha := dialSimAppClient(t, adapter.Addr(), "/app/v11", "app:app.alpha", onebotapp.VersionV11, "app:app.alpha", simReplyOnGroup("to-b", 0, "100"))
	waitForSimCandidates(t, adapter, "app:app.alpha")

	a := dialSimQQClient(t, protocol.Addr(), "/onebot/v11", "bot-a", onebotapp.VersionV11)
	b := dialSimQQClient(t, protocol.Addr(), "/onebot/v12", "bot-b", onebotapp.VersionV12)
	if err := b.SendGroupMessage("m-b-warm", "200", "8", "warmup"); err != nil {
		t.Fatalf("bot-b 推送预热事件: %v", err)
	}
	waitForSimBotEndpoint(env, "bot-b")
	alpha.WaitEvents(1, 2*time.Second)

	if err := a.SendGroupMessage("m-a", "100", "7", "target other bot"); err != nil {
		t.Fatalf("bot-a 推送群消息: %v", err)
	}
	alpha.WaitEvents(2, 2*time.Second)
	b.WaitActions(1, 2*time.Second)

	if got := a.ActionCount(); got != 0 {
		t.Fatalf("事件来源连接收到动作 = %d，期望 0（应定向 bot-b）：%#v", got, a.Actions())
	}
	action := b.Actions()[0]
	if action.Action != "send_message" || action.Text() != "to-b" || action.GroupID() != "100" {
		t.Fatalf("v12 定向动作 = %#v，期望 send_message/to-b/100", action)
	}
	if action.Params["detail_type"] != "group" {
		t.Fatalf("v12 定向动作 detail_type = %#v，期望 group", action.Params["detail_type"])
	}
	if env.DeadLetterCount() != 0 {
		t.Fatalf("跨连接定向不应产生死信: %#v", env.Dead.Entries())
	}
}

// H3：selective 规则按应用端稳定身份选择目标；被选中的应用端回复，
// 另一条真实应用连接既收不到事件，也不会进入响应竞争。
func TestScenarioRealOneBotSelectiveUsesStableApplicationIdentity(t *testing.T) {
	h := newSim(t).withAllowAllPolicy()
	protocol := h.addOneBotProtocol()
	h.addRouting("sim.routing.h3", &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:       "only-alpha",
		Priority: 100,
		Match:    contract.RuleMatch{Topic: groupTopic},
		Action: contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app:app.alpha"}, Response: contract.ResponsePolicy{
			Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second,
		}},
	}}})
	env := h.start(context.Background())

	adapter := startSimOneBotApplication(t, env, "server", nil)
	alpha := dialSimAppClient(t, adapter.Addr(), "/app/v11", "app:app.alpha", onebotapp.VersionV11, "app:app.alpha", simReplyOnGroup("only-alpha", 0, "100"))
	beta := dialSimAppClient(t, adapter.Addr(), "/app/v12", "app:app.beta", onebotapp.VersionV12, "app:app.beta", simReplyOnGroup("must-not-reply", 0, "100"))
	waitForSimCandidates(t, adapter, "app:app.alpha", "app:app.beta")
	waitForSimEndpoint(env, "app:app.alpha")
	waitForSimEndpoint(env, "app:app.beta")

	qq := dialSimQQClient(t, protocol.Addr(), "/onebot/v11", "bot-a", onebotapp.VersionV11)
	if err := qq.SendGroupMessage("m-a", "100", "7", "selective hello"); err != nil {
		t.Fatalf("推送群消息: %v", err)
	}
	alpha.WaitEvents(1, 2*time.Second)
	qq.WaitActions(1, 2*time.Second)

	if got := beta.EventCount(); got != 0 {
		t.Fatalf("非目标应用端收到事件 = %d，期望 0：%#v", got, beta.Events())
	}
	if action := qq.Actions()[0]; action.Text() != "only-alpha" {
		t.Fatalf("selective 回复 = %q，期望 only-alpha", action.Text())
	}
	if env.DeadLetterCount() != 0 {
		t.Fatalf("selective 路由不应产生死信: %#v", env.Dead.Entries())
	}
}

// H4：应用端 both 模式同时接受本地应用连入并主动连接远端应用；
// 两条应用路径都收到事件并参与 first-wins，最终只有一个动作发出。
func TestScenarioRealOneBotApplicationBothModeCompetes(t *testing.T) {
	h := newSim(t).withAllowAllPolicy()
	protocol := h.addOneBotProtocol()
	h.addRouting("sim.routing.h4", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second}))
	env := h.start(context.Background())

	remote := newSimRemoteApp(t, onebotapp.VersionV11, simReplyOnGroup("remote", 40*time.Millisecond, "100"))
	adapter := startSimOneBotApplication(t, env, "both", remote)
	waitForSimCandidateCount(t, adapter, 1)

	local := dialSimAppClient(t, adapter.Addr(), "/app/v11", "app:app.local", onebotapp.VersionV11, "app:app.local", simReplyOnGroup("local", 5*time.Millisecond, "100"))
	waitForSimCandidates(t, adapter, "app:app.local")
	waitForSimCandidateCount(t, adapter, 2)

	qq := dialSimQQClient(t, protocol.Addr(), "/onebot/v11", "bot-a", onebotapp.VersionV11)
	if err := qq.SendGroupMessage("m-a", "100", "7", "both mode hello"); err != nil {
		t.Fatalf("推送群消息: %v", err)
	}
	local.WaitEvents(1, 2*time.Second)
	remote.WaitEvents(1, 2*time.Second)

	qq.WaitActions(1, 2*time.Second)
	remote.WaitAcks(1, 2*time.Second)
	if action := qq.Actions()[0]; action.Text() != "local" {
		t.Fatalf("both 模式胜出回复 = %q，期望 local", action.Text())
	}
	if got := qq.ActionCount(); got != 1 {
		t.Fatalf("both 模式动作数 = %d，期望 1：%#v", got, qq.Actions())
	}
	if env.DeadLetterCount() != 0 {
		t.Fatalf("both 模式闭环不应产生死信: %#v", env.Dead.Entries())
	}
}

// H5：去重策略作用于真实双协议连接；drop 只投递首条，forward 仍投递并继续后续事件。
func TestScenarioRealOneBotDedupAcrossProtocolConnections(t *testing.T) {
	cases := []struct {
		name          string
		mode          contract.DuplicateMode
		wantEvents    int
		wantDuplicate int
	}{
		{name: "drop", mode: contract.DuplicateDrop, wantEvents: 2, wantDuplicate: 0},
		{name: "forward", mode: contract.DuplicateForward, wantEvents: 3, wantDuplicate: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newSim(t).withAllowAllPolicy()
			protocol := h.addOneBotProtocol()
			h.addRouting("sim.routing.dedup", dedupSpec(contract.DedupMessageID, tc.mode, 5*time.Second))
			env := h.start(context.Background())

			adapter := startSimOneBotApplication(t, env, "server", nil)
			app := dialSimAppClient(t, adapter.Addr(), "/app/v11", "app:app.alpha", onebotapp.VersionV11, "app:app.alpha", nil)
			waitForSimCandidates(t, adapter, "app:app.alpha")

			a := dialSimQQClient(t, protocol.Addr(), "/onebot/v11", "bot-a", onebotapp.VersionV11)
			b := dialSimQQClient(t, protocol.Addr(), "/onebot/v12", "bot-b", onebotapp.VersionV12)
			if err := a.SendGroupMessage("dup-1", "100", "7", "same message"); err != nil {
				t.Fatalf("bot-a 推送首条: %v", err)
			}
			app.WaitMessageID("dup-1", 2*time.Second)

			// 另一协议连接发送相同 message_id；随后再发一条唯一消息作为处理屏障。
			if err := b.SendGroupMessage("dup-1", "100", "7", "same message"); err != nil {
				t.Fatalf("bot-b 推送重复消息: %v", err)
			}
			if err := b.SendGroupMessage("unique-2", "100", "7", "new message"); err != nil {
				t.Fatalf("bot-b 推送唯一消息: %v", err)
			}
			waitForSimBotEndpoint(env, "bot-b")
			app.WaitMessageID("unique-2", 2*time.Second)

			if got := app.EventCount(); got != tc.wantEvents {
				t.Fatalf("去重 %s 模式收到事件 = %d，期望 %d：%#v", tc.name, got, tc.wantEvents, app.Events())
			}
			duplicates := 0
			for _, event := range app.Events() {
				if event.MessageID == "dup-1" {
					duplicates++
				}
			}
			if duplicates != 1+tc.wantDuplicate {
				t.Fatalf("dup-1 投递次数 = %d，期望 %d：%#v", duplicates, 1+tc.wantDuplicate, app.Events())
			}
			if a.ActionCount() != 0 || b.ActionCount() != 0 {
				t.Fatal("无回复应用端不应触发协议动作")
			}
			if env.DeadLetterCount() != 0 {
				t.Fatalf("去重闭环不应产生死信: %#v", env.Dead.Entries())
			}
		})
	}
}

// H6：应用端适配器已接入但没有任何应用连接时，事件投递应进入死信，
// 而不是被静默丢弃；协议端也不会收到动作。
func TestScenarioRealOneBotApplicationWithoutConnectionsGoesToDeadLetter(t *testing.T) {
	h := newSim(t).withAllowAllPolicy()
	protocol := h.addOneBotProtocol()
	env := h.start(context.Background())

	// 只启动适配器并注册 sink，不建立任何应用端连接。
	startSimOneBotApplication(t, env, "server", nil)

	qq := dialSimQQClient(t, protocol.Addr(), "/onebot/v11", "bot-a", onebotapp.VersionV11)
	if err := qq.SendGroupMessage("m-a", "100", "7", "no application"); err != nil {
		t.Fatalf("推送群消息: %v", err)
	}
	env.waitForCount(2*time.Second, "无应用连接时记录死信", env.DeadLetterCount, 1)
	entries := env.Dead.Entries()
	if entries[0].SubscriberID != "event-sink" {
		t.Fatalf("死信订阅者 = %q，期望 event-sink", entries[0].SubscriberID)
	}
	if entries[0].Event.ID == "" {
		t.Fatal("死信应关联原事件")
	}
	if got := qq.ActionCount(); got != 0 {
		t.Fatalf("无应用连接时动作数 = %d，期望 0", got)
	}
}
