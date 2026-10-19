package e2e_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/management"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// G1：清单声明订阅但权限策略不放行时，插件启动应被拒（deny-by-default）。
func TestScenarioPolicyDeniesSubscription(t *testing.T) {
	h := newSim(t)
	h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	h.addApp(simAppConfig{ID: "app.unauthorized", DenySubscription: true})

	_, err := h.startE(context.Background())
	if err == nil {
		t.Fatal("权限策略拒绝订阅时，运行时启动应失败")
	}
}

// G2：应用端清单虽声明动作依赖，但静态策略拒绝后，完整消息链路不得发出动作。
func TestScenarioPolicyDeniesActionInvocation(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	app := h.addApp(simAppConfig{
		ID: "app.denied",
		Reply: func(contract.Event, contract.GroupMessage) *contract.CandidateReply {
			return &contract.CandidateReply{Text: "must not send"}
		},
		DenyActionPolicy: true,
	})
	h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second}))
	env := h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err == nil {
		t.Fatal("静态策略拒绝动作调用时，事件处理应返回权限错误")
	}
	if proto.SentCount() != 0 {
		t.Fatalf("越权动作不应到达协议端，发送数 = %d", proto.SentCount())
	}
	if len(app.Errors()) == 0 {
		t.Fatal("应用端应记录被拒绝的响应提交")
	}
	if env.DeadLetterCount() == 0 {
		t.Fatal("被拒绝调用应沿事件链路进入死信")
	}
	var serviceErr *contract.ServiceError
	if !errors.As(app.Errors()[0], &serviceErr) || serviceErr.Code != contract.ErrorPermissionDenied {
		t.Fatalf("拒绝错误 = %v，期望 permission_denied", app.Errors()[0])
	}
}

// G3：事件缺 TraceID 时总线补齐，并贯穿到候选回复与实际发出的协议动作。
func TestScenarioTraceIDPropagatesThroughLoop(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha", Reply: textReply("pong")})
	h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second}))
	h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "ping"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}
	received := alpha.Received()
	if len(received) != 1 || received[0].TraceID == "" {
		t.Fatalf("应用端应收到带 TraceID 的事件: %#v", received)
	}
	sent := proto.Sent()
	if len(sent) != 1 || sent[0].TraceID == "" {
		t.Fatalf("发出的动作应带 TraceID，实际: %#v", sent)
	}
	if sent[0].TraceID != received[0].TraceID {
		t.Fatalf("动作 TraceID = %q，与事件 TraceID %q 不一致", sent[0].TraceID, received[0].TraceID)
	}
}

// G4：应用端处理器超时后，事件进入死信且处理器错误被记录。
func TestScenarioHandlerTimeoutBecomesDeadLetter(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{
		ID:                  "app.slow",
		Delay:               100 * time.Millisecond,
		SubscriptionTimeout: 10 * time.Millisecond,
		Reply: func(contract.Event, contract.GroupMessage) *contract.CandidateReply {
			return &contract.CandidateReply{Text: "too late"}
		},
	})
	env := h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-timeout", GroupID: "100", UserID: "7", Text: "slow"}); err == nil {
		t.Fatal("处理器超时应向上返回错误")
	}
	if alpha.ReceivedCount() != 1 {
		t.Fatalf("应用端收到事件数 = %d，期望 1", alpha.ReceivedCount())
	}
	if got := env.DeadLetterCount(); got != 1 {
		t.Fatalf("死信数量 = %d，期望 1: %#v", got, env.Dead.Entries())
	}
	if errors := alpha.Errors(); len(errors) != 0 {
		t.Fatalf("处理器延时期间已超时，不应提交回复，错误记录 = %v", errors)
	}
}

// G4b：应用端处理器返回错误时，事件进入死信且处理器错误被记录。
func TestScenarioHandlerErrorBecomesDeadLetter(t *testing.T) {
	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	failing := errors.New("应用端处理器故障")
	alpha := h.addApp(simAppConfig{ID: "app.failing", HandlerErr: failing})
	env := h.start(context.Background())

	err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "boom"})
	if err == nil {
		t.Fatal("处理器返回错误应向上抛出")
	}
	if env.DeadLetterCount() != 1 {
		t.Fatalf("死信数量 = %d，期望 1: %#v", env.DeadLetterCount(), env.Dead.Entries())
	}
	if len(alpha.Errors()) != 1 {
		t.Fatalf("应用端记录错误数 = %d，期望 1", len(alpha.Errors()))
	}
}

// G6：运行时健康检查反映插件运行状态。
func TestScenarioHealthReflectsPluginState(t *testing.T) {
	h := newSim(t)
	h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	app := h.addApp(simAppConfig{ID: "app.alpha"})
	env := h.start(context.Background())

	health := env.Manager.Health(context.Background())
	status, ok := health[app.Manifest().ID]
	if !ok {
		t.Fatalf("健康表缺少插件 %s：%v", app.Manifest().ID, health)
	}
	if status.State != contract.HealthHealthy {
		t.Fatalf("运行中插件健康状态 = %s，期望 healthy", status.State)
	}
}

// G5：管理 API 在仿真 Runtime 上同样要求 Bearer Token，并对死信载荷脱敏。
func TestScenarioManagementAPIAuthAndRedaction(t *testing.T) {
	h := newSim(t)
	h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	env := h.start(context.Background())
	env.Dead.Record(eventbus.DeadLetter{
		Event: contract.Event{
			ID: "secret-event", Topic: "bot.message.group", TraceID: "trace-1",
			Payload: []byte("private message payload"), Headers: map[string]string{"private-header": "private-value"},
		},
		SubscriberID: "app.alpha", Error: "handler failed",
	})
	server, err := management.New("127.0.0.1:0", "simulation-management-token", env.Manager, env.Dead)
	if err != nil {
		t.Fatalf("创建管理 API: %v", err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("启动管理 API: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("关闭管理 API: %v", err)
		}
	})

	resp, err := http.Get("http://" + server.Addr() + "/healthz")
	if err != nil {
		t.Fatalf("未授权 health 请求: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("未授权状态码 = %d，期望 401", resp.StatusCode)
	}

	req, err := http.NewRequest(http.MethodGet, "http://"+server.Addr()+"/v1/dead-letters", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer simulation-management-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("已授权死信请求: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("授权状态码 = %d，期望 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"private message payload", "private-value"} {
		if strings.Contains(string(body), secret) {
			t.Fatalf("管理 API 泄露了敏感数据 %q：%s", secret, body)
		}
	}
	if !strings.Contains(string(body), "secret-event") {
		t.Fatalf("管理 API 响应缺少事件 ID：%s", body)
	}
}
