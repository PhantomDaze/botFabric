package e2e_test

import (
	"context"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/PhantomDaze/botfabric/plugins/classifier"
)

// D 组公共构造：真实 LLM 分类器插件自带一条把群消息交给分类器的路由规则，
// 因此只需装载分类器插件本身（addRealLLMClassifier），不必再叠加规则插件。

// D1：分类器返回 selective 时，事件只投递给目标应用端。
func TestScenarioClassifierSelectiveRoutesTargets(t *testing.T) {
	newSimLLM(t, classifier.RealLLMFormatOpenAIChatCompletions,
		contract.DispatchPlan{Mode: contract.DispatchSelective, Targets: []string{"app.alpha"}})

	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	beta := h.addApp(simAppConfig{ID: "app.beta"})
	h.addRealLLMClassifier()
	h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}

	if alpha.ReceivedCount() != 1 {
		t.Fatalf("目标应用端收到 = %d，期望 1", alpha.ReceivedCount())
	}
	if beta.ReceivedCount() != 0 {
		t.Fatalf("非目标应用端收到 = %d，期望 0", beta.ReceivedCount())
	}
}

// D2：分类器返回 drop 时，事件不投递给任何应用端。
func TestScenarioClassifierDropSuppressesEvent(t *testing.T) {
	newSimLLM(t, classifier.RealLLMFormatOpenAIChatCompletions,
		contract.DispatchPlan{Mode: contract.DispatchDrop})

	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	h.addRealLLMClassifier()
	env := h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("被丢弃事件发布不应报错: %v", err)
	}
	if alpha.ReceivedCount() != 0 {
		t.Fatalf("drop 分类结果下收到 = %d，期望 0", alpha.ReceivedCount())
	}
	if env.DeadLetterCount() != 0 {
		t.Fatalf("drop 不应产生死信: %#v", env.Dead.Entries())
	}
}

// D3：分类服务返回 5xx 时，Router 回退为默认广播。
func TestScenarioClassifierHTTPFailureFallsBackToBroadcast(t *testing.T) {
	stub := newSimLLM(t, classifier.RealLLMFormatOpenAIChatCompletions,
		contract.DispatchPlan{Mode: contract.DispatchDrop}).withStatus(500)

	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	beta := h.addApp(simAppConfig{ID: "app.beta"})
	h.addRealLLMClassifier()
	h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}

	if stub.Calls() != 1 {
		t.Fatalf("分类器调用次数 = %d，期望 1", stub.Calls())
	}
	// 分类失败回退广播：两个应用端都收到。
	if alpha.ReceivedCount() != 1 || beta.ReceivedCount() != 1 {
		t.Fatalf("回退广播后 alpha=%d beta=%d，期望 1/1", alpha.ReceivedCount(), beta.ReceivedCount())
	}
}

// D5：分类计划包含候选集之外的目标时视为无效，回退广播。
func TestScenarioClassifierRejectsTargetOutsideCandidates(t *testing.T) {
	stub := newSimLLM(t, classifier.RealLLMFormatOpenAIChatCompletions,
		contract.DispatchPlan{Mode: contract.DispatchSelective, Targets: []string{"app.ghost"}})

	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	beta := h.addApp(simAppConfig{ID: "app.beta"})
	h.addRealLLMClassifier()
	h.start(context.Background())

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("发布事件: %v", err)
	}

	if stub.Calls() != 1 {
		t.Fatalf("分类器调用次数 = %d，期望 1", stub.Calls())
	}
	if alpha.ReceivedCount() != 1 || beta.ReceivedCount() != 1 {
		t.Fatalf("候选外目标回退广播后 alpha=%d beta=%d，期望 1/1", alpha.ReceivedCount(), beta.ReceivedCount())
	}
}

// D4：四种 API 格式各完成一次端到端 selective 路由，验证响应外壳解析正确。
func TestScenarioClassifierAllFourFormats(t *testing.T) {
	formats := []struct {
		name   string
		format classifier.RealLLMFormat
	}{
		{"openai_chat_completions", classifier.RealLLMFormatOpenAIChatCompletions},
		{"anthropic_messages", classifier.RealLLMFormatAnthropicMessages},
		{"openai_responses", classifier.RealLLMFormatOpenAIResponses},
		{"google_gemini", classifier.RealLLMFormatGoogleGemini},
	}
	for _, item := range formats {
		t.Run(item.name, func(t *testing.T) {
			stub := newSimLLM(t, item.format,
				contract.DispatchPlan{Mode: contract.DispatchSelective, Targets: []string{"app.alpha"}})

			h := newSim(t)
			proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
			alpha := h.addApp(simAppConfig{ID: "app.alpha"})
			beta := h.addApp(simAppConfig{ID: "app.beta"})
			h.addRealLLMClassifier()
			h.start(context.Background())

			if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
				t.Fatalf("发布事件: %v", err)
			}
			if stub.Calls() != 1 {
				t.Fatalf("%s：分类器调用次数 = %d，期望 1", item.name, stub.Calls())
			}
			if alpha.ReceivedCount() != 1 || beta.ReceivedCount() != 0 {
				t.Fatalf("%s：selective 后 alpha=%d beta=%d，期望 1/0", item.name, alpha.ReceivedCount(), beta.ReceivedCount())
			}
		})
	}
}
