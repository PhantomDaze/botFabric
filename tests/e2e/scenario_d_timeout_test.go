package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/PhantomDaze/botfabric/plugins/classifier"
)

// D6：分类服务超过 Router deadline 时取消请求并广播回退。
func TestScenarioClassifierTimeoutFallsBackToBroadcast(t *testing.T) {
	stub := newSimLLM(t, classifier.RealLLMFormatOpenAIChatCompletions,
		contract.DispatchPlan{Mode: contract.DispatchDrop}).withDelay(100 * time.Millisecond)

	h := newSim(t)
	proto := h.addProtocol(simProtocolConfig{ID: "sim.protocol"})
	alpha := h.addApp(simAppConfig{ID: "app.alpha"})
	beta := h.addApp(simAppConfig{ID: "app.beta"})
	h.addRealLLMClassifier()
	env := h.start(context.Background())
	if err := env.Manager.Router().SetClassifierTimeout(10 * time.Millisecond); err != nil {
		t.Fatalf("设置分类器 deadline: %v", err)
	}

	if err := proto.Emit(context.Background(), "", SimMessage{MessageID: "m-1", GroupID: "100", UserID: "7", Text: "hi"}); err != nil {
		t.Fatalf("超时回退发布: %v", err)
	}
	if stub.Calls() != 1 {
		t.Fatalf("分类器调用次数 = %d，期望 1", stub.Calls())
	}
	if alpha.ReceivedCount() != 1 || beta.ReceivedCount() != 1 {
		t.Fatalf("分类器超时回退后 alpha=%d beta=%d，期望 1/1", alpha.ReceivedCount(), beta.ReceivedCount())
	}
}
