package classifier

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

const (
	// RealLLMEntry 是真实 OpenAI-compatible 分类器的内置入口。
	RealLLMEntry = "builtin.classifier.real-llm"
	// RealLLMID 是真实模型分类器注册到路由器的 ID。
	RealLLMID = "classifier.real-llm"
)

// RealLLMPlugin 在启动时读取环境变量并注册真实模型分类器。
type RealLLMPlugin struct {
	getenv     func(string) string
	classifier *RealLLMClassifier
}

// NewRealLLMPlugin 创建真实模型分类器插件。
func NewRealLLMPlugin() *RealLLMPlugin {
	return &RealLLMPlugin{getenv: os.Getenv}
}

// Manifest 返回真实模型分类器插件清单。
func (*RealLLMPlugin) Manifest() contract.Manifest {
	return contract.Manifest{
		ID:      RealLLMID,
		Name:    "OpenAI-compatible Route Classifier",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   RealLLMEntry,
		Limits:  contract.Limits{Timeout: time.Second},
		Routing: &contract.RoutingSpec{Rules: []contract.RoutingRule{{
			ID:       "classify-group-messages",
			Priority: 100,
			Match:    contract.RuleMatch{Topic: "bot.message.group"},
			Action: contract.RuleAction{
				Mode:       contract.DispatchBroadcast,
				Classifier: RealLLMID,
			},
		}}},
	}
}

// Start 创建并注册分类器；未配置密钥时安全失败且不会发起网络请求。
func (p *RealLLMPlugin) Start(_ context.Context, host contract.Host) error {
	config, err := RealLLMConfigFromEnv(p.getenv)
	if err != nil {
		return err
	}
	classifier, err := NewRealLLMClassifier(config)
	if err != nil {
		return err
	}
	registrar, ok := host.(contract.RouteClassifierRegistrar)
	if !ok {
		return errors.New("宿主不支持路由分类器注册")
	}
	if err := registrar.RegisterRouteClassifier(RealLLMID, classifier); err != nil {
		return err
	}
	p.classifier = classifier
	return nil
}

// Stop 停止真实模型分类器插件。
func (*RealLLMPlugin) Stop(context.Context) error { return nil }

// Health 返回分类器插件健康状态。
func (p *RealLLMPlugin) Health(context.Context) contract.HealthStatus {
	state, message := contract.HealthHealthy, "真实 LLM 分类器已配置"
	if p.classifier == nil {
		state, message = contract.HealthUnhealthy, "真实 LLM 分类器未启动"
	}
	return contract.HealthStatus{State: state, Message: message, CheckedAt: time.Now().UTC()}
}

var _ contract.Plugin = (*RealLLMPlugin)(nil)
