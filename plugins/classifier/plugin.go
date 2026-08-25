// Package classifier 提供可选的路由分类器插件实现。
package classifier

import (
	"context"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

const (
	// MockEntry 是固定规则 mock 分类器的内置入口。
	MockEntry = "builtin.classifier.mock"
	// MockID 是默认注册到 Router 的分类器 ID。
	MockID = "classifier.mock"
)

// MockClassifier 返回预设的路由计划，用于测试和离线开发。
type MockClassifier struct {
	Plan contract.DispatchPlan
}

// NewMock 创建固定计划分类器。
func NewMock(plan contract.DispatchPlan) *MockClassifier { return &MockClassifier{Plan: plan} }

// Classify 实现 routing.RouteClassifier。
func (c *MockClassifier) Classify(ctx context.Context, _ contract.Event, candidates []string) (contract.DispatchPlan, error) {
	select {
	case <-ctx.Done():
		return contract.DispatchPlan{}, ctx.Err()
	default:
	}
	plan := c.Plan
	if plan.Mode == "" {
		plan.Mode = contract.DispatchBroadcast
	}
	if len(plan.Targets) == 0 && plan.Mode == contract.DispatchBroadcast {
		plan.Targets = append([]string(nil), candidates...)
	}
	return plan, nil
}

// Plugin 是只负责注册 mock 分类器的内置插件。
type Plugin struct {
	classifier *MockClassifier
}

// NewPlugin 创建 mock 分类器插件。
func NewPlugin(plan contract.DispatchPlan) *Plugin { return &Plugin{classifier: NewMock(plan)} }

// Manifest 返回 mock 分类器插件清单。
func (*Plugin) Manifest() contract.Manifest {
	return contract.Manifest{
		ID:      "classifier.mock",
		Name:    "Mock Route Classifier",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   MockEntry,
		Limits:  contract.Limits{Timeout: time.Second},
	}
}

// Start 注册分类器到宿主路由器。
func (p *Plugin) Start(_ context.Context, host contract.Host) error {
	registrar, ok := host.(contract.RouteClassifierRegistrar)
	if !ok {
		return &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "宿主不支持路由分类器注册"}
	}
	return registrar.RegisterRouteClassifier(MockID, p.classifier)
}

// Stop 停止分类器插件。
func (p *Plugin) Stop(_ context.Context) error {
	return nil
}

// Health 返回健康状态。
func (*Plugin) Health(context.Context) contract.HealthStatus {
	return contract.HealthStatus{State: contract.HealthHealthy, Message: "mock 分类器可用", CheckedAt: time.Now().UTC()}
}

var _ contract.RouteClassifier = (*MockClassifier)(nil)
