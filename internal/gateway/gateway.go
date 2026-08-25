package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/PhantomDaze/botfabric/internal/activity"
	"github.com/PhantomDaze/botfabric/internal/observability"
	"github.com/PhantomDaze/botfabric/internal/policy"
	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// Gateway 是带权限检查、超时和审计日志的本地服务网关。
type Gateway struct {
	registry       *registry.Registry
	policy         policy.Checker
	logger         *slog.Logger
	defaultTimeout time.Duration
	activity       *activity.Tracker
}

// New 创建本地服务网关。
func New(registry *registry.Registry, checker policy.Checker, logger *slog.Logger, defaultTimeout time.Duration) (*Gateway, error) {
	if registry == nil {
		return nil, errors.New("服务注册表不能为空")
	}
	if checker == nil {
		return nil, errors.New("权限检查器不能为空")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if defaultTimeout <= 0 {
		return nil, errors.New("默认服务超时必须大于零")
	}
	return &Gateway{
		registry:       registry,
		policy:         checker,
		logger:         logger,
		defaultTimeout: defaultTimeout,
		activity:       activity.NewTracker(),
	}, nil
}

// DeactivateOwner 停止指定插件发起新的服务调用。
func (g *Gateway) DeactivateOwner(pluginID string) {
	g.activity.Deactivate(pluginID)
}

// WaitOwner 等待指定插件的在途服务调用结束。
func (g *Gateway) WaitOwner(ctx context.Context, pluginID string) error {
	return g.activity.Wait(ctx, pluginID)
}

// DeactivateAndWaitOwner 停止发起新调用并等待在途调用结束。
func (g *Gateway) DeactivateAndWaitOwner(ctx context.Context, pluginID string) error {
	g.DeactivateOwner(pluginID)
	return g.WaitOwner(ctx, pluginID)
}

// ActivateOwner 允许指定插件发起服务调用。
func (g *Gateway) ActivateOwner(pluginID string) {
	g.activity.Activate(pluginID)
}

// ForPlugin 返回绑定可信插件身份的服务网关视图。
func (g *Gateway) ForPlugin(pluginID string) contract.ServiceGateway {
	g.activity.Activate(pluginID)
	return g.ViewForPlugin(pluginID)
}

// ViewForPlugin 返回不改变 Activity 状态的插件服务网关视图。
func (g *Gateway) ViewForPlugin(pluginID string) contract.ServiceGateway {
	return &scopedGateway{gateway: g, pluginID: pluginID}
}

type scopedGateway struct {
	gateway  *Gateway
	pluginID string
}

func (s *scopedGateway) Invoke(ctx context.Context, req contract.Request) (contract.Response, error) {
	return s.gateway.invoke(ctx, s.pluginID, req)
}

func (s *scopedGateway) Register(service contract.ServiceDescriptor, handler contract.ServiceHandler) error {
	return s.gateway.register(s.pluginID, service, handler)
}

func (s *scopedGateway) Unregister(serviceID string) error {
	return s.gateway.registry.UnregisterService(s.pluginID, serviceID)
}

func (g *Gateway) invoke(ctx context.Context, caller string, req contract.Request) (response contract.Response, err error) {
	if caller == "" || req.Service == "" || req.Method == "" {
		return serviceFailure(contract.ErrorInvalidArgument, "调用方、服务名和方法名不能为空", false)
	}
	if !g.policy.AllowInvoke(caller, req.Service, req.Method) {
		g.logger.Warn("拒绝未授权服务调用", "caller", caller, "service", req.Service, "method", req.Method)
		return serviceFailure(contract.ErrorPermissionDenied, "无权调用目标服务方法", false)
	}

	entry, exists := g.registry.Service(req.Service)
	if !exists {
		return serviceFailure(contract.ErrorServiceNotFound, "服务未注册: "+req.Service, false)
	}
	if !contains(entry.Descriptor.Methods, req.Method) {
		return serviceFailure(contract.ErrorMethodNotFound, "服务未声明方法: "+req.Method, false)
	}

	ctx, traceID := observability.EnsureTraceID(ctx)
	if req.TraceID != "" {
		traceID = req.TraceID
		ctx = observability.ContextWithTraceID(ctx, traceID)
	}
	req.Caller = caller
	req.TraceID = traceID

	doneCaller, accepted := g.activity.Begin(caller)
	if !accepted {
		return serviceFailure(contract.ErrorUnavailable, "调用方已停止发起新请求", false)
	}

	var doneProvider func()
	if entry.Owner != caller {
		var providerAccepted bool
		doneProvider, providerAccepted = g.activity.Begin(entry.Owner)
		if !providerAccepted {
			doneCaller()
			return serviceFailure(contract.ErrorUnavailable, "服务提供方已停止接收新请求", true)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, g.defaultTimeout)
	defer cancel()

	started := time.Now()
	defer func() {
		logger := observability.LoggerWithTrace(g.logger, ctx)
		logger.Info("服务调用完成",
			"caller", caller,
			"service", req.Service,
			"method", req.Method,
			"duration", time.Since(started),
			"error", err,
		)
	}()

	completed := make(chan callResult, 1)
	go func() {
		defer func() {
			if doneProvider != nil {
				doneProvider()
			}
			doneCaller()
		}()
		result := callResult{}
		defer func() {
			if recovered := recover(); recovered != nil {
				result.err = fmt.Errorf("服务处理器 panic: %v", recovered)
			}
			completed <- result
		}()
		result.response, result.err = entry.Handler(ctx, cloneRequest(req))
	}()

	select {
	case result := <-completed:
		if result.err != nil {
			var serviceError *contract.ServiceError
			if errors.As(result.err, &serviceError) {
				return contract.Response{Error: serviceError}, serviceError
			}
			return serviceFailure(contract.ErrorInternal, result.err.Error(), false)
		}
		return result.response, nil
	case <-ctx.Done():
		return serviceFailure(contract.ErrorTimeout, ctx.Err().Error(), true)
	}
}

type callResult struct {
	response contract.Response
	err      error
}

func (g *Gateway) register(owner string, descriptor contract.ServiceDescriptor, handler contract.ServiceHandler) error {
	if owner == "" || descriptor.Name == "" || len(descriptor.Methods) == 0 || handler == nil {
		return &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "服务所有者、名称、方法和处理器不能为空"}
	}
	if !g.policy.AllowProvide(owner, descriptor.Name) {
		g.logger.Warn("拒绝未授权服务注册", "plugin_id", owner, "service", descriptor.Name)
		return &contract.ServiceError{Code: contract.ErrorPermissionDenied, Message: "无权提供服务 " + descriptor.Name}
	}
	seen := make(map[string]struct{}, len(descriptor.Methods))
	for _, method := range descriptor.Methods {
		if method == "" {
			return &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "服务方法名不能为空"}
		}
		if _, exists := seen[method]; exists {
			return &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "服务方法重复: " + method}
		}
		seen[method] = struct{}{}
	}
	if err := g.registry.RegisterService(owner, descriptor, handler); err != nil {
		if errors.Is(err, registry.ErrServiceExists) {
			return &contract.ServiceError{Code: contract.ErrorAlreadyExists, Message: err.Error()}
		}
		return err
	}
	return nil
}

func serviceFailure(code contract.ErrorCode, message string, retryable bool) (contract.Response, error) {
	serviceError := &contract.ServiceError{Code: code, Message: message, Retryable: retryable}
	return contract.Response{Error: serviceError}, serviceError
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func cloneRequest(req contract.Request) contract.Request {
	cloned := req
	cloned.Payload = append([]byte(nil), req.Payload...)
	if req.Metadata != nil {
		cloned.Metadata = make(map[string]string, len(req.Metadata))
		for key, value := range req.Metadata {
			cloned.Metadata[key] = value
		}
	}
	return cloned
}
