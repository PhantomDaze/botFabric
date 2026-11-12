package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/internal/routing"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// endpointRegistrar 是插件宿主可用的端点注册子集；真实注册表与暂存捕获器均实现该接口。
type endpointRegistrar interface {
	Register(contract.EndpointInfo) error
	UnregisterOwned(owner, id string) error
}

// classifierRegistrar 是插件宿主可用的路由分类器注册子集。
type classifierRegistrar interface {
	RegisterClassifierForOwner(owner, id string, classifier contract.RouteClassifier) error
	UnregisterClassifier(id string) error
}

// captureEndpoints 在热重载暂存阶段记录插件注册的端点而不触碰真实注册表。
// 这样旧实例仍在使用同一端点 ID 时，新实例的 Start 不会因 ErrEndpointExists 失败。
type captureEndpoints struct {
	mu    sync.Mutex
	owner string
	infos []contract.EndpointInfo
}

func newCaptureEndpoints(owner string) *captureEndpoints {
	return &captureEndpoints{owner: owner}
}

func (e *captureEndpoints) Register(info contract.EndpointInfo) error {
	if info.ID == "" {
		return &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "端点 ID 不能为空"}
	}
	if info.Owner == "" {
		info.Owner = e.owner
	}
	if info.Owner != e.owner {
		return &contract.ServiceError{Code: contract.ErrorPermissionDenied, Message: "不能注册其他插件拥有的端点"}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, existing := range e.infos {
		if existing.ID == info.ID {
			return &contract.ServiceError{Code: contract.ErrorAlreadyExists, Message: "暂存端点重复: " + info.ID}
		}
	}
	e.infos = append(e.infos, cloneEndpointInfo(info))
	return nil
}

func (e *captureEndpoints) UnregisterOwned(owner, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for index, info := range e.infos {
		if info.ID != id {
			continue
		}
		if info.Owner != owner {
			return &contract.ServiceError{Code: contract.ErrorPermissionDenied, Message: "不能注销其他插件拥有的端点"}
		}
		e.infos = append(e.infos[:index], e.infos[index+1:]...)
		return nil
	}
	return nil
}

func (e *captureEndpoints) Specs() []contract.EndpointInfo {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := make([]contract.EndpointInfo, len(e.infos))
	for index, info := range e.infos {
		result[index] = cloneEndpointInfo(info)
	}
	return result
}

// captureClassifiers 在热重载暂存阶段记录分类器注册，切换成功后再原子迁移。
type captureClassifiers struct {
	mu    sync.Mutex
	owner string
	specs []routing.ClassifierSpec
}

func newCaptureClassifiers(owner string) *captureClassifiers {
	return &captureClassifiers{owner: owner}
}

func (c *captureClassifiers) RegisterClassifierForOwner(owner, id string, classifier contract.RouteClassifier) error {
	if owner != c.owner {
		return &contract.ServiceError{Code: contract.ErrorPermissionDenied, Message: "不能以其他插件身份注册分类器"}
	}
	if id == "" || classifier == nil {
		return &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "分类器 ID 和实现不能为空"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, existing := range c.specs {
		if existing.ID == id {
			return &contract.ServiceError{Code: contract.ErrorAlreadyExists, Message: "暂存分类器重复: " + id}
		}
	}
	c.specs = append(c.specs, routing.ClassifierSpec{ID: id, Classifier: classifier})
	return nil
}

func (c *captureClassifiers) UnregisterClassifier(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for index, spec := range c.specs {
		if spec.ID == id {
			c.specs = append(c.specs[:index], c.specs[index+1:]...)
			return nil
		}
	}
	return nil
}

func (c *captureClassifiers) Specs() []routing.ClassifierSpec {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]routing.ClassifierSpec, len(c.specs))
	copy(result, c.specs)
	return result
}

// switchableEndpoints 允许暂存宿主在切换瞬间从捕获器切到真实端点注册表。
type switchableEndpoints struct {
	mu     sync.RWMutex
	target endpointRegistrar
}

func (e *switchableEndpoints) Register(info contract.EndpointInfo) error {
	e.mu.RLock()
	target := e.target
	e.mu.RUnlock()
	return target.Register(info)
}

func (e *switchableEndpoints) UnregisterOwned(owner, id string) error {
	e.mu.RLock()
	target := e.target
	e.mu.RUnlock()
	return target.UnregisterOwned(owner, id)
}

func (e *switchableEndpoints) swap(target endpointRegistrar) {
	e.mu.Lock()
	e.target = target
	e.mu.Unlock()
}

// switchableClassifiers 允许暂存宿主在切换瞬间从捕获器切到真实路由器。
type switchableClassifiers struct {
	mu     sync.RWMutex
	target classifierRegistrar
}

func (c *switchableClassifiers) RegisterClassifierForOwner(owner, id string, classifier contract.RouteClassifier) error {
	c.mu.RLock()
	target := c.target
	c.mu.RUnlock()
	return target.RegisterClassifierForOwner(owner, id, classifier)
}

func (c *switchableClassifiers) UnregisterClassifier(id string) error {
	c.mu.RLock()
	target := c.target
	c.mu.RUnlock()
	return target.UnregisterClassifier(id)
}

func (c *switchableClassifiers) swap(target classifierRegistrar) {
	c.mu.Lock()
	c.target = target
	c.mu.Unlock()
}

type captureEventBus struct {
	mu      sync.Mutex
	timeout time.Duration
	specs   []eventbus.SubscriptionSpec
}

func newCaptureEventBus(timeout time.Duration) *captureEventBus {
	return &captureEventBus{timeout: timeout}
}

func (b *captureEventBus) Publish(context.Context, contract.Event) error {
	return &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "暂存插件尚未切换到运行时事件总线"}
}

func (b *captureEventBus) Subscribe(topic string, handler contract.EventHandler, opts ...contract.SubOption) (contract.Subscription, error) {
	if topic == "" || handler == nil {
		return nil, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "暂存订阅无效"}
	}
	config := contract.SubscriptionConfig{Timeout: b.timeout}
	for _, option := range opts {
		if option != nil {
			option(&config)
		}
	}
	if config.Timeout <= 0 {
		return nil, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "暂存订阅超时必须大于零"}
	}
	b.mu.Lock()
	b.specs = append(b.specs, eventbus.SubscriptionSpec{Topic: topic, Priority: config.Priority, Timeout: config.Timeout, Handler: handler})
	b.mu.Unlock()
	return noopSubscription{}, nil
}

func (b *captureEventBus) Specs() []eventbus.SubscriptionSpec {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]eventbus.SubscriptionSpec, len(b.specs))
	copy(result, b.specs)
	return result
}

type captureGateway struct {
	mu    sync.Mutex
	specs []registry.ServiceSpec
}

func (g *captureGateway) Invoke(context.Context, contract.Request) (contract.Response, error) {
	err := &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "暂存插件尚未切换到运行时服务网关"}
	return contract.Response{Error: err}, err
}

func (g *captureGateway) Register(service contract.ServiceDescriptor, handler contract.ServiceHandler) error {
	if service.Name == "" || len(service.Methods) == 0 || handler == nil {
		return &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "暂存服务无效"}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, spec := range g.specs {
		if spec.Descriptor.Name == service.Name {
			return &contract.ServiceError{Code: contract.ErrorAlreadyExists, Message: "暂存服务重复: " + service.Name}
		}
	}
	g.specs = append(g.specs, registry.ServiceSpec{Descriptor: cloneServiceDescriptor(service), Handler: handler})
	return nil
}

func (g *captureGateway) Unregister(string) error { return nil }

func (g *captureGateway) Specs() []registry.ServiceSpec {
	g.mu.Lock()
	defer g.mu.Unlock()
	result := make([]registry.ServiceSpec, len(g.specs))
	copy(result, g.specs)
	return result
}

type noopSubscription struct{}

func (noopSubscription) Unsubscribe() error { return nil }

type switchableEventBus struct {
	mu     sync.RWMutex
	target contract.EventBus
}

func (b *switchableEventBus) Publish(ctx context.Context, event contract.Event) error {
	b.mu.RLock()
	target := b.target
	b.mu.RUnlock()
	return target.Publish(ctx, event)
}

func (b *switchableEventBus) Subscribe(topic string, handler contract.EventHandler, opts ...contract.SubOption) (contract.Subscription, error) {
	b.mu.RLock()
	target := b.target
	b.mu.RUnlock()
	return target.Subscribe(topic, handler, opts...)
}

func (b *switchableEventBus) swap(target contract.EventBus) {
	b.mu.Lock()
	b.target = target
	b.mu.Unlock()
}

type switchableGateway struct {
	mu     sync.RWMutex
	target contract.ServiceGateway
}

func (g *switchableGateway) Invoke(ctx context.Context, request contract.Request) (contract.Response, error) {
	g.mu.RLock()
	target := g.target
	g.mu.RUnlock()
	return target.Invoke(ctx, request)
}

func (g *switchableGateway) Register(service contract.ServiceDescriptor, handler contract.ServiceHandler) error {
	g.mu.RLock()
	target := g.target
	g.mu.RUnlock()
	return target.Register(service, handler)
}

func (g *switchableGateway) Unregister(serviceID string) error {
	g.mu.RLock()
	target := g.target
	g.mu.RUnlock()
	return target.Unregister(serviceID)
}

func (g *switchableGateway) swap(target contract.ServiceGateway) {
	g.mu.Lock()
	g.target = target
	g.mu.Unlock()
}

func newStagingHost(manifest contract.Manifest, _ contract.EventBus, _ contract.ServiceGateway, logger contract.Logger, botID string) (*pluginHost, *captureEventBus, *captureGateway, *captureEndpoints, *captureClassifiers) {
	captureBus := newCaptureEventBus(time.Second)
	captureGateway := &captureGateway{}
	switchBus := &switchableEventBus{target: captureBus}
	switchGateway := &switchableGateway{target: captureGateway}
	captureEndpoints := newCaptureEndpoints(manifest.ID)
	captureClassifiers := newCaptureClassifiers(manifest.ID)
	host := newPluginHost(switchBus, switchGateway, logger, botID, manifest)
	// 暂存阶段不触碰真实端点注册表与路由器：旧实例仍持有同名端点/分类器，
	// 直接注册会返回 ErrEndpointExists 或“分类器已注册”。
	host.endpoints = &switchableEndpoints{target: captureEndpoints}
	host.classifiers = &switchableClassifiers{target: captureClassifiers}
	return host, captureBus, captureGateway, captureEndpoints, captureClassifiers
}

func (h *pluginHost) switchTo(eventBus contract.EventBus, gateway contract.ServiceGateway, endpoints endpointRegistrar, classifiers classifierRegistrar) error {
	switchBus, ok := h.eventBus.(*switchableEventBus)
	if !ok {
		return errors.New("staging EventBus 不是可切换视图")
	}
	switchGateway, ok := h.gateway.(*switchableGateway)
	if !ok {
		return errors.New("staging Gateway 不是可切换视图")
	}
	switchEndpoints, ok := h.endpoints.(*switchableEndpoints)
	if !ok {
		return errors.New("staging 端点注册器不是可切换视图")
	}
	switchClassifiers, ok := h.classifiers.(*switchableClassifiers)
	if !ok {
		return errors.New("staging 分类器注册器不是可切换视图")
	}
	switchBus.swap(eventBus)
	switchGateway.swap(gateway)
	switchEndpoints.swap(endpoints)
	switchClassifiers.swap(classifiers)
	h.active.Store(true)
	return nil
}

func cloneServiceDescriptor(descriptor contract.ServiceDescriptor) contract.ServiceDescriptor {
	descriptor.Methods = append([]string(nil), descriptor.Methods...)
	return descriptor
}

func validateStagingResources(manifest contract.Manifest, services []registry.ServiceSpec, subscriptions []eventbus.SubscriptionSpec) error {
	if len(services) != len(manifest.Provides.Services) {
		return fmt.Errorf("插件 %s 暂存服务数量与清单不一致", manifest.ID)
	}
	for _, descriptor := range manifest.Provides.Services {
		found := false
		for _, spec := range services {
			if spec.Descriptor.Name == descriptor.Name && sameStringSet(makeStringSet(descriptor.Methods), spec.Descriptor.Methods) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("插件 %s 未暂存清单服务 %s", manifest.ID, descriptor.Name)
		}
	}
	allowedTopics := makeStringSet(manifest.Provides.Subscriptions)
	for _, subscription := range subscriptions {
		if _, allowed := allowedTopics[subscription.Topic]; !allowed {
			return fmt.Errorf("插件 %s 暂存了未声明 Topic %s", manifest.ID, subscription.Topic)
		}
	}
	return nil
}

// validateStagingCapabilities 校验暂存阶段捕获的端点与分类器归属清单所有者。
func validateStagingCapabilities(manifest contract.Manifest, endpoints []contract.EndpointInfo, classifiers []routing.ClassifierSpec) error {
	for _, info := range endpoints {
		if info.ID == "" {
			return fmt.Errorf("插件 %s 暂存了空端点 ID", manifest.ID)
		}
		if info.Owner != manifest.ID {
			return fmt.Errorf("插件 %s 暂存了其他 owner (%s) 的端点 %s", manifest.ID, info.Owner, info.ID)
		}
	}
	captured := make(map[string]struct{}, len(classifiers))
	for _, spec := range classifiers {
		if spec.ID == "" || spec.Classifier == nil {
			return fmt.Errorf("插件 %s 暂存了无效分类器", manifest.ID)
		}
		captured[spec.ID] = struct{}{}
	}
	if manifest.Routing == nil {
		return nil
	}
	for _, rule := range manifest.Routing.Rules {
		if rule.Action.Classifier == "" {
			continue
		}
		if _, exists := captured[rule.Action.Classifier]; !exists {
			return fmt.Errorf("插件 %s 的路由规则 %s 引用了未暂存的分类器 %s", manifest.ID, rule.ID, rule.Action.Classifier)
		}
	}
	return nil
}

func cloneEndpointInfo(info contract.EndpointInfo) contract.EndpointInfo {
	info.Capabilities = append([]string(nil), info.Capabilities...)
	return info
}
