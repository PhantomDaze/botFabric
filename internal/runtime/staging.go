package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

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

func newStagingHost(manifest contract.Manifest, _ contract.EventBus, _ contract.ServiceGateway, logger contract.Logger, botID string) (*pluginHost, *captureEventBus, *captureGateway) {
	captureBus := newCaptureEventBus(time.Second)
	captureGateway := &captureGateway{}
	switchBus := &switchableEventBus{target: captureBus}
	switchGateway := &switchableGateway{target: captureGateway}
	host := newPluginHost(switchBus, switchGateway, logger, botID, manifest)
	return host, captureBus, captureGateway
}

func (h *pluginHost) switchTo(eventBus contract.EventBus, gateway contract.ServiceGateway) error {
	switchBus, ok := h.eventBus.(*switchableEventBus)
	if !ok {
		return errors.New("staging EventBus 不是可切换视图")
	}
	switchGateway, ok := h.gateway.(*switchableGateway)
	if !ok {
		return errors.New("staging Gateway 不是可切换视图")
	}
	switchBus.swap(eventBus)
	switchGateway.swap(gateway)
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
