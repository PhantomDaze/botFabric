package registry

import (
	"errors"
	"sync"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

var (
	ErrPluginExists    = errors.New("插件已注册")
	ErrPluginNotFound  = errors.New("插件不存在")
	ErrServiceExists   = errors.New("服务已注册")
	ErrServiceNotFound = errors.New("服务不存在")
)

// PluginEntry 是运行时维护的插件注册信息。
type PluginEntry struct {
	Manifest contract.Manifest
	State    contract.PluginState
	Plugin   contract.Plugin
}

// ServiceEntry 是服务描述、处理器和可信所有者的绑定。
type ServiceEntry struct {
	Descriptor contract.ServiceDescriptor
	Owner      string
	Handler    contract.ServiceHandler
}

// Registry 是插件与服务的线程安全注册表。
type Registry struct {
	mu       sync.RWMutex
	plugins  map[string]PluginEntry
	services map[string]ServiceEntry
}

// New 创建空注册表。
func New() *Registry {
	return &Registry{
		plugins:  make(map[string]PluginEntry),
		services: make(map[string]ServiceEntry),
	}
}

// RegisterPlugin 注册一个新发现的插件。
func (r *Registry) RegisterPlugin(entry PluginEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.plugins[entry.Manifest.ID]; exists {
		return ErrPluginExists
	}
	r.plugins[entry.Manifest.ID] = PluginEntry{
		Manifest: cloneManifest(entry.Manifest),
		State:    entry.State,
		Plugin:   entry.Plugin,
	}
	return nil
}

// Plugin 返回插件注册信息的副本。
func (r *Registry) Plugin(id string) (PluginEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, exists := r.plugins[id]
	entry.Manifest = cloneManifest(entry.Manifest)
	return entry, exists
}

// SetPluginState 更新一个已注册插件的生命周期状态。
func (r *Registry) SetPluginState(id string, state contract.PluginState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, exists := r.plugins[id]
	if !exists {
		return ErrPluginNotFound
	}
	entry.State = state
	r.plugins[id] = entry
	return nil
}

// RemovePlugin 移除一个插件及其服务。
func (r *Registry) RemovePlugin(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.plugins, id)
	for name, service := range r.services {
		if service.Owner == id {
			delete(r.services, name)
		}
	}
}

// Plugins 返回所有插件注册信息的快照。
func (r *Registry) Plugins() []PluginEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entries := make([]PluginEntry, 0, len(r.plugins))
	for _, entry := range r.plugins {
		entry.Manifest = cloneManifest(entry.Manifest)
		entries = append(entries, entry)
	}
	return entries
}

// RegisterService 将服务与可信插件所有者绑定。
func (r *Registry) RegisterService(owner string, descriptor contract.ServiceDescriptor, handler contract.ServiceHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.services[descriptor.Name]; exists {
		return ErrServiceExists
	}
	r.services[descriptor.Name] = ServiceEntry{Descriptor: cloneDescriptor(descriptor), Owner: owner, Handler: handler}
	return nil
}

// Service 返回服务注册信息。
func (r *Registry) Service(name string) (ServiceEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, exists := r.services[name]
	entry.Descriptor = cloneDescriptor(entry.Descriptor)
	return entry, exists
}

// UnregisterService 仅允许服务所有者取消注册。
func (r *Registry) UnregisterService(owner, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, exists := r.services[name]
	if !exists {
		return ErrServiceNotFound
	}
	if entry.Owner != owner {
		return errors.New("不能取消其他插件提供的服务")
	}
	delete(r.services, name)
	return nil
}

// UnregisterServicesByOwner 移除指定插件提供的全部服务。
func (r *Registry) UnregisterServicesByOwner(owner string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, entry := range r.services {
		if entry.Owner == owner {
			delete(r.services, name)
		}
	}
}

func cloneManifest(manifest contract.Manifest) contract.Manifest {
	manifest.Provides.Services = make([]contract.ServiceDescriptor, len(manifest.Provides.Services))
	for index, service := range manifest.Provides.Services {
		manifest.Provides.Services[index] = cloneDescriptor(service)
	}
	manifest.Provides.Subscriptions = append([]string(nil), manifest.Provides.Subscriptions...)
	manifest.Requires.Services = append([]string(nil), manifest.Requires.Services...)
	manifest.Requires.Permissions = append([]string(nil), manifest.Requires.Permissions...)
	if manifest.Routing != nil {
		routing := *manifest.Routing
		routing.Rules = append([]contract.RoutingRule(nil), routing.Rules...)
		for index := range routing.Rules {
			rule := &routing.Rules[index]
			rule.Match.Headers = cloneStringMap(rule.Match.Headers)
			rule.Action.Targets = append([]string(nil), rule.Action.Targets...)
			rule.Action.Response.FallbackBots = append([]string(nil), rule.Action.Response.FallbackBots...)
		}
		manifest.Routing = &routing
	}
	return manifest
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func cloneDescriptor(descriptor contract.ServiceDescriptor) contract.ServiceDescriptor {
	descriptor.Methods = append([]string(nil), descriptor.Methods...)
	return descriptor
}
