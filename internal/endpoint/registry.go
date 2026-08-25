// Package endpoint 维护接入中间层的协议端与应用端注册表。
// 它是路由器与响应协调器共享读取的端点元数据来源。
package endpoint

import (
	"errors"
	"sync"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

var (
	// ErrEndpointExists 表示端点 ID 已注册。
	ErrEndpointExists = errors.New("端点已注册")
	// ErrEndpointNotFound 表示端点 ID 不存在。
	ErrEndpointNotFound = errors.New("端点不存在")
)

// Registry 是线程安全的端点注册表。
type Registry struct {
	mu        sync.RWMutex
	endpoints map[string]contract.EndpointInfo
}

// New 创建空端点注册表。
func New() *Registry {
	return &Registry{endpoints: make(map[string]contract.EndpointInfo)}
}

// Register 注册一个端点；ID 重复时返回错误。
func (r *Registry) Register(info contract.EndpointInfo) error {
	if info.ID == "" {
		return errors.New("端点 ID 不能为空")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.endpoints[info.ID]; exists {
		return ErrEndpointExists
	}
	r.endpoints[info.ID] = cloneInfo(info)
	return nil
}

// Unregister 移除一个端点。
func (r *Registry) Unregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.endpoints[id]; !exists {
		return ErrEndpointNotFound
	}
	delete(r.endpoints, id)
	return nil
}

// Update 更新已注册端点的元数据；端点不存在时返回错误。
func (r *Registry) Update(info contract.EndpointInfo) error {
	if info.ID == "" {
		return errors.New("端点 ID 不能为空")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.endpoints[info.ID]; !exists {
		return ErrEndpointNotFound
	}
	r.endpoints[info.ID] = cloneInfo(info)
	return nil
}

// UnregisterOwned 移除指定 owner 拥有的端点。
// 该方法用于 Runtime 撤销插件能力后执行生命周期清理。
func (r *Registry) UnregisterOwned(owner, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, exists := r.endpoints[id]
	if !exists {
		return ErrEndpointNotFound
	}
	if info.Owner != owner {
		return errors.New("不能注销其他插件拥有的端点")
	}
	delete(r.endpoints, id)
	return nil
}

// UnregisterOwner 移除指定插件拥有的全部端点。
func (r *Registry) UnregisterOwner(owner string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, info := range r.endpoints {
		if info.Owner == owner {
			delete(r.endpoints, id)
		}
	}
}

// Get 返回端点信息副本。
func (r *Registry) Get(id string) (contract.EndpointInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, exists := r.endpoints[id]
	return cloneInfo(info), exists
}

// ByBotID 返回指定 BotID 的端点副本；protocol 端点优先。
func (r *Registry) ByBotID(botID string) []contract.EndpointInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []contract.EndpointInfo
	for _, info := range r.endpoints {
		if info.BotID == botID {
			result = append(result, cloneInfo(info))
		}
	}
	return result
}

// ByKind 返回指定类型的端点副本。
func (r *Registry) ByKind(kind contract.EndpointKind) []contract.EndpointInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []contract.EndpointInfo
	for _, info := range r.endpoints {
		if info.Kind == kind {
			result = append(result, cloneInfo(info))
		}
	}
	return result
}

// List 返回全部端点副本。
func (r *Registry) List() []contract.EndpointInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]contract.EndpointInfo, 0, len(r.endpoints))
	for _, info := range r.endpoints {
		result = append(result, cloneInfo(info))
	}
	return result
}

func cloneInfo(info contract.EndpointInfo) contract.EndpointInfo {
	info.Capabilities = append([]string(nil), info.Capabilities...)
	return info
}
