package registry

import (
	"fmt"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// ServiceSpec 是可暂存和切换的服务资源。
type ServiceSpec struct {
	Descriptor contract.ServiceDescriptor
	Handler    contract.ServiceHandler
}

// SnapshotServices 返回指定所有者的服务资源副本。
func (r *Registry) SnapshotServices(owner string) []ServiceSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []ServiceSpec
	for _, entry := range r.services {
		if entry.Owner == owner {
			result = append(result, ServiceSpec{Descriptor: cloneDescriptor(entry.Descriptor), Handler: entry.Handler})
		}
	}
	return result
}

// ReplaceServices 原子替换指定所有者的全部服务资源。
func (r *Registry) ReplaceServices(owner string, specs []ServiceSpec) error {
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		if spec.Descriptor.Name == "" || len(spec.Descriptor.Methods) == 0 || spec.Handler == nil {
			return fmt.Errorf("暂存服务资源无效")
		}
		if _, exists := seen[spec.Descriptor.Name]; exists {
			return fmt.Errorf("暂存服务重复: %s", spec.Descriptor.Name)
		}
		seen[spec.Descriptor.Name] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for name, entry := range r.services {
		if entry.Owner != owner {
			if _, replacing := seen[name]; replacing {
				return fmt.Errorf("服务 %s 已由插件 %s 提供", name, entry.Owner)
			}
		}
	}
	for name, entry := range r.services {
		if entry.Owner == owner {
			delete(r.services, name)
		}
	}
	for _, spec := range specs {
		r.services[spec.Descriptor.Name] = ServiceEntry{Descriptor: cloneDescriptor(spec.Descriptor), Owner: owner, Handler: spec.Handler}
	}
	return nil
}
