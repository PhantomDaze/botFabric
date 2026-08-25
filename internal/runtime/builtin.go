package runtime

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// BuiltinLoader 通过编译期工厂注册表加载同进程插件。
type BuiltinLoader struct {
	mu        sync.RWMutex
	factories map[string]contract.PluginFactory
}

// NewBuiltinLoader 创建空的同进程加载器。
func NewBuiltinLoader() *BuiltinLoader {
	return &BuiltinLoader{factories: make(map[string]contract.PluginFactory)}
}

// Kind 返回加载器处理的执行模型。
func (l *BuiltinLoader) Kind() contract.PluginRuntime {
	return contract.RuntimeInProcess
}

// RegisterFactory 注册插件工厂。
func (l *BuiltinLoader) RegisterFactory(pluginID string, factory contract.PluginFactory) error {
	if pluginID == "" || factory == nil {
		return errors.New("插件 ID 和工厂不能为空")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.factories[pluginID]; exists {
		return fmt.Errorf("插件工厂 %s 已注册", pluginID)
	}
	l.factories[pluginID] = factory
	return nil
}

// Load 由清单 Entry 选择编译进二进制的插件工厂。
func (l *BuiltinLoader) Load(manifest contract.Manifest, _ contract.Host) (contract.Plugin, error) {
	l.mu.RLock()
	factory, exists := l.factories[manifest.Entry]
	l.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("未找到同进程插件入口 %s", manifest.Entry)
	}
	plugin := factory()
	if plugin == nil {
		return nil, fmt.Errorf("插件工厂 %s 返回 nil", manifest.Entry)
	}
	actual := plugin.Manifest()
	if !manifestsEqual(actual, manifest) {
		return nil, fmt.Errorf("插件入口 %s 的内置清单与配置清单不一致", manifest.Entry)
	}
	return plugin, nil
}

func manifestsEqual(left, right contract.Manifest) bool {
	if left.ID != right.ID || left.Name != right.Name || left.Version != right.Version ||
		left.Runtime != right.Runtime || left.Entry != right.Entry || left.Limits != right.Limits {
		return false
	}
	if !reflect.DeepEqual(left.Routing, right.Routing) {
		return false
	}
	if !slices.Equal(left.Provides.Subscriptions, right.Provides.Subscriptions) ||
		!slices.Equal(left.Requires.Services, right.Requires.Services) ||
		!slices.Equal(left.Requires.Permissions, right.Requires.Permissions) ||
		len(left.Provides.Services) != len(right.Provides.Services) {
		return false
	}
	for index, leftService := range left.Provides.Services {
		rightService := right.Provides.Services[index]
		if leftService.Name != rightService.Name || !slices.Equal(leftService.Methods, rightService.Methods) {
			return false
		}
	}
	return true
}
