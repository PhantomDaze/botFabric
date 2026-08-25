package runtime

import (
	"context"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// PluginInfo 是面向诊断端点的插件只读快照。
type PluginInfo struct {
	Manifest contract.Manifest     `json:"manifest"`
	State    contract.PluginState  `json:"state"`
	Health   contract.HealthStatus `json:"health"`
}

// ListPlugins 返回所有已装载插件的状态与健康快照。
func (m *Manager) ListPlugins(ctx context.Context) []PluginInfo {
	m.mu.Lock()
	plugins := make(map[string]*managedPlugin, len(m.plugins))
	for id, plugin := range m.plugins {
		plugins[id] = plugin
	}
	m.mu.Unlock()

	result := make([]PluginInfo, 0, len(plugins))
	for id, plugin := range plugins {
		state, _ := m.State(id)
		result = append(result, PluginInfo{Manifest: plugin.manifest, State: state, Health: plugin.plugin.Health(ctx)})
	}
	return result
}

// PluginInfo 返回单个插件的诊断快照。
func (m *Manager) PluginInfo(ctx context.Context, pluginID string) (PluginInfo, bool) {
	m.mu.Lock()
	plugin, exists := m.plugins[pluginID]
	m.mu.Unlock()
	if !exists {
		return PluginInfo{}, false
	}
	state, _ := m.State(pluginID)
	return PluginInfo{Manifest: plugin.manifest, State: state, Health: plugin.plugin.Health(ctx)}, true
}
