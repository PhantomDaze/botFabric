package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PhantomDaze/botfabric/internal/config"
	"github.com/PhantomDaze/botfabric/internal/dedup"
	"github.com/PhantomDaze/botfabric/internal/endpoint"
	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/gateway"
	"github.com/PhantomDaze/botfabric/internal/observability"
	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/internal/response"
	"github.com/PhantomDaze/botfabric/internal/routing"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// Manager 负责插件发现后的装载、启停、回滚和健康检查。
type Manager struct {
	mu              sync.Mutex
	reloadMu        sync.Mutex
	registry        *registry.Registry
	eventBus        *eventbus.Bus
	gateway         *gateway.Gateway
	endpoints       *endpoint.Registry
	router          *routing.Router
	coordinator     *response.Coordinator
	dedupStore      dedup.Store // 共享去重存储；nil 时各插件回退到进程内内存实现
	logger          *slog.Logger
	botID           string
	shutdownTimeout time.Duration
	loaders         map[contract.PluginRuntime]contract.PluginLoader
	plugins         map[string]*managedPlugin
	started         []string
	metrics         atomic.Pointer[observability.Metrics]
}

type managedPlugin struct {
	lifecycle sync.Mutex
	plugin    contract.Plugin
	manifest  contract.Manifest
	host      *pluginHost
}

// NewManager 创建插件运行时。
func NewManager(
	registry *registry.Registry,
	eventBus *eventbus.Bus,
	gateway *gateway.Gateway,
	logger *slog.Logger,
	botID string,
	shutdownTimeout time.Duration,
) (*Manager, error) {
	return NewManagerWithEndpoints(registry, eventBus, gateway, endpoint.New(), logger, botID, shutdownTimeout)
}

// NewManagerWithEndpoints 创建带共享端点注册表的插件运行时。
func NewManagerWithEndpoints(
	registry *registry.Registry,
	eventBus *eventbus.Bus,
	gateway *gateway.Gateway,
	endpoints *endpoint.Registry,
	logger *slog.Logger,
	botID string,
	shutdownTimeout time.Duration,
) (*Manager, error) {
	if registry == nil || eventBus == nil || gateway == nil || endpoints == nil {
		return nil, errors.New("注册表、事件总线、服务网关和端点注册表不能为空")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if botID == "" {
		return nil, errors.New("BotID 不能为空")
	}
	if shutdownTimeout <= 0 {
		return nil, errors.New("停机超时必须大于零")
	}
	manager := &Manager{
		registry:        registry,
		eventBus:        eventBus,
		gateway:         gateway,
		endpoints:       endpoints,
		router:          routing.New(),
		logger:          logger,
		botID:           botID,
		shutdownTimeout: shutdownTimeout,
		loaders:         make(map[contract.PluginRuntime]contract.PluginLoader),
		plugins:         make(map[string]*managedPlugin),
	}
	coordinator, err := response.New(endpoints, func(ctx context.Context, caller string, request contract.Request) (contract.Response, error) {
		return gateway.ViewForPlugin(caller).Invoke(ctx, request)
	})
	if err != nil {
		return nil, err
	}
	manager.coordinator = coordinator
	if err := registry.RegisterService(response.ServiceName, contract.ServiceDescriptor{Name: response.ServiceName, Methods: []string{response.SubmitMethod, response.CancelMethod}}, coordinator.HandleServiceRequest); err != nil {
		return nil, fmt.Errorf("注册响应协调服务: %w", err)
	}
	gateway.ActivateOwner(response.ServiceName)
	eventBus.SetRouter(manager.router)
	eventBus.SetResponseHook(coordinator.RegisterEvent)
	return manager, nil
}

// Endpoints 返回运行时共享的端点注册表。
func (m *Manager) Endpoints() *endpoint.Registry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.endpoints
}

// Router 返回运行时共享的路由器。
func (m *Manager) Router() *routing.Router {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.router
}

// ResponseCoordinator 返回运行时共享的响应协调器。
func (m *Manager) ResponseCoordinator() *response.Coordinator {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.coordinator
}

// SetMetrics 注入运行时指标，并同步到事件总线、路由器与响应协调器。
// 应在开始接收流量前调用；传 nil 可关闭指标采集。
func (m *Manager) SetMetrics(metrics *observability.Metrics) {
	m.metrics.Store(metrics)
	m.eventBus.SetMetrics(metrics)
	m.router.SetMetrics(metrics)
	m.coordinator.SetMetrics(metrics)
}

// SetDedupStore 注入共享去重存储（通常为 Redis 实现）。
// 注入后所有启用了去重的插件复用同一存储，实现跨实例去重。
// 应在并发使用前、插件加载前调用；传 nil 清除注入回退到内存实现。
func (m *Manager) SetDedupStore(store dedup.Store) {
	m.mu.Lock()
	m.dedupStore = store
	m.mu.Unlock()
}

// RegisterLoader 注册一种插件执行模型。
func (m *Manager) RegisterLoader(loader contract.PluginLoader) error {
	if loader == nil {
		return errors.New("插件加载器不能为空")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	kind := loader.Kind()
	if _, exists := m.loaders[kind]; exists {
		return fmt.Errorf("执行模型 %s 的加载器已注册", kind)
	}
	m.loaders[kind] = loader
	return nil
}

// Load 校验清单并装载一个插件，但不启动它。
func (m *Manager) Load(manifest contract.Manifest) error {
	if err := config.ValidateManifest(manifest); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.plugins[manifest.ID]; exists {
		return fmt.Errorf("插件 %s 已装载", manifest.ID)
	}
	loader, exists := m.loaders[manifest.Runtime]
	if !exists {
		return fmt.Errorf("未注册执行模型 %s 的加载器", manifest.Runtime)
	}

	host := newPluginHost(
		m.eventBus.ForPlugin(manifest.ID),
		m.gateway.ForPlugin(manifest.ID),
		m.logger.With("plugin_id", manifest.ID),
		m.botID,
		manifest,
	)
	host.endpoints = m.endpoints
	host.router = m.router
	if manifest.Routing != nil {
		if err := m.router.RegisterRules(manifest.ID, manifest.Routing); err != nil {
			return fmt.Errorf("注册插件 %s 的路由规则: %w", manifest.ID, err)
		}
	}
	plugin, err := loader.Load(manifest, host)
	if err != nil {
		m.router.UnregisterRules(manifest.ID)
		return fmt.Errorf("装载插件 %s: %w", manifest.ID, err)
	}
	entry := registry.PluginEntry{
		Manifest: manifest,
		State:    contract.PluginDiscovered,
		Plugin:   plugin,
	}
	if err := m.registry.RegisterPlugin(entry); err != nil {
		m.router.UnregisterRules(manifest.ID)
		return fmt.Errorf("注册插件 %s: %w", manifest.ID, err)
	}
	if err := m.registry.SetPluginState(manifest.ID, contract.PluginValidated); err != nil {
		m.registry.RemovePlugin(manifest.ID)
		m.router.UnregisterRules(manifest.ID)
		return err
	}
	m.plugins[manifest.ID] = &managedPlugin{plugin: plugin, manifest: manifest, host: host}
	if err := m.refreshDeduplicator(); err != nil {
		delete(m.plugins, manifest.ID)
		m.registry.RemovePlugin(manifest.ID)
		m.router.UnregisterRules(manifest.ID)
		return err
	}
	return nil
}

// StartAll 按服务依赖拓扑顺序启动全部已装载插件。
func (m *Manager) StartAll(ctx context.Context) error {
	ids, err := m.startOrder()
	if err != nil {
		return err
	}

	var started []string
	for _, id := range ids {
		if err := m.Start(ctx, id); err != nil {
			for index := len(started) - 1; index >= 0; index-- {
				_ = m.Stop(context.Background(), started[index])
			}
			return err
		}
		started = append(started, id)
	}
	return nil
}

// Start 启动一个插件；失败时回滚其运行时注册。
func (m *Manager) Start(ctx context.Context, pluginID string) error {
	m.mu.Lock()
	managed, exists := m.plugins[pluginID]
	m.mu.Unlock()
	if !exists {
		return fmt.Errorf("插件 %s 未装载", pluginID)
	}
	managed.lifecycle.Lock()
	defer managed.lifecycle.Unlock()
	entry, exists := m.registry.Plugin(pluginID)
	if !exists {
		return fmt.Errorf("插件 %s 未注册", pluginID)
	}
	if entry.State != contract.PluginValidated {
		return fmt.Errorf("插件 %s 当前状态 %s 不允许启动", pluginID, entry.State)
	}
	if err := m.registry.SetPluginState(pluginID, contract.PluginStarting); err != nil {
		return err
	}

	timeout := managed.manifest.Limits.Timeout
	startCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	completed := make(chan error, 1)
	go func() { completed <- managed.plugin.Start(startCtx, managed.host) }()

	var startErr error
	select {
	case startErr = <-completed:
	case <-startCtx.Done():
		startErr = startCtx.Err()
	}
	if startErr == nil {
		startErr = m.validateRegisteredServices(managed.manifest)
	}
	if startErr != nil {
		if metrics := m.metrics.Load(); metrics != nil {
			metrics.PluginStartFailure()
		}
		managed.host.revoke()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), m.shutdownTimeout)
		_ = m.deactivateOwner(cleanupCtx, pluginID)
		cleanupCancel()
		m.rollback(pluginID)
		_ = m.registry.SetPluginState(pluginID, contract.PluginFailed)
		return fmt.Errorf("启动插件 %s: %w", pluginID, startErr)
	}
	if err := m.registry.SetPluginState(pluginID, contract.PluginRunning); err != nil {
		managed.host.revoke()
		m.rollback(pluginID)
		return err
	}
	m.eventBus.ActivateOwner(pluginID)
	m.gateway.ActivateOwner(pluginID)
	m.mu.Lock()
	m.started = append(m.started, pluginID)
	m.mu.Unlock()
	m.logger.Info("插件已启动", "plugin_id", pluginID, "version", managed.manifest.Version)
	return nil
}

// NotifyPluginExit 处理外部插件进程异常退出：注销其服务、订阅与端点，
// 并把手握状态迁移为 failed，避免 Runtime 继续把请求路由到已死进程。
// 自动重启策略留给上层编排，这里只保证状态与资源一致。
func (m *Manager) NotifyPluginExit(pluginID string, err error) {
	m.mu.Lock()
	managed, exists := m.plugins[pluginID]
	m.mu.Unlock()
	if !exists {
		return
	}
	// lifecycle 锁与 Start/Stop 串行：Start 结束前插件状态尚未到 running，
	// Stop 进行中则说明是主动停止，均忽略。
	managed.lifecycle.Lock()
	defer managed.lifecycle.Unlock()
	entry, exists := m.registry.Plugin(pluginID)
	if !exists || entry.State != contract.PluginRunning {
		return
	}
	m.logger.Error("外部插件进程异常退出，回滚运行时资源", "plugin_id", pluginID, "error", err)
	managed.host.revoke()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), m.shutdownTimeout)
	_ = m.deactivateOwner(cleanupCtx, pluginID)
	cancel()
	m.rollback(pluginID)
	m.mu.Lock()
	m.started = removeID(m.started, pluginID)
	m.mu.Unlock()
	_ = m.registry.SetPluginState(pluginID, contract.PluginFailed)
}

// Stop 在超时预算内停止一个插件并清理其注册。
func (m *Manager) Stop(ctx context.Context, pluginID string) error {
	m.mu.Lock()
	managed, exists := m.plugins[pluginID]
	m.mu.Unlock()
	if !exists {
		return fmt.Errorf("插件 %s 未装载", pluginID)
	}
	managed.lifecycle.Lock()
	defer managed.lifecycle.Unlock()
	entry, exists := m.registry.Plugin(pluginID)
	if !exists || entry.State == contract.PluginStopped || entry.State == contract.PluginFailed || entry.State == contract.PluginValidated {
		return nil
	}
	_ = m.registry.SetPluginState(pluginID, contract.PluginStopping)
	managed.host.revoke()
	stopCtx, cancel := context.WithTimeout(ctx, m.shutdownTimeout)
	defer cancel()
	stopErr := m.deactivateOwner(stopCtx, pluginID)
	m.rollback(pluginID)
	completed := make(chan error, 1)
	go func() { completed <- managed.plugin.Stop(stopCtx) }()

	select {
	case pluginErr := <-completed:
		stopErr = errors.Join(stopErr, pluginErr)
	case <-stopCtx.Done():
		stopErr = errors.Join(stopErr, stopCtx.Err())
		if forceStopper, ok := managed.plugin.(contract.ForceStopper); ok {
			if forceErr := forceStopper.ForceStop(); forceErr != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("强制终止插件: %w", forceErr))
			}
		}
	}
	if stopErr != nil {
		_ = m.registry.SetPluginState(pluginID, contract.PluginFailed)
	} else {
		_ = m.registry.SetPluginState(pluginID, contract.PluginStopped)
	}
	m.mu.Lock()
	m.started = removeID(m.started, pluginID)
	m.mu.Unlock()
	if stopErr != nil {
		return fmt.Errorf("停止插件 %s: %w", pluginID, stopErr)
	}
	m.logger.Info("插件已停止", "plugin_id", pluginID)
	return nil
}

// Unload 安全停止并移除一个插件。
func (m *Manager) Unload(ctx context.Context, pluginID string) error {
	m.mu.Lock()
	managed, exists := m.plugins[pluginID]
	m.mu.Unlock()
	if !exists {
		return fmt.Errorf("插件 %s 未装载", pluginID)
	}
	if dependent := m.runningDependent(pluginID); dependent != "" {
		return fmt.Errorf("插件 %s 仍被运行中的插件 %s 依赖", pluginID, dependent)
	}

	state, _ := m.State(pluginID)
	if state == contract.PluginRunning {
		if err := m.Stop(ctx, pluginID); err != nil {
			return err
		}
	} else if state != contract.PluginStopped && state != contract.PluginFailed && state != contract.PluginValidated {
		return fmt.Errorf("插件 %s 当前状态 %s 不允许卸载", pluginID, state)
	}
	if state == contract.PluginFailed {
		managed.host.revoke()
		cleanupCtx, cancel := context.WithTimeout(ctx, m.shutdownTimeout)
		defer cancel()
		_ = m.deactivateOwner(cleanupCtx, pluginID)
		m.rollback(pluginID)
	} else if state == contract.PluginStopped || state == contract.PluginValidated {
		m.router.UnregisterRules(pluginID)
	}
	m.refreshDeduplicator()
	m.mu.Lock()
	delete(m.plugins, pluginID)
	m.started = removeID(m.started, pluginID)
	m.mu.Unlock()
	m.registry.RemovePlugin(pluginID)
	return nil
}

func (m *Manager) runningDependent(providerID string) string {
	m.mu.Lock()
	plugins := make(map[string]*managedPlugin, len(m.plugins))
	for id, plugin := range m.plugins {
		plugins[id] = plugin
	}
	m.mu.Unlock()
	for id, plugin := range plugins {
		if id == providerID {
			continue
		}
		state, _ := m.State(id)
		if state != contract.PluginRunning {
			continue
		}
		for _, service := range plugin.manifest.Requires.Services {
			entry, exists := m.registry.Service(service)
			if exists && entry.Owner == providerID {
				return id
			}
		}
	}
	return ""
}

// Reload 用新实例成功启动后替换旧插件；新实例失败时旧实例保持运行。
func (m *Manager) Reload(ctx context.Context, manifest contract.Manifest) error {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	if err := config.ValidateManifest(manifest); err != nil {
		return err
	}
	m.mu.Lock()
	old, exists := m.plugins[manifest.ID]
	m.mu.Unlock()
	if !exists {
		return fmt.Errorf("插件 %s 未装载", manifest.ID)
	}
	if old.manifest.Runtime != manifest.Runtime || old.manifest.Entry != manifest.Entry {
		return fmt.Errorf("重载不能改变插件 %s 的 runtime 或 entry", manifest.ID)
	}
	m.mu.Lock()
	loader := m.loaders[manifest.Runtime]
	m.mu.Unlock()
	if loader == nil {
		return fmt.Errorf("未注册执行模型 %s 的加载器", manifest.Runtime)
	}

	stagingHost, captureBus, captureGateway, captureEndpoints, captureClassifiers := newStagingHost(manifest, nil, nil, m.logger.With("plugin_id", manifest.ID, "staging", true), m.botID)
	// 只做编译预校验，不提前替换线上规则：失败重载不得改变旧实例的生效路由。
	if err := m.router.ValidateRules(manifest.ID, manifest.Routing); err != nil {
		stagingHost.revoke()
		return fmt.Errorf("准备重载插件 %s 的路由规则: %w", manifest.ID, err)
	}
	staged, err := loader.Load(manifest, stagingHost)
	if err != nil {
		stagingHost.revoke()
		return fmt.Errorf("装载重载插件 %s: %w", manifest.ID, err)
	}
	startCtx, cancel := context.WithTimeout(ctx, manifest.Limits.Timeout)
	startErr := runPluginStart(startCtx, staged, stagingHost)
	cancel()
	if startErr != nil {
		m.cleanupStaging(staged, stagingHost)
		return fmt.Errorf("启动重载插件 %s: %w", manifest.ID, startErr)
	}
	if err := validateStagingResources(manifest, captureGateway.Specs(), captureBus.Specs()); err != nil {
		m.cleanupStaging(staged, stagingHost)
		return err
	}
	if err := validateStagingCapabilities(manifest, captureEndpoints.Specs(), captureClassifiers.Specs()); err != nil {
		m.cleanupStaging(staged, stagingHost)
		return err
	}
	if err := m.cutover(ctx, old, staged, manifest, stagingHost, captureBus.Specs(), captureGateway.Specs(), captureEndpoints.Specs(), captureClassifiers.Specs()); err != nil {
		m.cleanupStaging(staged, stagingHost)
		return err
	}
	return nil
}

// cleanupStaging 在重载失败时撤销暂存宿主并带超时停止新实例，避免 Stop 卡死永久持锁。
func (m *Manager) cleanupStaging(staged contract.Plugin, host *pluginHost) {
	host.revoke()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), m.shutdownTimeout)
	defer cancel()
	completed := make(chan error, 1)
	go func() { completed <- staged.Stop(cleanupCtx) }()
	select {
	case <-completed:
	case <-cleanupCtx.Done():
		if forceStopper, ok := staged.(contract.ForceStopper); ok {
			_ = forceStopper.ForceStop()
		}
	}
}

func runPluginStart(ctx context.Context, plugin contract.Plugin, host contract.Host) error {
	completed := make(chan error, 1)
	go func() { completed <- plugin.Start(ctx, host) }()
	select {
	case err := <-completed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) cutover(ctx context.Context, old *managedPlugin, staged contract.Plugin, manifest contract.Manifest, stagingHost *pluginHost, subscriptions []eventbus.SubscriptionSpec, services []registry.ServiceSpec, endpoints []contract.EndpointInfo, classifiers []routing.ClassifierSpec) error {
	old.lifecycle.Lock()
	defer old.lifecycle.Unlock()
	state, _ := m.State(manifest.ID)
	if state != contract.PluginRunning {
		return fmt.Errorf("插件 %s 在重载期间已不再运行", manifest.ID)
	}
	old.host.revoke()
	stopCtx, cancel := context.WithTimeout(ctx, m.shutdownTimeout)
	defer cancel()
	if err := m.deactivateOwner(stopCtx, manifest.ID); err != nil {
		old.host.active.Store(true)
		m.eventBus.ActivateOwner(manifest.ID)
		m.gateway.ActivateOwner(manifest.ID)
		return fmt.Errorf("重载切换前无法排空旧插件: %w", err)
	}
	oldServices := m.registry.SnapshotServices(manifest.ID)
	oldSubscriptions := m.eventBus.SnapshotOwner(manifest.ID)
	oldEndpoints := endpointsOwnedBy(m.endpoints.List(), manifest.ID)
	oldClassifiers := m.router.SnapshotClassifiers(manifest.ID)
	// 旧实例已排空但尚未停止，先 fence 其清理调用，防止 Stop 删除新实例接管的同名资源。
	old.host.fenced.Store(true)
	restore := func() {
		old.host.fenced.Store(false)
		m.restorePluginResources(manifest.ID, oldEndpoints, oldClassifiers)
		m.restoreOwner(manifest.ID, oldServices, oldSubscriptions)
	}
	if err := m.registry.ReplaceServices(manifest.ID, services); err != nil {
		restore()
		return err
	}
	if _, err := m.eventBus.ReplaceOwner(manifest.ID, subscriptions); err != nil {
		restore()
		return err
	}
	if err := m.replacePluginEndpoints(manifest.ID, endpoints); err != nil {
		restore()
		return err
	}
	if err := m.replacePluginClassifiers(manifest.ID, classifiers); err != nil {
		restore()
		return err
	}
	if err := stagingHost.switchTo(m.eventBus.ViewForPlugin(manifest.ID), m.gateway.ViewForPlugin(manifest.ID), m.endpoints, m.router); err != nil {
		restore()
		return err
	}
	// 规则在切换瞬间才替换；失败时旧规则仍在路由器中，旧实例继续使用。
	if err := m.router.RegisterRules(manifest.ID, manifest.Routing); err != nil {
		restore()
		return err
	}
	if err := old.plugin.Stop(stopCtx); err != nil {
		m.logger.Warn("旧插件停止失败，但新实例已接管", "plugin_id", manifest.ID, "error", err)
	}
	m.mu.Lock()
	m.plugins[manifest.ID] = &managedPlugin{plugin: staged, manifest: manifest, host: stagingHost}
	m.mu.Unlock()
	m.eventBus.ActivateOwner(manifest.ID)
	m.gateway.ActivateOwner(manifest.ID)
	_ = m.registry.SetPluginState(manifest.ID, contract.PluginRunning)
	return nil
}

// endpointsOwnedBy 过滤指定 owner 的端点快照。
func endpointsOwnedBy(infos []contract.EndpointInfo, owner string) []contract.EndpointInfo {
	result := make([]contract.EndpointInfo, 0, len(infos))
	for _, info := range infos {
		if info.Owner == owner {
			result = append(result, info)
		}
	}
	return result
}

// replacePluginEndpoints 用新实例捕获的端点替换旧实例端点；失败时清空已写入部分。
func (m *Manager) replacePluginEndpoints(owner string, endpoints []contract.EndpointInfo) error {
	m.endpoints.UnregisterOwner(owner)
	for _, info := range endpoints {
		if err := m.endpoints.Register(info); err != nil {
			m.endpoints.UnregisterOwner(owner)
			return fmt.Errorf("迁移插件 %s 的端点 %s: %w", owner, info.ID, err)
		}
	}
	return nil
}

// replacePluginClassifiers 用新实例捕获的分类器替换旧实例分类器；失败时清空已写入部分。
func (m *Manager) replacePluginClassifiers(owner string, classifiers []routing.ClassifierSpec) error {
	m.router.UnregisterClassifiersByOwner(owner)
	for _, spec := range classifiers {
		if err := m.router.RegisterClassifierForOwner(owner, spec.ID, spec.Classifier); err != nil {
			m.router.UnregisterClassifiersByOwner(owner)
			return fmt.Errorf("迁移插件 %s 的分类器 %s: %w", owner, spec.ID, err)
		}
	}
	return nil
}

// restorePluginResources 回滚失败切换前的端点与分类器。
func (m *Manager) restorePluginResources(owner string, endpoints []contract.EndpointInfo, classifiers []routing.ClassifierSpec) {
	m.endpoints.UnregisterOwner(owner)
	for _, info := range endpoints {
		_ = m.endpoints.Register(info)
	}
	m.router.UnregisterClassifiersByOwner(owner)
	for _, spec := range classifiers {
		_ = m.router.RegisterClassifierForOwner(owner, spec.ID, spec.Classifier)
	}
}

func (m *Manager) StopAll(ctx context.Context) error {
	m.mu.Lock()
	ids := append([]string(nil), m.started...)
	m.mu.Unlock()
	var failures []error
	for index := len(ids) - 1; index >= 0; index-- {
		if err := m.Stop(ctx, ids[index]); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// Health 返回当前已装载插件的健康状态。
func (m *Manager) Health(ctx context.Context) map[string]contract.HealthStatus {
	m.mu.Lock()
	plugins := make(map[string]contract.Plugin, len(m.plugins))
	for id, managed := range m.plugins {
		plugins[id] = managed.plugin
	}
	m.mu.Unlock()

	statuses := make(map[string]contract.HealthStatus, len(plugins))
	for id, plugin := range plugins {
		statuses[id] = plugin.Health(ctx)
	}
	return statuses
}

// State 返回指定插件的当前生命周期状态。
func (m *Manager) State(pluginID string) (contract.PluginState, bool) {
	entry, exists := m.registry.Plugin(pluginID)
	return entry.State, exists
}

func (m *Manager) restoreOwner(owner string, services []registry.ServiceSpec, subscriptions []eventbus.SubscriptionSpec) {
	_ = m.registry.ReplaceServices(owner, services)
	_, _ = m.eventBus.ReplaceOwner(owner, subscriptions)
	m.eventBus.ActivateOwner(owner)
	m.gateway.ActivateOwner(owner)
	m.mu.Lock()
	if managed, exists := m.plugins[owner]; exists {
		managed.host.active.Store(true)
	}
	m.mu.Unlock()
}

func (m *Manager) validateRegisteredServices(manifest contract.Manifest) error {
	for _, descriptor := range manifest.Provides.Services {
		entry, exists := m.registry.Service(descriptor.Name)
		if !exists || entry.Owner != manifest.ID {
			return fmt.Errorf("插件 %s 未注册声明的服务 %s", manifest.ID, descriptor.Name)
		}
		if !sameStringSet(makeStringSet(descriptor.Methods), entry.Descriptor.Methods) {
			return fmt.Errorf("插件 %s 注册服务 %s 的方法与清单不一致", manifest.ID, descriptor.Name)
		}
	}
	return nil
}

func (m *Manager) deactivateOwner(ctx context.Context, pluginID string) error {
	m.eventBus.DeactivateOwner(pluginID)
	m.gateway.DeactivateOwner(pluginID)

	var joined error
	if err := m.eventBus.WaitOwner(ctx, pluginID); err != nil {
		joined = errors.Join(joined, fmt.Errorf("等待事件处理完成: %w", err))
	}
	if err := m.gateway.WaitOwner(ctx, pluginID); err != nil {
		joined = errors.Join(joined, fmt.Errorf("等待服务调用完成: %w", err))
	}
	return joined
}

func (m *Manager) refreshDeduplicator() error {
	for _, managed := range m.plugins {
		if managed.manifest.Routing == nil || !managed.manifest.Routing.Dedup.Enabled {
			continue
		}
		// 优先使用注入的共享 store（分布式）；未注入时回退到进程内内存实现。
		store := m.dedupStore
		if store == nil {
			store = dedup.NewMemoryStore()
		}
		deduplicator, err := dedup.New(store, managed.manifest.Routing.Dedup)
		if err != nil {
			return fmt.Errorf("插件 %s 的去重配置无效: %w", managed.manifest.ID, err)
		}
		m.eventBus.SetDeduplicator(deduplicator)
		return nil
	}
	m.eventBus.SetDeduplicator(nil)
	return nil
}

func (m *Manager) rollback(pluginID string) {
	m.eventBus.UnsubscribeOwner(pluginID)
	m.registry.UnregisterServicesByOwner(pluginID)
	m.endpoints.UnregisterOwner(pluginID)
	m.router.UnregisterRules(pluginID)
	m.router.UnregisterClassifiersByOwner(pluginID)
}

func (m *Manager) startOrder() ([]string, error) {
	m.mu.Lock()
	manifests := make(map[string]contract.Manifest, len(m.plugins))
	for id, managed := range m.plugins {
		manifests[id] = managed.manifest
	}
	m.mu.Unlock()

	providers := make(map[string]string)
	for id, manifest := range manifests {
		for _, service := range manifest.Provides.Services {
			if existing, exists := providers[service.Name]; exists {
				return nil, fmt.Errorf("服务 %s 由插件 %s 和 %s 重复提供", service.Name, existing, id)
			}
			providers[service.Name] = id
		}
	}

	dependencies := make(map[string]map[string]struct{}, len(manifests))
	dependents := make(map[string][]string, len(manifests))
	for id, manifest := range manifests {
		dependencies[id] = make(map[string]struct{})
		for _, service := range manifest.Requires.Services {
			if service == response.ServiceName {
				continue
			}
			provider, exists := providers[service]
			if !exists {
				return nil, fmt.Errorf("插件 %s 依赖未提供的服务 %s", id, service)
			}
			if provider == id {
				continue
			}
			if _, exists := dependencies[id][provider]; !exists {
				dependencies[id][provider] = struct{}{}
				dependents[provider] = append(dependents[provider], id)
			}
		}
	}

	ready := make([]string, 0, len(manifests))
	for id, deps := range dependencies {
		if len(deps) == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)

	order := make([]string, 0, len(manifests))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		for _, dependent := range dependents[id] {
			delete(dependencies[dependent], id)
			if len(dependencies[dependent]) == 0 {
				ready = append(ready, dependent)
				sort.Strings(ready)
			}
		}
	}
	if len(order) != len(manifests) {
		return nil, errors.New("插件服务依赖存在循环")
	}
	return order, nil
}

func removeID(ids []string, wanted string) []string {
	for index, id := range ids {
		if id == wanted {
			return append(ids[:index], ids[index+1:]...)
		}
	}
	return ids
}

type pluginHost struct {
	eventBus         contract.EventBus
	gateway          contract.ServiceGateway
	logger           contract.Logger
	botID            string
	owner            string
	endpoints        endpointRegistrar
	router           *routing.Router
	classifiers      classifierRegistrar
	allowedSubscribe map[string]struct{}
	allowedInvoke    map[string]struct{}
	allowedProvide   map[string]map[string]struct{}
	active           atomic.Bool
	// fenced 在热重载切换后置位：旧实例的清理调用不得再触碰已迁移给新实例的资源。
	fenced atomic.Bool
}

func newPluginHost(
	eventBus contract.EventBus,
	gateway contract.ServiceGateway,
	logger contract.Logger,
	botID string,
	manifest contract.Manifest,
) *pluginHost {
	host := &pluginHost{
		eventBus:         eventBus,
		gateway:          gateway,
		logger:           logger,
		botID:            botID,
		owner:            manifest.ID,
		endpoints:        nil,
		router:           nil,
		classifiers:      nil,
		allowedSubscribe: makeStringSet(manifest.Provides.Subscriptions),
		allowedInvoke:    makeStringSet(manifest.Requires.Services),
		allowedProvide:   make(map[string]map[string]struct{}, len(manifest.Provides.Services)),
	}
	for _, service := range manifest.Provides.Services {
		host.allowedProvide[service.Name] = makeStringSet(service.Methods)
	}
	host.active.Store(true)
	return host
}

func (h *pluginHost) EventBus() contract.EventBus {
	return &guardedEventBus{host: h, delegate: h.eventBus}
}

func (h *pluginHost) Gateway() contract.ServiceGateway {
	return &guardedGateway{host: h, delegate: h.gateway}
}

func (h *pluginHost) Logger() contract.Logger { return h.logger }
func (h *pluginHost) BotID() string           { return h.botID }
func (h *pluginHost) RegisterEndpoint(info contract.EndpointInfo) error {
	if err := h.ensureActive(); err != nil {
		return err
	}
	if h.endpoints == nil {
		return &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "端点注册表不可用"}
	}
	if info.Owner == "" {
		info.Owner = h.owner
	}
	return h.endpoints.Register(info)
}
func (h *pluginHost) UnregisterEndpoint(id string) error {
	// 热重载完成后旧实例已 fenced，其清理不得删除新实例接管的同名端点。
	if h.fenced.Load() {
		return nil
	}
	if h.endpoints == nil {
		return &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "端点注册表不可用"}
	}
	if err := h.endpoints.UnregisterOwned(h.owner, id); errors.Is(err, endpoint.ErrEndpointNotFound) {
		return nil
	} else {
		return err
	}
}
func (h *pluginHost) RegisterRouteClassifier(id string, classifier contract.RouteClassifier) error {
	if err := h.ensureActive(); err != nil {
		return err
	}
	// 暂存宿主优先写入捕获器，切换成功后再迁移到真实路由器。
	if h.classifiers != nil {
		return h.classifiers.RegisterClassifierForOwner(h.owner, id, classifier)
	}
	if h.router == nil {
		return &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "路由器不可用"}
	}
	return h.router.RegisterClassifierForOwner(h.owner, id, classifier)
}

func (h *pluginHost) UnregisterRouteClassifier(id string) error {
	if h.fenced.Load() {
		return nil
	}
	if h.classifiers != nil {
		return h.classifiers.UnregisterClassifier(id)
	}
	if h.router == nil {
		return &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "路由器不可用"}
	}
	if err := h.router.UnregisterClassifier(id); err != nil {
		return err
	}
	return nil
}

func (h *pluginHost) revoke() { h.active.Store(false) }

func (h *pluginHost) ensureActive() error {
	if !h.active.Load() {
		return &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "插件宿主能力已撤销"}
	}
	return nil
}

func makeStringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func sameStringSet(expected map[string]struct{}, actual []string) bool {
	if len(expected) != len(actual) {
		return false
	}
	for _, value := range actual {
		if _, exists := expected[value]; !exists {
			return false
		}
	}
	return true
}

type guardedEventBus struct {
	host     *pluginHost
	delegate contract.EventBus
}

func (b *guardedEventBus) Publish(ctx context.Context, event contract.Event) error {
	if err := b.host.ensureActive(); err != nil {
		return err
	}
	return b.delegate.Publish(ctx, event)
}

func (b *guardedEventBus) Subscribe(topic string, handler contract.EventHandler, opts ...contract.SubOption) (contract.Subscription, error) {
	if err := b.host.ensureActive(); err != nil {
		return nil, err
	}
	if _, allowed := b.host.allowedSubscribe[topic]; !allowed {
		return nil, &contract.ServiceError{Code: contract.ErrorPermissionDenied, Message: "插件清单未声明订阅 Topic " + topic}
	}
	subscription, err := b.delegate.Subscribe(topic, handler, opts...)
	if err != nil {
		return nil, err
	}
	if err := b.host.ensureActive(); err != nil {
		_ = subscription.Unsubscribe()
		return nil, err
	}
	return subscription, nil
}

type guardedGateway struct {
	host     *pluginHost
	delegate contract.ServiceGateway
}

func (g *guardedGateway) Invoke(ctx context.Context, request contract.Request) (contract.Response, error) {
	if err := g.host.ensureActive(); err != nil {
		serviceError, _ := err.(*contract.ServiceError)
		return contract.Response{Error: serviceError}, err
	}
	if _, allowed := g.host.allowedInvoke[request.Service]; !allowed {
		serviceError := &contract.ServiceError{Code: contract.ErrorPermissionDenied, Message: "插件清单未声明依赖服务 " + request.Service}
		return contract.Response{Error: serviceError}, serviceError
	}
	return g.delegate.Invoke(ctx, request)
}

func (g *guardedGateway) Register(service contract.ServiceDescriptor, handler contract.ServiceHandler) error {
	if err := g.host.ensureActive(); err != nil {
		return err
	}
	methods, allowed := g.host.allowedProvide[service.Name]
	if !allowed || !sameStringSet(methods, service.Methods) {
		return &contract.ServiceError{Code: contract.ErrorPermissionDenied, Message: "插件清单未声明该服务及方法集合"}
	}
	if err := g.delegate.Register(service, handler); err != nil {
		return err
	}
	if err := g.host.ensureActive(); err != nil {
		_ = g.delegate.Unregister(service.Name)
		return err
	}
	return nil
}

func (g *guardedGateway) Unregister(serviceID string) error {
	if err := g.host.ensureActive(); err != nil {
		return err
	}
	return g.delegate.Unregister(serviceID)
}
