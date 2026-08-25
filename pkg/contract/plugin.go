package contract

import (
	"context"
	"time"
)

// PluginRuntime 是插件的执行模型。
type PluginRuntime string

const (
	// RuntimeInProcess 表示插件与核心运行时编译在同一进程中。
	RuntimeInProcess PluginRuntime = "in_process"
	// RuntimeProcess 表示插件由核心运行时托管为独立进程。
	RuntimeProcess PluginRuntime = "process"
)

// Plugin 是所有插件必须实现的生命周期契约。
type Plugin interface {
	Manifest() Manifest
	Start(ctx context.Context, host Host) error
	Stop(ctx context.Context) error
	Health(ctx context.Context) HealthStatus
}

// ForceStopper 由能够被运行时强制终止的外部插件实现。
type ForceStopper interface {
	ForceStop() error
}

// Host 是运行时按插件身份授予的最小能力集合。
type Host interface {
	EventBus() EventBus
	Gateway() ServiceGateway
	Logger() Logger
	BotID() string
}

// Logger 是插件可使用的结构化日志能力。
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// PluginFactory 构造一个同进程插件实例。
type PluginFactory func() Plugin

// PluginLoader 按执行模型装载插件。
type PluginLoader interface {
	Kind() PluginRuntime
	Load(manifest Manifest, host Host) (Plugin, error)
}

// Manifest 是经过解析和校验的插件清单。
type Manifest struct {
	ID       string        `json:"id" yaml:"id"`
	Name     string        `json:"name" yaml:"name"`
	Version  string        `json:"version" yaml:"version"`
	Runtime  PluginRuntime `json:"runtime" yaml:"runtime"`
	Entry    string        `json:"entry" yaml:"entry"`
	Provides Provides      `json:"provides" yaml:"provides"`
	Requires Requires      `json:"requires" yaml:"requires"`
	Limits   Limits        `json:"limits" yaml:"limits"`
	Routing  *RoutingSpec  `json:"routing,omitempty" yaml:"routing,omitempty"`
}

// Provides 描述插件对宿主公开的能力。
type Provides struct {
	Services      []ServiceDescriptor `json:"services" yaml:"services"`
	Subscriptions []string            `json:"subscriptions" yaml:"subscriptions"`
}

// Requires 描述插件依赖的服务和权限。
type Requires struct {
	Services    []string `json:"services" yaml:"services"`
	Permissions []string `json:"permissions" yaml:"permissions"`
}

// Limits 描述运行时对插件施加的资源与调用限制。
type Limits struct {
	Timeout time.Duration `json:"timeout" yaml:"-"`
	Memory  string        `json:"memory" yaml:"memory"`
}

// HealthState 是插件的健康状态。
type HealthState string

const (
	HealthUnknown   HealthState = "unknown"
	HealthHealthy   HealthState = "healthy"
	HealthDegraded  HealthState = "degraded"
	HealthUnhealthy HealthState = "unhealthy"
)

// HealthStatus 是一次插件健康检查结果。
type HealthStatus struct {
	State     HealthState       `json:"state" yaml:"state"`
	Message   string            `json:"message,omitempty" yaml:"message,omitempty"`
	CheckedAt time.Time         `json:"checked_at" yaml:"checked_at"`
	Details   map[string]string `json:"details,omitempty" yaml:"details,omitempty"`
}

// PluginState 是由运行时维护的插件生命周期状态。
type PluginState string

const (
	PluginDiscovered PluginState = "discovered"
	PluginValidated  PluginState = "validated"
	PluginStarting   PluginState = "starting"
	PluginRunning    PluginState = "running"
	PluginStopping   PluginState = "stopping"
	PluginStopped    PluginState = "stopped"
	PluginFailed     PluginState = "failed"
)
