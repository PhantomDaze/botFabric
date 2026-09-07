package process

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

var ErrTransportNotImplemented = errors.New("外部进程传输尚未实现")

// HandshakeState 是外部插件握手状态。
type HandshakeState string

const (
	HandshakeCreated        HandshakeState = "created"
	HandshakeProcessStarted HandshakeState = "process_started"
	HandshakeConnected      HandshakeState = "connected"
	HandshakeAuthenticated  HandshakeState = "authenticated"
	HandshakeReady          HandshakeState = "ready"
	HandshakeFailed         HandshakeState = "failed"
	HandshakeStopped        HandshakeState = "stopped"
)

// Session 是运行时为外部插件签发的可信会话。
type Session struct {
	PluginID  string
	Token     string
	ExpiresAt time.Time
}

// SessionIssuer 负责签发不由插件自报的短期会话凭据。
type SessionIssuer interface {
	Issue(pluginID string, ttl time.Duration) (Session, error)
}

// SessionCallbacks 是外部插件向宿主发起操作时使用的可信回调。
// 回调由 managedPlugin 绑定到受 Manifest 守卫的 Host 视图，不能由子进程替换调用方身份。
type SessionCallbacks struct {
	Publish func(context.Context, contract.Event) error
	Invoke  func(context.Context, contract.Request) (contract.Response, error)
}

// Transport 是外部进程生命周期与连接的基础边界。
// 保留该接口以兼容仅实现握手骨架的自定义 transport；完整双向 transport
// 可额外实现 SessionTransport。
type Transport interface {
	Start(ctx context.Context, manifest contract.Manifest, socketPath string) error
	Connect(ctx context.Context, socketPath string) error
	Handshake(ctx context.Context, manifest contract.Manifest, session Session) error
	Health(ctx context.Context, session Session) (contract.HealthStatus, error)
	Shutdown(ctx context.Context, session Session) error
	Kill() error
}

// SessionTransport 是完整外部插件 transport 的可选扩展。
// 它在同一条 Unix Socket gRPC Session 上复用事件投递和服务调用。
type SessionTransport interface {
	Transport
	StartWithSession(ctx context.Context, manifest contract.Manifest, socketPath string, session Session) error
	HandshakeWithCallbacks(ctx context.Context, manifest contract.Manifest, session Session, callbacks SessionCallbacks) error
	Deliver(ctx context.Context, session Session, event contract.Event) error
	Invoke(ctx context.Context, session Session, request contract.Request) (contract.Response, error)
}

// TransportFactory 为每个外部插件实例创建独立 transport，避免连接和 pending 请求串线。
type TransportFactory func(manifest contract.Manifest) (Transport, error)

// Loader 是外部进程插件加载器。
type Loader struct {
	mu               sync.Mutex
	logger           *slog.Logger
	socketDirectory  string
	issuer           SessionIssuer
	transport        Transport
	transportFactory TransportFactory
	sessionTTL       time.Duration
}

// NewLoader 创建使用固定 transport 的外部进程加载器。
// 固定 transport 主要用于兼容旧调用和单元测试；生产环境建议使用 NewLoaderWithFactory。
func NewLoader(logger *slog.Logger, socketDirectory string, issuer SessionIssuer, transport Transport) (*Loader, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if socketDirectory == "" {
		return nil, errors.New("Unix Socket 目录不能为空")
	}
	return &Loader{
		logger:          logger,
		socketDirectory: socketDirectory,
		issuer:          issuer,
		transport:       transport,
		sessionTTL:      5 * time.Minute,
	}, nil
}

// NewLoaderWithFactory 创建为每个插件实例独立创建 transport 的加载器。
func NewLoaderWithFactory(logger *slog.Logger, socketDirectory string, issuer SessionIssuer, factory TransportFactory) (*Loader, error) {
	if factory == nil {
		return NewLoader(logger, socketDirectory, issuer, nil)
	}
	loader, err := NewLoader(logger, socketDirectory, issuer, nil)
	if err != nil {
		return nil, err
	}
	loader.transportFactory = factory
	return loader, nil
}

// Kind 返回加载器处理的执行模型。
func (l *Loader) Kind() contract.PluginRuntime {
	return contract.RuntimeProcess
}

// Load 创建外部进程插件生命周期包装器。
func (l *Loader) Load(manifest contract.Manifest, host contract.Host) (contract.Plugin, error) {
	if manifest.Runtime != contract.RuntimeProcess {
		return nil, fmt.Errorf("外部进程加载器不能处理 runtime=%s", manifest.Runtime)
	}
	if manifest.Entry == "" {
		return nil, errors.New("外部进程入口不能为空")
	}
	if l.issuer == nil || (l.transport == nil && l.transportFactory == nil) {
		return nil, ErrTransportNotImplemented
	}
	transport := l.transport
	if l.transportFactory != nil {
		var err error
		transport, err = l.transportFactory(manifest)
		if err != nil {
			return nil, fmt.Errorf("创建插件 %s 的 transport: %w", manifest.ID, err)
		}
		if transport == nil {
			return nil, errors.New("transport factory 返回 nil")
		}
	}
	return &managedPlugin{
		manifest:        manifest,
		host:            host,
		logger:          l.logger.With("plugin_id", manifest.ID),
		socketDirectory: l.socketDirectory,
		issuer:          l.issuer,
		transport:       transport,
		sessionTTL:      l.sessionTTL,
		state:           HandshakeCreated,
	}, nil
}
