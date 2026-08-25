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

// Transport 隔离进程启动、Unix Socket 和 gRPC 代码生成细节。
type Transport interface {
	Start(ctx context.Context, manifest contract.Manifest, socketPath string) error
	Connect(ctx context.Context, socketPath string) error
	Handshake(ctx context.Context, manifest contract.Manifest, session Session) error
	Health(ctx context.Context, session Session) (contract.HealthStatus, error)
	Shutdown(ctx context.Context, session Session) error
	Kill() error
}

// Loader 是外部进程插件加载器骨架。
type Loader struct {
	mu              sync.Mutex
	logger          *slog.Logger
	socketDirectory string
	issuer          SessionIssuer
	transport       Transport
	sessionTTL      time.Duration
}

// NewLoader 创建外部进程加载器骨架。
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
	if l.issuer == nil || l.transport == nil {
		return nil, ErrTransportNotImplemented
	}
	return &managedPlugin{
		manifest:        manifest,
		host:            host,
		logger:          l.logger.With("plugin_id", manifest.ID),
		socketDirectory: l.socketDirectory,
		issuer:          l.issuer,
		transport:       l.transport,
		sessionTTL:      l.sessionTTL,
		state:           HandshakeCreated,
	}, nil
}
