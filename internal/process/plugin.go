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

type managedPlugin struct {
	mu         sync.RWMutex
	manifest   contract.Manifest
	host       contract.Host
	logger     *slog.Logger
	socketPath string
	issuer     SessionIssuer
	transport  Transport
	sessionTTL time.Duration
	state      HandshakeState
	session    Session
	// failureHandler 在外部进程异常退出时通知 Runtime 回滚插件资源。
	failureHandler func(pluginID string, err error)
	stopping       bool
}

// exitNotifier 由支持退出通知的 transport 实现。
type exitNotifier interface {
	Done() <-chan struct{}
	ExitError() error
}

func (p *managedPlugin) Manifest() contract.Manifest {
	return p.manifest
}

func (p *managedPlugin) Start(ctx context.Context, host contract.Host) error {
	if host != nil {
		p.host = host
	}
	var session Session
	var err error
	sessionTransport, completeTransport := p.transport.(SessionTransport)
	if completeTransport {
		session, err = p.issuer.Issue(p.manifest.ID, p.sessionTTL)
		if err != nil {
			return p.fail("签发插件会话", err)
		}
		if err := validateSession(session, p.manifest.ID); err != nil {
			return p.fail("校验插件会话", err)
		}
	}
	socketPath := p.socketPath
	if socketPath == "" {
		return p.fail("生成外部插件 Unix Socket 路径", errors.New("Unix Socket 路径为空"))
	}
	if completeTransport {
		if err := sessionTransport.StartWithSession(ctx, p.manifest, socketPath, session); err != nil {
			return p.fail("启动外部插件进程", err)
		}
	} else if err := p.transport.Start(ctx, p.manifest, socketPath); err != nil {
		return p.fail("启动外部插件进程", err)
	}
	p.setState(HandshakeProcessStarted)
	if err := p.transport.Connect(ctx, socketPath); err != nil {
		_ = p.transport.Kill()
		return p.fail("连接外部插件 Unix Socket", err)
	}
	p.setState(HandshakeConnected)
	if !completeTransport {
		session, err = p.issuer.Issue(p.manifest.ID, p.sessionTTL)
		if err != nil {
			_ = p.transport.Kill()
			return p.fail("签发插件会话", err)
		}
		if err := validateSession(session, p.manifest.ID); err != nil {
			_ = p.transport.Kill()
			return p.fail("校验插件会话", err)
		}
	}
	p.setState(HandshakeAuthenticated)
	if completeTransport {
		callbacks := SessionCallbacks{}
		if p.host != nil {
			callbacks.Publish = p.host.EventBus().Publish
			callbacks.Invoke = p.host.Gateway().Invoke
		}
		if err := sessionTransport.HandshakeWithCallbacks(ctx, p.manifest, session, callbacks); err != nil {
			_ = p.transport.Kill()
			return p.fail("完成外部插件握手", err)
		}
		if err := p.registerSessionResources(sessionTransport, session); err != nil {
			_ = p.transport.Kill()
			return p.fail("注册外部插件资源", err)
		}
	} else if err := p.transport.Handshake(ctx, p.manifest, session); err != nil {
		_ = p.transport.Kill()
		return p.fail("完成外部插件握手", err)
	}
	p.mu.Lock()
	p.session = session
	p.state = HandshakeReady
	p.mu.Unlock()
	// 握手成功后监听进程退出，异常退出时回传 Runtime 迁移状态并回滚资源。
	if notifier, ok := p.transport.(exitNotifier); ok && p.failureHandler != nil {
		go p.watchExit(notifier)
	}
	return nil
}

// watchExit 等待 transport 终止信号；主动停止不触发失败回调。
func (p *managedPlugin) watchExit(notifier exitNotifier) {
	select {
	case <-notifier.Done():
	}
	err := notifier.ExitError()
	p.mu.Lock()
	stopping := p.stopping || p.state == HandshakeStopped || p.state == HandshakeFailed
	if !stopping {
		p.state = HandshakeFailed
	}
	p.mu.Unlock()
	if stopping || err == nil {
		return
	}
	p.logger.Error("外部插件进程异常退出", "error", err)
	p.failureHandler(p.manifest.ID, err)
}

func validateSession(session Session, pluginID string) error {
	if session.PluginID != pluginID || session.Token == "" || !session.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("会话内容无效")
	}
	return nil
}

func (p *managedPlugin) registerSessionResources(transport SessionTransport, session Session) error {
	if p.host == nil {
		return nil
	}
	for _, descriptor := range p.manifest.Provides.Services {
		descriptor := descriptor
		if err := p.host.Gateway().Register(descriptor, func(ctx context.Context, request contract.Request) (contract.Response, error) {
			return transport.Invoke(ctx, session, request)
		}); err != nil {
			return err
		}
	}
	for _, topic := range p.manifest.Provides.Subscriptions {
		topic := topic
		if _, err := p.host.EventBus().Subscribe(topic, func(ctx context.Context, event contract.Event) error {
			return transport.Deliver(ctx, session, event)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (p *managedPlugin) Stop(ctx context.Context) error {
	p.mu.RLock()
	session := p.session
	state := p.state
	p.mu.RUnlock()
	if state == HandshakeStopped || state == HandshakeCreated {
		return nil
	}
	p.mu.Lock()
	p.stopping = true
	p.mu.Unlock()
	if err := p.transport.Shutdown(ctx, session); err != nil {
		if killErr := p.transport.Kill(); killErr != nil {
			p.setState(HandshakeFailed)
			return fmt.Errorf("优雅停机失败: %v；强制终止失败: %w", err, killErr)
		}
		p.setState(HandshakeStopped)
		return err
	}
	p.setState(HandshakeStopped)
	return nil
}

// ForceStop 由 Runtime 在 Stop 超时后调用。
func (p *managedPlugin) ForceStop() error {
	p.mu.Lock()
	p.stopping = true
	p.mu.Unlock()
	err := p.transport.Kill()
	p.setState(HandshakeStopped)
	return err
}

func (p *managedPlugin) Health(ctx context.Context) contract.HealthStatus {
	p.mu.RLock()
	state := p.state
	session := p.session
	p.mu.RUnlock()
	if state != HandshakeReady {
		return contract.HealthStatus{
			State:     contract.HealthUnhealthy,
			Message:   "外部插件未完成握手: " + string(state),
			CheckedAt: time.Now().UTC(),
		}
	}
	status, err := p.transport.Health(ctx, session)
	if err != nil {
		return contract.HealthStatus{
			State:     contract.HealthUnhealthy,
			Message:   err.Error(),
			CheckedAt: time.Now().UTC(),
		}
	}
	return status
}

func (p *managedPlugin) setState(state HandshakeState) {
	p.mu.Lock()
	p.state = state
	p.mu.Unlock()
}

func (p *managedPlugin) fail(action string, err error) error {
	p.setState(HandshakeFailed)
	p.logger.Error(action+"失败", "error", err)
	return fmt.Errorf("%s: %w", action, err)
}
