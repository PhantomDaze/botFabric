package process

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type managedPlugin struct {
	mu              sync.RWMutex
	manifest        contract.Manifest
	host            contract.Host
	logger          *slog.Logger
	socketDirectory string
	issuer          SessionIssuer
	transport       Transport
	sessionTTL      time.Duration
	state           HandshakeState
	session         Session
}

func (p *managedPlugin) Manifest() contract.Manifest {
	return p.manifest
}

func (p *managedPlugin) Start(ctx context.Context, _ contract.Host) error {
	socketPath := filepath.Join(p.socketDirectory, p.manifest.ID+".sock")
	if err := p.transport.Start(ctx, p.manifest, socketPath); err != nil {
		return p.fail("启动外部插件进程", err)
	}
	p.setState(HandshakeProcessStarted)
	if err := p.transport.Connect(ctx, socketPath); err != nil {
		_ = p.transport.Kill()
		return p.fail("连接外部插件 Unix Socket", err)
	}
	p.setState(HandshakeConnected)

	session, err := p.issuer.Issue(p.manifest.ID, p.sessionTTL)
	if err != nil {
		_ = p.transport.Kill()
		return p.fail("签发插件会话", err)
	}
	if session.PluginID != p.manifest.ID || session.Token == "" || !session.ExpiresAt.After(time.Now()) {
		_ = p.transport.Kill()
		return p.fail("校验插件会话", fmt.Errorf("会话内容无效"))
	}
	p.setState(HandshakeAuthenticated)
	if err := p.transport.Handshake(ctx, p.manifest, session); err != nil {
		_ = p.transport.Kill()
		return p.fail("完成外部插件握手", err)
	}
	p.mu.Lock()
	p.session = session
	p.state = HandshakeReady
	p.mu.Unlock()
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
