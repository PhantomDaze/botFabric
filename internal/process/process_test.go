package process

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type fakeIssuer struct {
	session Session
	err     error
}

func (i fakeIssuer) Issue(string, time.Duration) (Session, error) {
	return i.session, i.err
}

type fakeTransport struct {
	calls        []string
	health       contract.HealthStatus
	startErr     error
	connectErr   error
	handshakeErr error
	shutdownErr  error
	killErr      error
}

func (t *fakeTransport) Start(context.Context, contract.Manifest, string) error {
	t.calls = append(t.calls, "start")
	return t.startErr
}

func (t *fakeTransport) Connect(context.Context, string) error {
	t.calls = append(t.calls, "connect")
	return t.connectErr
}

func (t *fakeTransport) Handshake(context.Context, contract.Manifest, Session) error {
	t.calls = append(t.calls, "handshake")
	return t.handshakeErr
}

func (t *fakeTransport) Health(context.Context, Session) (contract.HealthStatus, error) {
	t.calls = append(t.calls, "health")
	return t.health, nil
}

func (t *fakeTransport) Shutdown(context.Context, Session) error {
	t.calls = append(t.calls, "shutdown")
	return t.shutdownErr
}

func (t *fakeTransport) Kill() error {
	t.calls = append(t.calls, "kill")
	return t.killErr
}

func TestLoaderCreatesUniqueSocketPathPerInstance(t *testing.T) {
	loader, err := NewLoader(nil, t.TempDir(), fakeIssuer{session: Session{PluginID: "process.example", Token: "token", ExpiresAt: time.Now().Add(time.Minute)}}, &fakeTransport{})
	if err != nil {
		t.Fatal(err)
	}
	manifest := processManifest()
	first, err := loader.Load(manifest, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := loader.Load(manifest, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstPlugin := first.(*managedPlugin)
	secondPlugin := second.(*managedPlugin)
	if firstPlugin.socketPath == secondPlugin.socketPath {
		t.Fatalf("两个插件实例共享 Socket 路径: %q", firstPlugin.socketPath)
	}
	if filepath.Dir(firstPlugin.socketPath) != filepath.Dir(secondPlugin.socketPath) {
		t.Fatal("插件实例 Socket 未使用配置目录")
	}
	if !strings.HasPrefix(filepath.Base(firstPlugin.socketPath), "plugin-") || !strings.HasSuffix(firstPlugin.socketPath, ".sock") {
		t.Fatalf("Socket 路径格式异常: %q", firstPlugin.socketPath)
	}
}

func TestLoaderRequiresImplementedBoundary(t *testing.T) {
	loader, err := NewLoader(nil, "/tmp", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = loader.Load(processManifest(), nil)
	if !errors.Is(err, ErrTransportNotImplemented) {
		t.Fatalf("错误 = %v，期望 ErrTransportNotImplemented", err)
	}
}

func TestManagedPluginCompletesHandshakeAndShutdown(t *testing.T) {
	transport := &fakeTransport{health: contract.HealthStatus{State: contract.HealthHealthy}}
	issuer := fakeIssuer{session: Session{
		PluginID:  "process.example",
		Token:     "runtime-issued-token",
		ExpiresAt: time.Now().Add(time.Minute),
	}}
	loader, err := NewLoader(slog.New(slog.NewTextHandler(io.Discard, nil)), "/tmp", issuer, transport)
	if err != nil {
		t.Fatal(err)
	}
	plugin, err := loader.Load(processManifest(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := plugin.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if status := plugin.Health(context.Background()); status.State != contract.HealthHealthy {
		t.Fatalf("健康状态 = %s", status.State)
	}
	if err := plugin.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start", "connect", "handshake", "health", "shutdown"}
	if !reflect.DeepEqual(transport.calls, want) {
		t.Fatalf("调用顺序 = %v，期望 %v", transport.calls, want)
	}
}

func TestInvalidSessionKillsProcessAndFailsHandshake(t *testing.T) {
	transport := &fakeTransport{}
	issuer := fakeIssuer{session: Session{
		PluginID:  "forged.plugin",
		Token:     "token",
		ExpiresAt: time.Now().Add(time.Minute),
	}}
	loader, err := NewLoader(nil, "/tmp", issuer, transport)
	if err != nil {
		t.Fatal(err)
	}
	plugin, err := loader.Load(processManifest(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := plugin.Start(context.Background(), nil); err == nil {
		t.Fatal("插件 ID 不匹配的会话应使启动失败")
	}
	if !reflect.DeepEqual(transport.calls, []string{"start", "connect", "kill"}) {
		t.Fatalf("调用顺序 = %v", transport.calls)
	}
	if status := plugin.Health(context.Background()); status.State != contract.HealthUnhealthy {
		t.Fatalf("失败握手后的健康状态 = %s", status.State)
	}
}

func TestForceStopKillsTransport(t *testing.T) {
	transport := &fakeTransport{}
	plugin := &managedPlugin{transport: transport, state: HandshakeReady}
	if err := plugin.ForceStop(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(transport.calls, []string{"kill"}) {
		t.Fatalf("调用顺序 = %v", transport.calls)
	}
	if plugin.state != HandshakeStopped {
		t.Fatalf("状态 = %s，期望 stopped", plugin.state)
	}
}

func processManifest() contract.Manifest {
	return contract.Manifest{
		ID:      "process.example",
		Name:    "Process Example",
		Version: "0.1.0",
		Runtime: contract.RuntimeProcess,
		Entry:   "./process-example",
		Limits: contract.Limits{
			Timeout: time.Second,
			Memory:  "128MiB",
		},
	}
}

// TestValidateRequestSessionIgnoresWallClockExpiry 验证会话过期只影响握手认证，
// 请求期不再按墙钟拒绝：运行超过会话 TTL 的长驻插件仍能接收投递与健康检查。
func TestValidateRequestSessionIgnoresWallClockExpiry(t *testing.T) {
	transport, err := NewGRPCTransport(GRPCTransportConfig{})
	if err != nil {
		t.Fatal(err)
	}
	manifest := processManifest()
	session := Session{PluginID: manifest.ID, Token: "runtime-token", ExpiresAt: time.Now().Add(-time.Hour)}
	transport.mu.Lock()
	transport.manifest = manifest
	transport.expected = session
	transport.started = true
	transport.ready = true
	transport.disconnected = make(chan struct{})
	transport.mu.Unlock()
	if err := transport.validateRequestSession(session); err != nil {
		t.Fatalf("过期会话在请求期仍应可用: %v", err)
	}
	// 凭据不匹配仍必须拒绝。
	forged := session
	forged.Token = "forged-token"
	if err := transport.validateRequestSession(forged); err == nil {
		t.Fatal("凭据不匹配的会话应被拒绝")
	}
}

// TestLoaderSessionTTLConfigurable 验证会话有效期可配置并传递到插件实例。
func TestLoaderSessionTTLConfigurable(t *testing.T) {
	loader, err := NewLoader(nil, t.TempDir(), fakeIssuer{session: Session{PluginID: "process.example", Token: "token", ExpiresAt: time.Now().Add(time.Minute)}}, &fakeTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err := loader.SetSessionTTL(0); err == nil {
		t.Fatal("非正会话有效期应被拒绝")
	}
	const ttl = 100 * time.Millisecond
	if err := loader.SetSessionTTL(ttl); err != nil {
		t.Fatal(err)
	}
	plugin, err := loader.Load(processManifest(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := plugin.(*managedPlugin).sessionTTL; got != ttl {
		t.Fatalf("插件会话有效期 = %v，期望 %v", got, ttl)
	}
}

// exitTransport 在前端 transport 基础上暴露退出通知，用于验证异常退出回调。
type exitTransport struct {
	*fakeTransport
	done    chan struct{}
	exitErr error
}

func (t *exitTransport) Done() <-chan struct{} { return t.done }
func (t *exitTransport) ExitError() error      { return t.exitErr }

// TestManagedPluginReportsAbnormalExit 验证 transport 退出后 managedPlugin
// 迁移为 failed 并调用注入的失败回调，供 Runtime 回滚资源。
func TestManagedPluginReportsAbnormalExit(t *testing.T) {
	transport := &exitTransport{fakeTransport: &fakeTransport{}, done: make(chan struct{})}
	failures := make(chan error, 1)
	plugin := &managedPlugin{
		manifest:   processManifest(),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		socketPath: filepath.Join(t.TempDir(), "plugin.sock"),
		issuer:     fakeIssuer{session: Session{PluginID: "process.example", Token: "token", ExpiresAt: time.Now().Add(time.Minute)}},
		transport:  transport,
		sessionTTL: time.Minute,
		state:      HandshakeCreated,
		failureHandler: func(pluginID string, err error) {
			failures <- errors.New(pluginID + ": " + err.Error())
		},
	}
	if err := plugin.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	transport.exitErr = errors.New("进程崩溃")
	close(transport.done)
	select {
	case err := <-failures:
		if !strings.Contains(err.Error(), "进程崩溃") {
			t.Fatalf("失败回调错误 = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("异常退出未触发失败回调")
	}
	plugin.mu.RLock()
	state := plugin.state
	plugin.mu.RUnlock()
	if state != HandshakeFailed {
		t.Fatalf("握手状态 = %s，期望 failed", state)
	}
}

// TestManagedPluginIgnoresExpectedExit 验证主动停止后 transport 退出不触发失败回调。
func TestManagedPluginIgnoresExpectedExit(t *testing.T) {
	transport := &exitTransport{fakeTransport: &fakeTransport{}, done: make(chan struct{})}
	failures := make(chan error, 1)
	plugin := &managedPlugin{
		manifest:   processManifest(),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		socketPath: filepath.Join(t.TempDir(), "plugin.sock"),
		issuer:     fakeIssuer{session: Session{PluginID: "process.example", Token: "token", ExpiresAt: time.Now().Add(time.Minute)}},
		transport:  transport,
		sessionTTL: time.Minute,
		state:      HandshakeCreated,
		failureHandler: func(_ string, err error) {
			failures <- err
		},
	}
	if err := plugin.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := plugin.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	transport.exitErr = errors.New("正常关闭")
	close(transport.done)
	select {
	case err := <-failures:
		t.Fatalf("主动停止不应触发失败回调: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}
