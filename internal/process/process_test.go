package process

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
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
