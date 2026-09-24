package process

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/gateway"
	"github.com/PhantomDaze/botfabric/internal/registry"
	runtimepkg "github.com/PhantomDaze/botfabric/internal/runtime"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type integrationAllowPolicy struct{}

func (integrationAllowPolicy) AllowPublish(string, string) bool        { return true }
func (integrationAllowPolicy) AllowSubscribe(string, string) bool      { return true }
func (integrationAllowPolicy) AllowInvoke(string, string, string) bool { return true }
func (integrationAllowPolicy) AllowProvide(string, string) bool        { return true }

func TestGRPCTransportProcessSession(t *testing.T) {
	helper := buildProcessHelper(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := registry.New()
	bus, err := eventbus.New(time.Second, integrationAllowPolicy{}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := gateway.New(reg, integrationAllowPolicy{}, logger, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runtimepkg.NewManager(reg, bus, gw, logger, "test-bot", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	const (
		pluginID       = "process.integration"
		remoteService  = "process.remote"
		hostService    = "host.echo"
		eventTopic     = "process.input"
		hostEventTopic = "process.from.helper"
	)
	manifest := contract.Manifest{
		ID:      pluginID,
		Name:    "Process Integration",
		Version: "0.1.0",
		Runtime: contract.RuntimeProcess,
		Entry:   helper,
		Provides: contract.Provides{
			Services:      []contract.ServiceDescriptor{{Name: remoteService, Methods: []string{"Run"}}},
			Subscriptions: []string{eventTopic},
		},
		Requires: contract.Requires{Services: []string{hostService}},
		Limits:   contract.Limits{Timeout: time.Second, Memory: "128MiB"},
	}

	var transport *GRPCTransport
	loader, err := NewLoaderWithFactory(logger, t.TempDir(), NewSessionIssuer(), func(manifest contract.Manifest) (Transport, error) {
		var err error
		transport, err = NewGRPCTransport(GRPCTransportConfig{Logger: logger, MaxMessageSize: 1 << 20, MaxInFlight: 8})
		return transport, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterLoader(loader); err != nil {
		t.Fatal(err)
	}

	var hostCaller string
	hostCall := make(chan contract.Request, 1)
	if err := gw.ForPlugin("host-provider").Register(contract.ServiceDescriptor{Name: hostService, Methods: []string{"Run"}}, func(_ context.Context, request contract.Request) (contract.Response, error) {
		hostCaller = request.Caller
		hostCall <- request
		return contract.Response{Payload: append([]byte("host:"), request.Payload...)}, nil
	}); err != nil {
		t.Fatal(err)
	}

	helperEvent := make(chan contract.Event, 1)
	if _, err := bus.ForPlugin("observer").Subscribe(hostEventTopic, func(_ context.Context, event contract.Event) error {
		helperEvent <- event
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	t.Setenv("BOTFABRIC_HELPER_SEND_HOST_CALLS", "1")
	t.Setenv("BOTFABRIC_HELPER_PUBLISH_TOPIC", hostEventTopic)
	t.Setenv("BOTFABRIC_HELPER_HOST_SERVICE", hostService)

	if err := manager.Load(manifest); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), pluginID); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if state, _ := manager.State(pluginID); state == contract.PluginRunning {
			_ = manager.Stop(context.Background(), pluginID)
		}
		_ = manager.Unload(context.Background(), pluginID)
	}()

	if state, _ := manager.State(pluginID); state != contract.PluginRunning {
		t.Fatalf("插件状态 = %s，期望 running", state)
	}
	entry, exists := reg.Service(remoteService)
	if !exists || entry.Owner != pluginID || len(entry.Descriptor.Methods) != 1 || entry.Descriptor.Methods[0] != "Run" {
		t.Fatalf("远程服务注册异常: exists=%v entry=%#v", exists, entry)
	}

	socketPath := filepath.Join(filepath.Dir(filepath.Join(t.TempDir(), "unused")), "unused")
	transport.mu.RLock()
	socketPath = transport.socketPath
	transport.mu.RUnlock()
	info, err := os.Stat(socketPath)
	if err != nil {
		t.Fatalf("读取 Unix Socket: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("Unix Socket 权限 = %o，期望 600", got)
	}

	response, err := gw.ForPlugin("caller").Invoke(context.Background(), contract.Request{
		Service:       remoteService,
		Method:        "Run",
		Caller:        "forged-caller",
		TraceID:       "trace-from-test",
		CorrelationID: "correlation-from-test",
		Payload:       []byte("request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Payload) != "plugin:request" {
		t.Fatalf("远程服务响应 = %q", response.Payload)
	}

	if err := bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{
		Topic:   eventTopic,
		Schema:  "botfabric.test/v1",
		BotID:   "test-bot",
		Payload: []byte("event-payload"),
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case event := <-helperEvent:
		if event.Source != pluginID {
			t.Fatalf("helper 发布事件 Source = %q，期望 %q", event.Source, pluginID)
		}
		if event.Topic != hostEventTopic || string(event.Payload) != "from-helper" {
			t.Fatalf("helper 发布事件 = %#v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("等待 helper 发布事件超时")
	}
	select {
	case request := <-hostCall:
		if request.Service != hostService || string(request.Payload) != "host-request" {
			t.Fatalf("宿主服务请求 = %#v", request)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("等待 helper 调用宿主服务超时")
	}
	if hostCaller != pluginID {
		t.Fatalf("宿主服务收到 Caller = %q，期望 %q", hostCaller, pluginID)
	}

	statuses := manager.Health(context.Background())
	if status := statuses[pluginID]; status.State != contract.HealthHealthy || status.Details["source"] != "process-helper" {
		t.Fatalf("helper 健康状态 = %#v", status)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Stop(stopCtx, pluginID); err != nil {
		t.Fatal(err)
	}
	if state, _ := manager.State(pluginID); state != contract.PluginStopped {
		t.Fatalf("停止后插件状态 = %s", state)
	}
	select {
	case <-transport.processDone:
	case <-time.After(2 * time.Second):
		t.Fatal("helper 进程未退出")
	}
	if _, err := os.Stat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("停止后 Unix Socket 仍存在: %v", err)
	}
	if _, exists := reg.Service(remoteService); exists {
		t.Fatal("停止后远程服务仍注册")
	}
	if err := manager.Unload(context.Background(), pluginID); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCTransportRejectsInvalidSession(t *testing.T) {
	helper := buildProcessHelper(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	transport, err := NewGRPCTransport(GRPCTransportConfig{Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	manifest := processManifest()
	manifest.Entry = helper
	t.Setenv("BOTFABRIC_HELPER_BAD_TOKEN", "1")
	session := Session{PluginID: manifest.ID, Token: "runtime-token", ExpiresAt: time.Now().Add(time.Minute)}
	socketPath := filepath.Join(t.TempDir(), "plugin.sock")
	if err := transport.StartWithSession(context.Background(), manifest, socketPath, session); err != nil {
		t.Fatal(err)
	}
	defer transport.Kill()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := transport.Connect(ctx, socketPath); err == nil {
		t.Fatal("错误 token 应拒绝连接")
	}
	if err := transport.HandshakeWithCallbacks(ctx, manifest, session, SessionCallbacks{}); err == nil {
		t.Fatal("错误 token 应使握手失败")
	}
	select {
	case <-transport.processDone:
	case <-time.After(2 * time.Second):
		t.Fatal("错误 token helper 未退出")
	}
	if _, err := os.Stat(socketPath); err == nil {
		t.Fatal("失败握手后 Unix Socket 应清理")
	}
}

func buildProcessHelper(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法定位 process 测试源文件")
	}
	output := filepath.Join(t.TempDir(), "process-helper")
	command := exec.Command("go", "build", "-o", output, filepath.Join(filepath.Dir(source), "testdata", "helper"))
	result, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("构建 process helper: %v\n%s", err, result)
	}
	return output
}

func TestGRPCTransportKillsBlockedProcess(t *testing.T) {
	helper := buildProcessHelper(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	transport, err := NewGRPCTransport(GRPCTransportConfig{Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	manifest := processManifest()
	manifest.Entry = helper
	t.Setenv("BOTFABRIC_HELPER_BLOCK_INVOKE", "1")
	session := Session{PluginID: manifest.ID, Token: "runtime-token", ExpiresAt: time.Now().Add(time.Minute)}
	socketPath := filepath.Join(t.TempDir(), "plugin.sock")
	if err := transport.StartWithSession(context.Background(), manifest, socketPath, session); err != nil {
		t.Fatal(err)
	}
	defer transport.Kill()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := transport.Connect(ctx, socketPath); err != nil {
		t.Fatal(err)
	}
	if err := transport.HandshakeWithCallbacks(ctx, manifest, session, SessionCallbacks{}); err != nil {
		t.Fatal(err)
	}
	invokeCtx, invokeCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer invokeCancel()
	_, err = transport.Invoke(invokeCtx, session, contract.Request{Service: "process.remote", Method: "Run"})
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("阻塞调用错误 = %v，期望 context deadline exceeded", err)
	}
}

// TestGRPCTransportEnforcesMemoryLimit 验证 Manifest Limits.Memory 通过
// RSS 监控真正作用于外部进程：持续 RSS 超限时进程被强制终止，
// 而正常握手与调用不受影响。
func TestGRPCTransportEnforcesMemoryLimit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 不支持 /proc RSS 读取")
	}
	helper := buildProcessHelper(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	transport, err := NewGRPCTransport(GRPCTransportConfig{Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	manifest := processManifest()
	manifest.Entry = helper
	// 64MiB 远小于 probe 分配的 512MiB，触发持续超限。
	manifest.Limits.Memory = "64MiB"
	// probe 模式下 helper 不连接 Runtime，直接尝试大内存分配并驻留。
	t.Setenv("BOTFABRIC_HELPER_ALLOC_PROBE", "1")
	t.Setenv("BOTFABRIC_HELPER_ALLOC_BYTES", "536870912")

	session := Session{PluginID: manifest.ID, Token: "runtime-token", ExpiresAt: time.Now().Add(time.Minute)}
	socketPath := filepath.Join(t.TempDir(), "plugin.sock")
	if err := transport.StartWithSession(context.Background(), manifest, socketPath, session); err != nil {
		t.Fatal(err)
	}
	defer transport.Kill()

	// RSS 监控在连续超限采样后终止进程；上限约 5 次采样周期加进程退出时间。
	select {
	case <-transport.processDone:
	case <-time.After(5 * time.Second):
		t.Fatal("持续内存超限的进程未被终止")
	}
	if _, err := os.Stat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("超限终止后 Unix Socket 仍存在: %v", err)
	}

	// 交叉验证：同 helper 不设 probe、正常运行时握手不受 64MiB 限制影响。
	transport2, err := NewGRPCTransport(GRPCTransportConfig{Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer transport2.Kill()
	os.Unsetenv("BOTFABRIC_HELPER_ALLOC_PROBE")
	os.Unsetenv("BOTFABRIC_HELPER_ALLOC_BYTES")
	socketPath2 := filepath.Join(t.TempDir(), "plugin.sock")
	if err := transport2.StartWithSession(context.Background(), manifest, socketPath2, session); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := transport2.Connect(ctx, socketPath2); err != nil {
		t.Fatalf("64MiB 限制不应阻断正常握手: %v", err)
	}
	if err := transport2.HandshakeWithCallbacks(ctx, manifest, session, SessionCallbacks{}); err != nil {
		t.Fatalf("64MiB 限制不应阻断正常握手: %v", err)
	}
}
func processAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func TestGRPCTransportKillsProcessGroup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 不支持 POSIX 进程组")
	}
	helper := buildProcessHelper(t)
	// childLog 捕获 helper stderr 中的子进程 PID 行。
	childLog, err := os.CreateTemp(t.TempDir(), "helper-child-*.log")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(childLog, nil))
	transport, err := NewGRPCTransport(GRPCTransportConfig{Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	manifest := processManifest()
	manifest.Entry = helper
	t.Setenv("BOTFABRIC_HELPER_FORK_CHILD", "1")
	session := Session{PluginID: manifest.ID, Token: "runtime-token", ExpiresAt: time.Now().Add(time.Minute)}
	socketPath := filepath.Join(t.TempDir(), "plugin.sock")
	if err := transport.StartWithSession(context.Background(), manifest, socketPath, session); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := transport.Connect(ctx, socketPath); err != nil {
		t.Fatal(err)
	}
	if err := transport.HandshakeWithCallbacks(ctx, manifest, session, SessionCallbacks{}); err != nil {
		t.Fatal(err)
	}

	transport.mu.RLock()
	pgid := transport.processGroupID
	transport.mu.RUnlock()
	if pgid == 0 {
		t.Fatal("外部插件进程组未建立")
	}

	if err := transport.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-transport.processDone:
	case <-time.After(2 * time.Second):
		t.Fatal("helper 进程未退出")
	}

	// 子进程 PID 从 helper 的 stderr 日志解析。
	if err := childLog.Sync(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(childLog.Name())
	if err != nil {
		t.Fatal(err)
	}
	var childPID int
	for _, line := range strings.Split(string(content), "\n") {
		// slog 文本格式把 helper stderr 行包进 message="..."，先剥壳再匹配前缀。
		if idx := strings.Index(line, `message="`); idx >= 0 {
			if end := strings.Index(line[idx+9:], `"`); end >= 0 {
				line = line[idx+9 : idx+9+end]
			}
		}
		line = strings.TrimSuffix(line, `\n`)
		if prefix := "helper child pid: "; strings.HasPrefix(line, prefix) {
			value, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, prefix)))
			if err == nil {
				childPID = value
			}
		}
	}
	if childPID == 0 {
		t.Fatalf("未能从 helper 日志解析子进程 PID:\n%s", content)
	}
	// SIGKILL 送达后子进程从内核消失需要短暂时间，轮询等待而不是立即判定。
	deadline := time.Now().Add(2 * time.Second)
	for processAlive(childPID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if processAlive(childPID) {
		t.Fatalf("Kill 后子孙进程 %d 仍在运行", childPID)
	}
	if _, err := os.Stat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Kill 后 Unix Socket 仍存在: %v", err)
	}
}
