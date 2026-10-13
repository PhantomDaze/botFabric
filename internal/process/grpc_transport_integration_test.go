package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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

func TestGRPCTransportCapabilityBoundingSetIsolation(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("capability bounding set 仅 Linux 支持")
	}
	baselineStatus, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	if capabilityBoundingMask(t, baselineStatus) == "0" {
		t.Skip("宿主进程 capability bounding set 已为空，无法验证收窄")
	}
	systemdRun, err := exec.LookPath("systemd-run")
	if err != nil {
		t.Skipf("systemd-run 不可用: %v", err)
	}
	output, err := exec.Command(systemdRun, "--user", "--wait", "--collect", "--pipe", "-p", "CapabilityBoundingSet=CAP_NET_BIND_SERVICE", "/bin/sh", "-c", "grep CapBnd /proc/self/status").CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "218/CAPABILITIES") || strings.Contains(string(output), "CapabilityBoundingSet") {
			t.Skipf("宿主不允许 capability bounding set 限制: %s", strings.TrimSpace(string(output)))
		}
		t.Fatalf("运行 capability bounding set 探针: %v (%s)", err, strings.TrimSpace(string(output)))
	}
	if mask := capabilityBoundingMask(t, output); mask != "400" {
		t.Fatalf("transient service capability bounding set = %s，期望 CAP_NET_BIND_SERVICE (400)", mask)
	}
}

func capabilityBoundingMask(t *testing.T, status []byte) string {
	t.Helper()
	match := regexp.MustCompile(`(?m)^CapBnd:\s+([[:xdigit:]]+)$`).FindSubmatch(status)
	if match == nil {
		t.Fatalf("进程状态缺少 CapBnd: %s", status)
	}
	mask := strings.TrimLeft(strings.ToLower(string(match[1])), "0")
	if mask == "" {
		return "0"
	}
	return mask
}

func TestGRPCTransportPrivateNetworkIsolation(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("PrivateNetwork 仅 Linux 支持")
	}
	probeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer probeListener.Close()

	logFile, err := os.CreateTemp(t.TempDir(), "private-network-*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	logger := slog.New(slog.NewTextHandler(logFile, nil))
	helper := buildProcessHelper(t)
	transport, err := NewGRPCTransport(GRPCTransportConfig{Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	manifest := processManifest()
	manifest.Entry = helper
	manifest.Limits.PrivateNetwork = true
	manifest.Limits.NoNewPrivileges = true
	t.Setenv("BOTFABRIC_HELPER_PROBE_TCP", probeListener.Addr().String())
	session := Session{PluginID: manifest.ID, Token: "runtime-token", ExpiresAt: time.Now().Add(time.Minute)}
	socketPath := filepath.Join(t.TempDir(), "plugin.sock")
	if err := transport.StartWithSession(context.Background(), manifest, socketPath, session); err != nil {
		t.Skipf("PrivateNetwork transient service 不可用: %v", err)
	}
	defer transport.Kill()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transport.Connect(ctx, socketPath); err != nil {
		t.Fatalf("插件即使隔离网络仍应连接 Unix Socket Session: %v", err)
	}
	if err := transport.HandshakeWithCallbacks(ctx, manifest, session, SessionCallbacks{}); err != nil {
		t.Fatal(err)
	}

	transport.mu.RLock()
	command := transport.process
	transport.mu.RUnlock()
	if command == nil || command.Process == nil {
		t.Fatal("缺少由 systemd 启动的插件进程")
	}
	status, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(command.Process.Pid), "status"))
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?m)^NoNewPrivs:\s+1$`).Match(status) {
		t.Fatalf("插件进程未启用 NoNewPrivileges: %s", status)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := logFile.Sync(); err != nil {
			t.Fatal(err)
		}
		content, err := os.ReadFile(logFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(content), "private network probe: blocked") {
			return
		}
		if strings.Contains(string(content), "private network probe: connected") {
			t.Fatalf("隔离插件仍可连接宿主 loopback: %s", content)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("未观察到网络隔离探测结果")
}

func TestGRPCTransportRestrictSyscallsEnablesSeccomp(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("系统调用过滤仅 Linux 支持")
	}
	baselineStatus, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	baselineFilters := seccompFilterCount(t, baselineStatus)
	helper := buildProcessHelper(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	transport, err := NewGRPCTransport(GRPCTransportConfig{Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	manifest := processManifest()
	manifest.Entry = helper
	manifest.Limits.RestrictSyscalls = true
	session := Session{PluginID: manifest.ID, Token: "runtime-token", ExpiresAt: time.Now().Add(time.Minute)}
	socketPath := filepath.Join(t.TempDir(), "plugin.sock")
	if err := transport.StartWithSession(context.Background(), manifest, socketPath, session); err != nil {
		t.Skipf("syscall filter transient service 不可用: %v", err)
	}
	defer transport.Kill()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transport.Connect(ctx, socketPath); err != nil {
		t.Fatalf("过滤 syscall 后插件仍应连接 Unix Socket Session: %v", err)
	}
	if err := transport.HandshakeWithCallbacks(ctx, manifest, session, SessionCallbacks{}); err != nil {
		t.Fatal(err)
	}

	transport.mu.RLock()
	command := transport.process
	transport.mu.RUnlock()
	if command == nil || command.Process == nil {
		t.Fatal("缺少由 systemd 启动的插件进程")
	}
	status, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(command.Process.Pid), "status"))
	if err != nil {
		t.Fatal(err)
	}
	if seccompFilterCount(t, status) <= baselineFilters {
		t.Fatalf("插件进程未增加 systemd seccomp filter：baseline=%d status=%s", baselineFilters, status)
	}
}

func seccompFilterCount(t *testing.T, status []byte) int {
	t.Helper()
	match := regexp.MustCompile(`(?m)^Seccomp_filters:\s+(\d+)$`).FindSubmatch(status)
	if match == nil {
		t.Fatalf("进程状态缺少 Seccomp_filters: %s", status)
	}
	count, err := strconv.Atoi(string(match[1]))
	if err != nil {
		t.Fatalf("解析 seccomp filter 数量: %v", err)
	}
	return count
}

func TestGRPCTransportPrivateTmpRejectsSocketInPrivateDirectory(t *testing.T) {
	for _, directory := range []string{"/tmp", "/var/tmp"} {
		t.Run(directory, func(t *testing.T) {
			transport, err := NewGRPCTransport(GRPCTransportConfig{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				t.Fatal(err)
			}
			manifest := processManifest()
			manifest.Limits.PrivateTmp = true
			socketPath := filepath.Join(directory, "botfabric-private-tmp-test", "plugin.sock")
			err = transport.Start(context.Background(), manifest, socketPath)
			if err == nil || !strings.Contains(err.Error(), "Unix Socket 不能位于") {
				t.Fatalf("PrivateTmp 应在启动前拒绝临时目录中的 Socket: %v", err)
			}
			if _, statErr := os.Lstat(filepath.Dir(socketPath)); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("拒绝 Socket 路径前不应创建目录: %v", statErr)
			}
		})
	}
}

func TestGRPCTransportPrivateTmpIsolation(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("PrivateTmp 仅 Linux 支持")
	}
	var tmpMarker, varTmpMarker string
	for _, directory := range []string{"/tmp", "/var/tmp"} {
		marker, createErr := os.CreateTemp(directory, "botfabric-private-tmp-marker-")
		if createErr != nil {
			t.Skipf("无法在 %s 创建 PrivateTmp 探测标记: %v", directory, createErr)
		}
		if directory == "/tmp" {
			tmpMarker = marker.Name()
		} else {
			varTmpMarker = marker.Name()
		}
		_ = marker.Close()
		defer os.Remove(marker.Name())
	}

	logFile, err := os.CreateTemp(t.TempDir(), "private-tmp-*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	logger := slog.New(slog.NewTextHandler(logFile, nil))
	helperDirectory, err := os.MkdirTemp(os.Getenv("XDG_RUNTIME_DIR"), "botfabric-private-tmp-helper-")
	if err != nil {
		t.Skipf("无法在 XDG_RUNTIME_DIR 创建 PrivateTmp helper: %v", err)
	}
	defer os.RemoveAll(helperDirectory)
	helper := buildProcessHelperInDir(t, helperDirectory)
	transport, err := NewGRPCTransport(GRPCTransportConfig{Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	manifest := processManifest()
	manifest.Entry = helper
	manifest.Limits.PrivateTmp = true
	t.Setenv("BOTFABRIC_HELPER_PRIVATE_TMP_TMP_MARKER", tmpMarker)
	t.Setenv("BOTFABRIC_HELPER_PRIVATE_TMP_VARTMP_MARKER", varTmpMarker)
	session := Session{PluginID: manifest.ID, Token: "runtime-token", ExpiresAt: time.Now().Add(time.Minute)}
	socketDirectory, err := os.MkdirTemp("/dev/shm", "botfabric-private-tmp-socket-")
	if err != nil {
		t.Skipf("无法在 /dev/shm 创建私有 Socket 目录: %v", err)
	}
	defer os.RemoveAll(socketDirectory)
	socketPath := filepath.Join(socketDirectory, fmt.Sprintf("plugin-%d.sock", os.Getpid()))
	if err := transport.StartWithSession(context.Background(), manifest, socketPath, session); err != nil {
		t.Skipf("PrivateTmp transient service 不可用: %v", err)
	}
	defer transport.Kill()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transport.Connect(ctx, socketPath); err != nil {
		content, _ := os.ReadFile(logFile.Name())
		t.Fatalf("启用 PrivateTmp 后插件仍应连接 Unix Socket Session: %v; plugin logs: %s", err, content)
	}
	if err := transport.HandshakeWithCallbacks(ctx, manifest, session, SessionCallbacks{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := logFile.Sync(); err != nil {
			t.Fatal(err)
		}
		content, err := os.ReadFile(logFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(content), "private tmp probe: isolated") {
			return
		}
		if strings.Contains(string(content), "private tmp probe: exposed") || strings.Contains(string(content), "private tmp probe: unable-to-write") {
			t.Fatalf("PrivateTmp 行为不符合预期: %s", content)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("未观察到 PrivateTmp 探测结果")
}

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
	return buildProcessHelperInDir(t, t.TempDir())
}

func buildProcessHelperInDir(t *testing.T, directory string) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法定位 process 测试源文件")
	}
	output := filepath.Join(directory, "process-helper")
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

// TestGRPCTransportCgroupCoversDescendants 验证 cgroup 路径下内存上限对
// 孙进程同样生效：helper fork 出触碰大内存的子进程，内核 OOM 在 cgroup
// 限额内回收它，Runtime 不需要主动杀。
func TestGRPCTransportCgroupCoversDescendants(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("cgroup v2 内存限制仅在 Linux 可用")
	}
	helper := buildProcessHelper(t)
	childLog, err := os.CreateTemp(t.TempDir(), "helper-alloc-*.log")
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
	manifest.Limits.Memory = "64MiB"
	startFile := filepath.Join(t.TempDir(), "start-allocation")
	t.Setenv("BOTFABRIC_HELPER_FORK_ALLOC", "1")
	t.Setenv("BOTFABRIC_HELPER_FORK_ALLOC_BYTES", "536870912")
	t.Setenv("BOTFABRIC_HELPER_ALLOC_START_FILE", startFile)

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
	transport.mu.RLock()
	managed := transport.memoryEnforcer != nil && transport.memoryEnforcer.managed
	transport.mu.RUnlock()
	if !managed {
		_ = transport.Kill()
		t.Skip("systemd user service 或 cgroup v2 memory controller 不可用")
	}

	// 等待子进程报告 PID 后再开始分配，避免降级环境误触发 RSS 监控。
	var allocChild int
	var content []byte
	pidDeadline := time.Now().Add(3 * time.Second)
	for allocChild == 0 && time.Now().Before(pidDeadline) {
		if err := childLog.Sync(); err != nil {
			t.Fatal(err)
		}
		content, err := os.ReadFile(childLog.Name())
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(content), "\n") {
			if idx := strings.Index(line, `message="`); idx >= 0 {
				if end := strings.Index(line[idx+9:], `"`); end >= 0 {
					line = line[idx+9 : idx+9+end]
				}
			}
			line = strings.TrimSuffix(line, `\n`)
			if prefix := "alloc child pid: "; strings.HasPrefix(line, prefix) {
				allocChild, _ = strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, prefix)))
			}
		}
		if allocChild == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if allocChild == 0 {
		t.Fatalf("未能解析孙进程 PID:\n%s", content)
	}
	if err := os.WriteFile(startFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	// 主进程存活（内存由孙进程占用，cgroup 内核 OOM 应只杀孙进程）。
	select {
	case <-transport.processDone:
		t.Fatal("cgroup OOM 不应误杀内存占用正常的 helper 主进程")
	case <-time.After(3 * time.Second):
	}

	// 确认内核已回收超过 cgroup 限额的孙进程。
	deadline := time.Now().Add(5 * time.Second)
	for processAlive(allocChild) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if processAlive(allocChild) {
		t.Fatalf("触碰 512MiB 的孙进程 %d 未被 cgroup 内存上限回收", allocChild)
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
	manifest.Limits.Memory = ""
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

	// 子进程 PID 从 helper 的 stderr 日志解析；异步日志 tail 可能稍晚于 processDone 写完。
	var childPID int
	var content []byte
	logDeadline := time.Now().Add(2 * time.Second)
	for childPID == 0 && time.Now().Before(logDeadline) {
		if err := childLog.Sync(); err != nil {
			t.Fatal(err)
		}
		content, err = os.ReadFile(childLog.Name())
		if err != nil {
			t.Fatal(err)
		}
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
			time.Sleep(10 * time.Millisecond)
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
