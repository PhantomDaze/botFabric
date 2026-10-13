package process

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	botfabricv1 "github.com/PhantomDaze/botfabric/gen/botfabric/v1"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"google.golang.org/grpc"
)

const (
	processProtocolVersion = "v1"
	defaultProcessInFlight = 256
	defaultProcessMaxBytes = 4 * 1024 * 1024
)

var (
	errSessionClosed   = errors.New("外部插件 Session 已关闭")
	errSessionNotReady = errors.New("外部插件 Session 尚未就绪")
)

// GRPCTransportConfig 控制 Unix Socket gRPC transport。
type GRPCTransportConfig struct {
	Logger         *slog.Logger
	MaxMessageSize int64
	MaxInFlight    int
} // GRPCTransport 是 Runtime-owned Unix Socket 上的双向 gRPC transport。
// 每个实例只托管一个外部插件进程和一条 Session，避免不同插件共享状态。
type GRPCTransport struct {
	botfabricv1.UnimplementedProcessPluginServer

	mu                sync.RWMutex
	startMu           sync.Mutex
	config            GRPCTransportConfig
	manifest          contract.Manifest
	expected          Session
	socketPath        string
	socketInfo        os.FileInfo
	listener          net.Listener
	server            *grpc.Server
	process           *exec.Cmd
	processGroupID    int
	processDone       chan struct{}
	processDoneOnce   sync.Once
	processErr        error
	connected         chan struct{}
	connectedOnce     sync.Once
	authenticated     chan struct{}
	authOnce          sync.Once
	disconnected      chan struct{}
	disconnectOnce    sync.Once
	stream            *grpcSessionStream
	sessionClaimed    bool
	callbacks         SessionCallbacks
	callbacksBound    bool
	sessionCancel     context.CancelFunc
	handshakeErr      error
	ready             bool
	started           bool
	stopped           bool
	cleanupOnce       sync.Once
	cleanupErr        error
	shutdownRequested bool
	inboundWG         sync.WaitGroup
	inboundMu         sync.Mutex
	inboundClosing    bool
	inboundLimit      chan struct{}
	nextRequestID     atomic.Uint64
	pendingMu         sync.Mutex
	pending           map[string]chan wireFrame
	inFlight          chan struct{}
	memoryEnforcer    *memoryEnforcer
}

// NewGRPCTransport 创建一个外部插件 gRPC transport。
func NewGRPCTransport(config GRPCTransportConfig) (*GRPCTransport, error) {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.MaxMessageSize <= 0 {
		config.MaxMessageSize = defaultProcessMaxBytes
	}
	if config.MaxMessageSize > int64(int(^uint(0)>>1)) {
		return nil, errors.New("外部插件最大消息大小超出平台限制")
	}
	if config.MaxInFlight <= 0 {
		config.MaxInFlight = defaultProcessInFlight
	}
	return &GRPCTransport{config: config}, nil
}

// Start 为旧 Transport 接口保留入口；完整 transport 应使用 StartWithSession。
func (t *GRPCTransport) Start(ctx context.Context, manifest contract.Manifest, socketPath string) error {
	return t.start(ctx, manifest, socketPath, Session{})
}

// StartWithSession 启动 Runtime 监听端和外部插件进程。
func (t *GRPCTransport) StartWithSession(ctx context.Context, manifest contract.Manifest, socketPath string, session Session) error {
	if session.PluginID != manifest.ID || session.Token == "" || !session.ExpiresAt.After(time.Now()) {
		return errors.New("外部插件启动会话无效")
	}
	return t.start(ctx, manifest, socketPath, session)
}

// processIsolation 描述外部进程插件请求的 systemd 隔离属性，供平台相关启动
// 路径构建 transient service 参数。
type processIsolation struct {
	privateNetwork          bool
	noNewPrivileges         bool
	privateTmp              bool
	restrictSyscalls        bool
	restrictAddressFamilies bool
	capabilityBoundingSet   []string
}

// isolationFromManifest 从插件清单提取请求的隔离属性。
func isolationFromManifest(manifest contract.Manifest) processIsolation {
	return processIsolation{
		privateNetwork:          manifest.Limits.PrivateNetwork,
		noNewPrivileges:         manifest.Limits.NoNewPrivileges,
		privateTmp:              manifest.Limits.PrivateTmp,
		restrictSyscalls:        manifest.Limits.RestrictSyscalls,
		restrictAddressFamilies: manifest.Limits.RestrictAddressFamilies,
		capabilityBoundingSet:   manifest.Limits.CapabilityBoundingSet,
	}
}

// requested 报告是否请求了任何隔离属性；请求时启动必须走 systemd 且失败不降级。
func (o processIsolation) requested() bool {
	return o.privateNetwork || o.noNewPrivileges || o.privateTmp ||
		o.restrictSyscalls || o.restrictAddressFamilies || o.capabilityBoundingSet != nil
}

func (t *GRPCTransport) start(ctx context.Context, manifest contract.Manifest, socketPath string, session Session) error {
	t.startMu.Lock()
	defer t.startMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if manifest.Entry == "" {
		return errors.New("外部插件入口不能为空")
	}
	if socketPath == "" {
		return errors.New("外部插件 Unix Socket 路径不能为空")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	if t.started {
		t.mu.Unlock()
		return errors.New("外部插件 transport 不支持重复启动")
	}
	t.mu.Unlock()
	if manifest.Limits.PrivateTmp {
		if err := validatePrivateTmpSocketPath(socketPath); err != nil {
			return fmt.Errorf("启动需要 PrivateTmp 的外部插件失败: %w", err)
		}
	}
	if err := prepareSocketPath(socketPath); err != nil {
		return err
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("监听外部插件 Unix Socket: %w", err)
	}
	cleanupListener := true
	defer func() {
		if cleanupListener {
			_ = listener.Close()
			_ = removeOwnedSocket(socketPath, nil)
		}
	}()
	if err := os.Chmod(socketPath, 0o600); err != nil {
		return fmt.Errorf("设置外部插件 Unix Socket 权限: %w", err)
	}
	socketInfo, err := os.Stat(socketPath)
	if err != nil {
		return fmt.Errorf("读取外部插件 Unix Socket: %w", err)
	}

	maxBytes := int(t.config.MaxMessageSize)
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxBytes),
		grpc.MaxSendMsgSize(maxBytes),
	)
	t.mu.Lock()
	t.manifest = manifest
	t.expected = session
	t.socketPath = socketPath
	t.socketInfo = socketInfo
	t.listener = listener
	t.server = server
	t.processDone = make(chan struct{})
	t.connected = make(chan struct{})
	t.authenticated = make(chan struct{})
	t.disconnected = make(chan struct{})
	t.pending = make(map[string]chan wireFrame)
	t.inFlight = make(chan struct{}, t.config.MaxInFlight)
	t.inboundLimit = make(chan struct{}, t.config.MaxInFlight)
	t.started = true
	t.mu.Unlock()
	cleanupListener = false

	server.RegisterService(&botfabricv1.ProcessPlugin_ServiceDesc, &grpcService{transport: t})

	command := exec.Command(manifest.Entry)
	prepareProcessGroup(command)
	command.Env = append(os.Environ(),
		"BOTFABRIC_SOCKET="+socketPath,
		"BOTFABRIC_PLUGIN_ID="+manifest.ID,
		"BOTFABRIC_PLUGIN_VERSION="+manifest.Version,
		"BOTFABRIC_PROTOCOL_VERSION="+processProtocolVersion,
	)
	if session.Token != "" {
		command.Env = append(command.Env, "BOTFABRIC_SESSION_TOKEN="+session.Token)
	}
	command.Stdout = processLogWriter{logger: t.config.Logger, pluginID: manifest.ID, stream: "stdout"}
	command.Stderr = processLogWriter{logger: t.config.Logger, pluginID: manifest.ID, stream: "stderr"}
	if err := ctx.Err(); err != nil {
		_ = t.cleanup()
		t.closeProcessDone()
		return err
	}
	// 请求任何隔离属性时都必须使用 systemd transient service；失败时不降级。
	// 仅 memory 限制在 cgroup 不可用时回退 RSS 轮询（只覆盖直接子进程）。
	limit := memoryLimitBytes(manifest)
	enforcer, err := startCgroupMemoryLimit(command, limit, isolationFromManifest(manifest), t.config.Logger, manifest.ID)
	if err != nil {
		_ = t.cleanup()
		t.closeProcessDone()
		return fmt.Errorf("启动需要进程隔离的外部插件失败: %w", err)
	}
	if enforcer == nil {
		if err := command.Start(); err != nil {
			_ = t.cleanup()
			t.closeProcessDone()
			return fmt.Errorf("启动外部插件进程: %w", err)
		}
		if limit > 0 {
			t.config.Logger.Warn("cgroup v2 内存限制不可用，回退 RSS 轮询监控", "plugin_id", manifest.ID)
			enforcer = &memoryEnforcer{stop: startRSSMemoryMonitor(command, limit, t.config.Logger, manifest.ID, func() {
				_ = t.Kill()
			})}
		}
	}
	t.mu.Lock()
	t.process = command
	t.processGroupID = processGroupID(command.Process)
	t.memoryEnforcer = enforcer
	t.mu.Unlock()
	go t.waitProcess(command)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.mu.RLock()
			stopped := t.stopped
			t.mu.RUnlock()
			if !stopped {
				t.config.Logger.Error("外部插件 gRPC 服务退出", "plugin_id", manifest.ID, "error", err)
				t.markDisconnected(fmt.Errorf("gRPC 服务退出: %w", err))
				_ = t.killProcess()
				_ = t.cleanup()
			}
		}
	}()
	if err := ctx.Err(); err != nil {
		_ = t.Kill()
		return err
	}
	return nil
}

func (t *GRPCTransport) Connect(ctx context.Context, _ string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.RLock()
	if !t.started || t.connected == nil || t.processDone == nil || t.disconnected == nil {
		t.mu.RUnlock()
		return errSessionNotReady
	}
	connected, processDone, disconnected := t.connected, t.processDone, t.disconnected
	t.mu.RUnlock()
	select {
	case <-connected:
		return nil
	case <-disconnected:
		return t.handshakeFailureOrClosed()
	case <-processDone:
		return t.processExitError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Handshake 保留旧接口，完整 Session 使用 HandshakeWithCallbacks。
func (t *GRPCTransport) Handshake(ctx context.Context, manifest contract.Manifest, session Session) error {
	return t.HandshakeWithCallbacks(ctx, manifest, session, SessionCallbacks{})
}

// HandshakeWithCallbacks 完成 token/插件身份校验，并绑定 Host 回调。
func (t *GRPCTransport) HandshakeWithCallbacks(ctx context.Context, manifest contract.Manifest, session Session, callbacks SessionCallbacks) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := t.validateExpectedSession(manifest, session); err != nil {
		return err
	}
	t.mu.Lock()
	if t.callbacksBound {
		if (t.callbacks.Publish == nil) != (callbacks.Publish == nil) || (t.callbacks.Invoke == nil) != (callbacks.Invoke == nil) {
			t.mu.Unlock()
			return errors.New("外部插件 Session 回调已绑定")
		}
	} else {
		t.callbacks = callbacks
		t.callbacksBound = true
	}
	authenticated, processDone, disconnected := t.authenticated, t.processDone, t.disconnected
	t.mu.Unlock()
	if authenticated == nil || processDone == nil || disconnected == nil {
		return errSessionNotReady
	}
	select {
	case <-authenticated:
		t.mu.RLock()
		err := t.handshakeErr
		ready := t.ready
		t.mu.RUnlock()
		if err != nil {
			return err
		}
		if !ready {
			return errSessionClosed
		}
		return nil
	case <-disconnected:
		return t.handshakeFailureOrClosed()
	case <-processDone:
		return t.handshakeFailureOrClosed()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Deliver 将事件发送给外部插件，并等待处理确认。
func (t *GRPCTransport) Deliver(ctx context.Context, session Session, event contract.Event) error {
	if err := t.validateRequestSession(session); err != nil {
		return err
	}
	requestID := t.newRequestID("event")
	return t.request(ctx, requestID, wireFrame{Type: "event", RequestID: requestID, Event: eventToWire(event)}, "event_ack")
}

// Invoke 调用外部插件声明的服务。
func (t *GRPCTransport) Invoke(ctx context.Context, session Session, request contract.Request) (contract.Response, error) {
	if err := t.validateRequestSession(session); err != nil {
		return contract.Response{}, err
	}
	requestID := t.newRequestID("invoke")
	frame, err := t.requestFrame(ctx, requestID, wireFrame{Type: "invoke_plugin", RequestID: requestID, Request: requestToWire(request)}, "invoke_plugin_response")
	if err != nil {
		return contract.Response{}, err
	}
	return wireToResponse(frame.Response)
}

// Health 查询外部插件健康状态。
func (t *GRPCTransport) Health(ctx context.Context, session Session) (contract.HealthStatus, error) {
	if err := t.validateRequestSession(session); err != nil {
		return contract.HealthStatus{}, err
	}
	requestID := t.newRequestID("health")
	frame, err := t.requestFrame(ctx, requestID, wireFrame{Type: "health", RequestID: requestID}, "health_response")
	if err != nil {
		return contract.HealthStatus{}, err
	}
	if frame.Error != nil {
		return contract.HealthStatus{}, wireToError(frame.Error)
	}
	return contract.HealthStatus{State: contract.HealthState(frame.State), Message: frame.Message, Details: cloneStringMap(frame.Details), CheckedAt: time.Now().UTC()}, nil
}

// Shutdown 请求外部插件优雅退出，并清理 transport 资源。
func (t *GRPCTransport) Shutdown(ctx context.Context, session Session) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := t.validateRequestSession(session); err != nil {
		return err
	}
	t.mu.Lock()
	if t.shutdownRequested {
		t.mu.Unlock()
		return t.waitForProcess(ctx)
	}
	t.shutdownRequested = true
	disconnected := t.disconnected
	t.mu.Unlock()
	if disconnected == nil {
		return errSessionNotReady
	}
	if t.isDisconnected() {
		killErr := t.killProcess()
		cleanupErr := t.cleanup()
		return errors.Join(killErr, cleanupErr)
	}
	requestID := t.newRequestID("shutdown")
	_, requestErr := t.requestFrame(ctx, requestID, wireFrame{Type: "shutdown", RequestID: requestID}, "shutdown_ack")
	if requestErr != nil {
		if errors.Is(requestErr, errSessionClosed) || errors.Is(requestErr, context.Canceled) {
			if waitErr := t.waitForProcess(ctx); waitErr == nil {
				return t.cleanup()
			}
		}
		killErr := t.killProcess()
		cleanupErr := t.cleanup()
		return errors.Join(requestErr, killErr, cleanupErr)
	}
	waitErr := t.waitForProcess(ctx)
	cleanupErr := t.cleanup()
	return errors.Join(waitErr, cleanupErr)
}

// Kill 强制终止外部进程并回收 Unix Socket。
func (t *GRPCTransport) Kill() error {
	killErr := t.killProcess()
	cleanupErr := t.cleanup()
	return errors.Join(killErr, cleanupErr)
}

func (t *GRPCTransport) killProcess() error {
	t.mu.RLock()
	command := t.process
	pgid := t.processGroupID
	enforcer := t.memoryEnforcer
	t.mu.RUnlock()
	if command == nil || command.Process == nil {
		return nil
	}
	// cgroup 路径下 systemd 以 KillMode=control-group 管理整棵进程树；
	// 直接向进程组发信号会在 systemctl stop 前制造预期的 ESRCH。
	if enforcer != nil && enforcer.reaper != nil {
		return enforcer.reaper()
	}
	// 优先向整个进程组发信号，回收 fork 出的子孙进程；
	// 进程组不可用时退回为只终止直接子进程。
	if pgid != 0 {
		if err := terminateProcessGroup(pgid, sigForceKill); err != nil {
			return err
		}
	}
	if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}

func (t *GRPCTransport) waitForProcess(ctx context.Context) error {
	t.mu.RLock()
	processDone := t.processDone
	t.mu.RUnlock()
	if processDone == nil {
		return nil
	}
	select {
	case <-processDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *GRPCTransport) closeProcessDone() {
	t.processDoneOnce.Do(func() {
		t.mu.RLock()
		processDone := t.processDone
		t.mu.RUnlock()
		if processDone != nil {
			close(processDone)
		}
	})
}

func (t *GRPCTransport) waitProcess(command *exec.Cmd) {
	t.mu.RLock()
	managed := t.memoryEnforcer != nil && t.memoryEnforcer.managed
	t.mu.RUnlock()
	err := waitPluginProcess(command, managed)
	t.mu.Lock()
	t.processErr = err
	shutdown := t.shutdownRequested
	t.mu.Unlock()
	t.markDisconnected(processExitError(err))
	_ = t.cleanup()
	t.closeProcessDone()
	if err != nil && !shutdown {
		t.config.Logger.Warn("外部插件进程退出", "plugin_id", t.manifest.ID, "error", err)
	}
}

// waitPluginProcess 等待插件进程退出并返回其退出错误。
//
// 直接 exec 启动的进程走 exec.Cmd.Wait，能正确释放资源并报告退出状态。
// cgroup 路径下进程由 systemd transient service 启动，不是 Runtime 的
// 子进程，os.Process.Wait 只会返回 ECHILD；改用轮询探测 PID 消失。
func waitPluginProcess(command *exec.Cmd, managed bool) error {
	if managed {
		return waitManagedProcess(command)
	}
	return command.Wait()
}

func processExitError(err error) error {
	if err == nil {
		return errSessionClosed
	}
	return fmt.Errorf("外部插件进程已退出: %w", err)
}

func (t *GRPCTransport) processExitError() error {
	t.mu.RLock()
	err := t.processErr
	t.mu.RUnlock()
	return processExitError(err)
}

func (t *GRPCTransport) handshakeFailureOrClosed() error {
	t.mu.RLock()
	err := t.handshakeErr
	t.mu.RUnlock()
	if err != nil {
		return err
	}
	return errSessionClosed
}

func (t *GRPCTransport) validateExpectedSession(manifest contract.Manifest, session Session) error {
	if manifest.ID == "" || session.PluginID != manifest.ID || session.Token == "" || !session.ExpiresAt.After(time.Now()) {
		return errors.New("外部插件握手身份无效")
	}
	t.mu.RLock()
	expected := t.expected
	expectedManifest := t.manifest
	started := t.started
	t.mu.RUnlock()
	if !started {
		return errSessionNotReady
	}
	if expected.PluginID != session.PluginID || expected.Token != session.Token || !expected.ExpiresAt.Equal(session.ExpiresAt) {
		return errors.New("外部插件会话凭据不匹配")
	}
	if expectedManifest.ID != "" && (expectedManifest.ID != manifest.ID || expectedManifest.Version != manifest.Version) {
		return errors.New("外部插件握手清单不一致")
	}
	return nil
}

func (t *GRPCTransport) validateRequestSession(session Session) error {
	if session.PluginID == "" || session.Token == "" || !session.ExpiresAt.After(time.Now()) {
		return errors.New("外部插件会话无效")
	}
	t.mu.RLock()
	expected := t.expected
	started := t.started
	ready := t.ready
	t.mu.RUnlock()
	if !started {
		return errSessionNotReady
	}
	if session.PluginID != expected.PluginID || session.Token != expected.Token || !session.ExpiresAt.Equal(expected.ExpiresAt) {
		return errors.New("外部插件会话凭据不匹配")
	}
	if !ready || t.isDisconnected() {
		return errSessionClosed
	}
	return nil
}

func (t *GRPCTransport) markDisconnected(reason error) {
	t.disconnectOnce.Do(func() {
		t.mu.Lock()
		if t.handshakeErr == nil && reason != nil && !t.ready {
			t.handshakeErr = reason
		}
		t.ready = false
		t.stream = nil
		cancel := t.sessionCancel
		t.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if t.disconnected != nil {
			close(t.disconnected)
		}
		t.failPending(errSessionClosed)
	})
}

func (t *GRPCTransport) setHandshakeError(err error) {
	t.mu.Lock()
	if t.handshakeErr == nil {
		t.handshakeErr = err
	}
	t.mu.Unlock()
	t.authOnce.Do(func() { close(t.authenticated) })
}

func (t *GRPCTransport) isDisconnected() bool {
	t.mu.RLock()
	channel := t.disconnected
	t.mu.RUnlock()
	if channel == nil {
		return true
	}
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

func (t *GRPCTransport) newRequestID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, t.nextRequestID.Add(1))
}

func (t *GRPCTransport) request(ctx context.Context, requestID string, frame wireFrame, responseType string) error {
	_, err := t.requestFrame(ctx, requestID, frame, responseType)
	return err
}

func (t *GRPCTransport) requestFrame(ctx context.Context, requestID string, frame wireFrame, responseType string) (wireFrame, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.RLock()
	inFlight, disconnected := t.inFlight, t.disconnected
	t.mu.RUnlock()
	if inFlight == nil || disconnected == nil {
		return wireFrame{}, errSessionNotReady
	}
	select {
	case inFlight <- struct{}{}:
		defer func() { <-inFlight }()
	case <-disconnected:
		return wireFrame{}, errSessionClosed
	case <-ctx.Done():
		return wireFrame{}, ctx.Err()
	}

	response := make(chan wireFrame, 1)
	t.mu.Lock()
	if !t.ready || t.stream == nil || t.stopped {
		t.mu.Unlock()
		return wireFrame{}, errSessionClosed
	}
	t.pendingMu.Lock()
	t.pending[requestID] = response
	t.pendingMu.Unlock()
	stream := t.stream
	t.mu.Unlock()
	if err := stream.send(ctx, frame); err != nil {
		t.removePending(requestID)
		t.markDisconnected(err)
		_ = t.cleanup()
		return wireFrame{}, err
	}
	select {
	case result := <-response:
		if result.Type == "transport_error" {
			if result.Error == nil {
				return wireFrame{}, errSessionClosed
			}
			return wireFrame{}, wireToError(result.Error)
		}
		if result.Type != responseType {
			return wireFrame{}, fmt.Errorf("外部插件响应类型错误: %s", result.Type)
		}
		if result.Error != nil {
			return wireFrame{}, wireToError(result.Error)
		}
		return result, nil
	case <-disconnected:
		return wireFrame{}, errSessionClosed
	case <-ctx.Done():
		t.removePending(requestID)
		return wireFrame{}, ctx.Err()
	}
}

func (t *GRPCTransport) removePending(requestID string) {
	t.pendingMu.Lock()
	delete(t.pending, requestID)
	t.pendingMu.Unlock()
}

func (t *GRPCTransport) resolve(frame wireFrame) {
	if frame.RequestID == "" {
		return
	}
	t.pendingMu.Lock()
	waiter, exists := t.pending[frame.RequestID]
	if exists {
		delete(t.pending, frame.RequestID)
	}
	t.pendingMu.Unlock()
	if exists {
		waiter <- frame
	}
}

func (t *GRPCTransport) failPending(err error) {
	frame := wireFrame{Type: "transport_error", Error: &wireError{Code: string(contract.ErrorUnavailable), Message: err.Error(), Retryable: true}}
	t.pendingMu.Lock()
	waiters := make([]chan wireFrame, 0, len(t.pending))
	for id, waiter := range t.pending {
		delete(t.pending, id)
		waiters = append(waiters, waiter)
	}
	t.pendingMu.Unlock()
	for _, waiter := range waiters {
		waiter <- frame
	}
}

func (t *GRPCTransport) cleanup() error {
	t.cleanupOnce.Do(func() {
		t.mu.Lock()
		t.stopped = true
		server := t.server
		listener := t.listener
		socketPath := t.socketPath
		socketInfo := t.socketInfo
		cancel := t.sessionCancel
		stream := t.stream
		enforcer := t.memoryEnforcer
		t.server = nil
		t.listener = nil
		t.memoryEnforcer = nil
		t.inboundMu.Lock()
		t.inboundClosing = true
		t.inboundMu.Unlock()
		t.mu.Unlock()
		if enforcer != nil && enforcer.stop != nil {
			enforcer.stop()
		}
		if cancel != nil {
			cancel()
		}
		if stream != nil {
			stream.close()
		}
		t.markDisconnected(errSessionClosed)
		// 有限等待仍在运行中的 publish/invoke_host 回调退出，避免 cleanup 即丢回执。
		// 若回调自身卡死，则不能让 transport 永远阻塞 —— 超时就继续清理。
		done := make(chan struct{})
		go func() {
			t.inboundWG.Wait()
			close(done)
		}()
		wait := time.NewTimer(5 * time.Second)
		select {
		case <-done:
		case <-wait.C:
			t.config.Logger.Warn("外部插件入站回调未在超时内退出，继续清理")
		}
		wait.Stop()
		if server != nil {
			server.Stop()
		} else if listener != nil {
			_ = listener.Close()
		}
		if socketPath != "" {
			if err := removeOwnedSocket(socketPath, socketInfo); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.mu.Lock()
				t.cleanupErr = errors.Join(t.cleanupErr, err)
				t.mu.Unlock()
			}
		}
	})
	t.mu.RLock()
	err := t.cleanupErr
	t.mu.RUnlock()
	return err
}

// grpcSessionSend 是发送队列中的一项。队列和唯一 writer goroutine
// 保证底层 gRPC Send 永远不会被多个 goroutine 并发调用。
type grpcSessionSend struct {
	ctx    context.Context
	frame  wireFrame
	result chan error
}

// grpcSessionStream 负责将 gRPC 帧串行发送到底层流。
// gRPC ServerStream 没有带 context 的 Send API，因此通过单独 writer
// 将“排队/等待”做成可取消；真正已经进入 Send 的调用由 RPC context
// 和 server.Stop 负责中断，避免为每个发送创建无法回收的 goroutine。
type grpcSessionStream struct {
	stream    botfabricv1.ProcessPlugin_SessionServer
	sendQueue chan grpcSessionSend
	done      chan struct{}
	closeOnce sync.Once
}

func newGRPCSessionStream(stream botfabricv1.ProcessPlugin_SessionServer) *grpcSessionStream {
	result := &grpcSessionStream{
		stream:    stream,
		sendQueue: make(chan grpcSessionSend, 32),
		done:      make(chan struct{}),
	}
	go result.runSender()
	return result
}

func (s *grpcSessionStream) runSender() {
	for {
		select {
		case <-s.done:
			return
		case request := <-s.sendQueue:
			if err := request.ctx.Err(); err != nil {
				request.result <- err
				continue
			}
			value, err := encodeFrame(request.frame)
			if err == nil {
				err = s.stream.Send(value)
			}
			request.result <- err
			if err != nil {
				s.close()
				return
			}
		}
	}
}

func (s *grpcSessionStream) close() {
	s.closeOnce.Do(func() { close(s.done) })
}

// send 支持在进入发送队列前及排队等待期间响应 context 取消。
func (s *grpcSessionStream) send(ctx context.Context, frame wireFrame) error {
	if ctx == nil {
		ctx = context.Background()
	}
	request := grpcSessionSend{ctx: ctx, frame: frame, result: make(chan error, 1)}
	select {
	case <-s.done:
		return errSessionClosed
	case <-ctx.Done():
		return ctx.Err()
	case s.sendQueue <- request:
	}
	select {
	case err := <-request.result:
		return err
	case <-s.done:
		select {
		case err := <-request.result:
			return err
		default:
			return errSessionClosed
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// grpcService 将数据面 Session 与保留的旧控制面 RPC 隔离，避免 Transport 的兼容方法
// 与生成 gRPC 服务方法发生签名冲突。
type grpcService struct {
	botfabricv1.UnimplementedProcessPluginServer
	transport *GRPCTransport
}

func (s *grpcService) Session(stream botfabricv1.ProcessPlugin_SessionServer) error {
	return s.transport.serveSession(stream)
}

func (s *grpcService) Handshake(context.Context, *botfabricv1.HandshakeRequest) (*botfabricv1.HandshakeResponse, error) {
	return nil, errors.New("外部插件旧版 Handshake RPC 不受支持，请使用 Session")
}

func (s *grpcService) Invoke(context.Context, *botfabricv1.InvokeRequest) (*botfabricv1.InvokeResponse, error) {
	return nil, errors.New("外部插件旧版 Invoke RPC 不受支持，请使用 Session")
}

func (s *grpcService) Subscribe(*botfabricv1.SubscribeRequest, botfabricv1.ProcessPlugin_SubscribeServer) error {
	return errors.New("外部插件旧版 Subscribe RPC 不受支持，请使用 Session")
}

func (s *grpcService) Health(context.Context, *botfabricv1.HealthRequest) (*botfabricv1.HealthResponse, error) {
	return nil, errors.New("外部插件旧版 Health RPC 不受支持，请使用 Session")
}

func (s *grpcService) Shutdown(context.Context, *botfabricv1.ShutdownRequest) (*botfabricv1.ShutdownResponse, error) {
	return nil, errors.New("外部插件旧版 Shutdown RPC 不受支持，请使用 Session")
}

// Session 实现外部插件 gRPC 双向 Session。
func (t *GRPCTransport) Session(stream botfabricv1.ProcessPlugin_SessionServer) error {
	return t.serveSession(stream)
}

func (t *GRPCTransport) serveSession(stream botfabricv1.ProcessPlugin_SessionServer) error {
	value, err := stream.Recv()
	if err != nil {
		t.setHandshakeError(err)
		t.markDisconnected(err)
		_ = t.cleanup()
		return err
	}
	frame, err := decodeFrame(value)
	if err != nil {
		t.setHandshakeError(err)
		t.markDisconnected(err)
		_ = t.cleanup()
		return err
	}
	if frame.Type != "hello" {
		err = errors.New("外部插件 Session 首帧必须是 hello")
		t.setHandshakeError(err)
		t.markDisconnected(err)
		_ = t.cleanup()
		return err
	}
	if err := t.authenticate(frame); err != nil {
		t.setHandshakeError(err)
		if value, encodeErr := encodeFrame(wireFrame{Type: "ready", Accepted: false, Error: &wireError{Code: string(contract.ErrorPermissionDenied), Message: "握手失败"}}); encodeErr == nil {
			_ = stream.Send(value)
		}
		t.markDisconnected(err)
		_ = t.killProcess()
		_ = t.cleanup()
		return err
	}

	connection := newGRPCSessionStream(stream)
	t.mu.Lock()
	if t.sessionClaimed || t.stream != nil || t.stopped {
		t.mu.Unlock()
		return errors.New("外部插件重复建立 Session")
	}
	t.sessionClaimed = true
	t.stream = connection
	t.ready = true
	sessionContext, cancel := context.WithCancel(stream.Context())
	t.sessionCancel = cancel
	t.mu.Unlock()
	t.connectedOnce.Do(func() { close(t.connected) })
	if err := connection.send(stream.Context(), wireFrame{Type: "ready", Accepted: true, PluginID: t.manifest.ID, ProtocolVersion: processProtocolVersion, ExpiresUnix: t.expected.ExpiresAt.UnixMilli()}); err != nil {
		t.setHandshakeError(err)
		t.markDisconnected(err)
		_ = t.cleanup()
		return err
	}
	t.authOnce.Do(func() { close(t.authenticated) })
	defer func() {
		cancel()
		t.markDisconnected(errSessionClosed)
		_ = t.cleanup()
	}()

	for {
		value, err := stream.Recv()
		if err != nil {
			return err
		}
		frame, err := decodeFrame(value)
		if err != nil {
			return err
		}
		t.handleFrame(connection, sessionContext, frame)
	}
}

func (t *GRPCTransport) authenticate(frame wireFrame) error {
	t.mu.RLock()
	expected := t.expected
	manifest := t.manifest
	t.mu.RUnlock()
	if expected.PluginID == "" || frame.PluginID != expected.PluginID || frame.PluginID != manifest.ID {
		return errors.New("外部插件身份与清单不一致")
	}
	if frame.SessionToken == "" || frame.SessionToken != expected.Token {
		return errors.New("外部插件会话凭据无效")
	}
	if frame.PluginVersion != manifest.Version {
		return errors.New("外部插件版本与清单不一致")
	}
	if frame.ProtocolVersion != processProtocolVersion {
		return errors.New("外部插件协议版本不兼容")
	}
	if !expected.ExpiresAt.After(time.Now()) {
		return errors.New("外部插件会话已过期")
	}
	return nil
}

func (t *GRPCTransport) handleFrame(stream *grpcSessionStream, ctx context.Context, frame wireFrame) {
	switch frame.Type {
	case "event_ack", "invoke_plugin_response", "health_response", "shutdown_ack":
		t.resolve(frame)
	case "publish_event":
		t.startInbound(ctx, func() { t.handlePublish(ctx, stream, frame) })
	case "invoke_host":
		t.startInbound(ctx, func() { t.handleInvokeHost(ctx, stream, frame) })
	default:
		_ = stream.send(ctx, wireFrame{Type: "error", RequestID: frame.RequestID, Error: &wireError{Code: string(contract.ErrorInvalidArgument), Message: "未知外部插件帧类型: " + frame.Type}})
	}
}

func (t *GRPCTransport) startInbound(ctx context.Context, handler func()) {
	t.inboundMu.Lock()
	if t.inboundClosing {
		t.inboundMu.Unlock()
		return
	}
	t.inboundWG.Add(1)
	t.inboundMu.Unlock()

	t.mu.RLock()
	semaphore := t.inboundLimit
	t.mu.RUnlock()
	if semaphore == nil {
		t.inboundWG.Done()
		return
	}
	select {
	case semaphore <- struct{}{}:
	case <-ctx.Done():
		t.inboundWG.Done()
		return
	}
	go func() {
		defer t.inboundWG.Done()
		defer func() { <-semaphore }()
		handler()
	}()
}

func (t *GRPCTransport) handlePublish(ctx context.Context, stream *grpcSessionStream, frame wireFrame) {
	response := wireFrame{Type: "publish_result", RequestID: frame.RequestID}
	if frame.RequestID == "" || frame.Event == nil {
		response.Error = &wireError{Code: string(contract.ErrorInvalidArgument), Message: "publish_event 缺少 request_id 或 event"}
		_ = stream.send(ctx, response)
		return
	}
	t.mu.RLock()
	callback := t.callbacks.Publish
	t.mu.RUnlock()
	var err error
	if callback == nil {
		err = errSessionNotReady
	} else {
		err = callback(ctx, wireToEvent(frame.Event))
	}
	if err != nil {
		response.Error = errorToWire(asServiceErrorValue(err))
	}
	_ = stream.send(ctx, response)
}

func (t *GRPCTransport) handleInvokeHost(ctx context.Context, stream *grpcSessionStream, frame wireFrame) {
	response := wireFrame{Type: "invoke_host_response", RequestID: frame.RequestID}
	if frame.RequestID == "" || frame.Request == nil {
		response.Error = &wireError{Code: string(contract.ErrorInvalidArgument), Message: "invoke_host 缺少 request_id 或 request"}
		response.Response = &wireResponse{Error: response.Error}
		_ = stream.send(ctx, response)
		return
	}
	t.mu.RLock()
	callback := t.callbacks.Invoke
	t.mu.RUnlock()
	var result contract.Response
	var err error
	if callback == nil {
		err = errSessionNotReady
	} else {
		result, err = callback(ctx, wireToRequest(frame.Request))
	}
	response.Response = responseToWire(result, err)
	_ = stream.send(ctx, response)
}

func asServiceErrorValue(err error) *contract.ServiceError {
	var serviceError *contract.ServiceError
	if errors.As(err, &serviceError) {
		return serviceError
	}
	return &contract.ServiceError{Code: contract.ErrorInternal, Message: err.Error()}
}

func validatePrivateTmpSocketPath(socketPath string) error {
	absolute, err := filepath.Abs(filepath.Clean(socketPath))
	if err != nil {
		return fmt.Errorf("解析外部插件 Unix Socket 路径: %w", err)
	}

	resolved := filepath.Dir(absolute)
	var suffix []string
	for {
		candidate := resolved
		resolved, err = filepath.EvalSymlinks(candidate)
		if err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("解析 Unix Socket 目录: %w", err)
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return fmt.Errorf("无法解析 Unix Socket 目录: %s", filepath.Dir(absolute))
		}
		suffix = append(suffix, filepath.Base(candidate))
		resolved = parent
	}

	// 即使目录当前不存在也拒绝临时根下的路径；若父目录可解析，再检查符号链接目标。
	directory := filepath.Dir(absolute)
	for _, root := range []string{"/tmp", "/var/tmp"} {
		blocked := root
		if target, resolveErr := filepath.EvalSymlinks(root); resolveErr == nil {
			blocked = target
		}
		for _, candidate := range []string{directory, resolved} {
			for _, base := range []string{root, blocked} {
				relative, relErr := filepath.Rel(base, candidate)
				if relErr == nil && (relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))) {
					return fmt.Errorf("PrivateTmp 插件的 Unix Socket 不能位于 %s 或其子目录", root)
				}
			}
		}
	}
	return nil
}

func prepareSocketPath(path string) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("创建外部插件 Unix Socket 目录: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("设置外部插件 Unix Socket 目录权限: %w", err)
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("检查外部插件 Unix Socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("外部插件 Unix Socket 路径已被非 Socket 文件占用: %s", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("清理外部插件残留 Unix Socket: %w", err)
	}
	return nil
}

func removeOwnedSocket(path string, expected os.FileInfo) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("拒绝删除非 Socket 文件: %s", path)
	}
	if expected != nil && !os.SameFile(info, expected) {
		return fmt.Errorf("拒绝删除其他 transport 的 Unix Socket: %s", path)
	}
	return os.Remove(path)
}

type processLogWriter struct {
	logger   *slog.Logger
	pluginID string
	stream   string
}

func (w processLogWriter) Write(data []byte) (int, error) {
	if len(data) > 0 {
		message := string(data)
		if len(message) > 4096 {
			message = message[:4096] + "…"
		}
		w.logger.Info("外部插件输出", "plugin_id", w.pluginID, "stream", w.stream, "message", message)
	}
	return len(data), nil
}

// NewSessionIssuer 创建使用 crypto/rand 的随机会话签发器。
func NewSessionIssuer() SessionIssuer { return randomSessionIssuer{} }

type randomSessionIssuer struct{}

func (randomSessionIssuer) Issue(pluginID string, ttl time.Duration) (Session, error) {
	if pluginID == "" || ttl <= 0 {
		return Session{}, errors.New("插件 ID 和会话 TTL 必须有效")
	}
	bytes := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, bytes); err != nil {
		return Session{}, fmt.Errorf("生成外部插件会话凭据: %w", err)
	}
	return Session{PluginID: pluginID, Token: hex.EncodeToString(bytes), ExpiresAt: time.Now().Add(ttl)}, nil
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

// memoryLimitBytes 把清单 Memory 字段（如 128MiB）解析为字节数。
// 未声明或无法解析时返回 0，交由平台层跳过限制。
func memoryLimitBytes(manifest contract.Manifest) int64 {
	if manifest.Limits.Memory == "" {
		return 0
	}
	units := []struct {
		suffix string
		factor int64
	}{
		{"GiB", 1024 * 1024 * 1024},
		{"MiB", 1024 * 1024},
		{"KiB", 1024},
		{"B", 1},
	}
	for _, unit := range units {
		if strings.HasSuffix(manifest.Limits.Memory, unit.suffix) {
			number := strings.TrimSpace(strings.TrimSuffix(manifest.Limits.Memory, unit.suffix))
			value, err := strconv.ParseInt(number, 10, 64)
			if err != nil || value < 0 {
				return 0
			}
			if value > (1<<63-1)/unit.factor {
				return 0
			}
			return value * unit.factor
		}
	}
	return 0
}
