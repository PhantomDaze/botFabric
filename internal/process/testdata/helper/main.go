package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	botfabricv1 "github.com/PhantomDaze/botfabric/gen/botfabric/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

type frame struct {
	Type            string            `json:"type"`
	RequestID       string            `json:"request_id,omitempty"`
	PluginID        string            `json:"plugin_id,omitempty"`
	PluginVersion   string            `json:"plugin_version,omitempty"`
	ProtocolVersion string            `json:"protocol_version,omitempty"`
	SessionToken    string            `json:"session_token,omitempty"`
	Accepted        bool              `json:"accepted,omitempty"`
	State           string            `json:"state,omitempty"`
	Message         string            `json:"message,omitempty"`
	Details         map[string]string `json:"details,omitempty"`
	Event           *event            `json:"event,omitempty"`
	Request         *request          `json:"request,omitempty"`
	Response        *response         `json:"response,omitempty"`
}

type event struct {
	ID            string `json:"id"`
	Topic         string `json:"topic"`
	Schema        string `json:"schema"`
	TimestampUnix int64  `json:"timestamp_unix_millis"`
	Source        string `json:"source"`
	BotID         string `json:"bot_id"`
	Payload       []byte `json:"payload,omitempty"`
}

type request struct {
	Service       string            `json:"service"`
	Method        string            `json:"method"`
	Caller        string            `json:"caller,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Payload       []byte            `json:"payload,omitempty"`
}

type response struct {
	Payload []byte `json:"payload,omitempty"`
}

type sessionClient struct {
	stream grpc.BidiStreamingClient[structpb.Struct, structpb.Struct]
	mu     sync.Mutex
}

func main() {
	if os.Getenv("BOTFABRIC_HELPER_CHILD") == "1" {
		childMode()
		return
	}
	if os.Getenv("BOTFABRIC_HELPER_ALLOC_PROBE") == "1" {
		allocProbe()
		return
	}
	if os.Getenv("BOTFABRIC_HELPER_FORK_ALLOC") == "1" {
		// fork 一个 probe 子进程后主进程继续正常握手；
		// 用于验证 cgroup 内存上限对孙进程同样生效。
		forkAllocProbe()
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	socket := os.Getenv("BOTFABRIC_SOCKET")
	pluginID := os.Getenv("BOTFABRIC_PLUGIN_ID")
	pluginVersion := os.Getenv("BOTFABRIC_PLUGIN_VERSION")
	protocolVersion := os.Getenv("BOTFABRIC_PROTOCOL_VERSION")
	token := os.Getenv("BOTFABRIC_SESSION_TOKEN")
	if socket == "" || pluginID == "" || token == "" {
		return errors.New("helper 缺少 Runtime 注入的 Session 环境变量")
	}
	if os.Getenv("BOTFABRIC_HELPER_BAD_TOKEN") == "1" {
		token = "invalid-token"
	}
	if os.Getenv("BOTFABRIC_HELPER_FORK_CHILD") == "1" {
		if err := forkLongRunningChild(); err != nil {
			return fmt.Errorf("fork 长驻子进程: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "passthrough:///botfabric", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("连接 Runtime: %w", err)
	}
	defer conn.Close()

	client := botfabricv1.NewProcessPluginClient(conn)
	stream, err := client.Session(ctx)
	if err != nil {
		return fmt.Errorf("建立 Session: %w", err)
	}
	session := &sessionClient{stream: stream}
	if err := session.send(frame{
		Type:            "hello",
		PluginID:        pluginID,
		PluginVersion:   pluginVersion,
		ProtocolVersion: protocolVersion,
		SessionToken:    token,
	}); err != nil {
		return fmt.Errorf("发送 hello: %w", err)
	}
	value, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("接收 ready: %w", err)
	}
	ready, err := decode(value)
	if err != nil {
		return err
	}
	if ready.Type != "ready" || !ready.Accepted {
		return errors.New("Runtime 拒绝 helper 握手")
	}

	for {
		value, err := stream.Recv()
		if err != nil {
			return nil
		}
		incoming, err := decode(value)
		if err != nil {
			return err
		}
		switch incoming.Type {
		case "event":
			if os.Getenv("BOTFABRIC_HELPER_SEND_HOST_CALLS") == "1" {
				go session.sendHostCalls()
			}
			if err := session.send(frame{Type: "event_ack", RequestID: incoming.RequestID}); err != nil {
				return err
			}
		case "invoke_plugin":
			if os.Getenv("BOTFABRIC_HELPER_BLOCK_INVOKE") == "1" {
				select {}
			}
			payload := []byte("plugin:")
			if incoming.Request != nil {
				payload = append(payload, incoming.Request.Payload...)
			}
			if err := session.send(frame{
				Type:      "invoke_plugin_response",
				RequestID: incoming.RequestID,
				Response:  &response{Payload: payload},
			}); err != nil {
				return err
			}
		case "health":
			if err := session.send(frame{
				Type:      "health_response",
				RequestID: incoming.RequestID,
				State:     "healthy",
				Message:   "helper healthy",
				Details:   map[string]string{"source": "process-helper"},
			}); err != nil {
				return err
			}
		case "shutdown":
			if err := session.send(frame{Type: "shutdown_ack", RequestID: incoming.RequestID, Accepted: true}); err != nil {
				return err
			}
			return nil
		case "publish_result", "invoke_host_response":
			// 这些响应由测试 helper 发起的 Host 调用异步消费；此处无需处理。
		default:
			return fmt.Errorf("helper 收到未知帧类型: %s", incoming.Type)
		}
	}
}

// forkLongRunningChild fork 出一个长驻子进程，用于验证 Runtime 终止
// 进程组时能回收子孙进程。子进程向 stdout 输出自己的 PID 后阻塞等待。
func forkLongRunningChild() error {
	command := exec.Command(os.Args[0])
	command.Env = append(os.Environ(), "BOTFABRIC_HELPER_CHILD=1")
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	if err := command.Start(); err != nil {
		return err
	}
	// 子进程输出 PID 后长驻，不能用 ReadAll 等 EOF；按行读一次即返回。
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		return err
	}
	childPID := strings.TrimSpace(line)
	fmt.Fprintf(os.Stderr, "helper child pid: %s\n", childPID)
	return nil
}

// childMode 是被 fork 的子进程入口：输出自身 PID 后长驻。
// 通过 BOTFABRIC_HELPER_CHILD 环境变量区分，不再连接 Runtime。
func childMode() {
	fmt.Println(os.Getpid())
	select {}
}

// forkAllocProbe fork 一个执行 allocProbe 的子进程，主进程继续正常流程。
// 子进程的 PID 写到 stderr 供测试解析。
func forkAllocProbe() {
	command := exec.Command(os.Args[0])
	command.Env = append(os.Environ(),
		"BOTFABRIC_HELPER_ALLOC_PROBE=1",
		"BOTFABRIC_HELPER_ALLOC_BYTES="+os.Getenv("BOTFABRIC_HELPER_FORK_ALLOC_BYTES"),
	)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "fork alloc probe 失败: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "alloc child pid: %d\n", command.Process.Pid)
	go func() { _ = command.Wait() }()
}

// allocProbe 是被 fork 的探测子进程入口：申请并逐页触碰 limit 指向的
// 大内存后驻留，用于验证 RSS 内存监控是否生效。
func allocProbe() {
	limit := os.Getenv("BOTFABRIC_HELPER_ALLOC_BYTES")
	bytes, err := strconv.ParseInt(limit, 10, 64)
	if err != nil || bytes <= 0 {
		fmt.Fprintln(os.Stderr, "alloc probe: 缺少 BOTFABRIC_HELPER_ALLOC_BYTES")
		os.Exit(2)
	}
	fmt.Fprintln(os.Stderr, "alloc probe pid: ", os.Getpid())
	if startFile := os.Getenv("BOTFABRIC_HELPER_ALLOC_START_FILE"); startFile != "" {
		for {
			if _, err := os.Stat(startFile); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	// 逐页触碰让内核真正分配物理页；未触碰的匿名内存不计入 RSS。
	buffer := make([]byte, bytes)
	for offset := int64(0); offset < bytes; offset += 4096 {
		buffer[offset] = 1
	}
	fmt.Fprintln(os.Stderr, "alloc probe: succeeded")
	select {}
}

func (s *sessionClient) sendHostCalls() {
	_ = s.send(frame{
		Type:      "publish_event",
		RequestID: "helper-publish-1",
		Event: &event{
			ID:      "helper-event-1",
			Topic:   os.Getenv("BOTFABRIC_HELPER_PUBLISH_TOPIC"),
			Schema:  "botfabric.test/v1",
			Source:  "forged-source",
			BotID:   "helper-bot",
			Payload: []byte("from-helper"),
		},
	})
	_ = s.send(frame{
		Type:      "invoke_host",
		RequestID: "helper-invoke-1",
		Request: &request{
			Service: os.Getenv("BOTFABRIC_HELPER_HOST_SERVICE"),
			Method:  "Run",
			Caller:  "forged-caller",
			Payload: []byte("host-request"),
		},
	})
}

func (s *sessionClient) send(value frame) error {
	encoded, err := encode(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.Send(encoded)
}

func encode(value frame) (*structpb.Struct, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	return structpb.NewStruct(fields)
}

func decode(value *structpb.Struct) (frame, error) {
	if value == nil {
		return frame{}, errors.New("helper 收到空帧")
	}
	data, err := json.Marshal(value.AsMap())
	if err != nil {
		return frame{}, err
	}
	var result frame
	if err := json.Unmarshal(data, &result); err != nil {
		return frame{}, err
	}
	if result.Type == "" {
		return frame{}, errors.New("helper 收到缺少 type 的帧")
	}
	return result, nil
}
