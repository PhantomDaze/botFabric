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
