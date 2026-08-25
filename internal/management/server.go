package management

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/runtime"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// Server 是只读管理和诊断 HTTP 服务。
type Server struct {
	httpServer  *http.Server
	listener    net.Listener
	manager     *runtime.Manager
	deadLetters *eventbus.MemoryDeadLetterSink
	token       string
}

// New 创建 Bearer Token 保护的管理 API。
func New(address, token string, manager *runtime.Manager, deadLetters *eventbus.MemoryDeadLetterSink) (*Server, error) {
	if address == "" || len(token) < 16 || manager == nil || deadLetters == nil {
		return nil, fmt.Errorf("管理 API 地址、token、运行时和死信存储不能为空")
	}
	server := &Server{manager: manager, deadLetters: deadLetters, token: token}
	mux := http.NewServeMux()
	mux.Handle("/healthz", server.auth(server.handleHealth))
	mux.Handle("/v1/plugins", server.auth(server.handlePlugins))
	mux.Handle("/v1/dead-letters", server.auth(server.handleDeadLetters))
	server.httpServer = &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return server, nil
}

// Start 在后台监听管理 API。
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return err
	}
	s.listener = listener
	go func() { _ = s.httpServer.Serve(listener) }()
	return nil
}

// Addr 返回实际监听地址，便于测试动态端口。
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) auth(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		const prefix = "Bearer "
		value := request.Header.Get("Authorization")
		provided := []byte(strings.TrimPrefix(value, prefix))
		wanted := []byte(s.token)
		if !strings.HasPrefix(value, prefix) || len(provided) != len(wanted) || subtle.ConstantTimeCompare(provided, wanted) != 1 {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		next(writer, request)
	})
}

func (s *Server) handleHealth(writer http.ResponseWriter, request *http.Request) {
	statuses := s.manager.Health(request.Context())
	healthy := true
	for _, status := range statuses {
		if status.State != contract.HealthHealthy {
			healthy = false
			break
		}
	}
	if !healthy {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}
	writeJSON(writer, map[string]any{"healthy": healthy, "plugins": statuses})
}

func (s *Server) handlePlugins(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, s.manager.ListPlugins(request.Context()))
}

func (s *Server) handleDeadLetters(writer http.ResponseWriter, _ *http.Request) {
	entries := s.deadLetters.Entries()
	result := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		result = append(result, map[string]any{"event_id": entry.Event.ID, "topic": entry.Event.Topic, "trace_id": entry.Event.TraceID, "subscriber_id": entry.SubscriberID, "error": entry.Error, "failed_at": entry.FailedAt})
	}
	writeJSON(writer, result)
}

func writeJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(value)
}
