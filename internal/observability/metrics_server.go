package observability

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// MetricsServer 是只暴露 Prometheus 指标端点的独立 HTTP 服务。
// 指标不包含事件载荷或凭据，但部署时仍建议仅绑定回环或内网地址。
type MetricsServer struct {
	httpServer *http.Server
	listener   net.Listener
}

// NewMetricsServer 创建指标服务；地址不能为空。
func NewMetricsServer(address string, metrics *Metrics) (*MetricsServer, error) {
	if address == "" || metrics == nil {
		return nil, errors.New("指标服务地址和指标注册表不能为空")
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	return &MetricsServer{httpServer: &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}}, nil
}

// Start 监听并后台提供指标端点。
func (s *MetricsServer) Start() error {
	listener, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return err
	}
	s.listener = listener
	go func() { _ = s.httpServer.Serve(listener) }()
	return nil
}

// Addr 返回实际监听地址，便于测试动态端口。
func (s *MetricsServer) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Shutdown 在上下文预算内停止指标服务。
func (s *MetricsServer) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
