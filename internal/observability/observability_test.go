package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func TestEnsureTraceIDCreatesAndPreservesID(t *testing.T) {
	ctx, traceID := EnsureTraceID(context.Background())
	if len(traceID) != 32 {
		t.Fatalf("TraceID 长度 = %d，期望 32", len(traceID))
	}
	if got := TraceIDFromContext(ctx); got != traceID {
		t.Fatalf("上下文 TraceID = %q，期望 %q", got, traceID)
	}

	nextCtx, nextID := EnsureTraceID(ctx)
	if nextID != traceID || TraceIDFromContext(nextCtx) != traceID {
		t.Fatal("已有 TraceID 应被保留")
	}
	if another := NewTraceID(); another == traceID {
		t.Fatal("独立生成的 TraceID 不应重复")
	}
}

func TestNewLoggerValidatesFormat(t *testing.T) {
	var output bytes.Buffer
	logger, err := NewLogger(&output, "json", slog.LevelInfo)
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("测试日志", "answer", 42)
	if !bytes.Contains(output.Bytes(), []byte(`"answer":42`)) {
		t.Fatalf("结构化日志未包含字段: %s", output.String())
	}
	if _, err := NewLogger(&output, "xml", slog.LevelInfo); err == nil {
		t.Fatal("未知日志格式应被拒绝")
	}
}

type healthProvider map[string]contract.HealthStatus

func (p healthProvider) Health(context.Context) map[string]contract.HealthStatus {
	return p
}

func TestHealthHandlerReportsUnhealthyPlugin(t *testing.T) {
	handler := NewHealthHandler(healthProvider{
		"healthy": {State: contract.HealthHealthy},
		"broken":  {State: contract.HealthUnhealthy, Message: "连接断开"},
	})
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("状态码 = %d，期望 %d", response.Code, http.StatusServiceUnavailable)
	}
	var payload struct {
		Healthy bool                             `json:"healthy"`
		Plugins map[string]contract.HealthStatus `json:"plugins"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Healthy || payload.Plugins["broken"].Message != "连接断开" {
		t.Fatalf("健康响应内容错误: %#v", payload)
	}
}
