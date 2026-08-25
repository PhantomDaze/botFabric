package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
)

type traceIDKey struct{}

// NewTraceID 生成一个 128 位随机 TraceID。
func NewTraceID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic("生成 TraceID 失败: " + err.Error())
	}
	return hex.EncodeToString(value[:])
}

// ContextWithTraceID 将 TraceID 写入上下文；空值会替换为新生成的 ID。
func ContextWithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		traceID = NewTraceID()
	}
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

// TraceIDFromContext 读取上下文中的 TraceID。
func TraceIDFromContext(ctx context.Context) string {
	traceID, _ := ctx.Value(traceIDKey{}).(string)
	return traceID
}

// EnsureTraceID 返回包含 TraceID 的上下文和对应 ID。
func EnsureTraceID(ctx context.Context) (context.Context, string) {
	if traceID := TraceIDFromContext(ctx); traceID != "" {
		return ctx, traceID
	}
	traceID := NewTraceID()
	return ContextWithTraceID(ctx, traceID), traceID
}

// LoggerWithTrace 为日志记录器附加当前链路标识。
func LoggerWithTrace(logger *slog.Logger, ctx context.Context) *slog.Logger {
	if traceID := TraceIDFromContext(ctx); traceID != "" {
		return logger.With(slog.String("trace_id", traceID))
	}
	return logger
}
