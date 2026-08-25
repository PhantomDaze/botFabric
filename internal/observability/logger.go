package observability

import (
	"errors"
	"io"
	"log/slog"
	"strings"
)

// NewLogger 创建运行时统一使用的结构化日志记录器。
func NewLogger(output io.Writer, format string, level slog.Level) (*slog.Logger, error) {
	options := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	switch strings.ToLower(format) {
	case "json":
		handler = slog.NewJSONHandler(output, options)
	case "text":
		handler = slog.NewTextHandler(output, options)
	default:
		return nil, errors.New("log_format 仅支持 json 或 text")
	}
	return slog.New(handler), nil
}
