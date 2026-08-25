package contract

import (
	"testing"
	"time"
)

func TestSubscriptionOptions(t *testing.T) {
	config := SubscriptionConfig{}
	WithPriority(12)(&config)
	WithTimeout(750 * time.Millisecond)(&config)

	if config.Priority != 12 {
		t.Fatalf("优先级 = %d，期望 12", config.Priority)
	}
	if config.Timeout != 750*time.Millisecond {
		t.Fatalf("超时 = %s，期望 750ms", config.Timeout)
	}
}

func TestServiceErrorFormatting(t *testing.T) {
	if got := (*ServiceError)(nil).Error(); got != "" {
		t.Fatalf("nil 错误文本 = %q，期望空字符串", got)
	}
	err := &ServiceError{Code: ErrorPermissionDenied, Message: "无权调用"}
	if got := err.Error(); got != "permission_denied: 无权调用" {
		t.Fatalf("错误文本 = %q", got)
	}
}
