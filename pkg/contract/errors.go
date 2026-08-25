package contract

import "fmt"

// ErrorCode 是跨插件调用的稳定错误码。
type ErrorCode string

const (
	ErrorInvalidArgument  ErrorCode = "invalid_argument"
	ErrorPermissionDenied ErrorCode = "permission_denied"
	ErrorServiceNotFound  ErrorCode = "service_not_found"
	ErrorMethodNotFound   ErrorCode = "method_not_found"
	ErrorAlreadyExists    ErrorCode = "already_exists"
	ErrorTimeout          ErrorCode = "timeout"
	ErrorUnavailable      ErrorCode = "unavailable"
	ErrorInternal         ErrorCode = "internal"
)

// ServiceError 是可跨进程序列化的服务错误。
type ServiceError struct {
	Code      ErrorCode         `json:"code" yaml:"code"`
	Message   string            `json:"message" yaml:"message"`
	Retryable bool              `json:"retryable" yaml:"retryable"`
	Details   map[string]string `json:"details,omitempty" yaml:"details,omitempty"`
}

// Error 实现 error 接口。
func (e *ServiceError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}
