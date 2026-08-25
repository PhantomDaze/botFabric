package contract

import "context"

// ServiceGateway 提供有明确返回值的一对一服务调用。
type ServiceGateway interface {
	Invoke(ctx context.Context, req Request) (Response, error)
	Register(service ServiceDescriptor, handler ServiceHandler) error
	Unregister(serviceID string) error
}

// Request 是一次跨插件服务调用请求。
type Request struct {
	Service       string            `json:"service" yaml:"service"`
	Method        string            `json:"method" yaml:"method"`
	Caller        string            `json:"caller" yaml:"caller"`
	TraceID       string            `json:"trace_id" yaml:"trace_id"`
	CorrelationID string            `json:"correlation_id" yaml:"correlation_id"`
	Metadata      map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Payload       []byte            `json:"payload,omitempty" yaml:"payload,omitempty"`
}

// Response 是一次服务调用响应。
type Response struct {
	Payload []byte        `json:"payload,omitempty" yaml:"payload,omitempty"`
	Error   *ServiceError `json:"error,omitempty" yaml:"error,omitempty"`
}

// ServiceDescriptor 描述一个插件提供的服务及其方法。
type ServiceDescriptor struct {
	Name    string   `json:"name" yaml:"name"`
	Methods []string `json:"methods" yaml:"methods"`
}

// ServiceHandler 处理一次服务调用。
type ServiceHandler func(ctx context.Context, req Request) (Response, error)
