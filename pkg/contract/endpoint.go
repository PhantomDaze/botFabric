package contract

// EndpointKind 区分协议端与应用端接入类型。
type EndpointKind string

const (
	// EndpointProtocol 表示一个协议端连接：QQ 协议端接入中间层。
	EndpointProtocol EndpointKind = "protocol"
	// EndpointApplication 表示一个应用端连接：业务应用接入中间层。
	EndpointApplication EndpointKind = "application"
)

// EndpointInfo 描述一个接入中间层的端点。
// ID 类字段使用字符串，避免 QQ 等大整数经过浮点表示时丢失精度。
type EndpointInfo struct {
	ID           string       `json:"id" yaml:"id"`
	Owner        string       `json:"owner,omitempty" yaml:"owner,omitempty"`
	Kind         EndpointKind `json:"kind" yaml:"kind"`
	Protocol     string       `json:"protocol" yaml:"protocol"`
	Version      string       `json:"version" yaml:"version"`
	BotID        string       `json:"bot_id" yaml:"bot_id"`
	ConnectionID string       `json:"connection_id" yaml:"connection_id"`
	Capabilities []string     `json:"capabilities,omitempty" yaml:"capabilities,omitempty"`
}

// EndpointRegistrar 由运行时实现，供插件注册与注销端点。
// 插件通过对其收到的 Host 进行类型断言获取该能力；
// 不实现该接口的 Host 视为不支持端点注册，插件应优雅跳过。
type EndpointRegistrar interface {
	RegisterEndpoint(info EndpointInfo) error
	UnregisterEndpoint(id string) error
}
