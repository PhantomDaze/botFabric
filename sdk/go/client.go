package sdk

import (
	"context"
	"encoding/json"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// Client 是给 Go 插件作者使用的最小 SDK 外观。
// 它只依赖公共契约，不依赖 internal 包。
type Client struct {
	eventBus contract.EventBus
	gateway  contract.ServiceGateway
}

// New 从运行时注入的 Host 创建 SDK 客户端。
func New(host contract.Host) *Client {
	if host == nil {
		return &Client{}
	}
	return &Client{eventBus: host.EventBus(), gateway: host.Gateway()}
}

// Publish 发布一个标准事件。
func (c *Client) Publish(ctx context.Context, event contract.Event) error {
	if c.eventBus == nil {
		return &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "SDK EventBus 不可用"}
	}
	return c.eventBus.Publish(ctx, event)
}

// Subscribe 订阅精确 Topic。
func (c *Client) Subscribe(topic string, handler contract.EventHandler, opts ...contract.SubOption) (contract.Subscription, error) {
	if c.eventBus == nil {
		return nil, &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "SDK EventBus 不可用"}
	}
	return c.eventBus.Subscribe(topic, handler, opts...)
}

// Invoke 调用一个标准服务。
func (c *Client) Invoke(ctx context.Context, request contract.Request) (contract.Response, error) {
	if c.gateway == nil {
		return contract.Response{Error: &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "SDK Gateway 不可用"}}, &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "SDK Gateway 不可用"}
	}
	return c.gateway.Invoke(ctx, request)
}

// EncodeJSON 编码 JSON 业务载荷。
func EncodeJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

// DecodeJSON 解码 JSON 业务载荷。
func DecodeJSON(payload []byte, target any) error {
	return json.Unmarshal(payload, target)
}

// WithTimeout 返回一个有明确超时预算的上下文。
func WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}
