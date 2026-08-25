package contract

import (
	"context"
	"time"
)

// Event 是协议无关的标准内部事件。
type Event struct {
	ID               string            `json:"id" yaml:"id"`
	Topic            string            `json:"topic" yaml:"topic"`
	Schema           string            `json:"schema" yaml:"schema"`
	Timestamp        time.Time         `json:"timestamp" yaml:"timestamp"`
	Source           string            `json:"source" yaml:"source"`
	BotID            string            `json:"bot_id" yaml:"bot_id"`
	EndpointID       string            `json:"endpoint_id,omitempty" yaml:"endpoint_id,omitempty"`
	RequiredReplyBot string            `json:"required_reply_bot,omitempty" yaml:"required_reply_bot,omitempty"`
	ResponsePolicy   ResponsePolicy    `json:"response_policy,omitempty" yaml:"response_policy,omitempty"`
	CorrelationID    string            `json:"correlation_id" yaml:"correlation_id"`
	TraceID          string            `json:"trace_id" yaml:"trace_id"`
	Headers          map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
	Payload          []byte            `json:"payload,omitempty" yaml:"payload,omitempty"`
}

// EventHandler 处理一个标准事件。
type EventHandler func(ctx context.Context, event Event) error

// Subscription 表示一个可取消的事件订阅。
type Subscription interface {
	Unsubscribe() error
}

// SubscriptionConfig 是单个事件处理器的投递配置。
type SubscriptionConfig struct {
	Priority int
	Timeout  time.Duration
}

// SubOption 修改单个事件订阅的投递配置。
type SubOption func(*SubscriptionConfig)

// WithPriority 设置插件内订阅优先级，数值越大越先执行。
func WithPriority(priority int) SubOption {
	return func(config *SubscriptionConfig) {
		config.Priority = priority
	}
}

// WithTimeout 设置单个处理器的超时时间。
func WithTimeout(timeout time.Duration) SubOption {
	return func(config *SubscriptionConfig) {
		config.Timeout = timeout
	}
}

// EventBus 提供相互独立的发布与订阅能力。
type EventBus interface {
	Publish(ctx context.Context, event Event) error
	Subscribe(topic string, handler EventHandler, opts ...SubOption) (Subscription, error)
}
