package contract

import "encoding/json"

// GroupMessage 是协议无关的群消息模型。
// ID 类字段使用字符串，避免 QQ 等大整数经过浮点表示时丢失精度。
type GroupMessage struct {
	MessageID string `json:"message_id" yaml:"message_id"`
	GroupID   string `json:"group_id" yaml:"group_id"`
	UserID    string `json:"user_id" yaml:"user_id"`
	Text      string `json:"text" yaml:"text"`
	Protocol  string `json:"protocol" yaml:"protocol"`
	Version   string `json:"version" yaml:"version"`
}

// SendMessageRequest 是协议无关的发送群消息动作请求。
type SendMessageRequest struct {
	GroupID        string `json:"group_id" yaml:"group_id"`
	Text           string `json:"text" yaml:"text"`
	BotID          string `json:"bot_id,omitempty" yaml:"bot_id,omitempty"`
	EndpointID     string `json:"endpoint_id,omitempty" yaml:"endpoint_id,omitempty"`
	EventID        string `json:"event_id,omitempty" yaml:"event_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty" yaml:"idempotency_key,omitempty"`
}

// SendMessageResult 是协议无关的发送消息动作结果。
type SendMessageResult struct {
	MessageID string `json:"message_id" yaml:"message_id"`
}

// EncodeJSON 将公共载荷编码为 JSON。
func EncodeJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

// DecodeJSON 将 JSON 载荷解码到目标公共模型。
func DecodeJSON(payload []byte, target any) error {
	return json.Unmarshal(payload, target)
}
