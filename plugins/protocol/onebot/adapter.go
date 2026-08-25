package onebot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func (p *Plugin) handleV11(conn *connection, payload []byte) error {
	var event struct {
		SelfID      json.RawMessage `json:"self_id"`
		PostType    string          `json:"post_type"`
		MessageType string          `json:"message_type"`
		MessageID   json.RawMessage `json:"message_id"`
		GroupID     json.RawMessage `json:"group_id"`
		UserID      json.RawMessage `json:"user_id"`
		RawMessage  string          `json:"raw_message"`
		Message     json.RawMessage `json:"message"`
		Time        int64           `json:"time"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("解析 OneBot v11 事件: %w", err)
	}
	if event.PostType != "message" || event.MessageType != "group" {
		return nil
	}
	message := contract.GroupMessage{
		MessageID: stringValue(event.MessageID),
		GroupID:   stringValue(event.GroupID),
		UserID:    stringValue(event.UserID),
		Text:      messageText(event.Message, event.RawMessage),
		Protocol:  "onebot",
		Version:   VersionV11,
	}
	if botID := stringValue(event.SelfID); botID != "" {
		conn.setBotID(botID)
	}
	if message.GroupID == "" || message.UserID == "" || message.Text == "" {
		return errors.New("OneBot v11 群消息缺少 group_id、user_id 或 message")
	}
	return p.publishMessage(conn, message, event.Time)
}

func (p *Plugin) handleV12(conn *connection, payload []byte) error {
	var event struct {
		SelfID     json.RawMessage `json:"self_id"`
		Time       int64           `json:"time"`
		Type       string          `json:"type"`
		DetailType string          `json:"detail_type"`
		ID         json.RawMessage `json:"id"`
		GroupID    json.RawMessage `json:"group_id"`
		UserID     json.RawMessage `json:"user_id"`
		Message    json.RawMessage `json:"message"`
		AltMessage string          `json:"alt_message"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("解析 OneBot v12 事件: %w", err)
	}
	if event.Type != "message" || event.DetailType != "group" {
		return nil
	}
	message := contract.GroupMessage{
		MessageID: stringValue(event.ID),
		GroupID:   stringValue(event.GroupID),
		UserID:    stringValue(event.UserID),
		Text:      messageText(event.Message, event.AltMessage),
		Protocol:  "onebot",
		Version:   VersionV12,
	}
	if botID := stringValue(event.SelfID); botID != "" {
		conn.setBotID(botID)
	}
	if message.GroupID == "" || message.UserID == "" || message.Text == "" {
		return errors.New("OneBot v12 群消息缺少 group_id、user_id 或 message")
	}
	return p.publishMessage(conn, message, event.Time)
}

func (p *Plugin) publishMessage(conn *connection, message contract.GroupMessage, unixSeconds int64) error {
	p.mu.RLock()
	host := p.host
	p.mu.RUnlock()
	if host == nil {
		return errors.New("OneBot 宿主不可用")
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	timestamp := time.Now().UTC()
	if unixSeconds > 0 {
		timestamp = time.Unix(unixSeconds, 0).UTC()
	}
	return host.EventBus().Publish(context.Background(), contract.Event{
		Topic:      GroupTopic,
		Schema:     GroupSchema,
		Timestamp:  timestamp,
		BotID:      messageBotID(conn, host),
		EndpointID: conn.id,
		Payload:    payload,
		Headers:    map[string]string{"onebot_connection": conn.id},
	})
}

func messageBotID(conn *connection, host contract.Host) string {
	conn.mu.Lock()
	botID := conn.botID
	conn.mu.Unlock()
	if botID != "" {
		return botID
	}
	return host.BotID()
}

func selectConnection(connections []*connection, action contract.SendMessageRequest) *connection {
	var fallback *connection
	for _, conn := range connections {
		conn.mu.Lock()
		botID := conn.botID
		conn.mu.Unlock()
		if action.EndpointID != "" && conn.id == action.EndpointID {
			return conn
		}
		if action.BotID != "" && botID == action.BotID {
			if fallback == nil || conn.createdAt.After(fallback.createdAt) {
				fallback = conn
			}
			continue
		}
		if fallback == nil && action.BotID == "" {
			fallback = conn
		} else if action.BotID == "" && conn.createdAt.After(fallback.createdAt) {
			fallback = conn
		}
	}
	return fallback
}

func (p *Plugin) handleAction(ctx context.Context, request contract.Request) (contract.Response, error) {
	var action contract.SendMessageRequest
	if err := json.Unmarshal(request.Payload, &action); err != nil || action.GroupID == "" || action.Text == "" {
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "SendMessage 载荷无效"}
	}
	p.connMu.RLock()
	connections := make([]*connection, 0, len(p.conns))
	for _, conn := range p.conns {
		connections = append(connections, conn)
	}
	p.connMu.RUnlock()
	selected := selectConnection(connections, action)
	if selected == nil {
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "没有可用的 OneBot WebSocket 连接", Retryable: true}
	}
	echo := nextEcho(selected.version)
	responseCh := make(chan actionResponse, 1)
	selected.registerEcho(echo, responseCh)
	defer selected.unregisterEcho(echo)
	var envelope any
	switch selected.version {
	case VersionV11:
		envelope = map[string]any{"action": "send_group_msg", "params": map[string]any{"group_id": action.GroupID, "message": action.Text}, "echo": echo}
	case VersionV12:
		envelope = map[string]any{"action": "send_message", "params": map[string]any{"detail_type": "group", "group_id": action.GroupID, "message": []map[string]any{{"type": "text", "data": map[string]string{"text": action.Text}}}}, "echo": echo}
	default:
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorUnavailable, Message: "未知 OneBot 版本"}
	}
	if err := selected.writeJSON(envelope); err != nil {
		return contract.Response{}, err
	}
	select {
	case response := <-responseCh:
		if response.Retcode != 0 || response.Status == "failed" {
			return contract.Response{}, &contract.ServiceError{Code: contract.ErrorUnavailable, Message: response.Message, Retryable: true}
		}
		result, err := actionResult(response.Data)
		if err != nil {
			return contract.Response{}, err
		}
		return contract.Response{Payload: result}, nil
	case <-ctx.Done():
		return contract.Response{}, &contract.ServiceError{Code: contract.ErrorTimeout, Message: ctx.Err().Error(), Retryable: true}
	}
}
