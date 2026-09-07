package process

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
	"google.golang.org/protobuf/types/known/structpb"
)

// wireFrame 是进程插件 Session 数据面的统一帧。
// 使用 protobuf Struct 承载 JSON 形状的字段，保持 gRPC/Unix Socket 的跨语言边界，
// 同时让 Python、TypeScript 等 SDK 无需依赖 Go 生成代码。
type wireFrame struct {
	Type            string            `json:"type"`
	RequestID       string            `json:"request_id,omitempty"`
	PluginID        string            `json:"plugin_id,omitempty"`
	PluginVersion   string            `json:"plugin_version,omitempty"`
	ProtocolVersion string            `json:"protocol_version,omitempty"`
	SessionToken    string            `json:"session_token,omitempty"`
	Accepted        bool              `json:"accepted,omitempty"`
	ExpiresUnix     int64             `json:"expires_unix_millis,omitempty"`
	Topics          []string          `json:"topics,omitempty"`
	Event           *wireEvent        `json:"event,omitempty"`
	Request         *wireRequest      `json:"request,omitempty"`
	Response        *wireResponse     `json:"response,omitempty"`
	Error           *wireError        `json:"error,omitempty"`
	State           string            `json:"state,omitempty"`
	Message         string            `json:"message,omitempty"`
	Details         map[string]string `json:"details,omitempty"`
}

type wireEvent struct {
	ID               string            `json:"id"`
	Topic            string            `json:"topic"`
	Schema           string            `json:"schema"`
	TimestampUnix    int64             `json:"timestamp_unix_millis"`
	Source           string            `json:"source"`
	BotID            string            `json:"bot_id"`
	EndpointID       string            `json:"endpoint_id,omitempty"`
	RequiredReplyBot string            `json:"required_reply_bot,omitempty"`
	ResponseMode     string            `json:"response_mode,omitempty"`
	ResponseTimeout  int64             `json:"response_timeout_millis,omitempty"`
	FallbackBots     []string          `json:"fallback_bots,omitempty"`
	CorrelationID    string            `json:"correlation_id,omitempty"`
	TraceID          string            `json:"trace_id,omitempty"`
	Headers          map[string]string `json:"headers,omitempty"`
	Payload          []byte            `json:"payload,omitempty"`
}

type wireRequest struct {
	Service       string            `json:"service"`
	Method        string            `json:"method"`
	Caller        string            `json:"caller,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Payload       []byte            `json:"payload,omitempty"`
}

type wireResponse struct {
	Payload []byte     `json:"payload,omitempty"`
	Error   *wireError `json:"error,omitempty"`
}

type wireError struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Retryable bool              `json:"retryable,omitempty"`
	Details   map[string]string `json:"details,omitempty"`
}

func encodeFrame(frame wireFrame) (*structpb.Struct, error) {
	data, err := json.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("编码进程插件帧: %w", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("转换进程插件帧: %w", err)
	}
	value, err := structpb.NewStruct(fields)
	if err != nil {
		return nil, fmt.Errorf("构造进程插件帧: %w", err)
	}
	return value, nil
}

func decodeFrame(value *structpb.Struct) (wireFrame, error) {
	if value == nil {
		return wireFrame{}, fmt.Errorf("进程插件帧为空")
	}
	data, err := json.Marshal(value.AsMap())
	if err != nil {
		return wireFrame{}, fmt.Errorf("读取进程插件帧: %w", err)
	}
	var frame wireFrame
	if err := json.Unmarshal(data, &frame); err != nil {
		return wireFrame{}, fmt.Errorf("解码进程插件帧: %w", err)
	}
	if frame.Type == "" {
		return wireFrame{}, fmt.Errorf("进程插件帧缺少 type")
	}
	return frame, nil
}

func eventToWire(event contract.Event) *wireEvent {
	result := &wireEvent{
		ID:               event.ID,
		Topic:            event.Topic,
		Schema:           event.Schema,
		TimestampUnix:    event.Timestamp.UnixMilli(),
		Source:           event.Source,
		BotID:            event.BotID,
		EndpointID:       event.EndpointID,
		ResponseMode:     string(event.ResponsePolicy.Mode),
		ResponseTimeout:  event.ResponsePolicy.Timeout.Milliseconds(),
		RequiredReplyBot: firstNonEmpty(event.ResponsePolicy.RequiredReplyBot, event.RequiredReplyBot),
		FallbackBots:     append([]string(nil), event.ResponsePolicy.FallbackBots...),
		CorrelationID:    event.CorrelationID,
		TraceID:          event.TraceID,
		Payload:          append([]byte(nil), event.Payload...),
	}
	if event.Headers != nil {
		result.Headers = make(map[string]string, len(event.Headers))
		for key, value := range event.Headers {
			result.Headers[key] = value
		}
	}
	return result
}

func wireToEvent(value *wireEvent) contract.Event {
	if value == nil {
		return contract.Event{}
	}
	result := contract.Event{
		ID:               value.ID,
		Topic:            value.Topic,
		Schema:           value.Schema,
		Timestamp:        time.UnixMilli(value.TimestampUnix).UTC(),
		Source:           value.Source,
		BotID:            value.BotID,
		EndpointID:       value.EndpointID,
		RequiredReplyBot: value.RequiredReplyBot,
		ResponsePolicy: contract.ResponsePolicy{
			Mode:             contract.ResponseMode(value.ResponseMode),
			Timeout:          time.Duration(value.ResponseTimeout) * time.Millisecond,
			RequiredReplyBot: value.RequiredReplyBot,
			FallbackBots:     append([]string(nil), value.FallbackBots...),
		},
		CorrelationID: value.CorrelationID,
		TraceID:       value.TraceID,
		Payload:       append([]byte(nil), value.Payload...),
	}
	if value.Headers != nil {
		result.Headers = make(map[string]string, len(value.Headers))
		for key, item := range value.Headers {
			result.Headers[key] = item
		}
	}
	return result
}

func requestToWire(request contract.Request) *wireRequest {
	result := &wireRequest{
		Service:       request.Service,
		Method:        request.Method,
		Caller:        request.Caller,
		TraceID:       request.TraceID,
		CorrelationID: request.CorrelationID,
		Payload:       append([]byte(nil), request.Payload...),
	}
	if request.Metadata != nil {
		result.Metadata = make(map[string]string, len(request.Metadata))
		for key, value := range request.Metadata {
			result.Metadata[key] = value
		}
	}
	return result
}

func wireToRequest(value *wireRequest) contract.Request {
	if value == nil {
		return contract.Request{}
	}
	result := contract.Request{
		Service:       value.Service,
		Method:        value.Method,
		Caller:        value.Caller,
		TraceID:       value.TraceID,
		CorrelationID: value.CorrelationID,
		Payload:       append([]byte(nil), value.Payload...),
	}
	if value.Metadata != nil {
		result.Metadata = make(map[string]string, len(value.Metadata))
		for key, item := range value.Metadata {
			result.Metadata[key] = item
		}
	}
	return result
}

func responseToWire(response contract.Response, err error) *wireResponse {
	result := &wireResponse{Payload: append([]byte(nil), response.Payload...)}
	if response.Error != nil {
		result.Error = errorToWire(response.Error)
	} else if err != nil {
		var serviceError *contract.ServiceError
		if ok := asServiceError(err, &serviceError); ok {
			result.Error = errorToWire(serviceError)
		} else {
			result.Error = &wireError{Code: string(contract.ErrorInternal), Message: err.Error()}
		}
	}
	return result
}

func wireToResponse(value *wireResponse) (contract.Response, error) {
	if value == nil {
		return contract.Response{}, fmt.Errorf("进程插件响应缺少 response")
	}
	result := contract.Response{Payload: append([]byte(nil), value.Payload...)}
	if value.Error == nil {
		return result, nil
	}
	serviceError := wireToError(value.Error)
	result.Error = serviceError
	return result, serviceError
}

func errorToWire(value *contract.ServiceError) *wireError {
	if value == nil {
		return nil
	}
	result := &wireError{Code: string(value.Code), Message: value.Message, Retryable: value.Retryable}
	if value.Details != nil {
		result.Details = make(map[string]string, len(value.Details))
		for key, item := range value.Details {
			result.Details[key] = item
		}
	}
	return result
}

func wireToError(value *wireError) *contract.ServiceError {
	if value == nil {
		return nil
	}
	result := &contract.ServiceError{Code: contract.ErrorCode(value.Code), Message: value.Message, Retryable: value.Retryable}
	if value.Details != nil {
		result.Details = make(map[string]string, len(value.Details))
		for key, item := range value.Details {
			result.Details[key] = item
		}
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// asServiceError 将包装后的服务错误统一还原为协议错误。
func asServiceError(err error, target **contract.ServiceError) bool {
	var serviceError *contract.ServiceError
	if !errors.As(err, &serviceError) {
		return false
	}
	*target = serviceError
	return true
}
