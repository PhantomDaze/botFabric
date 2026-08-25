package onebot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// actionResponse 是两个 OneBot 版本共用的响应中间格式。
type actionResponse struct {
	Data    json.RawMessage
	Retcode int
	Status  string
	Message string
}

func responseFromPayload(payload []byte) (echo string, response actionResponse, ok bool) {
	var envelope struct {
		Echo    json.RawMessage `json:"echo"`
		Data    json.RawMessage `json:"data"`
		Retcode int             `json:"retcode"`
		Status  string          `json:"status"`
		Message string          `json:"message"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil || len(envelope.Echo) == 0 {
		return "", actionResponse{}, false
	}
	echo = stringValue(envelope.Echo)
	if echo == "" {
		return "", actionResponse{}, false
	}
	return echo, actionResponse{
		Data:    envelope.Data,
		Retcode: envelope.Retcode,
		Status:  envelope.Status,
		Message: envelope.Message,
	}, true
}

func stringValue(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	var text string
	if json.Unmarshal(trimmed, &text) == nil {
		return text
	}
	var number json.Number
	if json.Unmarshal(trimmed, &number) == nil {
		return number.String()
	}
	return strings.Trim(string(trimmed), `"`)
}

func messageText(raw json.RawMessage, fallback string) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return fallback
	}
	var text string
	if json.Unmarshal(trimmed, &text) == nil {
		return text
	}
	var segments []struct {
		Type string `json:"type"`
		Data struct {
			Text string `json:"text"`
		} `json:"data"`
	}
	if json.Unmarshal(trimmed, &segments) != nil {
		return fallback
	}
	var builder strings.Builder
	for _, segment := range segments {
		if segment.Type == "text" {
			builder.WriteString(segment.Data.Text)
		}
	}
	if builder.Len() == 0 {
		return fallback
	}
	return builder.String()
}

func actionResult(data json.RawMessage) ([]byte, error) {
	if len(data) == 0 || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return json.Marshal(contract.SendMessageResult{})
	}
	var raw struct {
		MessageID json.RawMessage `json:"message_id"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("解析 OneBot 动作响应: %w", err)
	}
	return json.Marshal(contract.SendMessageResult{MessageID: stringValue(raw.MessageID)})
}

func nextEcho(version string) string {
	return version + "-" + strconv.FormatInt(timeNowUnixNano(), 10)
}

var timeNowUnixNano = func() int64 {
	return time.Now().UnixNano()
}
