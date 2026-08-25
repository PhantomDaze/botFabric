package onebot

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/gorilla/websocket"
)

type actionHost struct{ gateway *actionGateway }

func (h actionHost) EventBus() contract.EventBus      { return nil }
func (h actionHost) Gateway() contract.ServiceGateway { return h.gateway }
func (h actionHost) Logger() contract.Logger          { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func (h actionHost) BotID() string                    { return "test-bot" }

type actionGateway struct{ handler contract.ServiceHandler }

func (g *actionGateway) Invoke(ctx context.Context, req contract.Request) (contract.Response, error) {
	return g.handler(ctx, req)
}
func (g *actionGateway) Register(_ contract.ServiceDescriptor, handler contract.ServiceHandler) error {
	g.handler = handler
	return nil
}
func (*actionGateway) Unregister(string) error { return nil }

func TestV11ActionRoundTrip(t *testing.T) {
	testActionRoundTrip(t, VersionV11)
}

func TestV12ActionRoundTrip(t *testing.T) {
	testActionRoundTrip(t, VersionV12)
}

func testActionRoundTrip(t *testing.T, version string) {
	config := testConfig()
	plugin := New(config)
	gateway := &actionGateway{}
	if err := plugin.Start(context.Background(), actionHost{gateway: gateway}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(plugin.upgradeHandler(version))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	u.Scheme = "ws"
	header := http.Header{}
	header.Set("Authorization", "Bearer test-token")
	socket, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()

	responseCh := make(chan contract.Response, 1)
	go func() {
		response, err := gateway.handler(context.Background(), contract.Request{Method: SendMessage, Payload: mustJSON(contract.SendMessageRequest{GroupID: "9223372036854775807", Text: "你好"})})
		if err != nil {
			responseCh <- contract.Response{Error: &contract.ServiceError{Code: contract.ErrorInternal, Message: err.Error()}}
			return
		}
		responseCh <- response
	}()

	_, actionPayload, err := socket.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var action map[string]any
	if err := json.Unmarshal(actionPayload, &action); err != nil {
		t.Fatal(err)
	}
	echo := action["echo"].(string)
	if action["action"] != map[string]any{VersionV11: "send_group_msg", VersionV12: "send_message"}[version] {
		t.Fatalf("动作名称错误: %#v", action)
	}
	var responsePayload map[string]any
	if version == VersionV11 {
		responsePayload = map[string]any{"status": "ok", "retcode": 0, "data": map[string]any{"message_id": 12345}, "echo": echo}
	} else {
		responsePayload = map[string]any{"status": "ok", "retcode": 0, "data": map[string]any{"message_id": "message-12"}, "echo": echo}
	}
	if err := socket.WriteJSON(responsePayload); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-responseCh:
		if response.Error != nil {
			t.Fatal(response.Error)
		}
		var result contract.SendMessageResult
		if err := json.Unmarshal(response.Payload, &result); err != nil {
			t.Fatal(err)
		}
		if result.MessageID == "" {
			t.Fatal("动作结果缺少 message_id")
		}
	case <-time.After(time.Second):
		t.Fatal("动作响应超时")
	}
}

func mustJSON(value any) []byte {
	payload, _ := json.Marshal(value)
	return payload
}
