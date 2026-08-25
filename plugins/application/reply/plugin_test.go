package reply

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type testHost struct {
	bus     *capturingBus
	gateway *capturingGateway
}

func (h testHost) EventBus() contract.EventBus      { return h.bus }
func (h testHost) Gateway() contract.ServiceGateway { return h.gateway }
func (h testHost) Logger() contract.Logger          { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func (h testHost) BotID() string                    { return "test-bot" }

type capturingBus struct {
	topic   string
	handler contract.EventHandler
}

func (*capturingBus) Publish(context.Context, contract.Event) error { return nil }

func (b *capturingBus) Subscribe(topic string, handler contract.EventHandler, _ ...contract.SubOption) (contract.Subscription, error) {
	b.topic = topic
	b.handler = handler
	return noopSubscription{}, nil
}

type noopSubscription struct{}

func (noopSubscription) Unsubscribe() error { return nil }

type capturingGateway struct {
	request contract.Request
}

func (g *capturingGateway) Invoke(_ context.Context, request contract.Request) (contract.Response, error) {
	g.request = request
	return contract.Response{Payload: []byte(`{"message_id":"mock-1"}`)}, nil
}

func (*capturingGateway) Register(contract.ServiceDescriptor, contract.ServiceHandler) error {
	return nil
}
func (*capturingGateway) Unregister(string) error { return nil }

func TestPluginSubscribesAndInvokesAction(t *testing.T) {
	bus := &capturingBus{}
	gateway := &capturingGateway{}
	plugin := New()
	if err := plugin.Start(context.Background(), testHost{bus: bus, gateway: gateway}); err != nil {
		t.Fatal(err)
	}
	if bus.topic != groupMessageTopic || bus.handler == nil {
		t.Fatal("Start 应订阅标准群消息 Topic")
	}
	payload, err := json.Marshal(contract.GroupMessage{GroupID: "123", UserID: "456", Text: "你好"})
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.handler(context.Background(), contract.Event{
		ID:            "event-1",
		TraceID:       "trace-1",
		CorrelationID: "correlation-1",
		Payload:       payload,
	}); err != nil {
		t.Fatal(err)
	}
	if gateway.request.Service != responseService || gateway.request.Method != submitMethod {
		t.Fatalf("响应请求目标错误: %#v", gateway.request)
	}
	if gateway.request.TraceID != "trace-1" || gateway.request.CorrelationID != "correlation-1" {
		t.Fatalf("链路字段未传播: %#v", gateway.request)
	}
	var reply contract.CandidateReply
	if err := json.Unmarshal(gateway.request.Payload, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.EventID == "" || reply.ApplicationID != PluginID || reply.GroupID != "123" || reply.Text != "收到：你好" {
		t.Fatalf("候选回复载荷错误: %#v", reply)
	}
}

func TestHandlerRejectsInvalidPayload(t *testing.T) {
	plugin := New()
	if err := plugin.handleGroupMessage(context.Background(), contract.Event{Payload: []byte("not-json")}); err == nil {
		t.Fatal("无效 JSON 应被拒绝")
	}
}
