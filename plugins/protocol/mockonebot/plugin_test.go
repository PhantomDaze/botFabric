package mockonebot

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type testHost struct {
	bus     *recordingBus
	gateway *recordingGateway
}

func (h testHost) EventBus() contract.EventBus      { return h.bus }
func (h testHost) Gateway() contract.ServiceGateway { return h.gateway }
func (h testHost) Logger() contract.Logger          { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func (h testHost) BotID() string                    { return "test-bot" }

type recordingBus struct {
	event contract.Event
}

func (b *recordingBus) Publish(_ context.Context, event contract.Event) error {
	b.event = event
	return nil
}

func (*recordingBus) Subscribe(string, contract.EventHandler, ...contract.SubOption) (contract.Subscription, error) {
	return nil, nil
}

type recordingGateway struct {
	descriptor contract.ServiceDescriptor
	handler    contract.ServiceHandler
}

func (*recordingGateway) Invoke(context.Context, contract.Request) (contract.Response, error) {
	return contract.Response{}, nil
}

func (g *recordingGateway) Register(descriptor contract.ServiceDescriptor, handler contract.ServiceHandler) error {
	g.descriptor = descriptor
	g.handler = handler
	return nil
}

func (*recordingGateway) Unregister(string) error { return nil }

func TestEmitGroupMessageNormalizesEvent(t *testing.T) {
	bus := &recordingBus{}
	gateway := &recordingGateway{}
	plugin := New()
	if err := plugin.Start(context.Background(), testHost{bus: bus, gateway: gateway}); err != nil {
		t.Fatal(err)
	}
	if gateway.descriptor.Name != ServiceName || gateway.handler == nil {
		t.Fatal("Start 应注册动作服务")
	}

	if err := plugin.EmitGroupMessage(context.Background(), RawGroupMessage{
		MessageID: "message-1",
		GroupID:   "9223372036854775807",
		UserID:    "10001",
		Text:      "你好",
	}); err != nil {
		t.Fatal(err)
	}
	if bus.event.Topic != GroupMessageTopic || bus.event.Schema != GroupMessageSchema || bus.event.BotID != "test-bot" {
		t.Fatalf("归一化事件字段错误: %#v", bus.event)
	}
	var message GroupMessage
	if err := json.Unmarshal(bus.event.Payload, &message); err != nil {
		t.Fatal(err)
	}
	if message.GroupID != "9223372036854775807" || message.Protocol != "onebot" {
		t.Fatalf("归一化载荷错误: %#v", message)
	}
}

func TestActionHandlerRecordsTrustedCallData(t *testing.T) {
	plugin := New()
	payload, err := json.Marshal(SendMessageRequest{GroupID: "123", Text: "回复"})
	if err != nil {
		t.Fatal(err)
	}
	response, err := plugin.handleAction(context.Background(), contract.Request{
		Method:  SendMessageMethod,
		Caller:  "trusted.reply",
		TraceID: "trace-1",
		Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	var result SendMessageResult
	if err := json.Unmarshal(response.Payload, &result); err != nil {
		t.Fatal(err)
	}
	if result.MessageID == "" {
		t.Fatal("动作响应应包含消息 ID")
	}
	messages := plugin.SentMessages()
	if len(messages) != 1 || messages[0].Caller != "trusted.reply" || messages[0].TraceID != "trace-1" {
		t.Fatalf("动作记录错误: %#v", messages)
	}
}

func TestEmitRejectsInvalidStateAndPayload(t *testing.T) {
	plugin := New()
	if err := plugin.EmitGroupMessage(context.Background(), RawGroupMessage{Text: "你好"}); err == nil {
		t.Fatal("未启动插件不应发布事件")
	}
	if err := plugin.Start(context.Background(), testHost{bus: &recordingBus{}, gateway: &recordingGateway{}}); err != nil {
		t.Fatal(err)
	}
	if err := plugin.EmitGroupMessage(context.Background(), RawGroupMessage{GroupID: "1", UserID: "2"}); err == nil {
		t.Fatal("空文本应被拒绝")
	}
}
