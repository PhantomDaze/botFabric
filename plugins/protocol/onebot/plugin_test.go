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

	"github.com/PhantomDaze/botfabric/internal/config"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/gorilla/websocket"
)

type testHost struct {
	bus     *captureBus
	gateway *captureGateway
}

func (h testHost) EventBus() contract.EventBus      { return h.bus }
func (h testHost) Gateway() contract.ServiceGateway { return h.gateway }
func (h testHost) Logger() contract.Logger          { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func (h testHost) BotID() string                    { return "test-bot" }

type captureBus struct{ event contract.Event }

func (b *captureBus) Publish(_ context.Context, event contract.Event) error {
	b.event = event
	return nil
}
func (*captureBus) Subscribe(string, contract.EventHandler, ...contract.SubOption) (contract.Subscription, error) {
	return nil, nil
}

type captureGateway struct{ request contract.Request }

func (g *captureGateway) Invoke(_ context.Context, request contract.Request) (contract.Response, error) {
	g.request = request
	return contract.Response{}, nil
}
func (*captureGateway) Register(contract.ServiceDescriptor, contract.ServiceHandler) error {
	return nil
}
func (*captureGateway) Unregister(string) error { return nil }

func testConfig() config.OneBotConfig {
	return config.OneBotConfig{
		Enabled:           true,
		ListenAddress:     "127.0.0.1:0",
		PathV11:           "/v11",
		PathV12:           "/v12",
		AccessToken:       "test-token",
		ConnectTimeout:    config.Duration{Duration: time.Second},
		HeartbeatInterval: config.Duration{Duration: time.Second},
		MaxMessageSize:    config.ByteSize(1024 * 1024),
	}
}

func TestValidBearer(t *testing.T) {
	if !validBearer("Bearer test-token", "test-token") {
		t.Fatal("正确 token 应通过")
	}
	if validBearer("Bearer wrong", "test-token") || validBearer("test-token", "test-token") {
		t.Fatal("错误 Bearer 应拒绝")
	}
}

func TestV11NormalizesGroupEvent(t *testing.T) {
	bus := &captureBus{}
	plugin := New(testConfig())
	plugin.host = testHost{bus: bus}
	conn := &connection{id: "v11-test", version: VersionV11, plugin: plugin}
	payload := []byte(`{"post_type":"message","message_type":"group","message_id":123,"group_id":9223372036854775807,"user_id":10001,"raw_message":"你好","message":"你好","time":1700000000}`)
	if err := plugin.handleV11(conn, payload); err != nil {
		t.Fatal(err)
	}
	var message contract.GroupMessage
	if err := json.Unmarshal(bus.event.Payload, &message); err != nil {
		t.Fatal(err)
	}
	if message.GroupID != "9223372036854775807" || message.Version != VersionV11 || bus.event.Topic != GroupTopic {
		t.Fatalf("v11 归一化错误: %#v / %#v", message, bus.event)
	}
}

func TestV12NormalizesGroupEvent(t *testing.T) {
	bus := &captureBus{}
	plugin := New(testConfig())
	plugin.host = testHost{bus: bus}
	conn := &connection{id: "v12-test", version: VersionV12, plugin: plugin}
	payload := []byte(`{"type":"message","detail_type":"group","id":"event-1","group_id":"9223372036854775807","user_id":"10001","message":[{"type":"text","data":{"text":"你好"}}],"time":1700000000}`)
	if err := plugin.handleV12(conn, payload); err != nil {
		t.Fatal(err)
	}
	var message contract.GroupMessage
	if err := json.Unmarshal(bus.event.Payload, &message); err != nil {
		t.Fatal(err)
	}
	if message.GroupID != "9223372036854775807" || message.Text != "你好" || message.Version != VersionV12 {
		t.Fatalf("v12 归一化错误: %#v", message)
	}
}

func TestUpgradeRejectsWrongToken(t *testing.T) {
	plugin := New(testConfig())
	server := httptest.NewServer(plugin.upgradeHandler(VersionV11))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	u.Scheme = "ws"
	dialer := websocket.DefaultDialer
	header := make(http.Header)
	header.Set("Authorization", "Bearer wrong")
	_, response, err := dialer.Dial(u.String(), header)
	if err == nil || response == nil || response.StatusCode != 401 {
		t.Fatalf("错误 token 握手结果 response=%v err=%v", response, err)
	}
}
