package onebotapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/endpoint"
	"github.com/PhantomDaze/botfabric/internal/response"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/gorilla/websocket"
)

type submitter struct {
	replies chan contract.CandidateReply
}

func (s *submitter) Submit(_ context.Context, reply contract.CandidateReply) (response.SubmitResult, error) {
	s.replies <- reply
	return response.SubmitResult{Status: "accepted", Winner: true}, nil
}

func testConfig() Config {
	return Config{Enabled: true, ListenAddress: "127.0.0.1:0", PathV11: "/v11", PathV12: "/v12", AccessToken: "test-token", ReadTimeout: time.Second, WriteTimeout: time.Second}
}

func TestV11EventRoundTripToCandidateReply(t *testing.T) {
	testRoundTrip(t, VersionV11)
}

func TestV12EventRoundTripToCandidateReply(t *testing.T) {
	testRoundTrip(t, VersionV12)
}

func testRoundTrip(t *testing.T, version string) {
	submitter := &submitter{replies: make(chan contract.CandidateReply, 1)}
	adapter, err := New(testConfig(), endpoint.New(), submitter)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(); err != nil {
		t.Fatal(err)
	}
	defer adapter.Shutdown(context.Background())

	u, err := url.Parse("ws://" + adapter.Addr() + map[string]string{VersionV11: "/v11", VersionV12: "/v12"}[version])
	if err != nil {
		t.Fatal(err)
	}
	header := http.Header{}
	header.Set("Authorization", "Bearer test-token")
	socket, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()

	eventPayload, _ := json.Marshal(contract.GroupMessage{MessageID: "msg-1", GroupID: "group-1", UserID: "user-1", Text: "你好"})
	connection := waitConnection(t, adapter)
	event := contract.Event{ID: "event-1", Timestamp: time.Unix(1700000000, 0).UTC(), BotID: "bot-1", Payload: eventPayload}
	if err := connection.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	_, payload, err := socket.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope["__botfabric_event_id"] != "event-1" {
		t.Fatalf("推送事件缺少关联 ID: %#v", envelope)
	}
	params := map[string]any{"group_id": "group-1", "message": "回复"}
	action := map[string]any{"action": "send_group_msg", "params": params, "echo": "event-1"}
	if version == VersionV12 {
		action["action"] = "send_message"
		action["params"] = map[string]any{"group_id": "group-1", "message": []map[string]any{{"type": "text", "data": map[string]string{"text": "回复"}}}}
	}
	if err := socket.WriteJSON(action); err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-submitter.replies:
		if reply.EventID != "event-1" || reply.ApplicationID != connection.id || reply.GroupID != "group-1" || reply.Text != "回复" {
			t.Fatalf("候选回复错误: %#v", reply)
		}
	case <-time.After(time.Second):
		t.Fatal("候选回复提交超时")
	}
}

func waitConnection(t *testing.T, adapter *Adapter) *connection {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		adapter.connMu.RLock()
		for _, conn := range adapter.conns {
			adapter.connMu.RUnlock()
			return conn
		}
		adapter.connMu.RUnlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("应用端连接未注册")
	return nil
}

func TestRejectsWrongToken(t *testing.T) {
	adapter, err := New(testConfig(), endpoint.New(), &submitter{replies: make(chan contract.CandidateReply, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(); err != nil {
		t.Fatal(err)
	}
	defer adapter.Shutdown(context.Background())
	u := "ws://" + adapter.Addr() + "/v11"
	header := http.Header{}
	header.Set("Authorization", "Bearer wrong")
	_, response, err := websocket.DefaultDialer.Dial(u, header)
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("错误 token 握手结果 response=%v err=%v", response, err)
	}
}
