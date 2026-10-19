package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/onebotapp"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/gorilla/websocket"
)

// E3：应用端 OneBot client 模式主动连接远端服务端，完整完成事件接收、候选提交与协议动作回路。
func TestScenarioOneBotApplicationClientRoundTrip(t *testing.T) {
	h := newSim(t).withAllowAllPolicy()
	protocol := h.addOneBotProtocol()
	h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second}))
	env := h.start(context.Background())

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	remoteResult := make(chan error, 1)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			remoteResult <- err
			return
		}
		defer socket.Close()
		_, payload, err := socket.ReadMessage()
		if err != nil {
			remoteResult <- err
			return
		}
		var event map[string]any
		if err := json.Unmarshal(payload, &event); err != nil {
			remoteResult <- err
			return
		}
		eventID, _ := event["__botfabric_event_id"].(string)
		if eventID == "" {
			remoteResult <- errors.New("client 模式收到的事件缺少 event ID")
			return
		}
		if err := socket.WriteJSON(map[string]any{
			"action": "send_group_msg",
			"params": map[string]any{"group_id": "9001", "message": "client mode reply", "__botfabric_event_id": eventID},
		}); err != nil {
			remoteResult <- err
			return
		}
		_, ack, err := socket.ReadMessage()
		if err != nil {
			remoteResult <- err
			return
		}
		var result map[string]any
		if err := json.Unmarshal(ack, &result); err != nil {
			remoteResult <- err
			return
		}
		if result["status"] != "ok" {
			remoteResult <- fmt.Errorf("应用端未确认候选回复: %s", ack)
			return
		}
		remoteResult <- nil
	}))
	defer remote.Close()
	remoteURL, err := url.Parse(remote.URL)
	if err != nil {
		t.Fatal(err)
	}

	app, err := onebotapp.New(onebotapp.Config{
		Enabled: true, Mode: "client", AccessToken: "sim-client-token",
		ReadTimeout: time.Second, WriteTimeout: time.Second, HeartbeatInterval: time.Second,
		RemoteAddress: remoteURL.Host, RemotePath: "/onebot", RemoteVersion: onebotapp.VersionV11,
	}, env.Endpoints, env.Manager.ResponseCoordinator())
	if err != nil {
		t.Fatalf("创建 client 模式应用端适配器: %v", err)
	}
	if err := app.Start(); err != nil {
		t.Fatalf("启动 client 模式适配器: %v", err)
	}
	if err := env.Bus.RegisterSink(app); err != nil {
		t.Fatalf("注册应用端 sink: %v", err)
	}
	t.Cleanup(func() {
		env.Bus.UnregisterSink(app)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := app.Shutdown(ctx); err != nil {
			t.Errorf("关闭 client 适配器: %v", err)
		}
	})
	if err := app.Connect(context.Background(), remoteURL.Host, "/onebot", onebotapp.VersionV11); err != nil {
		t.Fatalf("client 模式连接远端应用: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for len(app.Candidates()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(app.Candidates()) == 0 {
		t.Fatal("client 连接建立后未注册应用端候选")
	}

	protocolURL := url.URL{Scheme: "ws", Host: protocol.Addr(), Path: "/onebot/v11"}
	protocolHeader := http.Header{"Authorization": []string{"Bearer sim-protocol-token"}}
	protocolSocket, _, err := websocket.DefaultDialer.Dial(protocolURL.String(), protocolHeader)
	if err != nil {
		t.Fatalf("连接协议端: %v", err)
	}
	defer protocolSocket.Close()
	if err := protocolSocket.WriteJSON(map[string]any{
		"time": time.Now().Unix(), "self_id": "bot-client-1", "post_type": "message", "message_type": "group",
		"message_id": "in-client", "group_id": 9001, "user_id": 77, "raw_message": "client hello", "message": "client hello",
	}); err != nil {
		t.Fatalf("协议端发布事件: %v", err)
	}

	_ = protocolSocket.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, actionPayload, err := protocolSocket.ReadMessage()
	if err != nil {
		t.Fatalf("协议端未收到经仲裁的动作: %v", err)
	}
	var action struct {
		Action string         `json:"action"`
		Params map[string]any `json:"params"`
		Echo   string         `json:"echo"`
	}
	if err := json.Unmarshal(actionPayload, &action); err != nil {
		t.Fatalf("解码协议动作: %v", err)
	}
	if action.Action != "send_group_msg" || action.Params["message"] != "client mode reply" {
		t.Fatalf("client 模式候选未正确转发到协议端: %#v", action)
	}
	if err := protocolSocket.WriteJSON(map[string]any{
		"status": "ok", "retcode": 0, "data": map[string]any{"message_id": "client-sent"}, "echo": action.Echo,
	}); err != nil {
		t.Fatalf("回写协议动作结果: %v", err)
	}
	select {
	case err := <-remoteResult:
		if err != nil {
			t.Fatalf("远端应用服务端: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("远端应用未收到候选回复确认")
	}
}
