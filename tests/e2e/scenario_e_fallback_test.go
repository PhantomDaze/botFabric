package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/onebotapp"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/gorilla/websocket"
)

// E2：OneBot 应用端未在回复中携带 event_id/echo 时，按同群最近未决事件关联。
func TestScenarioOneBotReplyFallbackAssociatesSameGroup(t *testing.T) {
	h := newSim(t).withAllowAllPolicy()
	protocol := h.addOneBotProtocol()
	h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second}))
	env := h.start(context.Background())

	app, err := onebotapp.New(onebotapp.Config{
		Enabled: true, Mode: "server", ListenAddress: "127.0.0.1:0",
		PathV11: "/app/v11", PathV12: "/app/v12", AccessToken: "sim-application-token",
		ReadTimeout: time.Second, WriteTimeout: time.Second, HeartbeatInterval: time.Second,
	}, env.Endpoints, env.Manager.ResponseCoordinator())
	if err != nil {
		t.Fatalf("创建应用端适配器: %v", err)
	}
	if err := app.Start(); err != nil {
		t.Fatalf("启动应用端适配器: %v", err)
	}
	if err := env.Bus.RegisterSink(app); err != nil {
		t.Fatalf("注册应用端 sink: %v", err)
	}
	t.Cleanup(func() {
		env.Bus.UnregisterSink(app)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := app.Shutdown(ctx); err != nil {
			t.Errorf("关闭应用端适配器: %v", err)
		}
	})

	appURL := url.URL{Scheme: "ws", Host: app.Addr(), Path: "/app/v11"}
	appHeader := http.Header{"Authorization": []string{"Bearer sim-application-token"}}
	appSocket, _, err := websocket.DefaultDialer.Dial(appURL.String(), appHeader)
	if err != nil {
		t.Fatalf("连接应用端: %v", err)
	}
	defer appSocket.Close()

	protocolURL := url.URL{Scheme: "ws", Host: protocol.Addr(), Path: "/onebot/v11"}
	protocolHeader := http.Header{"Authorization": []string{"Bearer sim-protocol-token"}}
	protocolSocket, _, err := websocket.DefaultDialer.Dial(protocolURL.String(), protocolHeader)
	if err != nil {
		t.Fatalf("连接协议端: %v", err)
	}
	defer protocolSocket.Close()

	replyDone := make(chan error, 1)
	go func() {
		_, payload, err := appSocket.ReadMessage()
		if err != nil {
			replyDone <- err
			return
		}
		var pushed map[string]any
		if err := json.Unmarshal(payload, &pushed); err != nil {
			replyDone <- err
			return
		}
		if pushed["__botfabric_event_id"] == nil {
			replyDone <- errors.New("推送事件缺少原始 event ID")
			return
		}
		// 故意既不传 __botfabric_event_id，也不传 echo，触发同群 fallback。
		replyDone <- appSocket.WriteJSON(map[string]any{
			"action": "send_group_msg",
			"params": map[string]any{"group_id": "9001", "message": "fallback reply"},
		})
	}()

	if err := protocolSocket.WriteJSON(map[string]any{
		"time": time.Now().Unix(), "self_id": "bot-real-1", "post_type": "message", "message_type": "group",
		"message_id": "in-1", "group_id": 9001, "user_id": 77, "raw_message": "hello", "message": "hello",
	}); err != nil {
		t.Fatalf("推送群消息: %v", err)
	}
	select {
	case err := <-replyDone:
		if err != nil {
			t.Fatalf("应用端 fallback 回复: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("应用端未完成 fallback 回复")
	}

	_ = protocolSocket.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := protocolSocket.ReadMessage()
	if err != nil {
		t.Fatalf("协议端未收到 fallback 回复动作: %v", err)
	}
	var action map[string]any
	if err := json.Unmarshal(payload, &action); err != nil {
		t.Fatalf("解码发送动作: %v", err)
	}
	if action["action"] != "send_group_msg" {
		t.Fatalf("OneBot 动作 = %#v，期望 send_group_msg", action["action"])
	}
	params, _ := action["params"].(map[string]any)
	if params["message"] != "fallback reply" {
		t.Fatalf("fallback 回复文本 = %#v", params["message"])
	}
	echo, _ := action["echo"].(string)
	if echo == "" {
		t.Fatal("协议端动作应携带 echo")
	}
	if err := protocolSocket.WriteJSON(map[string]any{"status": "ok", "retcode": 0, "data": map[string]any{"message_id": "sent-1"}, "echo": echo}); err != nil {
		t.Fatalf("回写动作结果: %v", err)
	}
}
