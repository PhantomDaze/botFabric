package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/config"
	"github.com/PhantomDaze/botfabric/internal/onebotapp"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/PhantomDaze/botfabric/plugins/protocol/onebot"
	"github.com/gorilla/websocket"
)

// addOneBotProtocol 声明真实 OneBot v11/v12 反向 WebSocket 协议插件。
func (h *simHarness) addOneBotProtocol() *onebot.Plugin {
	h.t.Helper()
	cfg := config.OneBotConfig{
		Enabled:           true,
		ListenAddress:     "127.0.0.1:0",
		PathV11:           "/onebot/v11",
		PathV12:           "/onebot/v12",
		AccessToken:       "sim-protocol-token",
		ConnectTimeout:    config.Duration{Duration: time.Second},
		HeartbeatInterval: config.Duration{Duration: time.Second},
		MaxMessageSize:    config.ByteSize(1 << 20),
	}
	plugin := onebot.New(cfg)
	manifest := plugin.Manifest()
	h.declare(manifest, plugin, simRules{
		publish: []string{groupTopic},
		provide: []string{actionService},
	})
	return plugin
}

// E1：真实协议端 WebSocket → Runtime EventBus → 应用端 OneBot WebSocket →
// CandidateReply → 响应协调器 → 协议端 WebSocket 动作/echo，全链路覆盖 v11/v12。
func TestScenarioOneBotProtocolToApplicationRoundTrip(t *testing.T) {
	versions := []struct {
		name         string
		protocolPath string
		protocolVer  string
		appPath      string
		appVer       string
	}{
		{"v11_to_v11", "/onebot/v11", "v11", "/app/v11", onebotapp.VersionV11},
		{"v11_to_v12", "/onebot/v11", "v11", "/app/v12", onebotapp.VersionV12},
		{"v12_to_v11", "/onebot/v12", "v12", "/app/v11", onebotapp.VersionV11},
		{"v12_to_v12", "/onebot/v12", "v12", "/app/v12", onebotapp.VersionV12},
	}
	for _, tc := range versions {
		t.Run(tc.name, func(t *testing.T) {
			h := newSim(t).withAllowAllPolicy()
			protocol := h.addOneBotProtocol()
			h.addRouting("sim.routing.arb", arbRouting(contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: 2 * time.Second}))
			env := h.start(context.Background())

			appConfig := onebotapp.Config{
				Enabled:           true,
				Mode:              "server",
				ListenAddress:     "127.0.0.1:0",
				PathV11:           "/app/v11",
				PathV12:           "/app/v12",
				AccessToken:       "sim-application-token",
				ReadTimeout:       time.Second,
				WriteTimeout:      time.Second,
				HeartbeatInterval: time.Second,
			}
			app, err := onebotapp.New(appConfig, env.Endpoints, env.Manager.ResponseCoordinator())
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

			// 应用端以指定协议版本主动连入本机适配器。
			appURL := url.URL{Scheme: "ws", Host: app.Addr(), Path: tc.appPath}
			appHeader := http.Header{}
			appHeader.Set("Authorization", "Bearer sim-application-token")
			appSocket, _, err := websocket.DefaultDialer.Dial(appURL.String(), appHeader)
			if err != nil {
				t.Fatalf("连接应用端 WebSocket: %v", err)
			}
			defer appSocket.Close()

			// QQ 协议端连接本机 OneBot 协议适配器。
			protocolURL := url.URL{Scheme: "ws", Host: protocol.Addr(), Path: tc.protocolPath}
			protocolHeader := http.Header{}
			protocolHeader.Set("Authorization", "Bearer sim-protocol-token")
			protocolSocket, _, err := websocket.DefaultDialer.Dial(protocolURL.String(), protocolHeader)
			if err != nil {
				t.Fatalf("连接协议端 WebSocket: %v", err)
			}
			defer protocolSocket.Close()

			// 应用端异步读取事件、按携带的 event ID 回发标准 OneBot 回复。
			replyDone := make(chan error, 1)
			go func() {
				_, payload, err := appSocket.ReadMessage()
				if err != nil {
					replyDone <- fmt.Errorf("应用端读取事件: %w", err)
					return
				}
				var event map[string]any
				if err := json.Unmarshal(payload, &event); err != nil {
					replyDone <- fmt.Errorf("解码应用端事件: %w", err)
					return
				}
				eventID, _ := event["__botfabric_event_id"].(string)
				if eventID == "" {
					replyDone <- fmt.Errorf("推送事件缺少 __botfabric_event_id: %s", payload)
					return
				}
				var action map[string]any
				if tc.appVer == onebotapp.VersionV11 {
					action = map[string]any{
						"action": "send_group_msg",
						"params": map[string]any{"group_id": "9001", "message": "integrated reply", "__botfabric_event_id": eventID},
					}
				} else {
					action = map[string]any{
						"action": "send_message",
						"params": map[string]any{
							"detail_type":          "group",
							"group_id":             "9001",
							"message":              []map[string]any{{"type": "text", "data": map[string]string{"text": "integrated reply"}}},
							"__botfabric_event_id": eventID,
						},
					}
				}
				replyDone <- appSocket.WriteJSON(action)
			}()

			// 由真实协议端推送一条 v11/v12 群消息。
			var incoming map[string]any
			if tc.protocolVer == "v11" {
				incoming = map[string]any{
					"time": time.Now().Unix(), "self_id": "bot-real-1", "post_type": "message", "message_type": "group",
					"message_id": "in-v11", "group_id": 9001, "user_id": 77, "raw_message": "hello from v11", "message": "hello from v11",
				}
			} else {
				incoming = map[string]any{
					"time": time.Now().Unix(), "self_id": "bot-real-1", "type": "message", "detail_type": "group",
					"id": "in-v12", "group_id": "9001", "user_id": "77",
					"message": []map[string]any{{"type": "text", "data": map[string]string{"text": "hello from v12"}}},
				}
			}
			if err := protocolSocket.WriteJSON(incoming); err != nil {
				t.Fatalf("协议端推送事件: %v", err)
			}

			select {
			case err := <-replyDone:
				if err != nil {
					t.Fatalf("应用端回复: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("应用端未及时收到事件并回复")
			}

			// 仲裁器向协议端发出 OneBot 动作；校验版本专属动作名称与关联字段。
			_ = protocolSocket.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, actionPayload, err := protocolSocket.ReadMessage()
			if err != nil {
				t.Fatalf("协议端未收到发送动作: %v", err)
			}
			var action struct {
				Action string         `json:"action"`
				Params map[string]any `json:"params"`
				Echo   string         `json:"echo"`
			}
			if err := json.Unmarshal(actionPayload, &action); err != nil {
				t.Fatalf("解码协议动作: %v", err)
			}
			wantAction := map[string]string{"v11": "send_group_msg", "v12": "send_message"}[tc.protocolVer]
			if action.Action != wantAction {
				t.Fatalf("协议动作 = %q，期望 %q", action.Action, wantAction)
			}
			if action.Echo == "" {
				t.Fatal("协议动作应携带 echo")
			}
			if action.Params["group_id"] != "9001" && action.Params["group_id"] != float64(9001) {
				t.Fatalf("动作群号 = %#v，期望 9001", action.Params["group_id"])
			}
			if tc.protocolVer == "v11" {
				if action.Params["message"] != "integrated reply" {
					t.Fatalf("v11 动作回复文本 = %#v，期望 integrated reply", action.Params["message"])
				}
			} else {
				segments, ok := action.Params["message"].([]any)
				if !ok || len(segments) != 1 {
					t.Fatalf("v12 动作消息段无效: %#v", action.Params["message"])
				}
				segment, ok := segments[0].(map[string]any)
				if !ok {
					t.Fatalf("v12 动作消息段类型无效: %#v", segments[0])
				}
				data, _ := segment["data"].(map[string]any)
				if data["text"] != "integrated reply" {
					t.Fatalf("v12 动作回复文本 = %#v，期望 integrated reply", data["text"])
				}
			}

			if err := protocolSocket.WriteJSON(map[string]any{
				"status": "ok", "retcode": 0, "data": map[string]any{"message_id": "sent-1"}, "echo": action.Echo,
			}); err != nil {
				t.Fatalf("回写协议动作结果: %v", err)
			}
			_ = appSocket.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, appAckPayload, err := appSocket.ReadMessage()
			if err != nil {
				t.Fatalf("应用端未收到候选回复受理结果: %v", err)
			}
			var appAck map[string]any
			if err := json.Unmarshal(appAckPayload, &appAck); err != nil {
				t.Fatalf("解码应用端受理结果: %v", err)
			}
			if appAck["status"] != "ok" {
				t.Fatalf("应用端回复未被受理: %s", appAckPayload)
			}
			if env.DeadLetterCount() != 0 {
				t.Fatalf("完整闭环不应产生死信: %#v", env.Dead.Entries())
			}
		})
	}
}
