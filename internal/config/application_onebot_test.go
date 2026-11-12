package config

import (
	"testing"
	"time"
)

func validApplicationOneBotConfig(mode string) ApplicationOneBotConfig {
	return ApplicationOneBotConfig{
		Enabled:       true,
		Mode:          mode,
		ListenAddress: "127.0.0.1:0",
		PathV11:       "/v11",
		PathV12:       "/v12",
		AccessToken:   "test-token",
		ReadTimeout:   Duration{Duration: time.Second},
		WriteTimeout:  Duration{Duration: time.Second},
		RemoteAddress: "127.0.0.1:9000",
		RemotePath:    "/onebot",
		RemoteVersion: "v11",
	}
}

func TestApplicationOneBotModes(t *testing.T) {
	for _, mode := range []string{"server", "client", "both"} {
		config := validApplicationOneBotConfig(mode)
		if mode == "client" {
			config.ListenAddress = ""
			config.PathV11 = ""
			config.PathV12 = ""
		}
		if err := config.Validate(); err != nil {
			t.Fatalf("mode %s 校验失败: %v", mode, err)
		}
	}
	invalid := validApplicationOneBotConfig("invalid")
	if err := invalid.Validate(); err == nil {
		t.Fatal("无效 mode 应被拒绝")
	}
}

func TestApplicationOneBotReconnectValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*ApplicationOneBotConfig)
		wantErr bool
	}{
		{"reconnect_interval 负数", func(c *ApplicationOneBotConfig) { c.ReconnectInterval = Duration{Duration: -time.Second} }, true},
		{"reconnect_max_interval 负数", func(c *ApplicationOneBotConfig) { c.ReconnectMaxInterval = Duration{Duration: -time.Second} }, true},
		{"max 小于 base", func(c *ApplicationOneBotConfig) {
			c.ReconnectInterval = Duration{Duration: 5 * time.Second}
			c.ReconnectMaxInterval = Duration{Duration: time.Second}
		}, true},
		{"合法重连配置", func(c *ApplicationOneBotConfig) {
			c.ReconnectInterval = Duration{Duration: 2 * time.Second}
			c.ReconnectMaxInterval = Duration{Duration: 16 * time.Second}
			c.ReconnectMaxTries = 0
		}, false},
		{"不配置重连(0) 合法", func(c *ApplicationOneBotConfig) {}, false},
		{"heartbeat_interval 负数", func(c *ApplicationOneBotConfig) { c.HeartbeatInterval = Duration{Duration: -time.Second} }, true},
		{"heartbeat_interval 0 合法", func(c *ApplicationOneBotConfig) { c.HeartbeatInterval = Duration{} }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validApplicationOneBotConfig("client")
			tc.mutate(&c)
			err := c.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("期望校验失败，实际通过")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("期望通过，实际失败: %v", err)
			}
		})
	}
}

// TestApplicationOneBotMaxMessageSizeValidation 验证应用端读消息上限的边界校验：
// 未配置(0)由适配器补默认值，允许到 64MiB，超过上限或负数被拒绝。
func TestApplicationOneBotMaxMessageSizeValidation(t *testing.T) {
	base := validApplicationOneBotConfig("server")
	if err := base.Validate(); err != nil {
		t.Fatalf("未配置 max_message_size 应合法: %v", err)
	}
	atLimit := validApplicationOneBotConfig("server")
	atLimit.MaxMessageSize = MaxApplicationMessageSize
	if err := atLimit.Validate(); err != nil {
		t.Fatalf("等于上限应合法: %v", err)
	}
	overLimit := validApplicationOneBotConfig("server")
	overLimit.MaxMessageSize = MaxApplicationMessageSize + 1
	if err := overLimit.Validate(); err == nil {
		t.Fatal("超过 64MiB 应被拒绝")
	}
	negative := validApplicationOneBotConfig("server")
	negative.MaxMessageSize = -1
	if err := negative.Validate(); err == nil {
		t.Fatal("负数 max_message_size 应被拒绝")
	}
}
