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
