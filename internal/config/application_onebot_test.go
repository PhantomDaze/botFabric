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
