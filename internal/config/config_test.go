package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRejectsUnknownField(t *testing.T) {
	path := writeTempConfig(t, `
runtime:
  plugin_directory: ./plugins
  shutdown_timeout: 10s
  default_handler_timeout: 2s
  bot_id: test
  misspelled: true
transport:
  unix_socket_directory: ./run
  max_message_size: 4MiB
policy:
  file: ./config/policy.yaml
  default: deny
observability:
  log_format: json
  metrics_enabled: true
  metrics_listen_address: 127.0.0.1:0
  tracing_enabled: false
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "misspelled") {
		t.Fatalf("未知字段应被拒绝，实际错误: %v", err)
	}
}

func TestLoadParsesDurationAndByteSize(t *testing.T) {
	path := writeTempConfig(t, `
runtime:
  plugin_directory: ./plugins
  shutdown_timeout: 10s
  default_handler_timeout: 2s
  bot_id: test
transport:
  unix_socket_directory: ./run
  max_message_size: 4MiB
policy:
  file: ./config/policy.yaml
  default: deny
observability:
  log_format: json
  metrics_enabled: true
  metrics_listen_address: 127.0.0.1:0
  tracing_enabled: false
`)
	config, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Transport.MaxMessageSize != 4*1024*1024 {
		t.Fatalf("消息大小 = %d，期望 %d", config.Transport.MaxMessageSize, 4*1024*1024)
	}
}

func TestLoadRejectsMetricsWithoutListenAddress(t *testing.T) {
	path := writeTempConfig(t, `
runtime:
  plugin_directory: ./plugins
  shutdown_timeout: 10s
  default_handler_timeout: 2s
  bot_id: test
transport:
  unix_socket_directory: ./run
  max_message_size: 4MiB
policy:
  file: ./config/policy.yaml
  default: deny
observability:
  log_format: json
  metrics_enabled: true
  tracing_enabled: false
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "metrics_listen_address") {
		t.Fatalf("启用指标但缺少监听地址应被拒绝，实际错误: %v", err)
	}
}

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
