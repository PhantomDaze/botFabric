package config

import (
	"strings"
	"testing"
)

// baseConfig 是各 Redis 测试用例共享的最小合法配置；用例在其上覆盖 redis 段。
const baseConfig = `
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
onebot:
  enabled: true
  listen_address: 127.0.0.1:8080
  path_v11: /onebot/v11
  path_v12: /onebot/v12
  access_token: 1234567890abcdef
  connect_timeout: 2s
  heartbeat_interval: 30s
  max_message_size: 4MiB
application_onebot:
  enabled: false
management:
  enabled: false
`

func TestRedisDisabledByDefault(t *testing.T) {
	path := writeTempConfig(t, baseConfig+`
redis:
  enabled: false
`)
	config, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Redis.Enabled {
		t.Fatal("默认应禁用 Redis")
	}
}

func TestRedisEnabledRequiresAddress(t *testing.T) {
	path := writeTempConfig(t, baseConfig+`
redis:
  enabled: true
  address: ""
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "redis.address") {
		t.Fatalf("缺少 address 应报错，实际: %v", err)
	}
}

func TestRedisRejectsNegativeDB(t *testing.T) {
	path := writeTempConfig(t, baseConfig+`
redis:
  enabled: true
  address: 127.0.0.1:6379
  db: -1
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "redis.db") {
		t.Fatalf("负 db 应报错，实际: %v", err)
	}
}

func TestRedisEnabledParsesPrefix(t *testing.T) {
	path := writeTempConfig(t, baseConfig+`
redis:
  enabled: true
  address: 127.0.0.1:6379
  db: 2
  key_prefix: "fabric:"
`)
	config, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Redis.Prefix() != "fabric:" {
		t.Fatalf("Prefix = %q，期望 %q", config.Redis.Prefix(), "fabric:")
	}
}

func TestRedisDefaultPrefix(t *testing.T) {
	path := writeTempConfig(t, baseConfig+`
redis:
  enabled: true
  address: 127.0.0.1:6379
`)
	config, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Redis.Prefix() != DefaultKeyPrefix {
		t.Fatalf("缺省 Prefix = %q，期望 %q", config.Redis.Prefix(), DefaultKeyPrefix)
	}
}
