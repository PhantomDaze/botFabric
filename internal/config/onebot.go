package config

// OneBotConfig 控制 v11/v12 反向 WebSocket 协议插件。
type OneBotConfig struct {
	Enabled           bool     `yaml:"enabled"`
	ListenAddress     string   `yaml:"listen_address"`
	PathV11           string   `yaml:"path_v11"`
	PathV12           string   `yaml:"path_v12"`
	AccessToken       string   `yaml:"access_token"`
	ConnectTimeout    Duration `yaml:"connect_timeout"`
	HeartbeatInterval Duration `yaml:"heartbeat_interval"`
	MaxMessageSize    ByteSize `yaml:"max_message_size"`
	// WriteTimeout 是数据帧写超时；0 表示不加写 deadline（保持旧行为）。
	WriteTimeout Duration `yaml:"write_timeout"`
}
