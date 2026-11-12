package config

import "errors"

const (
	// DefaultApplicationMaxMessageSize 是应用端 OneBot 单条消息的默认读上限。
	DefaultApplicationMaxMessageSize ByteSize = 4 << 20
	// MaxApplicationMessageSize 是应用端 OneBot 单条消息读上限的允许最大值。
	MaxApplicationMessageSize ByteSize = 64 << 20
)

// ApplicationOneBotConfig 控制应用端 OneBot 服务端和客户端接入。
type ApplicationOneBotConfig struct {
	Enabled              bool     `yaml:"enabled"`
	Mode                 string   `yaml:"mode"`
	ListenAddress        string   `yaml:"listen_address"`
	PathV11              string   `yaml:"path_v11"`
	PathV12              string   `yaml:"path_v12"`
	AccessToken          string   `yaml:"access_token"`
	ReadTimeout          Duration `yaml:"read_timeout"`
	WriteTimeout         Duration `yaml:"write_timeout"`
	HeartbeatInterval    Duration `yaml:"heartbeat_interval"`
	RemoteAddress        string   `yaml:"remote_address"`
	RemotePath           string   `yaml:"remote_path"`
	RemoteVersion        string   `yaml:"remote_version"`
	ReconnectInterval    Duration `yaml:"reconnect_interval"`
	ReconnectMaxInterval Duration `yaml:"reconnect_max_interval"`
	ReconnectMaxTries    int      `yaml:"reconnect_max_tries"`
	MaxMessageSize       ByteSize `yaml:"max_message_size"`
}

// Validate 检查应用端 OneBot 配置。
func (c ApplicationOneBotConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Mode == "" {
		c.Mode = "server"
	}
	if c.Mode != "server" && c.Mode != "client" && c.Mode != "both" {
		return errors.New("application_onebot.mode 仅支持 server、client 或 both")
	}
	if c.Mode == "server" || c.Mode == "both" {
		if c.ListenAddress == "" || c.PathV11 == "" || c.PathV12 == "" {
			return errors.New("application_onebot 服务端地址和路径不能为空")
		}
		if c.PathV11 == c.PathV12 {
			return errors.New("application_onebot v11/v12 路径不能相同")
		}
	}
	if c.Mode == "client" || c.Mode == "both" {
		if c.RemoteAddress == "" || c.RemotePath == "" || (c.RemoteVersion != "v11" && c.RemoteVersion != "v12") {
			return errors.New("application_onebot 客户端远端地址、路径和版本不能为空")
		}
	}
	if len(c.AccessToken) < 8 {
		return errors.New("application_onebot access token 长度不能少于 8")
	}
	if c.ReadTimeout.Duration <= 0 || c.WriteTimeout.Duration <= 0 {
		return errors.New("application_onebot 读写超时必须大于零")
	}
	// 心跳间隔：0 表示不启用心跳（向后兼容）；负数非法。
	if c.HeartbeatInterval.Duration < 0 {
		return errors.New("application_onebot heartbeat_interval 不能为负数")
	}
	// 重连配置：0 表示不自动重连（保持旧行为，首次拨号失败即返回）；>0 启用指数退避重连。
	if c.ReconnectInterval.Duration < 0 {
		return errors.New("application_onebot reconnect_interval 不能为负数")
	}
	if c.ReconnectMaxInterval.Duration < 0 {
		return errors.New("application_onebot reconnect_max_interval 不能为负数")
	}
	if c.ReconnectInterval.Duration > 0 && c.ReconnectMaxInterval.Duration > 0 && c.ReconnectMaxInterval.Duration < c.ReconnectInterval.Duration {
		return errors.New("application_onebot reconnect_max_interval 不能小于 reconnect_interval")
	}
	// 读消息上限：0 由适配器补默认值；负数与超过上限的值非法。
	if c.MaxMessageSize < 0 {
		return errors.New("application_onebot max_message_size 不能为负数")
	}
	if c.MaxMessageSize > MaxApplicationMessageSize {
		return errors.New("application_onebot max_message_size 超过允许上限 (64MiB)")
	}
	return nil
}
