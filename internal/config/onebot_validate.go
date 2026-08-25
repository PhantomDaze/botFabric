package config

import "errors"

// Validate 检查 OneBot 配置；关闭时不要求网络字段。
func (c OneBotConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.ListenAddress == "" {
		return errors.New("onebot.listen_address 不能为空")
	}
	if c.PathV11 == "" || c.PathV12 == "" {
		return errors.New("onebot.path_v11 和 path_v12 不能为空")
	}
	if c.PathV11 == c.PathV12 {
		return errors.New("onebot.path_v11 和 path_v12 不能相同")
	}
	if len(c.AccessToken) < 8 {
		return errors.New("onebot.access_token 长度不能少于 8")
	}
	if c.ConnectTimeout.Duration <= 0 {
		return errors.New("onebot.connect_timeout 必须大于零")
	}
	if c.HeartbeatInterval.Duration <= 0 {
		return errors.New("onebot.heartbeat_interval 必须大于零")
	}
	if c.MaxMessageSize <= 0 {
		return errors.New("onebot.max_message_size 必须大于零")
	}
	return nil
}
