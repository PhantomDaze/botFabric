package config

import "errors"

// ApplicationOneBotConfig 控制应用端 OneBot 服务端和客户端接入。
type ApplicationOneBotConfig struct {
	Enabled       bool     `yaml:"enabled"`
	Mode          string   `yaml:"mode"`
	ListenAddress string   `yaml:"listen_address"`
	PathV11       string   `yaml:"path_v11"`
	PathV12       string   `yaml:"path_v12"`
	AccessToken   string   `yaml:"access_token"`
	ReadTimeout   Duration `yaml:"read_timeout"`
	WriteTimeout  Duration `yaml:"write_timeout"`
	RemoteAddress string   `yaml:"remote_address"`
	RemotePath    string   `yaml:"remote_path"`
	RemoteVersion string   `yaml:"remote_version"`
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
	return nil
}
