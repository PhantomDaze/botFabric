package config

import "errors"

// ManagementConfig 控制 TCP 管理与诊断 API。
type ManagementConfig struct {
	Enabled       bool   `yaml:"enabled"`
	ListenAddress string `yaml:"listen_address"`
	BearerToken   string `yaml:"bearer_token"`
}

// Validate 检查启用时的管理 API 配置。
func (c ManagementConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.ListenAddress == "" {
		return errors.New("management.listen_address 不能为空")
	}
	if len(c.BearerToken) < 16 {
		return errors.New("management.bearer_token 长度不能少于 16")
	}
	return nil
}
