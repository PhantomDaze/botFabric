package config

import "errors"

// RedisConfig 控制分布式共享存储（去重与响应状态）的 Redis 连接。
// 未启用时两个子系统回退到进程内内存实现，不依赖 Redis。
type RedisConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Address   string `yaml:"address"`
	Password  string `yaml:"password"`
	DB        int    `yaml:"db"`
	KeyPrefix string `yaml:"key_prefix"`
}

// DefaultKeyPrefix 是未配置 key_prefix 时的缺省命名空间。
const DefaultKeyPrefix = "botfabric:"

// Prefix 返回实际使用的 key 前缀，未配置时取缺省值。
func (c RedisConfig) Prefix() string {
	if c.KeyPrefix == "" {
		return DefaultKeyPrefix
	}
	return c.KeyPrefix
}

// Validate 检查启用时的 Redis 配置。
func (c RedisConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Address == "" {
		return errors.New("redis.address 不能为空")
	}
	if c.DB < 0 {
		return errors.New("redis.db 不能为负数")
	}
	return nil
}
