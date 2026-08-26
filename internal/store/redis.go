// Package store 提供分布式共享存储的 Redis 封装。
// 去重与响应协调两个子系统共享同一个 Redis 客户端与 key 命名空间，
// 避免各自直连 Redis 造成连接管理分散。
package store

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config 控制 Redis 连接。由 internal/config.RedisConfig 装配时转换得到。
type Config struct {
	Address   string
	Password  string
	DB        int
	KeyPrefix string
}

// Client 封装 go-redis 客户端，供 dedup/response 复用。
type Client struct {
	rdb    *redis.Client
	prefix string
}

// New 创建 Redis 客户端并执行一次 Ping 探活；连接不可用时返回错误。
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Address == "" {
		return nil, errors.New("redis 地址不能为空")
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:        cfg.Address,
		Password:    cfg.Password,
		DB:          cfg.DB,
		DialTimeout: 3 * time.Second,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	return &Client{rdb: rdb, prefix: cfg.KeyPrefix}, nil
}

// Prefix 返回 key 前缀。
func (c *Client) Prefix() string { return c.prefix }

// Redis 暴露底层 go-redis 客户端，供 dedup/response 实现直接调用命令。
func (c *Client) Redis() *redis.Client { return c.rdb }

// Close 关闭底层连接。
func (c *Client) Close() error { return c.rdb.Close() }
