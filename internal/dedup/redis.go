package dedup

import (
	"context"
	"errors"
	"time"

	"github.com/PhantomDaze/botfabric/internal/store"
)

// RedisStore 是 Store 的 Redis 实现。
// 用 SET key value NX EX 原子完成首占与 TTL 过期：
// NX 保证仅当 key 不存在时设置成功（即 primary），EX 让 key 自动过期，
// 消除内存实现的时钟依赖与 Cleanup 需求。
type RedisStore struct {
	client *store.Client
	now    func() time.Time
}

// NewRedisStore 创建基于已连接 Redis 客户端的去重存储。
// now 用于写入 value（可观测），不参与判定；缺省 time.Now。
func NewRedisStore(client *store.Client) (*RedisStore, error) {
	if client == nil {
		return nil, errors.New("redis 客户端不能为空")
	}
	return &RedisStore{client: client, now: time.Now}, nil
}

// Claim 尝试占用一个具有 TTL 的键。
// 成功（首占）返回 true；键仍在有效期内返回 false。
// TTL 向下取整到秒（Redis EX 最小秒级）；不足 1s 按 1s 保持语义。
func (s *RedisStore) Claim(key string, ttl time.Duration, now time.Time) bool {
	if key == "" || ttl <= 0 {
		return false
	}
	seconds := int64(ttl / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	value := now
	if value.IsZero() {
		value = s.now()
	}
	ok, err := s.client.Redis().SetNX(context.Background(),
		s.client.Prefix()+"dedup:"+key,
		value.Unix(),
		time.Duration(seconds)*time.Second,
	).Result()
	if err != nil {
		// Redis 不可用：保守视为已被占用（返回 false），避免重复事件被误判为首占。
		return false
	}
	return ok
}
