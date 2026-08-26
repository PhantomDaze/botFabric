package response

import (
	"context"
	"errors"
	"time"

	"github.com/PhantomDaze/botfabric/internal/store"
	"github.com/redis/go-redis/v9"
)

// DefaultEventTTL 是 Redis 实现中事件状态 key 的兜底 TTL。
// 它必须不短于 coordinator 的实际 expire 时间（policy.Timeout，priority_override 再 +1min），
// 否则会在 coordinator 仍可能调用 Release 前提前过期。取一个宽裕默认值（10min），
// 覆盖常见超时；超出此值的超长事件会因 key 提前过期而被允许重复发送——
// 这是兜底清理与精确 TTL 的权衡，留作可配置项以便调优。
const DefaultEventTTL = 10 * time.Minute

// RedisStore 是 CandidateStore 的 Redis 实现。
// 每个事件用单个 Redis hash 表达状态：key 形如 prefix:resp:<eventID>，
// 字段 winner 存获胜候选 key，claim:<key> 存占位标记。
// 每次状态变更都刷新该 key 的 TTL，作为进程死亡时的兜底清理，
// 避免内存实现依赖 time.AfterFunc 而在进程崩溃后状态泄漏。
type RedisStore struct {
	client   *store.Client
	eventTTL time.Duration
}

// NewRedisStore 创建基于已连接 Redis 客户端的响应状态存储。
// eventTTL<=0 时取 DefaultEventTTL。
func NewRedisStore(client *store.Client, eventTTL time.Duration) (*RedisStore, error) {
	if client == nil {
		return nil, errors.New("redis 客户端不能为空")
	}
	if eventTTL <= 0 {
		eventTTL = DefaultEventTTL
	}
	return &RedisStore{client: client, eventTTL: eventTTL}, nil
}

func (s *RedisStore) key(eventID string) string { return s.client.Prefix() + "resp:" + eventID }

func (s *RedisStore) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

// claimScript 原子地执行 Claim：仅当 winner 未设置且该 claim 尚未存在时占位并刷新 TTL。
// 返回 1 表示成功，0 表示失败（已有 winner 或已 claim）。
var claimScript = redis.NewScript(`
if redis.call("HEXISTS", KEYS[1], "winner") == 1 then
	return 0
end
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1], ARGV[1], "1")
redis.call("EXPIRE", KEYS[1], ARGV[2])
return 1
`)

// Claim 占用一个候选 key；已有 winner 或已 claim 时返回 false。
func (s *RedisStore) Claim(eventID, key string) bool {
	if eventID == "" || key == "" {
		return false
	}
	ctx, cancel := s.ctx()
	defer cancel()
	res, err := claimScript.Run(ctx, s.client.Redis(),
		[]string{s.key(eventID)},
		"claim:"+key, int64(s.eventTTL/time.Second),
	).Int64()
	if err != nil {
		return false
	}
	return res == 1
}

// tryWinScript 原子地执行 TryWin：仅当 winner 未设置时写入 winner 并刷新 TTL。
var tryWinScript = redis.NewScript(`
if redis.call("HEXISTS", KEYS[1], "winner") == 1 then
	return 0
end
redis.call("HSET", KEYS[1], "winner", ARGV[1])
redis.call("EXPIRE", KEYS[1], ARGV[2])
return 1
`)

// TryWin 尝试成为该事件的唯一 winner；已有 winner 时返回 false。
func (s *RedisStore) TryWin(eventID, key string) bool {
	if eventID == "" || key == "" {
		return false
	}
	ctx, cancel := s.ctx()
	defer cancel()
	res, err := tryWinScript.Run(ctx, s.client.Redis(),
		[]string{s.key(eventID)},
		key, int64(s.eventTTL/time.Second),
	).Int64()
	if err != nil {
		return false
	}
	return res == 1
}

// Winner 返回该事件是否已有 winner。
func (s *RedisStore) Winner(eventID string) bool {
	if eventID == "" {
		return false
	}
	ctx, cancel := s.ctx()
	defer cancel()
	n, err := s.client.Redis().HExists(ctx, s.key(eventID), "winner").Result()
	if err != nil {
		return false
	}
	return n
}

// ReleaseWinner 删除 winner 字段，保留其余 claim 状态。
func (s *RedisStore) ReleaseWinner(eventID string) {
	if eventID == "" {
		return
	}
	ctx, cancel := s.ctx()
	defer cancel()
	_, _ = s.client.Redis().HDel(ctx, s.key(eventID), "winner").Result()
}

// Release 删除该事件的全部状态。
func (s *RedisStore) Release(eventID string) {
	if eventID == "" {
		return
	}
	ctx, cancel := s.ctx()
	defer cancel()
	_, _ = s.client.Redis().Del(ctx, s.key(eventID)).Result()
}
