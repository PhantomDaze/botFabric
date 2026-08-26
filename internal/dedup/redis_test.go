package dedup

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/store"
	"github.com/alicebob/miniredis/v2"
)

// newTestRedisStore 用 miniredis 构造一个已连接的 RedisStore。
// miniredis 是纯 Go 嵌入式 Redis，支持 SET NX EX 与 PING，无需真实实例。
func newTestRedisStore(t *testing.T) (*RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client, err := store.New(context.Background(), store.Config{
		Address:   mr.Addr(),
		KeyPrefix: "botfabric:",
	})
	if err != nil {
		t.Fatalf("连接 miniredis 失败: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	redisStore, err := NewRedisStore(client)
	if err != nil {
		t.Fatalf("创建 RedisStore 失败: %v", err)
	}
	return redisStore, mr
}

func TestRedisStoreClaimAtomicFirstTake(t *testing.T) {
	s, _ := newTestRedisStore(t)
	now := time.Unix(1000, 0)
	if !s.Claim("key-a", 10*time.Second, now) {
		t.Fatal("首次 Claim 应成功（首占）")
	}
	if s.Claim("key-a", 10*time.Second, now.Add(500*time.Millisecond)) {
		t.Fatal("TTL 内重复 Claim 应失败")
	}
	// 不同 key 互不影响。
	if !s.Claim("key-b", 10*time.Second, now) {
		t.Fatal("不同 key 首次 Claim 应成功")
	}
}

func TestRedisStoreClaimExpiryReclaimable(t *testing.T) {
	s, mr := newTestRedisStore(t)
	now := time.Unix(2000, 0)
	if !s.Claim("key-e", 2*time.Second, now) {
		t.Fatal("首次 Claim 应成功")
	}
	if s.Claim("key-e", 2*time.Second, now.Add(time.Second)) {
		t.Fatal("未过期前重复 Claim 应失败")
	}
	// 推进 miniredis 时钟使 key 过期。
	mr.FastForward(3 * time.Second)
	if !s.Claim("key-e", 2*time.Second, now.Add(3*time.Second)) {
		t.Fatal("过期后应可重新 Claim")
	}
}

func TestRedisStoreClaimSubSecondTTLClampsToOne(t *testing.T) {
	s, mr := newTestRedisStore(t)
	now := time.Unix(3000, 0)
	if !s.Claim("key-sub", 200*time.Millisecond, now) {
		t.Fatal("不足 1s 的 TTL 首次 Claim 应成功")
	}
	// EX 向下取整到 1s；FastForward 1.5s 后应已过期。
	mr.FastForward(1500 * time.Millisecond)
	if !s.Claim("key-sub", 200*time.Millisecond, now.Add(1500*time.Millisecond)) {
		t.Fatal("1s 后应可重新 Claim")
	}
}

func TestRedisStoreRejectsEmptyKeyAndNonPositiveTTL(t *testing.T) {
	s, _ := newTestRedisStore(t)
	if s.Claim("", time.Second, time.Unix(0, 0)) {
		t.Fatal("空 key 应直接失败")
	}
	if s.Claim("key-empty-ttl", 0, time.Unix(0, 0)) {
		t.Fatal("零 TTL 应直接失败")
	}
}

func TestRedisStoreWritesUnixTimestampValue(t *testing.T) {
	s, mr := newTestRedisStore(t)
	now := time.Unix(4242, 0)
	s.Claim("key-v", 10*time.Second, now)
	val, err := mr.Get("botfabric:dedup:key-v")
	if err != nil {
		t.Fatalf("读取 key 失败: %v", err)
	}
	if val != strconv.FormatInt(now.Unix(), 10) {
		t.Fatalf("key value = %q，期望 unix 时间戳 %d", val, now.Unix())
	}
}
