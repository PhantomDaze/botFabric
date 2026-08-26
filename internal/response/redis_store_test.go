package response

import (
	"context"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/store"
	"github.com/alicebob/miniredis/v2"
)

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
	rs, err := NewRedisStore(client, 0)
	if err != nil {
		t.Fatalf("创建 RedisStore 失败: %v", err)
	}
	return rs, mr
}

func TestRedisStoreClaimIdempotentAndBlockedByWinner(t *testing.T) {
	s, _ := newTestRedisStore(t)
	if !s.Claim("ev1", "k-a") {
		t.Fatal("首次 Claim 应成功")
	}
	if s.Claim("ev1", "k-a") {
		t.Fatal("重复 Claim 同 key 应失败")
	}
	if !s.Claim("ev1", "k-b") {
		t.Fatal("不同 key Claim 应成功")
	}
	if !s.TryWin("ev1", "k-a") {
		t.Fatal("TryWin 应成功")
	}
	// 已有 winner 后，新候选无法再 Claim。
	if s.Claim("ev1", "k-c") {
		t.Fatal("winner 已存在后 Claim 应失败")
	}
}

func TestRedisStoreTryWinOnceOnly(t *testing.T) {
	s, _ := newTestRedisStore(t)
	if !s.TryWin("ev2", "w1") {
		t.Fatal("首次 TryWin 应成功")
	}
	if s.TryWin("ev2", "w2") {
		t.Fatal("已有 winner 时 TryWin 应失败")
	}
	if !s.Winner("ev2") {
		t.Fatal("Winner 谓词应为 true")
	}
	if s.Winner("ev-missing") {
		t.Fatal("不存在的事件 Winner 应为 false")
	}
}

func TestRedisStoreReleaseWinnerKeepsClaims(t *testing.T) {
	s, mr := newTestRedisStore(t)
	s.Claim("ev3", "k-a")
	s.TryWin("ev3", "k-a")
	if !s.Winner("ev3") {
		t.Fatal("应有 winner")
	}
	s.ReleaseWinner("ev3")
	if s.Winner("ev3") {
		t.Fatal("ReleaseWinner 后不应再有 winner")
	}
	// claim 状态仍在，重复 Claim 同 key 应失败。
	if s.Claim("ev3", "k-a") {
		t.Fatal("ReleaseWinner 不应清除 claim，重复 Claim 应失败")
	}
	// 但可以重新 TryWin。
	if !s.TryWin("ev3", "k-b") {
		t.Fatal("ReleaseWinner 后应可重新 TryWin")
	}
	_ = mr
}

func TestRedisStoreReleaseClearsAll(t *testing.T) {
	s, mr := newTestRedisStore(t)
	s.Claim("ev4", "k-a")
	s.TryWin("ev4", "k-a")
	s.Release("ev4")
	if s.Winner("ev4") {
		t.Fatal("Release 后不应有 winner")
	}
	// 全部清除后可重新 Claim 与 TryWin。
	if !s.Claim("ev4", "k-a") {
		t.Fatal("Release 后应可重新 Claim")
	}
	if !mr.Exists("botfabric:resp:ev4") {
		t.Fatal("Claim 后 key 应存在")
	}
}

func TestRedisStoreKeyTTLExpiryFallback(t *testing.T) {
	s, mr := newTestRedisStore(t)
	// 用短 TTL 实例验证过期兜底逻辑。
	mr2 := miniredis.RunT(t)
	client, err := store.New(context.Background(), store.Config{Address: mr2.Addr(), KeyPrefix: "botfabric:"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	short, err := NewRedisStore(client, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	short.Claim("ev5", "k-a")
	short.TryWin("ev5", "k-a")
	if !short.Winner("ev5") {
		t.Fatal("应有 winner")
	}
	// 推进时钟超过 eventTTL，key 应过期，winner 消失（进程死亡兜底语义）。
	mr2.FastForward(3 * time.Second)
	if short.Winner("ev5") {
		t.Fatal("TTL 过期后 winner 应消失")
	}
	if !short.Claim("ev5", "k-a") {
		t.Fatal("TTL 过期后应可重新 Claim")
	}
	_ = s
	_ = mr
}

func TestRedisStoreRejectsEmptyArgs(t *testing.T) {
	s, _ := newTestRedisStore(t)
	if s.Claim("", "k") {
		t.Fatal("空 eventID 的 Claim 应失败")
	}
	if s.Claim("ev", "") {
		t.Fatal("空 key 的 Claim 应失败")
	}
	if s.TryWin("", "k") {
		t.Fatal("空 eventID 的 TryWin 应失败")
	}
	if s.Winner("") {
		t.Fatal("空 eventID 的 Winner 应失败")
	}
	// 空 eventID 的 Release/ReleaseWinner 不应 panic。
	s.ReleaseWinner("")
	s.Release("")
}
