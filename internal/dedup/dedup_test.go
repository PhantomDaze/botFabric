package dedup

import (
	"fmt"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func groupEvent(message contract.GroupMessage) contract.Event {
	payload, _ := contract.EncodeJSON(message)
	return contract.Event{Topic: "bot.message.group", Payload: payload}
}

func TestMemoryStoreClaimAndExpiry(t *testing.T) {
	store := NewMemoryStore()
	now := time.Unix(100, 0)
	if !store.Claim("key", time.Second, now) {
		t.Fatal("首次 Claim 应成功")
	}
	if store.Claim("key", time.Second, now.Add(500*time.Millisecond)) {
		t.Fatal("TTL 内重复 Claim 应失败")
	}
	if !store.Claim("key", time.Second, now.Add(time.Second)) {
		t.Fatal("过期后 Claim 应成功")
	}
	if removed := store.Cleanup(now.Add(1500 * time.Millisecond)); removed != 0 {
		t.Fatalf("Cleanup 不应删除新条目，删除 %d", removed)
	}
	if store.Len() != 1 {
		t.Fatalf("存储数量 = %d", store.Len())
	}
}

func TestMessageIDStrategy(t *testing.T) {
	dedup, err := New(NewMemoryStore(), contract.DedupSpec{
		Enabled:       true,
		Strategy:      contract.DedupMessageID,
		Window:        time.Second,
		DuplicateMode: contract.DuplicateDrop,
	})
	if err != nil {
		t.Fatal(err)
	}
	first := groupEvent(contract.GroupMessage{MessageID: "same", GroupID: "1", UserID: "2", Text: "hello"})
	second := groupEvent(contract.GroupMessage{MessageID: "same", GroupID: "99", UserID: "8", Text: "different"})
	primary, duplicate, key, err := dedup.Check(first)
	if err != nil || !primary || duplicate || key == "" {
		t.Fatalf("首次事件 = %v %v %q %v", primary, duplicate, key, err)
	}
	primary, duplicate, _, err = dedup.Check(second)
	if err != nil || primary || !duplicate {
		t.Fatalf("重复事件 = %v %v %v", primary, duplicate, err)
	}
	if !dedup.ShouldDrop(true) || dedup.ShouldDrop(false) {
		t.Fatal("drop 模式判断错误")
	}
}

func TestFingerprintAndAutoStrategy(t *testing.T) {
	for _, strategy := range []contract.DedupStrategy{contract.DedupFingerprint, contract.DedupAuto} {
		dedup, err := New(NewMemoryStore(), contract.DedupSpec{
			Enabled:       true,
			Strategy:      strategy,
			Window:        time.Second,
			DuplicateMode: contract.DuplicateForward,
		})
		if err != nil {
			t.Fatal(err)
		}
		first := groupEvent(contract.GroupMessage{GroupID: "1", UserID: "2", Text: "  hello   world "})
		second := groupEvent(contract.GroupMessage{GroupID: "1", UserID: "2", Text: "hello world"})
		primary, _, _, err := dedup.Check(first)
		if err != nil || !primary {
			t.Fatalf("strategy %s 首次事件错误: %v", strategy, err)
		}
		primary, duplicate, _, err := dedup.Check(second)
		if err != nil || primary || !duplicate {
			t.Fatalf("strategy %s 指纹重复错误: %v %v %v", strategy, primary, duplicate, err)
		}
		if dedup.ShouldDrop(true) {
			t.Fatalf("strategy %s forward 模式不应丢弃", strategy)
		}
	}
}

func TestMessageIDStrategyWithoutIDDoesNotGuess(t *testing.T) {
	dedup, err := New(NewMemoryStore(), contract.DedupSpec{
		Enabled:       true,
		Strategy:      contract.DedupMessageID,
		Window:        time.Second,
		DuplicateMode: contract.DuplicateDrop,
	})
	if err != nil {
		t.Fatal(err)
	}
	event := groupEvent(contract.GroupMessage{GroupID: "1", UserID: "2", Text: "hello"})
	primary, duplicate, key, err := dedup.Check(event)
	if err != nil || !primary || duplicate || key != "" {
		t.Fatalf("无 ID 不应猜测指纹: %v %v %q %v", primary, duplicate, key, err)
	}
}

// hasEntry 在持锁状态下检查条目是否存在（测试辅助）。
func hasEntry(store *MemoryStore, key string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, ok := store.entries[key]
	return ok
}

// TestMemoryStoreBoundsEntries 验证唯一键持续涌入时条目数不会超过上限，
// 且容量满后按最早过期时间淘汰旧条目。
func TestMemoryStoreBoundsEntries(t *testing.T) {
	const limit = 8
	store := NewMemoryStoreWithLimit(limit)
	now := time.Unix(100, 0)
	for i := 0; i < 100; i++ {
		if !store.Claim(fmt.Sprintf("key-%d", i), time.Hour, now) {
			t.Fatalf("唯一键 %d Claim 应成功", i)
		}
		if store.Len() > limit {
			t.Fatalf("条目数 %d 超过上限 %d", store.Len(), limit)
		}
	}
	if store.Len() != limit {
		t.Fatalf("条目数 = %d，期望上限 %d", store.Len(), limit)
	}
	if hasEntry(store, "key-0") {
		t.Fatal("最早过期的条目应被淘汰")
	}
	if !hasEntry(store, "key-99") {
		t.Fatal("最新条目应保留")
	}
}

// TestMemoryStoreLazySweepRemovesExpired 验证 Claim 会按节流惰性清理过期条目，
// 使唯一键在稳态运行下不会无界累积。
func TestMemoryStoreLazySweepRemovesExpired(t *testing.T) {
	store := NewMemoryStore()
	now := time.Unix(200, 0)
	for i := 0; i < 50; i++ {
		if !store.Claim(fmt.Sprintf("ttl-%d", i), time.Second, now) {
			t.Fatalf("键 %d Claim 应成功", i)
		}
	}
	if store.Len() != 50 {
		t.Fatalf("条目数 = %d，期望 50", store.Len())
	}
	// 推进超过 TTL 与清理节流间隔后，新的 Claim 应触发惰性清理。
	later := now.Add(3 * time.Second)
	if !store.Claim("fresh", time.Second, later) {
		t.Fatal("清理后的新键 Claim 应成功")
	}
	if store.Len() != 1 {
		t.Fatalf("惰性清理后条目数 = %d，期望 1", store.Len())
	}
	if hasEntry(store, "ttl-0") {
		t.Fatal("过期条目应被惰性清理删除")
	}
	if !hasEntry(store, "fresh") {
		t.Fatal("新条目应保留")
	}
}
