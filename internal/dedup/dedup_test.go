package dedup

import (
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
