// Package dedup 提供跨协议端消息去重能力。
package dedup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// Store 是去重状态的可插拔存储接口。
// 实现可以将状态放在 Redis 等共享存储中；本项目当前提供内存实现。
type Store interface {
	// Claim 在 key 未被占用或已过期时占用它并返回 true；
	// key 仍在有效期内时返回 false。
	Claim(key string, ttl time.Duration, now time.Time) bool
}

const (
	// defaultMaxEntries 是内存去重存储的默认条目上限，防止唯一键无界累积。
	defaultMaxEntries = 65536
	// sweepInterval 是惰性清理的最小间隔，避免每次 Claim 都全量遍历。
	sweepInterval = time.Second
)

// MemoryStore 是带过期时间的线程安全内存去重存储。
type MemoryStore struct {
	mu         sync.Mutex
	entries    map[string]time.Time
	maxEntries int       // 条目上限；<=0 表示不限
	lastSweep  time.Time // 上次惰性清理时间
}

// NewMemoryStore 创建空内存存储。
func NewMemoryStore() *MemoryStore {
	return NewMemoryStoreWithLimit(defaultMaxEntries)
}

// NewMemoryStoreWithLimit 创建带条目上限的内存存储；maxEntries<=0 表示不限。
func NewMemoryStoreWithLimit(maxEntries int) *MemoryStore {
	return &MemoryStore{entries: make(map[string]time.Time), maxEntries: maxEntries}
}

// Claim 尝试占用一个具有 TTL 的键。
// 为避免过期条目无界累积，Claim 会按时间节流惰性清理过期键，
// 并在条目数达到上限时淘汰最早过期的条目（可能牺牲一次去重判定，换取有界内存）。
func (s *MemoryStore) Claim(key string, ttl time.Duration, now time.Time) bool {
	if key == "" || ttl <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked(now)
	if expiresAt, exists := s.entries[key]; exists && expiresAt.After(now) {
		return false
	}
	for s.maxEntries > 0 && len(s.entries) >= s.maxEntries {
		if !s.evictEarliestLocked() {
			break
		}
	}
	s.entries[key] = now.Add(ttl)
	return true
}

// sweepLocked 按 sweepInterval 节流删除过期条目。调用方须持有 s.mu。
func (s *MemoryStore) sweepLocked(now time.Time) {
	if !s.lastSweep.IsZero() && now.Sub(s.lastSweep) < sweepInterval {
		return
	}
	s.lastSweep = now
	for key, expiresAt := range s.entries {
		if !expiresAt.After(now) {
			delete(s.entries, key)
		}
	}
}

// evictEarliestLocked 删除最早过期的条目；存储为空时返回 false。调用方须持有 s.mu。
func (s *MemoryStore) evictEarliestLocked() bool {
	var (
		oldestKey string
		oldestAt  time.Time
		found     bool
	)
	for key, expiresAt := range s.entries {
		if !found || expiresAt.Before(oldestAt) {
			oldestKey = key
			oldestAt = expiresAt
			found = true
		}
	}
	if !found {
		return false
	}
	delete(s.entries, oldestKey)
	return true
}

// Cleanup 删除已过期条目并返回删除数量。
func (s *MemoryStore) Cleanup(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key, expiresAt := range s.entries {
		if !expiresAt.After(now) {
			delete(s.entries, key)
			removed++
		}
	}
	return removed
}

// Len 返回当前存储条目数量。
func (s *MemoryStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// Deduplicator 根据配置判断事件是否首次出现。
type Deduplicator struct {
	store Store
	spec  contract.DedupSpec
	now   func() time.Time
}

// New 创建去重器。
func New(store Store, spec contract.DedupSpec) (*Deduplicator, error) {
	if store == nil {
		return nil, errors.New("去重存储不能为空")
	}
	if spec.Window <= 0 {
		return nil, errors.New("去重窗口必须大于零")
	}
	if spec.Strategy != contract.DedupMessageID && spec.Strategy != contract.DedupFingerprint && spec.Strategy != contract.DedupAuto {
		return nil, fmt.Errorf("不支持的去重策略: %s", spec.Strategy)
	}
	if spec.DuplicateMode != contract.DuplicateDrop && spec.DuplicateMode != contract.DuplicateForward {
		return nil, fmt.Errorf("不支持的重复处理模式: %s", spec.DuplicateMode)
	}
	return &Deduplicator{store: store, spec: spec, now: time.Now}, nil
}

// Spec 返回去重配置副本。
func (d *Deduplicator) Spec() contract.DedupSpec { return d.spec }

// Check 判定事件是否为首次出现。
// primary 为 true 表示本次事件抢到主事件资格；duplicate 为 true 表示命中既有事件。
func (d *Deduplicator) Check(event contract.Event) (primary bool, duplicate bool, key string, err error) {
	key, err = d.Key(event)
	if err != nil {
		return false, false, "", err
	}
	if key == "" {
		return true, false, "", nil
	}
	primary = d.store.Claim(key, d.spec.Window, d.now())
	return primary, !primary, key, nil
}

// Key 返回事件的稳定去重键；不满足当前策略所需字段时返回空键。
func (d *Deduplicator) Key(event contract.Event) (string, error) {
	var message contract.GroupMessage
	if len(event.Payload) > 0 {
		if err := json.Unmarshal(event.Payload, &message); err != nil {
			return "", fmt.Errorf("解析群消息载荷用于去重: %w", err)
		}
	}
	strategy := d.spec.Strategy
	if strategy == contract.DedupAuto {
		if message.MessageID != "" {
			strategy = contract.DedupMessageID
		} else {
			strategy = contract.DedupFingerprint
		}
	}
	switch strategy {
	case contract.DedupMessageID:
		if message.MessageID == "" {
			return "", nil
		}
		// message_id 优先跨协议端共享；EndpointID 仅作为无协议来源时的隔离。
		return "message:" + message.MessageID, nil
	case contract.DedupFingerprint:
		if message.GroupID == "" || message.UserID == "" || message.Text == "" {
			return "", nil
		}
		text := strings.Join(strings.Fields(message.Text), " ")
		raw := event.Topic + "\x00" + message.GroupID + "\x00" + message.UserID + "\x00" + text
		sum := sha256.Sum256([]byte(raw))
		return "fingerprint:" + hex.EncodeToString(sum[:]), nil
	default:
		return "", fmt.Errorf("不支持的去重策略: %s", strategy)
	}
}

// ShouldDrop 返回重复事件是否应被丢弃。
func (d *Deduplicator) ShouldDrop(duplicate bool) bool {
	return duplicate && d.spec.DuplicateMode == contract.DuplicateDrop
}
