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

// MemoryStore 是带过期时间的线程安全内存去重存储。
type MemoryStore struct {
	mu      sync.Mutex
	entries map[string]time.Time
}

// NewMemoryStore 创建空内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string]time.Time)}
}

// Claim 尝试占用一个具有 TTL 的键。
func (s *MemoryStore) Claim(key string, ttl time.Duration, now time.Time) bool {
	if key == "" || ttl <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if expiresAt, exists := s.entries[key]; exists && expiresAt.After(now) {
		return false
	}
	s.entries[key] = now.Add(ttl)
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
