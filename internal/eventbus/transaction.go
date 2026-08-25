package eventbus

import (
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// SubscriptionSpec 是可暂存和切换的事件订阅资源。
type SubscriptionSpec struct {
	Topic    string
	Priority int
	Timeout  time.Duration
	Handler  contract.EventHandler
}

// SnapshotOwner 返回指定插件当前订阅的副本。
func (b *Bus) SnapshotOwner(owner string) []SubscriptionSpec {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var result []SubscriptionSpec
	for _, subscriptions := range b.subscriptions {
		for _, subscription := range subscriptions {
			if subscription.owner == owner {
				result = append(result, SubscriptionSpec{Topic: subscription.topic, Priority: subscription.priority, Timeout: subscription.timeout, Handler: subscription.handler})
			}
		}
	}
	return result
}

// ReplaceOwner 原子替换指定插件的全部订阅资源。
func (b *Bus) ReplaceOwner(owner string, specs []SubscriptionSpec) ([]contract.Subscription, error) {
	for _, spec := range specs {
		if spec.Topic == "" || spec.Handler == nil || spec.Timeout <= 0 {
			return nil, &contract.ServiceError{Code: contract.ErrorInvalidArgument, Message: "暂存订阅资源无效"}
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for topic, subscriptions := range b.subscriptions {
		filtered := subscriptions[:0]
		for _, subscription := range subscriptions {
			if subscription.owner != owner {
				filtered = append(filtered, subscription)
			}
		}
		if len(filtered) == 0 {
			delete(b.subscriptions, topic)
		} else {
			b.subscriptions[topic] = filtered
		}
	}
	handles := make([]contract.Subscription, 0, len(specs))
	for _, spec := range specs {
		candidate := subscription{id: b.nextID.Add(1), owner: owner, topic: spec.Topic, priority: spec.Priority, timeout: spec.Timeout, handler: spec.Handler}
		b.subscriptions[spec.Topic] = append(b.subscriptions[spec.Topic], candidate)
		handles = append(handles, &subscriptionHandle{bus: b, topic: spec.Topic, id: candidate.id})
	}
	for topic := range b.subscriptions {
		stable := b.subscriptions[topic]
		sortSubscriptions(stable)
		b.subscriptions[topic] = stable
	}
	return handles, nil
}

func sortSubscriptions(subscriptions []subscription) {
	for index := 1; index < len(subscriptions); index++ {
		current := subscriptions[index]
		position := index
		for position > 0 && subscriptions[position-1].priority < current.priority {
			subscriptions[position] = subscriptions[position-1]
			position--
		}
		subscriptions[position] = current
	}
}
