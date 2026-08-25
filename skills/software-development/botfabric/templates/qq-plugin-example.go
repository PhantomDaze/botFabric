//go:build ignore

package main

import (
	"context"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// QQPlugin 是同进程协议插件的最小模板。
type QQPlugin struct {
	subscription contract.Subscription
}

func (p *QQPlugin) Manifest() contract.Manifest {
	return contract.Manifest{
		ID:      "protocol.example",
		Name:    "Example QQ Protocol",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   "builtin.protocol.example",
		Provides: contract.Provides{
			Subscriptions: []string{"bot.message.group"},
		},
		Limits: contract.Limits{Timeout: 2 * time.Second},
	}
}

func (p *QQPlugin) Start(_ context.Context, host contract.Host) error {
	subscription, err := host.EventBus().Subscribe("bot.message.group", p.handleMessage)
	if err != nil {
		return err
	}
	p.subscription = subscription
	return nil
}

func (p *QQPlugin) Stop(context.Context) error {
	if p.subscription == nil {
		return nil
	}
	return p.subscription.Unsubscribe()
}

func (p *QQPlugin) Health(context.Context) contract.HealthStatus {
	return contract.HealthStatus{
		State:     contract.HealthHealthy,
		Message:   "示例插件运行中",
		CheckedAt: time.Now().UTC(),
	}
}

func (p *QQPlugin) handleMessage(context.Context, contract.Event) error {
	return nil
}

func main() {}
