package routing

import (
	"context"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

const (
	BroadcastEntry   = "builtin.routing.preset.broadcast"
	RegexEntry       = "builtin.routing.preset.regex"
	CompetitionEntry = "builtin.routing.preset.competition"
	RequiredBotEntry = "builtin.routing.preset.required_bot"
)

const groupTopic = "bot.message.group"

// Plugin 是路由规则声明插件；规则由 Runtime 在 Load 时注册。
type Plugin struct {
	manifest contract.Manifest
}

// New 创建指定清单的路由声明插件。
func New(manifest contract.Manifest) *Plugin { return &Plugin{manifest: manifest} }

// Manifest 返回路由规则清单。
func (p *Plugin) Manifest() contract.Manifest { return p.manifest }

// Start 不注册事件或服务；Runtime 已负责读取 RoutingSpec。
func (*Plugin) Start(context.Context, contract.Host) error { return nil }

// Stop 停止路由声明插件。
func (*Plugin) Stop(context.Context) error { return nil }

// Health 返回路由规则插件健康状态。
func (*Plugin) Health(context.Context) contract.HealthStatus {
	return contract.HealthStatus{State: contract.HealthHealthy, Message: "路由规则已由运行时装载", CheckedAt: time.Now().UTC()}
}

// BroadcastManifest 返回默认全广播预设。
func BroadcastManifest() contract.Manifest {
	return presetManifest("routing.preset.broadcast", "Broadcast Preset", BroadcastEntry, &contract.RoutingSpec{})
}

// RegexManifest 返回按文本正则选择应用端的预设。
func RegexManifest() contract.Manifest {
	return presetManifest("routing.preset.regex", "Regex Routing Preset", RegexEntry, &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:       "command-help",
		Priority: 100,
		Match:    contract.RuleMatch{Topic: groupTopic, TextRegex: `^/help(?:\s|$)`},
		Action:   contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"example.reply"}},
	}}})
}

// CompetitionManifest 返回多应用端首个有效回复获胜预设。
func CompetitionManifest() contract.Manifest {
	return presetManifest("routing.preset.competition", "Competition Preset", CompetitionEntry, &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:       "first-valid-reply",
		Priority: 100,
		Match:    contract.RuleMatch{Topic: groupTopic},
		Action: contract.RuleAction{Mode: contract.DispatchBroadcast, Response: contract.ResponsePolicy{
			Mode:    contract.ResponseFirstWins,
			Timeout: 3 * time.Second,
		}},
	}}})
}

// RequiredBotManifest 返回代收消息但强制指定 Bot 回复的预设。
func RequiredBotManifest() contract.Manifest {
	return presetManifest("routing.preset.required_bot", "Required Bot Preset", RequiredBotEntry, &contract.RoutingSpec{
		Dedup: contract.DedupSpec{Enabled: true, Strategy: contract.DedupAuto, Window: 2 * time.Second, DuplicateMode: contract.DuplicateForward},
		Rules: []contract.RoutingRule{{
			ID:       "required-bot-reply",
			Priority: 100,
			Match:    contract.RuleMatch{Topic: groupTopic},
			Action: contract.RuleAction{Mode: contract.DispatchBroadcast, Response: contract.ResponsePolicy{
				Mode:             contract.ResponseFirstWins,
				Timeout:          3 * time.Second,
				RequiredReplyBot: "bot-b",
				FallbackBots:     []string{"bot-a"},
			}},
		}},
	})
}

func presetManifest(id, name, entry string, routing *contract.RoutingSpec) contract.Manifest {
	return contract.Manifest{
		ID:      id,
		Name:    name,
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   entry,
		Limits:  contract.Limits{Timeout: 2 * time.Second},
		Routing: routing,
	}
}
