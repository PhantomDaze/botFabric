package contract

import (
	"context"
	"time"
)

// RouteClassifier 是可选的路由判断器，可由规则、LLM 或外部插件实现。
type RouteClassifier interface {
	Classify(ctx context.Context, event Event, candidates []string) (DispatchPlan, error)
}

// RouteClassifierRegistrar 由运行时实现，供分类器插件注册自身。
type RouteClassifierRegistrar interface {
	RegisterRouteClassifier(id string, classifier RouteClassifier) error
	UnregisterRouteClassifier(id string) error
}

// DedupStrategy 决定跨协议端去重的键计算方式。
type DedupStrategy string

const (
	// DedupMessageID 要求协议提供稳定唯一 message_id，
	// 按 EndpointID + MessageID 判定重复。
	DedupMessageID DedupStrategy = "message_id"
	// DedupFingerprint 不依赖协议 ID，按 GroupID + UserID + 归一化文本 +
	// 时间窗口判定重复。
	DedupFingerprint DedupStrategy = "fingerprint"
	// DedupAuto 有稳定 message_id 时用之，否则退化为指纹 + 窗口。
	DedupAuto DedupStrategy = "auto"
)

// DuplicateMode 决定判定为重复事件后的处理方式。
type DuplicateMode string

const (
	// DuplicateDrop 丢弃重复事件，不再投递给应用端。
	DuplicateDrop DuplicateMode = "drop"
	// DuplicateForward 仍投递重复事件（标记重复），用于"指定机器人"
	// 早收场景：其他协议端可代收消息提前让应用端响应。
	DuplicateForward DuplicateMode = "forward"
)

// DispatchMode 决定一条事件如何投递给候选应用端。
type DispatchMode string

const (
	// DispatchBroadcast 投递给所有匹配订阅者。
	DispatchBroadcast DispatchMode = "broadcast"
	// DispatchSelective 仅投递给 RuleAction.Targets 列出的应用端。
	DispatchSelective DispatchMode = "selective"
	// DispatchDrop 丢弃该事件。
	DispatchDrop DispatchMode = "drop"
)

// ResponseMode 决定多个应用端回复的采纳方式。
type ResponseMode string

const (
	// ResponseNone 不启用响应仲裁：应用端可直接发送或自行处理。
	ResponseNone ResponseMode = "none"
	// ResponseFirstWins 第一个有效回复立即发送，其余后到的回复被丢弃。
	ResponseFirstWins ResponseMode = "first_wins"
	// ResponsePriorityOverride 在超时窗口内收集候选，取最高优先级有效回复发送。
	ResponsePriorityOverride ResponseMode = "priority_override"
)

// DedupSpec 描述跨协议端消息去重配置。
type DedupSpec struct {
	Enabled       bool          `json:"enabled" yaml:"enabled"`
	Strategy      DedupStrategy `json:"strategy" yaml:"strategy"`
	Window        time.Duration `json:"window" yaml:"-"`
	DuplicateMode DuplicateMode `json:"duplicate_mode" yaml:"duplicate_mode"`
}

// RuleMatch 描述一条路由规则的匹配条件；字段为零值表示不约束。
type RuleMatch struct {
	Topic     string            `json:"topic,omitempty" yaml:"topic,omitempty"`
	Schema    string            `json:"schema,omitempty" yaml:"schema,omitempty"`
	Protocol  string            `json:"protocol,omitempty" yaml:"protocol,omitempty"`
	Version   string            `json:"version,omitempty" yaml:"version,omitempty"`
	BotID     string            `json:"bot_id,omitempty" yaml:"bot_id,omitempty"`
	GroupID   string            `json:"group_id,omitempty" yaml:"group_id,omitempty"`
	UserID    string            `json:"user_id,omitempty" yaml:"user_id,omitempty"`
	TextRegex string            `json:"text_regex,omitempty" yaml:"text_regex,omitempty"`
	Headers   map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
}

// ResponsePolicy 描述一条路由规则的响应仲裁配置。
type ResponsePolicy struct {
	Mode             ResponseMode  `json:"mode,omitempty" yaml:"mode,omitempty"`
	Timeout          time.Duration `json:"timeout" yaml:"-"`
	RequiredReplyBot string        `json:"required_reply_bot,omitempty" yaml:"required_reply_bot,omitempty"`
	FallbackBots     []string      `json:"fallback_bots,omitempty" yaml:"fallback_bots,omitempty"`
}

// RuleAction 描述匹配规则后采取的动作。
type RuleAction struct {
	Mode       DispatchMode   `json:"mode" yaml:"mode"`
	Targets    []string       `json:"targets,omitempty" yaml:"targets,omitempty"`
	Classifier string         `json:"classifier,omitempty" yaml:"classifier,omitempty"`
	Response   ResponsePolicy `json:"response,omitempty" yaml:"response,omitempty"`
	Fallback   bool           `json:"fallback,omitempty" yaml:"fallback,omitempty"`
}

// RoutingRule 是一条路由规则。
type RoutingRule struct {
	ID       string     `json:"id" yaml:"id"`
	Priority int        `json:"priority" yaml:"priority"`
	Match    RuleMatch  `json:"match" yaml:"match"`
	Action   RuleAction `json:"action" yaml:"action"`
}

// RoutingSpec 是路由插件清单中声明的路由配置。
type RoutingSpec struct {
	Dedup DedupSpec     `json:"dedup,omitempty" yaml:"dedup,omitempty"`
	Rules []RoutingRule `json:"rules,omitempty" yaml:"rules,omitempty"`
}

// CandidateReply 是应用端提交给响应协调器的候选回复。
type CandidateReply struct {
	EventID        string `json:"event_id" yaml:"event_id"`
	ApplicationID  string `json:"application_id" yaml:"application_id"`
	GroupID        string `json:"group_id" yaml:"group_id"`
	Text           string `json:"text" yaml:"text"`
	TargetBotID    string `json:"target_bot_id,omitempty" yaml:"target_bot_id,omitempty"`
	Priority       int    `json:"priority,omitempty" yaml:"priority,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty" yaml:"idempotency_key,omitempty"`
	// DispatchSubject 是服务端内部填写的动作调用主体。
	// 它不参与序列化，客户端载荷无法伪造；为空时回退到 ApplicationID。
	DispatchSubject string `json:"-" yaml:"-"`
}

// DispatchPlan 是路由器计算出的投递计划。
type DispatchPlan struct {
	Targets   []string       `json:"targets,omitempty" yaml:"targets,omitempty"`
	Mode      DispatchMode   `json:"mode" yaml:"mode"`
	Policy    ResponsePolicy `json:"policy,omitempty" yaml:"policy,omitempty"`
	Drop      bool           `json:"drop,omitempty" yaml:"drop,omitempty"`
	Duplicate bool           `json:"duplicate,omitempty" yaml:"duplicate,omitempty"`
}
