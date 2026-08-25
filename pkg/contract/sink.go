package contract

import "context"

// EventSink 是可接收标准事件的外部应用端投递面。
// Candidates 返回可被路由规则选择的端点 ID；Deliver 的 targets 为空时表示广播到全部候选。
type EventSink interface {
	Candidates() []string
	Deliver(ctx context.Context, event Event, targets []string) error
}
