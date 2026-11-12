package observability

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
)

// Metrics 汇总运行时关键计数。所有字段使用原子操作更新，
// 可在事件投递、路由与响应仲裁等并发路径上安全使用。
type Metrics struct {
	eventsPublished     atomic.Uint64
	publishDenied       atomic.Uint64
	duplicatesDropped   atomic.Uint64
	duplicatesForwarded atomic.Uint64
	routeDropped        atomic.Uint64
	deliveries          atomic.Uint64
	deliveryFailures    atomic.Uint64
	deadLetters         atomic.Uint64
	planBroadcast       atomic.Uint64
	planSelective       atomic.Uint64
	planDrop            atomic.Uint64
	classifierFallbacks atomic.Uint64
	responseCandidates  atomic.Uint64
	responseAccepted    atomic.Uint64
	responseDiscarded   atomic.Uint64
	pluginStartFailures atomic.Uint64
}

// NewMetrics 创建一组零值计数器。
func NewMetrics() *Metrics {
	return &Metrics{}
}

// EventPublished 记录一次通过权限校验、进入投递流程的事件。
func (m *Metrics) EventPublished() { m.eventsPublished.Add(1) }

// PublishDenied 记录一次被权限策略拒绝的事件发布。
func (m *Metrics) PublishDenied() { m.publishDenied.Add(1) }

// DuplicateDropped 记录一次被去重器丢弃的重复事件。
func (m *Metrics) DuplicateDropped() { m.duplicatesDropped.Add(1) }

// DuplicateForwarded 记录一次标记为重复但仍继续投递的事件。
func (m *Metrics) DuplicateForwarded() { m.duplicatesForwarded.Add(1) }

// RouteDropped 记录一次被路由规则丢弃的事件。
func (m *Metrics) RouteDropped() { m.routeDropped.Add(1) }

// DeliveryCompleted 记录一次成功完成的订阅者投递。
func (m *Metrics) DeliveryCompleted() { m.deliveries.Add(1) }

// DeliveryFailed 记录一次失败的订阅者投递。
func (m *Metrics) DeliveryFailed() { m.deliveryFailures.Add(1) }

// DeadLetter 记录一条进入死信存储的投递失败。
func (m *Metrics) DeadLetter() { m.deadLetters.Add(1) }

// PlanBroadcast 记录一次广播投递计划。
func (m *Metrics) PlanBroadcast() { m.planBroadcast.Add(1) }

// PlanSelective 记录一次选择性投递计划。
func (m *Metrics) PlanSelective() { m.planSelective.Add(1) }

// PlanDropped 记录一次丢弃投递计划。
func (m *Metrics) PlanDropped() { m.planDrop.Add(1) }

// ClassifierFallback 记录一次因分类器缺失或调用失败而回退的计划。
func (m *Metrics) ClassifierFallback() { m.classifierFallbacks.Add(1) }

// ResponseCandidate 记录一个通过基本校验的候选回复。
func (m *Metrics) ResponseCandidate() { m.responseCandidates.Add(1) }

// ResponseAccepted 记录一个成功发送的候选回复。
func (m *Metrics) ResponseAccepted() { m.responseAccepted.Add(1) }

// ResponseDiscarded 记录一个被竞争仲裁丢弃的候选回复。
func (m *Metrics) ResponseDiscarded() { m.responseDiscarded.Add(1) }

// PluginStartFailure 记录一次插件启动失败。
func (m *Metrics) PluginStartFailure() { m.pluginStartFailures.Add(1) }

// Handler 返回暴露 Prometheus 文本格式的只读 HTTP 处理器。
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			writer.Header().Set("Allow", "GET, HEAD")
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = m.WritePrometheus(writer)
	})
}

// WritePrometheus 以 Prometheus 文本格式输出全部指标。
func (m *Metrics) WritePrometheus(writer io.Writer) error {
	counters := []struct {
		name  string
		help  string
		value uint64
	}{
		{"botfabric_events_published_total", "Events accepted for dispatch after authorization.", m.eventsPublished.Load()},
		{"botfabric_event_publish_denied_total", "Event publishes rejected by the permission policy.", m.publishDenied.Load()},
		{"botfabric_route_dropped_events_total", "Events dropped by routing rules.", m.routeDropped.Load()},
		{"botfabric_event_deliveries_total", "Successful event deliveries to subscribers.", m.deliveries.Load()},
		{"botfabric_event_delivery_failures_total", "Failed event deliveries to subscribers.", m.deliveryFailures.Load()},
		{"botfabric_dead_letters_total", "Delivery failures recorded in the dead-letter store.", m.deadLetters.Load()},
		{"botfabric_classifier_fallbacks_total", "Routing plans that fell back because a classifier was missing or failed.", m.classifierFallbacks.Load()},
		{"botfabric_response_candidates_total", "Candidate replies submitted by applications.", m.responseCandidates.Load()},
		{"botfabric_response_accepted_total", "Candidate replies accepted and sent.", m.responseAccepted.Load()},
		{"botfabric_response_discarded_total", "Candidate replies discarded by response arbitration.", m.responseDiscarded.Load()},
		{"botfabric_plugin_start_failures_total", "Plugin starts rejected by plugin initialization or registration validation.", m.pluginStartFailures.Load()},
	}
	for _, counter := range counters {
		if err := writeCounter(writer, counter.name, counter.help, counter.value); err != nil {
			return err
		}
	}

	duplicates := []struct {
		action string
		value  uint64
	}{
		{"drop", m.duplicatesDropped.Load()},
		{"forward", m.duplicatesForwarded.Load()},
	}
	if err := writeCounterHeader(writer, "botfabric_duplicate_events_total", "Duplicate events observed by the deduplicator."); err != nil {
		return err
	}
	for _, duplicate := range duplicates {
		if _, err := fmt.Fprintf(writer, "botfabric_duplicate_events_total{action=%q} %d\n", duplicate.action, duplicate.value); err != nil {
			return err
		}
	}

	plans := []struct {
		mode  string
		value uint64
	}{
		{"broadcast", m.planBroadcast.Load()},
		{"selective", m.planSelective.Load()},
		{"drop", m.planDrop.Load()},
	}
	if err := writeCounterHeader(writer, "botfabric_dispatch_plans_total", "Dispatch plans produced by the router."); err != nil {
		return err
	}
	for _, plan := range plans {
		if _, err := fmt.Fprintf(writer, "botfabric_dispatch_plans_total{mode=%q} %d\n", plan.mode, plan.value); err != nil {
			return err
		}
	}
	return nil
}

func writeCounter(writer io.Writer, name, help string, value uint64) error {
	if err := writeCounterHeader(writer, name, help); err != nil {
		return err
	}
	_, err := fmt.Fprintf(writer, "%s %d\n", name, value)
	return err
}

func writeCounterHeader(writer io.Writer, name, help string) error {
	if _, err := fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s counter\n", name, help, name); err != nil {
		return err
	}
	return nil
}
