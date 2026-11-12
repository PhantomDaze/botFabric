package eventbus

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/observability"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type denyPolicy struct{}

func (denyPolicy) AllowPublish(string, string) bool        { return false }
func (denyPolicy) AllowSubscribe(string, string) bool      { return false }
func (denyPolicy) AllowInvoke(string, string, string) bool { return false }
func (denyPolicy) AllowProvide(string, string) bool        { return false }

func metricsText(t *testing.T, metrics *observability.Metrics) string {
	t.Helper()
	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	return output.String()
}

func TestMetricsRecordPublishAndDelivery(t *testing.T) {
	bus := newTestBus(t, time.Second, nil)
	metrics := observability.NewMetrics()
	bus.SetMetrics(metrics)
	if _, err := bus.ForPlugin("subscriber").Subscribe("topic", func(context.Context, contract.Event) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{Topic: "topic"}); err != nil {
		t.Fatal(err)
	}
	text := metricsText(t, metrics)
	for _, expected := range []string{
		"botfabric_events_published_total 1\n",
		"botfabric_event_deliveries_total 1\n",
		"botfabric_event_delivery_failures_total 0\n",
		"botfabric_dead_letters_total 0\n",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("指标输出缺少 %q:\n%s", expected, text)
		}
	}
}

func TestMetricsRecordDeliveryFailureAsDeadLetter(t *testing.T) {
	bus := newTestBus(t, time.Second, &MemoryDeadLetterSink{})
	metrics := observability.NewMetrics()
	bus.SetMetrics(metrics)
	if _, err := bus.ForPlugin("subscriber").Subscribe("topic", func(context.Context, contract.Event) error {
		return errors.New("boom")
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{Topic: "topic"}); err == nil {
		t.Fatal("处理器失败应返回错误")
	}
	text := metricsText(t, metrics)
	for _, expected := range []string{
		"botfabric_event_deliveries_total 0\n",
		"botfabric_event_delivery_failures_total 1\n",
		"botfabric_dead_letters_total 1\n",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("指标输出缺少 %q:\n%s", expected, text)
		}
	}
}

func TestMetricsRecordDeniedPublish(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bus, err := New(time.Second, denyPolicy{}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	metrics := observability.NewMetrics()
	bus.SetMetrics(metrics)
	if err := bus.ForPlugin("publisher").Publish(context.Background(), contract.Event{Topic: "topic"}); err == nil {
		t.Fatal("未授权发布应失败")
	}
	text := metricsText(t, metrics)
	if !strings.Contains(text, "botfabric_event_publish_denied_total 1\n") {
		t.Fatalf("指标输出缺少拒绝计数:\n%s", text)
	}
	if !strings.Contains(text, "botfabric_events_published_total 0\n") {
		t.Fatalf("拒绝发布不应计入已发布事件:\n%s", text)
	}
}
