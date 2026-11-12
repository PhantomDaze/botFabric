package observability

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMetricsWritesPrometheusText(t *testing.T) {
	metrics := NewMetrics()
	metrics.EventPublished()
	metrics.EventPublished()
	metrics.PublishDenied()
	metrics.DuplicateDropped()
	metrics.DuplicateForwarded()
	metrics.RouteDropped()
	metrics.DeliveryCompleted()
	metrics.DeliveryFailed()
	metrics.DeadLetter()
	metrics.PlanBroadcast()
	metrics.PlanSelective()
	metrics.PlanDropped()
	metrics.ClassifierFallback()
	metrics.ResponseCandidate()
	metrics.ResponseAccepted()
	metrics.ResponseDiscarded()
	metrics.PluginStartFailure()

	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, expected := range []string{
		"# TYPE botfabric_events_published_total counter\nbotfabric_events_published_total 2\n",
		"botfabric_event_publish_denied_total 1\n",
		`botfabric_duplicate_events_total{action="drop"} 1`,
		`botfabric_duplicate_events_total{action="forward"} 1`,
		"botfabric_route_dropped_events_total 1\n",
		"botfabric_event_deliveries_total 1\n",
		"botfabric_event_delivery_failures_total 1\n",
		"botfabric_dead_letters_total 1\n",
		`botfabric_dispatch_plans_total{mode="broadcast"} 1`,
		`botfabric_dispatch_plans_total{mode="selective"} 1`,
		`botfabric_dispatch_plans_total{mode="drop"} 1`,
		"botfabric_classifier_fallbacks_total 1\n",
		"botfabric_response_candidates_total 1\n",
		"botfabric_response_accepted_total 1\n",
		"botfabric_response_discarded_total 1\n",
		"botfabric_plugin_start_failures_total 1\n",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("指标输出缺少 %q:\n%s", expected, text)
		}
	}
}

func TestMetricsHandlerServesGetOnly(t *testing.T) {
	metrics := NewMetrics()
	metrics.EventPublished()
	handler := metrics.Handler()

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/plain; version=0.0.4") {
		t.Fatalf("Content-Type = %q", got)
	}
	if !strings.Contains(recorder.Body.String(), "botfabric_events_published_total 1") {
		t.Fatalf("响应缺少指标: %s", recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST 状态码 = %d，期望 405", recorder.Code)
	}
}

func TestMetricsServerExposesEndpoint(t *testing.T) {
	metrics := NewMetrics()
	metrics.DeliveryCompleted()
	server, err := NewMetricsServer("127.0.0.1:0", metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	response, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "botfabric_event_deliveries_total 1") {
		t.Fatalf("响应缺少指标: %s", body)
	}

	notFound, err := http.Get("http://" + server.Addr() + "/unknown")
	if err != nil {
		t.Fatal(err)
	}
	defer notFound.Body.Close()
	if notFound.StatusCode != http.StatusNotFound {
		t.Fatalf("未知路径状态码 = %d", notFound.StatusCode)
	}
}

func TestMetricsConcurrentUpdates(t *testing.T) {
	metrics := NewMetrics()
	var wait sync.WaitGroup
	for index := 0; index < 8; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for count := 0; count < 100; count++ {
				metrics.EventPublished()
				metrics.PlanBroadcast()
			}
		}()
	}
	wait.Wait()
	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "botfabric_events_published_total 800") {
		t.Fatalf("并发计数不正确:\n%s", output.String())
	}
}
