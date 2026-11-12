package response

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/observability"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func TestMetricsRecordResponseArbitration(t *testing.T) {
	coordinator, _ := newTestCoordinator(t)
	metrics := observability.NewMetrics()
	coordinator.SetMetrics(metrics)
	policy := contract.ResponsePolicy{Mode: contract.ResponseFirstWins, Timeout: time.Second}
	coordinator.RegisterEvent(testEvent(policy), policy)

	first, err := coordinator.Submit(context.Background(), contract.CandidateReply{EventID: "event-1", ApplicationID: "app-a", GroupID: "1", Text: "first"})
	if err != nil || first.Status != "accepted" {
		t.Fatalf("首个回复 = %#v, err=%v", first, err)
	}
	second, err := coordinator.Submit(context.Background(), contract.CandidateReply{EventID: "event-1", ApplicationID: "app-b", GroupID: "1", Text: "second"})
	if err != nil || second.Status != "discarded" {
		t.Fatalf("迟到回复 = %#v, err=%v", second, err)
	}

	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, expected := range []string{
		"botfabric_response_candidates_total 2\n",
		"botfabric_response_accepted_total 1\n",
		"botfabric_response_discarded_total 1\n",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("指标输出缺少 %q:\n%s", expected, text)
		}
	}
}
