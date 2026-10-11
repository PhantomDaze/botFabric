package classifier

import (
	"context"
	"errors"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func TestEvaluateClassificationsCountsFailuresAndTargets(t *testing.T) {
	metrics, err := EvaluateClassifications([]EvaluationCase{
		{Input: "help", Candidates: []string{"help", "chat"}, Expected: contract.DispatchPlan{Mode: contract.DispatchSelective, Targets: []string{"help"}}},
		{Input: "talk", Candidates: []string{"help", "chat"}, Expected: contract.DispatchPlan{Mode: contract.DispatchBroadcast, Targets: []string{"help", "chat"}}},
	}, func(_ context.Context, input string, candidates []string) (contract.DispatchPlan, error) {
		if input == "talk" {
			return contract.DispatchPlan{}, errors.New("model request failed")
		}
		return contract.DispatchPlan{Mode: contract.DispatchSelective, Targets: []string{candidates[0]}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Total != 2 || metrics.Failures != 1 || metrics.ExactMatches != 1 || metrics.ExactAccuracy != 0.5 || metrics.TargetPrecision != 1 || metrics.TargetRecall != 1.0/3.0 {
		t.Fatalf("metrics = %#v", metrics)
	}
}
