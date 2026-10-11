package classifier

import (
	"context"
	"errors"
	"sort"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// EvaluationCase 描述一条离线路由质量样本。
type EvaluationCase struct {
	Input      string
	Candidates []string
	Expected   contract.DispatchPlan
}

// EvaluationMetrics 汇总分类结果与期望结果的比较。
type EvaluationMetrics struct {
	Total           int
	ExactMatches    int
	ExactAccuracy   float64
	ModeAccuracy    float64
	TargetPrecision float64
	TargetRecall    float64
	Failures        int
}

// EvaluationClassifier 用于将评估数据集连接到分类器或确定性 stub。
type EvaluationClassifier func(context.Context, string, []string) (contract.DispatchPlan, error)

// EvaluateClassifications 在固定数据集上运行分类器并计算准确率。
func EvaluateClassifications(cases []EvaluationCase, classify EvaluationClassifier) (EvaluationMetrics, error) {
	if classify == nil {
		return EvaluationMetrics{}, errors.New("评估分类器不能为空")
	}
	metrics := EvaluationMetrics{Total: len(cases)}
	modeMatches := 0
	truePositive, falsePositive, falseNegative := 0, 0, 0
	for _, testCase := range cases {
		plan, err := classify(context.Background(), testCase.Input, append([]string(nil), testCase.Candidates...))
		if err != nil {
			metrics.Failures++
			for range targetSet(testCase.Expected) {
				falseNegative++
			}
			continue
		}
		if plan.Mode == testCase.Expected.Mode && plan.Drop == testCase.Expected.Drop {
			modeMatches++
		}
		if plansEqual(plan, testCase.Expected) {
			metrics.ExactMatches++
		}
		actualTargets := targetSet(plan)
		expectedTargets := targetSet(testCase.Expected)
		for target := range actualTargets {
			if _, ok := expectedTargets[target]; ok {
				truePositive++
			} else {
				falsePositive++
			}
		}
		for target := range expectedTargets {
			if _, ok := actualTargets[target]; !ok {
				falseNegative++
			}
		}
	}
	if metrics.Total == 0 {
		return metrics, nil
	}
	metrics.ExactAccuracy = float64(metrics.ExactMatches) / float64(metrics.Total)
	metrics.ModeAccuracy = float64(modeMatches) / float64(metrics.Total)
	if truePositive+falsePositive > 0 {
		metrics.TargetPrecision = float64(truePositive) / float64(truePositive+falsePositive)
	}
	if truePositive+falseNegative > 0 {
		metrics.TargetRecall = float64(truePositive) / float64(truePositive+falseNegative)
	}
	return metrics, nil
}

func targetSet(plan contract.DispatchPlan) map[string]struct{} {
	if plan.Drop {
		return map[string]struct{}{}
	}
	set := make(map[string]struct{}, len(plan.Targets))
	for _, target := range plan.Targets {
		set[target] = struct{}{}
	}
	return set
}

func plansEqual(actual, expected contract.DispatchPlan) bool {
	if actual.Mode != expected.Mode || actual.Drop != expected.Drop || len(actual.Targets) != len(expected.Targets) {
		return false
	}
	actualTargets := append([]string(nil), actual.Targets...)
	expectedTargets := append([]string(nil), expected.Targets...)
	sort.Strings(actualTargets)
	sort.Strings(expectedTargets)
	for index := range actualTargets {
		if actualTargets[index] != expectedTargets[index] {
			return false
		}
	}
	return true
}
