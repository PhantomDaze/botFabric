package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/PhantomDaze/botfabric/plugins/classifier"
)

const maxEvaluationCases = 500

type evaluationFileCase struct {
	Input      string                `json:"input"`
	Candidates []string              `json:"candidates"`
	Expected   contract.DispatchPlan `json:"expected"`
}

func main() {
	input := flag.String("input", "", "JSONL 评估集路径")
	confirm := flag.Bool("confirm-live-requests", false, "确认会向配置的模型服务逐条发送评估样本并产生费用")
	flag.Parse()
	if *input == "" || !*confirm {
		fmt.Fprintln(os.Stderr, "用法: classifier-eval --input <dataset.jsonl> --confirm-live-requests")
		os.Exit(2)
	}
	if err := run(*input); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(path string) error {
	config, err := classifier.RealLLMConfigFromEnv(os.Getenv)
	if err != nil {
		return err
	}
	llm, err := classifier.NewRealLLMClassifier(config)
	if err != nil {
		return err
	}
	cases, err := readDataset(path)
	if err != nil {
		return err
	}
	if len(cases) == 0 {
		return errors.New("评估集为空")
	}
	metrics, err := classifier.EvaluateClassifications(cases, func(ctx context.Context, input string, candidates []string) (contract.DispatchPlan, error) {
		payload, err := contract.EncodeJSON(contract.GroupMessage{Text: input})
		if err != nil {
			return contract.DispatchPlan{}, err
		}
		return llm.Classify(ctx, contract.Event{
			Topic: "bot.message.group", Schema: "bot.message@v1", Payload: payload,
		}, candidates)
	})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(metrics)
}

func readDataset(path string) ([]classifier.EvaluationCase, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开评估集: %w", err)
	}
	defer file.Close()

	cases := make([]classifier.EvaluationCase, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 64*1024)
	for line := 1; scanner.Scan(); line++ {
		if len(cases) == maxEvaluationCases {
			return nil, fmt.Errorf("评估集超过 %d 条样本限制", maxEvaluationCases)
		}
		var item evaluationFileCase
		decoder := json.NewDecoder(strings.NewReader(string(scanner.Bytes())))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&item); err != nil {
			return nil, fmt.Errorf("评估集第 %d 行无效: %w", line, err)
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, fmt.Errorf("评估集第 %d 行包含多余数据", line)
		}
		if item.Input == "" || len(item.Candidates) == 0 {
			return nil, fmt.Errorf("评估集第 %d 行必须包含 input 和 candidates", line)
		}
		if item.Expected.Mode != contract.DispatchBroadcast && item.Expected.Mode != contract.DispatchSelective && item.Expected.Mode != contract.DispatchDrop {
			return nil, fmt.Errorf("评估集第 %d 行 expected.mode 无效", line)
		}
		if item.Expected.Mode == contract.DispatchDrop {
			item.Expected.Drop = true
		}
		if item.Expected.Mode == contract.DispatchBroadcast && len(item.Expected.Targets) == 0 {
			item.Expected.Targets = append([]string(nil), item.Candidates...)
		}
		if item.Expected.Mode == contract.DispatchSelective && len(item.Expected.Targets) == 0 {
			return nil, fmt.Errorf("评估集第 %d 行 selective 期望计划必须包含 targets", line)
		}
		cases = append(cases, classifier.EvaluationCase{
			Input: item.Input, Candidates: item.Candidates, Expected: item.Expected,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取评估集: %w", err)
	}
	return cases, nil
}
