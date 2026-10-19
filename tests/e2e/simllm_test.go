package e2e_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/PhantomDaze/botfabric/plugins/classifier"
)

// simLLM 是分类器的本地 HTTP stub 服务，按所选 API 格式返回分类结果。
// 只回本地 httptest 地址与测试密钥，绝不发起真实网络请求。
type simLLM struct {
	server    *httptest.Server
	format    classifier.RealLLMFormat
	plan      contract.DispatchPlan
	calls     atomic.Int64
	requestMu sync.Mutex
	// status 非零时以该状态码响应，用于模拟服务失败。
	status int
	// rawResponse 非空时直接返回该原文，用于模拟无效响应。
	rawResponse string
	// delay 大于分类器 deadline 时用于模拟服务超时。
	delay time.Duration
	// requests 记录收到的请求体，供断言请求内容。
	requests []map[string]json.RawMessage
}

// newSimLLM 启动 stub 服务并注入分类器所需环境变量。
func newSimLLM(t *testing.T, format classifier.RealLLMFormat, plan contract.DispatchPlan) *simLLM {
	t.Helper()
	stub := &simLLM{format: format, plan: plan}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)

	// 分类器插件从环境变量读取配置；t.Setenv 在测试结束后自动恢复。
	// httptest 监听 127.0.0.1，满足 endpoint 的 loopback HTTP 校验。
	t.Setenv(classifier.RealLLMEndpointEnv, stub.server.URL)
	t.Setenv(classifier.RealLLMModelEnv, "sim-model")
	t.Setenv(classifier.RealLLMAPIKeyEnv, "sim-test-key")
	t.Setenv(classifier.RealLLMFormatEnv, string(format))
	return stub
}

// withStatus 让 stub 以固定错误状态码响应。
func (s *simLLM) withStatus(status int) *simLLM {
	s.status = status
	return s
}

// withRawResponse 让 stub 返回固定原文（用于无效响应场景）。
func (s *simLLM) withRawResponse(raw string) *simLLM {
	s.rawResponse = raw
	return s
}

// withDelay 让 stub 延迟固定时间后才响应。
func (s *simLLM) withDelay(delay time.Duration) *simLLM {
	s.delay = delay
	return s
}

// Calls 返回 stub 收到的请求次数。
func (s *simLLM) Calls() int { return int(s.calls.Load()) }

// Requests 返回收到的请求体快照。
func (s *simLLM) Requests() []map[string]json.RawMessage {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	snapshot := make([]map[string]json.RawMessage, len(s.requests))
	for i, request := range s.requests {
		clone := make(map[string]json.RawMessage, len(request))
		for key, value := range request {
			clone[key] = append(json.RawMessage(nil), value...)
		}
		snapshot[i] = clone
	}
	return snapshot
}

func (s *simLLM) handle(w http.ResponseWriter, r *http.Request) {
	s.calls.Add(1)
	body, _ := io.ReadAll(r.Body)
	var parsed map[string]json.RawMessage
	_ = json.Unmarshal(body, &parsed)
	s.requestMu.Lock()
	s.requests = append(s.requests, parsed)
	s.requestMu.Unlock()

	if s.delay > 0 {
		timer := time.NewTimer(s.delay)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
	}
	if s.status != 0 {
		w.WriteHeader(s.status)
		return
	}
	if s.rawResponse != "" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, s.rawResponse)
		return
	}
	// classification 按各 API 格式的响应外壳包装同一个分类结果。
	// targets 必须是数组：broadcast/drop 用空数组，避免被 strict 解码判为无效。
	targets := s.plan.Targets
	if targets == nil {
		targets = []string{}
	}
	classification, err := json.Marshal(map[string]any{
		"dispatch_mode": string(s.plan.Mode),
		"targets":       targets,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch s.format {
	case classifier.RealLLMFormatOpenAIChatCompletions:
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": string(classification)}}},
		})
	case classifier.RealLLMFormatAnthropicMessages:
		_ = json.NewEncoder(w).Encode(map[string]any{
			"content": []map[string]any{{"type": "text", "text": string(classification)}},
		})
	case classifier.RealLLMFormatOpenAIResponses:
		_ = json.NewEncoder(w).Encode(map[string]any{
			"output": []map[string]any{{"type": "message", "content": []map[string]any{{"type": "output_text", "text": string(classification)}}}},
		})
	case classifier.RealLLMFormatGoogleGemini:
		_ = json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{"content": map[string]any{"parts": []map[string]any{{"text": string(classification)}}}}},
		})
	default:
		_ = json.NewEncoder(w).Encode(map[string]any{})
	}
}
