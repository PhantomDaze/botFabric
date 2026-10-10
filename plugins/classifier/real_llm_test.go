package classifier

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PhantomDaze/botfabric/internal/routing"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func newTestClassifier(t *testing.T, handler http.HandlerFunc) (*RealLLMClassifier, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	classifier, err := NewRealLLMClassifier(RealLLMConfig{
		Endpoint: server.URL,
		Model:    "test-model",
		APIKey:   "secret-test-key",
	})
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return classifier, server
}

func eventForClassification(t *testing.T, text string) contract.Event {
	t.Helper()
	payload, err := contract.EncodeJSON(contract.GroupMessage{Text: text})
	if err != nil {
		t.Fatal(err)
	}
	return contract.Event{Topic: "bot.message.group", Schema: "bot.message@v1", Payload: payload}
}

func TestRealLLMClassifySendsLimitedInputAndValidatesTargets(t *testing.T) {
	classifier, server := newTestClassifier(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-test-key" {
			t.Errorf("Authorization = %q", got)
		}
		var request chatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("解码请求: %v", err)
		}
		if request.Model != "test-model" || request.ResponseFormat.Type != "json_object" {
			t.Errorf("请求配置 = %#v", request)
		}
		if len(request.Messages) != 2 || !strings.Contains(request.Messages[1].Content, `"topic":"bot.message.group"`) || !strings.Contains(request.Messages[1].Content, `"text":"hello"`) {
			t.Errorf("请求没有包含预期的最小事件摘要: %#v", request.Messages)
		}
		if strings.Contains(request.Messages[1].Content, "private-header") || strings.Contains(request.Messages[1].Content, "private-group") {
			t.Errorf("请求泄漏无关事件元数据: %s", request.Messages[1].Content)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-test","choices":[{"message":{"content":"{\"dispatch_mode\":\"selective\",\"targets\":[\"app-b\"]}"}}],"usage":{"total_tokens":12}}`)
	})
	defer server.Close()

	event := eventForClassification(t, "hello")
	event.Headers = map[string]string{"secret": "private-header"}
	plan, err := classifier.Classify(context.Background(), event, []string{"app-a", "app-b"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != contract.DispatchSelective || len(plan.Targets) != 1 || plan.Targets[0] != "app-b" {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestRealLLMRejectsInvalidPlans(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{name: "unknown mode", content: `{"dispatch_mode":"invoke","targets":["app-a"]}`},
		{name: "unknown target", content: `{"dispatch_mode":"selective","targets":["outside"]}`},
		{name: "duplicate target", content: `{"dispatch_mode":"selective","targets":["app-a","app-a"]}`},
		{name: "empty selective", content: `{"dispatch_mode":"selective","targets":[]}`},
		{name: "targets on drop", content: `{"dispatch_mode":"drop","targets":["app-a"]}`},
		{name: "unknown field", content: `{"dispatch_mode":"broadcast","targets":[],"response_policy":{"mode":"first_wins"}}`},
		{name: "duplicate field", content: `{"dispatch_mode":"drop","dispatch_mode":"broadcast","targets":[]}`},
		{name: "missing targets", content: `{"dispatch_mode":"broadcast"}`},
		{name: "null targets", content: `{"dispatch_mode":"drop","targets":null}`},
		{name: "non-array targets", content: `{"dispatch_mode":"broadcast","targets":"app-a"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			classifier, server := newTestClassifier(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, `{"choices":[{"message":{"content":`+mustJSON(t, test.content)+`}}]}`)
			})
			defer server.Close()
			_, err := classifier.Classify(context.Background(), eventForClassification(t, "hello"), []string{"app-a"})
			if !errors.Is(err, ErrInvalidClassification) {
				t.Fatalf("err = %v, want ErrInvalidClassification", err)
			}
		})
	}
}

func TestRealLLMBuildsBroadcastAndDropPlans(t *testing.T) {
	for _, test := range []struct {
		mode string
		drop bool
	}{
		{mode: "broadcast"},
		{mode: "drop", drop: true},
	} {
		t.Run(test.mode, func(t *testing.T) {
			classifier, server := newTestClassifier(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"{\"dispatch_mode\":\"`+test.mode+`\",\"targets\":[]}"}}]}`)
			})
			defer server.Close()
			plan, err := classifier.Classify(context.Background(), eventForClassification(t, "hello"), []string{"app-a", "app-b"})
			if err != nil {
				t.Fatal(err)
			}
			if plan.Drop != test.drop || plan.Mode != contract.DispatchMode(test.mode) {
				t.Fatalf("plan = %#v", plan)
			}
			if !test.drop && (len(plan.Targets) != 2 || plan.Targets[0] != "app-a" || plan.Targets[1] != "app-b") {
				t.Fatalf("broadcast targets = %#v", plan.Targets)
			}
		})
	}
}

func TestRealLLMContextCancellationAndHTTPFailure(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		classifier, server := newTestClassifier(t, func(_ http.ResponseWriter, r *http.Request) {
			close(entered)
			select {
			case <-r.Context().Done():
			case <-release:
			}
		})
		defer func() {
			close(release)
			server.Close()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := classifier.Classify(ctx, eventForClassification(t, "hello"), []string{"app-a"})
		if err == nil {
			t.Fatal("expected context cancellation")
		}
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("server did not receive request")
		}
	})

	t.Run("non-success status", func(t *testing.T) {
		classifier, server := newTestClassifier(t, func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "provider failure", http.StatusServiceUnavailable)
		})
		defer server.Close()
		_, err := classifier.Classify(context.Background(), eventForClassification(t, "hello"), []string{"app-a"})
		if err == nil || strings.Contains(err.Error(), "secret-test-key") || strings.Contains(err.Error(), "provider failure") {
			t.Fatalf("非敏感 HTTP 错误预期，实际: %v", err)
		}
	})
}

func TestRealLLMRejectsOversizedInputAndCandidates(t *testing.T) {
	classifier, server := newTestClassifier(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Errorf("server should not receive oversized input")
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer server.Close()
	if _, err := classifier.Classify(context.Background(), contract.Event{Payload: make([]byte, maxLLMRequestBytes+1)}, []string{"app-a"}); err == nil {
		t.Fatal("expected oversized payload error")
	}
	if _, err := classifier.Classify(context.Background(), eventForClassification(t, "hello"), make([]string, maxLLMCandidates+1)); err == nil {
		t.Fatal("expected oversized candidates error")
	}
}

func TestRealLLMRejectsOversizedResponse(t *testing.T) {
	classifier, server := newTestClassifier(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"choices":[]}`+strings.Repeat(" ", maxLLMReplyBytes))
	})
	defer server.Close()
	_, err := classifier.Classify(context.Background(), eventForClassification(t, "hello"), []string{"app-a"})
	if err == nil || !strings.Contains(err.Error(), "超过大小限制") {
		t.Fatalf("err = %v, want oversized response error", err)
	}
}

func TestRealLLMFailureUsesRouterBroadcastFallback(t *testing.T) {
	classifier, server := newTestClassifier(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "provider unavailable", http.StatusServiceUnavailable)
	})
	defer server.Close()

	router := routing.New()
	if err := router.RegisterClassifier(RealLLMID, classifier); err != nil {
		t.Fatal(err)
	}
	if err := router.RegisterRules("llm-test", &contract.RoutingSpec{Rules: []contract.RoutingRule{{
		ID:     "classify",
		Action: contract.RuleAction{Mode: contract.DispatchSelective, Targets: []string{"app-a"}, Classifier: RealLLMID},
	}}}); err != nil {
		t.Fatal(err)
	}
	plan := router.Plan(context.Background(), eventForClassification(t, "hello"), []string{"app-a", "app-b"})
	if plan.Mode != contract.DispatchBroadcast || len(plan.Targets) != 2 || plan.Targets[1] != "app-b" {
		t.Fatalf("classifier failure should fallback to all candidates: %#v", plan)
	}
}

func TestRealLLMConfigValidatesEndpointAndEnvironment(t *testing.T) {
	for _, endpoint := range []string{"http://example.com/chat/completions", "https://user:pass@example.com/chat/completions", "https://example.com/chat?token=x"} {
		if _, err := NewRealLLMClassifier(RealLLMConfig{Endpoint: endpoint, Model: "model", APIKey: "key"}); err == nil {
			t.Errorf("endpoint %q 应该拒绝", endpoint)
		}
	}
	config, err := RealLLMConfigFromEnv(func(key string) string {
		values := map[string]string{
			RealLLMEndpointEnv: "https://llm.example/v1/chat/completions",
			RealLLMModelEnv:    "model",
			RealLLMAPIKeyEnv:   "key",
		}
		return values[key]
	})
	if err != nil || config.APIKey != "key" {
		t.Fatalf("config = %#v, err = %v", config, err)
	}
	if _, err := RealLLMConfigFromEnv(func(string) string { return "" }); err == nil || strings.Contains(err.Error(), "key") {
		t.Fatalf("缺少凭据时应提示变量名且不包含凭据，err = %v", err)
	}
}

func mustJSON(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
