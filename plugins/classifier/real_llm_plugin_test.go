package classifier

import (
	"context"
	"strings"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type classifierRegistrar struct {
	id         string
	classifier contract.RouteClassifier
}

func (r *classifierRegistrar) RegisterRouteClassifier(id string, classifier contract.RouteClassifier) error {
	r.id, r.classifier = id, classifier
	return nil
}

func (r *classifierRegistrar) UnregisterRouteClassifier(string) error { return nil }

type pluginTestHost struct {
	contract.Host
	registrar *classifierRegistrar
}

func (h pluginTestHost) RegisterRouteClassifier(id string, classifier contract.RouteClassifier) error {
	return h.registrar.RegisterRouteClassifier(id, classifier)
}

func (h pluginTestHost) UnregisterRouteClassifier(id string) error {
	return h.registrar.UnregisterRouteClassifier(id)
}

func TestRealLLMPluginStartsFromEnvironment(t *testing.T) {
	registrar := &classifierRegistrar{}
	plugin := &RealLLMPlugin{getenv: func(key string) string {
		values := map[string]string{
			RealLLMEndpointEnv: "https://llm.example/v1/chat/completions",
			RealLLMModelEnv:    "test-model",
			RealLLMAPIKeyEnv:   "secret",
		}
		return values[key]
	}}
	if err := plugin.Start(context.Background(), pluginTestHost{registrar: registrar}); err != nil {
		t.Fatal(err)
	}
	if registrar.id != RealLLMID || registrar.classifier == nil {
		t.Fatalf("registered classifier = %q, %T", registrar.id, registrar.classifier)
	}
	if status := plugin.Health(context.Background()); status.State != contract.HealthHealthy {
		t.Fatalf("health = %#v", status)
	}
}

func TestRealLLMPluginMissingConfigurationDoesNotRegister(t *testing.T) {
	registrar := &classifierRegistrar{}
	plugin := &RealLLMPlugin{getenv: func(string) string { return "" }}
	err := plugin.Start(context.Background(), pluginTestHost{registrar: registrar})
	if err == nil || !strings.Contains(err.Error(), RealLLMAPIKeyEnv) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("configuration error = %v", err)
	}
	if registrar.classifier != nil {
		t.Fatal("classifier must not register without configuration")
	}
}

func TestRealLLMPluginRequiresClassifierRegistrar(t *testing.T) {
	plugin := &RealLLMPlugin{getenv: func(key string) string {
		values := map[string]string{
			RealLLMEndpointEnv: "https://llm.example/v1/chat/completions",
			RealLLMModelEnv:    "test-model",
			RealLLMAPIKeyEnv:   "secret",
		}
		return values[key]
	}}
	if err := plugin.Start(context.Background(), struct{ contract.Host }{}); err == nil || !strings.Contains(err.Error(), "不支持路由分类器注册") {
		t.Fatalf("registrar error = %v", err)
	}
}
