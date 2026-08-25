package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func TestLoadManifestRouting(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "routing.yaml")
	content := `id: routing.test
name: Routing Test
version: 0.1.0
runtime: in_process
entry: builtin.routing-test
provides:
  services: []
  subscriptions: [bot.message.group]
requires:
  services: []
  permissions: []
limits:
  timeout: 2s
  memory: ""
routing:
  dedup:
    enabled: true
    strategy: auto
    window: 2s
    duplicate_mode: drop
  rules:
    - id: command
      priority: 10
      match:
        topic: bot.message.group
        text_regex: '^/help'
      action:
        mode: selective
        targets: [application.help]
        response:
          mode: first_wins
          timeout: 3s
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Routing == nil || !manifest.Routing.Dedup.Enabled || manifest.Routing.Dedup.Strategy != contract.DedupAuto {
		t.Fatalf("路由配置未解析: %#v", manifest.Routing)
	}
	if len(manifest.Routing.Rules) != 1 || manifest.Routing.Rules[0].Action.Mode != contract.DispatchSelective {
		t.Fatalf("路由规则未解析: %#v", manifest.Routing.Rules)
	}
	if manifest.Routing.Rules[0].Action.Response.Timeout.String() != "3s" {
		t.Fatalf("响应超时 = %s", manifest.Routing.Rules[0].Action.Response.Timeout)
	}
}

func TestValidateManifestRejectsInvalidRoutingRegex(t *testing.T) {
	manifest := contract.Manifest{
		ID:      "routing.test",
		Name:    "Routing Test",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   "routing.test",
		Limits:  contract.Limits{Timeout: 1},
		Routing: &contract.RoutingSpec{Rules: []contract.RoutingRule{{
			ID:     "bad",
			Match:  contract.RuleMatch{TextRegex: "["},
			Action: contract.RuleAction{Mode: contract.DispatchBroadcast},
		}}},
	}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("无效正则应被拒绝")
	}
}
