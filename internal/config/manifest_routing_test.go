package config

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

func TestValidateCapabilityBoundingSet(t *testing.T) {
	for _, test := range []struct {
		name      string
		values    []string
		wantValid bool
	}{
		{name: "valid", values: []string{"CAP_SYS_ADMIN", "CAP_NET_BIND_SERVICE"}, wantValid: true},
		{name: "explicit-empty", values: []string{}, wantValid: true},
		{name: "empty-name", values: []string{""}},
		{name: "missing-prefix", values: []string{"SYS_ADMIN"}},
		{name: "invalid-character", values: []string{"CAP_SYS-ADMIN"}},
		{name: "duplicate", values: []string{"CAP_SYS_ADMIN", "CAP_SYS_ADMIN"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest := contract.Manifest{ID: "process.capabilities", Name: "Process Capabilities", Version: "0.1.0", Runtime: contract.RuntimeProcess, Entry: "./plugin", Limits: contract.Limits{Timeout: 1, Memory: "64MiB", CapabilityBoundingSet: test.values}}
			if err := ValidateManifest(manifest); (err == nil) != test.wantValid {
				t.Fatalf("ValidateManifest() error = %v, wantValid %v", err, test.wantValid)
			}
		})
	}
}

func TestValidateManifestRejectsCapabilityBoundingSetForInProcess(t *testing.T) {
	manifest := contract.Manifest{ID: "inprocess.capabilities", Name: "In-process Capabilities", Version: "0.1.0", Runtime: contract.RuntimeInProcess, Entry: "builtin.example", Limits: contract.Limits{Timeout: 1, CapabilityBoundingSet: []string{}}}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("in_process 插件不应声明 capability_bounding_set")
	}
}

func TestValidateManifestRejectsCapabilityBoundingSetOnUnsupportedPlatform(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("Linux 支持 capability_bounding_set")
	}
	manifest := contract.Manifest{ID: "process.capabilities", Name: "Process Capabilities", Version: "0.1.0", Runtime: contract.RuntimeProcess, Entry: "./plugin", Limits: contract.Limits{Timeout: 1, Memory: "64MiB", CapabilityBoundingSet: []string{"CAP_SYS_ADMIN"}}}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("非 Linux 平台不应接受 capability_bounding_set")
	}
}

func TestLoadManifestCapabilityBoundingSet(t *testing.T) {
	for _, test := range []struct {
		name             string
		field            string
		wantCapabilities []string
	}{
		{name: "omitted", wantCapabilities: nil},
		{name: "empty", field: "  capability_bounding_set: []\n", wantCapabilities: []string{}},
		{name: "configured", field: "  capability_bounding_set: [CAP_SYS_ADMIN, CAP_NET_BIND_SERVICE]\n", wantCapabilities: []string{"CAP_SYS_ADMIN", "CAP_NET_BIND_SERVICE"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest.yaml")
			content := "id: process.capabilities\nname: Process Capabilities\nversion: 0.1.0\nruntime: process\nentry: ./plugin\nprovides:\n  services: []\n  subscriptions: []\nrequires:\n  services: []\n  permissions: []\nlimits:\n  timeout: 2s\n  memory: 64MiB\n" + test.field
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			manifest, err := LoadManifest(path)
			if err != nil {
				t.Fatal(err)
			}
			actual := manifest.Limits.CapabilityBoundingSet
			if !reflect.DeepEqual(actual, test.wantCapabilities) {
				t.Fatalf("CapabilityBoundingSet = %#v, want %#v", actual, test.wantCapabilities)
			}
		})
	}
}

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

func TestLoadManifestPrivateNetwork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private-network.yaml")
	content := `id: process.network
name: Process Network
version: 0.1.0
runtime: process
entry: ./plugin
provides:
  services: []
  subscriptions: []
requires:
  services: []
  permissions: []
limits:
  timeout: 2s
  memory: 64MiB
  private_network: true
  no_new_privileges: true
  private_tmp: true
  restrict_syscalls: true
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !manifest.Limits.PrivateNetwork || !manifest.Limits.NoNewPrivileges || !manifest.Limits.PrivateTmp || !manifest.Limits.RestrictSyscalls {
		t.Fatalf("隔离限制未解析: %#v", manifest.Limits)
	}
}

func TestValidateManifestRejectsPrivateNetworkForInProcess(t *testing.T) {
	manifest := contract.Manifest{
		ID:      "inprocess.network",
		Name:    "In-process Network",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   "builtin.example",
		Limits:  contract.Limits{Timeout: 1, PrivateNetwork: true},
	}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("in_process 插件不应声明 private_network")
	}
}

func TestValidateManifestAcceptsPrivateNetworkForProcess(t *testing.T) {
	manifest := contract.Manifest{
		ID:      "process.network",
		Name:    "Process Network",
		Version: "0.1.0",
		Runtime: contract.RuntimeProcess,
		Entry:   "./plugin",
		Limits:  contract.Limits{Timeout: 1, Memory: "64MiB", PrivateNetwork: true},
	}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("process 插件的 private_network 应有效: %v", err)
	}
}

func TestValidateManifestRejectsIsolationOnUnsupportedPlatform(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("Linux 支持插件隔离选项")
	}
	for _, limits := range []contract.Limits{
		{Timeout: 1, Memory: "64MiB", PrivateNetwork: true},
		{Timeout: 1, Memory: "64MiB", NoNewPrivileges: true},
		{Timeout: 1, Memory: "64MiB", PrivateTmp: true},
		{Timeout: 1, Memory: "64MiB", RestrictSyscalls: true},
		{Timeout: 1, Memory: "64MiB", CapabilityBoundingSet: []string{}},
	} {
		manifest := contract.Manifest{
			ID:      "process.isolation",
			Name:    "Process Isolation",
			Version: "0.1.0",
			Runtime: contract.RuntimeProcess,
			Entry:   "./plugin",
			Limits:  limits,
		}
		if err := ValidateManifest(manifest); err == nil {
			t.Fatalf("非 Linux 平台不应接受隔离配置: %#v", limits)
		}
	}
}

func TestValidateManifestRejectsNoNewPrivilegesForInProcess(t *testing.T) {
	manifest := contract.Manifest{
		ID:      "inprocess.nnp",
		Name:    "In-process NNP",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   "builtin.example",
		Limits:  contract.Limits{Timeout: 1, NoNewPrivileges: true},
	}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("in_process 插件不应声明 no_new_privileges")
	}
}

func TestValidateManifestAcceptsNoNewPrivilegesForProcess(t *testing.T) {
	manifest := contract.Manifest{
		ID:      "process.nnp",
		Name:    "Process NNP",
		Version: "0.1.0",
		Runtime: contract.RuntimeProcess,
		Entry:   "./plugin",
		Limits:  contract.Limits{Timeout: 1, Memory: "64MiB", NoNewPrivileges: true},
	}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("process 插件的 no_new_privileges 应有效: %v", err)
	}
}

func TestValidateManifestRejectsRestrictSyscallsForInProcess(t *testing.T) {
	manifest := contract.Manifest{
		ID:      "inprocess.syscalls",
		Name:    "In-process Syscalls",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   "builtin.example",
		Limits:  contract.Limits{Timeout: 1, RestrictSyscalls: true},
	}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("in_process 插件不应声明 restrict_syscalls")
	}
}

func TestValidateManifestAcceptsRestrictSyscallsForProcess(t *testing.T) {
	manifest := contract.Manifest{
		ID:      "process.syscalls",
		Name:    "Process Syscalls",
		Version: "0.1.0",
		Runtime: contract.RuntimeProcess,
		Entry:   "./plugin",
		Limits:  contract.Limits{Timeout: 1, Memory: "64MiB", RestrictSyscalls: true},
	}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("process 插件的 restrict_syscalls 应有效: %v", err)
	}
}

func TestValidateManifestRejectsRestrictSyscallsOnUnsupportedPlatform(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("Linux 支持 restrict_syscalls")
	}
	manifest := contract.Manifest{
		ID:      "process.syscalls",
		Name:    "Process Syscalls",
		Version: "0.1.0",
		Runtime: contract.RuntimeProcess,
		Entry:   "./plugin",
		Limits:  contract.Limits{Timeout: 1, Memory: "64MiB", RestrictSyscalls: true},
	}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("非 Linux 平台不应接受 restrict_syscalls")
	}
}

func TestValidateManifestRejectsPrivateTmpForInProcess(t *testing.T) {
	manifest := contract.Manifest{
		ID:      "inprocess.private-tmp",
		Name:    "In-process PrivateTmp",
		Version: "0.1.0",
		Runtime: contract.RuntimeInProcess,
		Entry:   "builtin.example",
		Limits:  contract.Limits{Timeout: 1, PrivateTmp: true},
	}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("in_process 插件不应声明 private_tmp")
	}
}

func TestValidateManifestAcceptsPrivateTmpForProcess(t *testing.T) {
	manifest := contract.Manifest{
		ID:      "process.private-tmp",
		Name:    "Process PrivateTmp",
		Version: "0.1.0",
		Runtime: contract.RuntimeProcess,
		Entry:   "./plugin",
		Limits:  contract.Limits{Timeout: 1, Memory: "64MiB", PrivateTmp: true},
	}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("process 插件的 private_tmp 应有效: %v", err)
	}
}

func TestValidateManifestRejectsPrivateTmpOnUnsupportedPlatform(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("Linux 支持 PrivateTmp")
	}
	manifest := contract.Manifest{
		ID:      "process.private-tmp",
		Name:    "Process PrivateTmp",
		Version: "0.1.0",
		Runtime: contract.RuntimeProcess,
		Entry:   "./plugin",
		Limits:  contract.Limits{Timeout: 1, Memory: "64MiB", PrivateTmp: true},
	}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("非 Linux 平台不应接受 private_tmp")
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
