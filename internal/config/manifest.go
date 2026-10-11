package config

import (
	"fmt"
	"os"
	"regexp"
	"runtime"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

type manifestFile struct {
	ID       string                 `yaml:"id"`
	Name     string                 `yaml:"name"`
	Version  string                 `yaml:"version"`
	Runtime  contract.PluginRuntime `yaml:"runtime"`
	Entry    string                 `yaml:"entry"`
	Provides contract.Provides      `yaml:"provides"`
	Requires contract.Requires      `yaml:"requires"`
	Limits   manifestLimits         `yaml:"limits"`
	Routing  *manifestRouting       `yaml:"routing"`
}

type manifestLimits struct {
	Timeout         Duration `yaml:"timeout"`
	Memory          string   `yaml:"memory"`
	PrivateNetwork  bool     `yaml:"private_network"`
	NoNewPrivileges bool     `yaml:"no_new_privileges"`
}

type manifestRouting struct {
	Dedup manifestDedup         `yaml:"dedup"`
	Rules []manifestRoutingRule `yaml:"rules"`
}

type manifestDedup struct {
	Enabled       bool                   `yaml:"enabled"`
	Strategy      contract.DedupStrategy `yaml:"strategy"`
	Window        Duration               `yaml:"window"`
	DuplicateMode contract.DuplicateMode `yaml:"duplicate_mode"`
}

type manifestRoutingRule struct {
	ID       string             `yaml:"id"`
	Priority int                `yaml:"priority"`
	Match    contract.RuleMatch `yaml:"match"`
	Action   manifestRuleAction `yaml:"action"`
}

type manifestRuleAction struct {
	Mode       contract.DispatchMode `yaml:"mode"`
	Targets    []string              `yaml:"targets"`
	Classifier string                `yaml:"classifier"`
	Response   manifestResponse      `yaml:"response"`
	Fallback   bool                  `yaml:"fallback"`
}

type manifestResponse struct {
	Mode             contract.ResponseMode `yaml:"mode"`
	Timeout          Duration              `yaml:"timeout"`
	RequiredReplyBot string                `yaml:"required_reply_bot"`
	FallbackBots     []string              `yaml:"fallback_bots"`
}

func convertRouting(raw *manifestRouting) *contract.RoutingSpec {
	if raw == nil {
		return nil
	}
	result := &contract.RoutingSpec{
		Dedup: contract.DedupSpec{
			Enabled:       raw.Dedup.Enabled,
			Strategy:      raw.Dedup.Strategy,
			Window:        raw.Dedup.Window.Duration,
			DuplicateMode: raw.Dedup.DuplicateMode,
		},
		Rules: make([]contract.RoutingRule, 0, len(raw.Rules)),
	}
	for _, rule := range raw.Rules {
		result.Rules = append(result.Rules, contract.RoutingRule{
			ID:       rule.ID,
			Priority: rule.Priority,
			Match:    rule.Match,
			Action: contract.RuleAction{
				Mode:       rule.Action.Mode,
				Targets:    append([]string(nil), rule.Action.Targets...),
				Classifier: rule.Action.Classifier,
				Fallback:   rule.Action.Fallback,
				Response: contract.ResponsePolicy{
					Mode:             rule.Action.Response.Mode,
					Timeout:          rule.Action.Response.Timeout.Duration,
					RequiredReplyBot: rule.Action.Response.RequiredReplyBot,
					FallbackBots:     append([]string(nil), rule.Action.Response.FallbackBots...),
				},
			},
		})
	}
	return result
}

func validateRouting(spec *contract.RoutingSpec) error {
	if spec == nil {
		return nil
	}
	if spec.Dedup.Enabled {
		if spec.Dedup.Strategy != contract.DedupMessageID && spec.Dedup.Strategy != contract.DedupFingerprint && spec.Dedup.Strategy != contract.DedupAuto {
			return fmt.Errorf("去重 strategy 无效: %s", spec.Dedup.Strategy)
		}
		if spec.Dedup.Window <= 0 {
			return fmt.Errorf("去重 window 必须大于零")
		}
		if spec.Dedup.DuplicateMode != contract.DuplicateDrop && spec.Dedup.DuplicateMode != contract.DuplicateForward {
			return fmt.Errorf("去重 duplicate_mode 无效: %s", spec.Dedup.DuplicateMode)
		}
	}
	seen := make(map[string]struct{}, len(spec.Rules))
	for _, rule := range spec.Rules {
		if rule.ID == "" {
			return fmt.Errorf("路由规则 ID 不能为空")
		}
		if _, exists := seen[rule.ID]; exists {
			return fmt.Errorf("路由规则 %s 重复", rule.ID)
		}
		seen[rule.ID] = struct{}{}
		if rule.Match.TextRegex != "" {
			if _, err := regexp.Compile(rule.Match.TextRegex); err != nil {
				return fmt.Errorf("路由规则 %s 的 text_regex 无效: %w", rule.ID, err)
			}
		}
		if rule.Action.Mode != contract.DispatchBroadcast && rule.Action.Mode != contract.DispatchSelective && rule.Action.Mode != contract.DispatchDrop {
			return fmt.Errorf("路由规则 %s 的 mode 无效: %s", rule.ID, rule.Action.Mode)
		}
		if rule.Action.Mode == contract.DispatchSelective && len(rule.Action.Targets) == 0 {
			return fmt.Errorf("路由规则 %s 的 selective 必须配置 targets", rule.ID)
		}
		if rule.Action.Response.Mode != "" && rule.Action.Response.Mode != contract.ResponseNone && rule.Action.Response.Mode != contract.ResponseFirstWins && rule.Action.Response.Mode != contract.ResponsePriorityOverride {
			return fmt.Errorf("路由规则 %s 的 response mode 无效: %s", rule.ID, rule.Action.Response.Mode)
		}
		if rule.Action.Response.Mode != "" && rule.Action.Response.Timeout <= 0 {
			return fmt.Errorf("路由规则 %s 的 response timeout 必须大于零", rule.ID)
		}
	}
	return nil
}

// LoadManifest 以严格模式读取并校验插件清单。
func LoadManifest(path string) (contract.Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return contract.Manifest{}, fmt.Errorf("打开插件清单: %w", err)
	}
	defer file.Close()

	var raw manifestFile
	if err := decodeStrict(file, &raw); err != nil {
		return contract.Manifest{}, fmt.Errorf("解析插件清单: %w", err)
	}
	manifest := contract.Manifest{
		ID:       raw.ID,
		Name:     raw.Name,
		Version:  raw.Version,
		Runtime:  raw.Runtime,
		Entry:    raw.Entry,
		Provides: raw.Provides,
		Requires: raw.Requires,
		Limits: contract.Limits{
			Timeout:         raw.Limits.Timeout.Duration,
			Memory:          raw.Limits.Memory,
			PrivateNetwork:  raw.Limits.PrivateNetwork,
			NoNewPrivileges: raw.Limits.NoNewPrivileges,
		},
		Routing: convertRouting(raw.Routing),
	}
	if err := ValidateManifest(manifest); err != nil {
		return contract.Manifest{}, err
	}
	return manifest, nil
}

// ValidateManifest 校验稳定清单契约。
func ValidateManifest(manifest contract.Manifest) error {
	if manifest.ID == "" || manifest.Name == "" || manifest.Version == "" {
		return fmt.Errorf("插件 id、name 和 version 均不能为空")
	}
	if manifest.Runtime != contract.RuntimeInProcess && manifest.Runtime != contract.RuntimeProcess {
		return fmt.Errorf("插件 %s 的 runtime 必须是 in_process 或 process", manifest.ID)
	}
	if manifest.Entry == "" {
		return fmt.Errorf("插件 %s 的 entry 不能为空", manifest.ID)
	}
	if manifest.Limits.Timeout <= 0 {
		return fmt.Errorf("插件 %s 的 timeout 必须大于零", manifest.ID)
	}
	if manifest.Runtime == contract.RuntimeProcess && manifest.Limits.Memory == "" {
		return fmt.Errorf("外部进程插件 %s 必须配置 memory", manifest.ID)
	}
	if (manifest.Limits.PrivateNetwork || manifest.Limits.NoNewPrivileges) && manifest.Runtime != contract.RuntimeProcess {
		return fmt.Errorf("插件 %s 的 private_network/no_new_privileges 仅外部进程 runtime 支持", manifest.ID)
	}
	if (manifest.Limits.PrivateNetwork || manifest.Limits.NoNewPrivileges) && runtime.GOOS != "linux" {
		return fmt.Errorf("插件 %s 的 private_network/no_new_privileges 仅在 Linux 平台支持", manifest.ID)
	}
	if manifest.Limits.Memory != "" {
		if _, err := parseByteSize(manifest.Limits.Memory); err != nil {
			return fmt.Errorf("插件 %s 的 memory 无效: %w", manifest.ID, err)
		}
	}
	if err := validateServices(manifest.Provides.Services); err != nil {
		return fmt.Errorf("插件 %s: %w", manifest.ID, err)
	}
	if err := validateUniqueStrings("订阅 Topic", manifest.Provides.Subscriptions); err != nil {
		return fmt.Errorf("插件 %s: %w", manifest.ID, err)
	}
	if err := validateUniqueStrings("依赖服务", manifest.Requires.Services); err != nil {
		return fmt.Errorf("插件 %s: %w", manifest.ID, err)
	}
	if err := validateUniqueStrings("权限", manifest.Requires.Permissions); err != nil {
		return fmt.Errorf("插件 %s: %w", manifest.ID, err)
	}
	if err := validateRouting(manifest.Routing); err != nil {
		return fmt.Errorf("插件 %s: %w", manifest.ID, err)
	}
	return nil
}

func validateUniqueStrings(field string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return fmt.Errorf("%s 包含空值", field)
		}
		if _, exists := seen[value]; exists {
			return fmt.Errorf("%s %s 重复声明", field, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateServices(services []contract.ServiceDescriptor) error {
	seen := make(map[string]struct{}, len(services))
	for _, service := range services {
		if service.Name == "" || len(service.Methods) == 0 {
			return fmt.Errorf("服务名和 methods 不能为空")
		}
		if _, exists := seen[service.Name]; exists {
			return fmt.Errorf("服务 %s 重复声明", service.Name)
		}
		seen[service.Name] = struct{}{}
		methods := make(map[string]struct{}, len(service.Methods))
		for _, method := range service.Methods {
			if method == "" {
				return fmt.Errorf("服务 %s 包含空方法名", service.Name)
			}
			if _, exists := methods[method]; exists {
				return fmt.Errorf("服务 %s 的方法 %s 重复声明", service.Name, method)
			}
			methods[method] = struct{}{}
		}
	}
	return nil
}
