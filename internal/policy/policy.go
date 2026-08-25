package policy

import (
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// Checker 是 EventBus 和 ServiceGateway 共用的授权边界。
type Checker interface {
	AllowPublish(pluginID, topic string) bool
	AllowSubscribe(pluginID, topic string) bool
	AllowInvoke(pluginID, service, method string) bool
	AllowProvide(pluginID, service string) bool
}

// InvokeRule 声明可调用的服务与方法。
type InvokeRule struct {
	Service string   `yaml:"service"`
	Methods []string `yaml:"methods"`
}

// PluginRules 是一个插件被授予的能力集合。
type PluginRules struct {
	Publish   []string     `yaml:"publish"`
	Subscribe []string     `yaml:"subscribe"`
	Invoke    []InvokeRule `yaml:"invoke"`
	Provide   []string     `yaml:"provide"`
}

// File 是静态 YAML 权限策略的格式。
type File struct {
	Default string                 `yaml:"default"`
	Plugins map[string]PluginRules `yaml:"plugins"`
}

// Static 是只读的精确匹配策略实现。
type Static struct {
	defaultAllow bool
	plugins      map[string]compiledRules
}

type compiledRules struct {
	publish   map[string]struct{}
	subscribe map[string]struct{}
	invoke    map[string]map[string]struct{}
	provide   map[string]struct{}
}

// Load 以严格模式读取静态 YAML 策略。
func Load(path string) (*Static, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开权限策略: %w", err)
	}
	defer file.Close()
	return Decode(file)
}

// Decode 从 reader 解析一份严格权限策略。
func Decode(reader io.Reader) (*Static, error) {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)

	var file File
	if err := decoder.Decode(&file); err != nil {
		return nil, fmt.Errorf("解析权限策略: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("权限策略只能包含一个 YAML 文档")
		}
		return nil, fmt.Errorf("解析权限策略: %w", err)
	}
	if file.Default != "deny" && file.Default != "allow" {
		return nil, errors.New("权限策略 default 仅支持 deny 或 allow")
	}

	compiled := &Static{
		defaultAllow: file.Default == "allow",
		plugins:      make(map[string]compiledRules, len(file.Plugins)),
	}
	for pluginID, rules := range file.Plugins {
		if pluginID == "" {
			return nil, errors.New("权限策略包含空插件 ID")
		}
		entry := compiledRules{
			publish:   make(map[string]struct{}, len(rules.Publish)),
			subscribe: make(map[string]struct{}, len(rules.Subscribe)),
			invoke:    make(map[string]map[string]struct{}, len(rules.Invoke)),
			provide:   make(map[string]struct{}, len(rules.Provide)),
		}
		var err error
		if entry.publish, err = checkedSet(rules.Publish, "publish"); err != nil {
			return nil, fmt.Errorf("插件 %s: %w", pluginID, err)
		}
		if entry.subscribe, err = checkedSet(rules.Subscribe, "subscribe"); err != nil {
			return nil, fmt.Errorf("插件 %s: %w", pluginID, err)
		}
		if entry.provide, err = checkedSet(rules.Provide, "provide"); err != nil {
			return nil, fmt.Errorf("插件 %s: %w", pluginID, err)
		}
		for _, rule := range rules.Invoke {
			if rule.Service == "" || len(rule.Methods) == 0 {
				return nil, fmt.Errorf("插件 %s 包含无效 invoke 规则", pluginID)
			}
			if _, exists := entry.invoke[rule.Service]; exists {
				return nil, fmt.Errorf("插件 %s 重复声明 invoke 服务 %s", pluginID, rule.Service)
			}
			methods, err := checkedSet(rule.Methods, "invoke methods")
			if err != nil {
				return nil, fmt.Errorf("插件 %s 的服务 %s: %w", pluginID, rule.Service, err)
			}
			entry.invoke[rule.Service] = methods
		}
		compiled.plugins[pluginID] = entry
	}
	return compiled, nil
}

func checkedSet(values []string, field string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return nil, fmt.Errorf("%s 包含空值", field)
		}
		if _, exists := result[value]; exists {
			return nil, fmt.Errorf("%s 重复声明 %s", field, value)
		}
		result[value] = struct{}{}
	}
	return result, nil
}

// AllowPublish 检查精确 Topic 发布权限。
func (s *Static) AllowPublish(pluginID, topic string) bool {
	rules, exists := s.plugins[pluginID]
	if !exists {
		return s.defaultAllow
	}
	_, allowed := rules.publish[topic]
	return allowed || s.defaultAllow
}

// AllowSubscribe 检查精确 Topic 订阅权限。
func (s *Static) AllowSubscribe(pluginID, topic string) bool {
	rules, exists := s.plugins[pluginID]
	if !exists {
		return s.defaultAllow
	}
	_, allowed := rules.subscribe[topic]
	return allowed || s.defaultAllow
}

// AllowInvoke 检查精确服务与方法调用权限。
func (s *Static) AllowInvoke(pluginID, service, method string) bool {
	rules, exists := s.plugins[pluginID]
	if !exists {
		return s.defaultAllow
	}
	methods, exists := rules.invoke[service]
	if !exists {
		return s.defaultAllow
	}
	_, allowed := methods[method]
	return allowed || s.defaultAllow
}

// AllowProvide 检查服务提供权限。
func (s *Static) AllowProvide(pluginID, service string) bool {
	rules, exists := s.plugins[pluginID]
	if !exists {
		return s.defaultAllow
	}
	_, allowed := rules.provide[service]
	return allowed || s.defaultAllow
}

// DefaultDecision 返回策略文件声明的默认决策。
func (s *Static) DefaultDecision() string {
	if s.defaultAllow {
		return "allow"
	}
	return "deny"
}
