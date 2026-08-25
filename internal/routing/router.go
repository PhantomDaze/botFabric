// Package routing 提供事件路由规则求值和投递计划生成。
package routing

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// RouteClassifier 是公共契约的别名，保持 internal Router 的旧 API。
type RouteClassifier = contract.RouteClassifier

// CandidateProvider 提供当前可投递的应用端候选 ID。
type CandidateProvider interface {
	Candidates() []string
}

// Router 按插件 owner 管理路由规则，并以优先级求值。
type Router struct {
	mu                sync.RWMutex
	rules             map[string][]compiledRule
	classifiers       map[string]RouteClassifier
	classifierOwners  map[string]string
	defaultPlan       contract.DispatchPlan
	classifierTimeout time.Duration
}

type compiledRule struct {
	owner string
	rule  contract.RoutingRule
	regex *regexp.Regexp
}

// New 创建默认广播路由器。
func New() *Router {
	return &Router{
		rules:             make(map[string][]compiledRule),
		classifiers:       make(map[string]RouteClassifier),
		classifierOwners:  make(map[string]string),
		defaultPlan:       contract.DispatchPlan{Mode: contract.DispatchBroadcast},
		classifierTimeout: time.Second,
	}
}

// SetClassifierTimeout 设置分类器调用的独立超时时间。
func (r *Router) SetClassifierTimeout(timeout time.Duration) error {
	if timeout <= 0 {
		return errors.New("分类器超时必须大于零")
	}
	r.mu.Lock()
	r.classifierTimeout = timeout
	r.mu.Unlock()
	return nil
}

// RegisterClassifier 注册一个路由分类器。
func (r *Router) RegisterClassifier(id string, classifier RouteClassifier) error {
	return r.registerClassifier("", id, classifier)
}

// RegisterClassifierForOwner 注册并绑定分类器所有者。
func (r *Router) RegisterClassifierForOwner(owner, id string, classifier RouteClassifier) error {
	return r.registerClassifier(owner, id, classifier)
}

func (r *Router) registerClassifier(owner, id string, classifier RouteClassifier) error {
	if id == "" || classifier == nil {
		return errors.New("分类器 ID 和实现不能为空")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.classifiers[id]; exists {
		return errors.New("分类器已注册: " + id)
	}
	r.classifiers[id] = classifier
	r.classifierOwners[id] = owner
	return nil
}

// UnregisterClassifier 移除一个分类器。
func (r *Router) UnregisterClassifier(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.classifiers[id]; !exists {
		return errors.New("分类器不存在: " + id)
	}
	delete(r.classifiers, id)
	delete(r.classifierOwners, id)
	return nil
}

// UnregisterClassifiersByOwner 移除指定 owner 注册的分类器。
// 当前分类器注册表保留 owner 记录以支持插件卸载清理。
func (r *Router) UnregisterClassifiersByOwner(owner string) {
	if owner == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, registeredOwner := range r.classifierOwners {
		if registeredOwner == owner {
			delete(r.classifiers, id)
			delete(r.classifierOwners, id)
		}
	}
}

// RegisterRules 编译并替换指定 owner 的规则。
func (r *Router) RegisterRules(owner string, spec *contract.RoutingSpec) error {
	if owner == "" {
		return errors.New("路由规则 owner 不能为空")
	}
	if spec == nil {
		r.UnregisterRules(owner)
		return nil
	}
	compiled := make([]compiledRule, 0, len(spec.Rules))
	for _, rule := range spec.Rules {
		if rule.ID == "" {
			return errors.New("路由规则 ID 不能为空")
		}
		var expression *regexp.Regexp
		var err error
		if rule.Match.TextRegex != "" {
			expression, err = regexp.Compile(rule.Match.TextRegex)
			if err != nil {
				return err
			}
		}
		compiled = append(compiled, compiledRule{owner: owner, rule: cloneRule(rule), regex: expression})
	}
	sort.SliceStable(compiled, func(i, j int) bool {
		return compiled[i].rule.Priority > compiled[j].rule.Priority
	})
	r.mu.Lock()
	r.rules[owner] = compiled
	r.mu.Unlock()
	return nil
}

// UnregisterRules 移除指定 owner 的规则。
func (r *Router) UnregisterRules(owner string) {
	r.mu.Lock()
	delete(r.rules, owner)
	r.mu.Unlock()
}

// Plan 根据事件和候选应用端生成投递计划。
// 无匹配规则时返回默认广播计划；规则的 Targets 为空时保持候选集不变。
func (r *Router) Plan(ctx context.Context, event contract.Event, candidates []string) contract.DispatchPlan {
	r.mu.RLock()
	rules := make([]compiledRule, 0)
	for _, ownerRules := range r.rules {
		rules = append(rules, ownerRules...)
	}
	classifiers := make(map[string]RouteClassifier, len(r.classifiers))
	for id, classifier := range r.classifiers {
		classifiers[id] = classifier
	}
	timeout := r.classifierTimeout
	defaultPlan := clonePlan(r.defaultPlan)
	r.mu.RUnlock()
	sort.SliceStable(rules, func(i, j int) bool {
		return rules[i].rule.Priority > rules[j].rule.Priority
	})

	var fallback *compiledRule
	for index := range rules {
		rule := &rules[index]
		if !matches(rule, event) {
			continue
		}
		if rule.rule.Action.Fallback {
			if fallback == nil {
				fallback = rule
			}
			continue
		}
		return r.planForRule(ctx, rule.rule.Action, event, candidates, classifiers, timeout, defaultPlan)
	}
	if fallback != nil {
		return r.planForRule(ctx, fallback.rule.Action, event, candidates, classifiers, timeout, defaultPlan)
	}
	defaultPlan.Targets = append([]string(nil), candidates...)
	return defaultPlan
}

func (r *Router) planForRule(ctx context.Context, action contract.RuleAction, event contract.Event, candidates []string, classifiers map[string]RouteClassifier, timeout time.Duration, fallback contract.DispatchPlan) contract.DispatchPlan {
	plan := contract.DispatchPlan{Mode: action.Mode, Policy: clonePolicy(action.Response)}
	if plan.Mode == "" {
		plan.Mode = contract.DispatchBroadcast
	}
	if action.Mode == contract.DispatchDrop {
		plan.Drop = true
		return plan
	}
	plan.Targets = append([]string(nil), candidates...)
	if action.Mode == contract.DispatchSelective {
		plan.Targets = append([]string(nil), action.Targets...)
	}
	if action.Classifier != "" {
		classifier := classifiers[action.Classifier]
		if classifier == nil {
			return fallbackWithTargets(fallback, candidates)
		}
		classifyCtx, cancel := context.WithTimeout(ctx, timeout)
		classified, err := classifier.Classify(classifyCtx, event, append([]string(nil), candidates...))
		cancel()
		if err != nil {
			return fallbackWithTargets(fallback, candidates)
		}
		if classified.Mode == "" {
			classified.Mode = plan.Mode
		}
		if classified.Policy.Mode == "" {
			classified.Policy = plan.Policy
		}
		return classified
	}
	return plan
}

func fallbackWithTargets(plan contract.DispatchPlan, candidates []string) contract.DispatchPlan {
	plan.Targets = append([]string(nil), candidates...)
	if plan.Mode == "" {
		plan.Mode = contract.DispatchBroadcast
	}
	return plan
}

func matches(rule *compiledRule, event contract.Event) bool {
	match := rule.rule.Match
	if match.Topic != "" && match.Topic != event.Topic || match.Schema != "" && match.Schema != event.Schema || match.BotID != "" && match.BotID != event.BotID {
		return false
	}
	var message contract.GroupMessage
	if len(event.Payload) > 0 {
		_ = json.Unmarshal(event.Payload, &message)
	}
	if match.Protocol != "" && match.Protocol != message.Protocol || match.Version != "" && match.Version != message.Version || match.GroupID != "" && match.GroupID != message.GroupID || match.UserID != "" && match.UserID != message.UserID {
		return false
	}
	if rule.regex != nil && !rule.regex.MatchString(message.Text) {
		return false
	}
	for key, expected := range match.Headers {
		if event.Headers[key] != expected {
			return false
		}
	}
	return true
}

func cloneRule(rule contract.RoutingRule) contract.RoutingRule {
	rule.Match.Headers = cloneStringMap(rule.Match.Headers)
	rule.Action.Targets = append([]string(nil), rule.Action.Targets...)
	rule.Action.Response.FallbackBots = append([]string(nil), rule.Action.Response.FallbackBots...)
	return rule
}

func clonePlan(plan contract.DispatchPlan) contract.DispatchPlan {
	plan.Targets = append([]string(nil), plan.Targets...)
	plan.Policy = clonePolicy(plan.Policy)
	return plan
}

func clonePolicy(policy contract.ResponsePolicy) contract.ResponsePolicy {
	policy.FallbackBots = append([]string(nil), policy.FallbackBots...)
	return policy
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
