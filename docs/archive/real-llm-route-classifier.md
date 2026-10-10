# 真实 LLM 路由分类器

**状态**：已完成
**开始日期**：2026-10-04
**完成日期**：2026-10-05
**范围**：实现一个可选、OpenAI-compatible Chat Completions 路由分类器插件；不改变路由核心接口，不默认启用，不依赖真实模型 API 进行测试。

## 实施结果

- 新增 `RealLLMClassifier` 与内置 `classifier.real-llm` 插件，使用 OpenAI-compatible Chat Completions API。
- endpoint、model、API Key 从环境变量读取；要求 HTTPS（loopback 测试允许 HTTP），禁用 HTTP 重定向；API Key 不进入日志或错误。
- 输入只包含 topic、schema、群消息文本和候选 ID；加入 payload/request/reply、候选数量与大小限制。
- 严格校验分类 JSON（拒绝未知/重复字段）、mode、候选唯一性和成员关系；模型不能设置 ResponsePolicy。请求失败通过 Router 既有机制广播回退。
- 插件显式加入扫描目录才会加载，内置 manifest 附带群消息分类规则；同步更新 README、架构和开发指南。
- 新增本地 HTTP stub 测试，覆盖请求/密钥头、选择性/广播/drop、非法计划、HTTP 错误、取消、配置校验、超限及 Router 广播回退。

## 验证

- `go test ./plugins/classifier ./plugins/builtin ./internal/routing ./internal/config`：通过。
- `go test ./plugins/classifier -run 'TestRealLLM' -count=1`：通过。
- `go test ./...`：通过。
- `go vet ./...`：通过。
- `git diff --check`：通过。

## 未覆盖范围

- 未使用真实凭据或访问真实供应商；未验证各服务对 JSON mode 的兼容差异。
- 未评估模型路由准确率、延迟、成本或数据保留策略。
- 高级沙箱和性能基准仍为后续切片。
