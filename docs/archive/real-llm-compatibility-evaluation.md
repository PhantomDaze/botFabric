# 真实 LLM 分类器多协议兼容与路由质量评估

**状态**：已完成（本地实现与测试）
**开始日期**：2026-10-11
**完成日期**：2026-10-11

## 范围

按项目 README 中的分类器待办，扩展多协议 API 格式并建立固定数据集评估能力。保留默认 OpenAI Chat Completions 行为、不默认加载分类器、不使用真实凭据测试。

## 已完成

- [x] 定义格式选择环境变量 `BOTFABRIC_CLASSIFIER_REAL_LLM_FORMAT`；为空时兼容旧配置并默认 OpenAI Chat Completions。
- [x] 增加四种协议适配：OpenAI Chat Completions、Anthropic Messages、OpenAI Responses、Google Gemini `generateContent`。
- [x] 每种协议使用本地 HTTP stub 验证 endpoint、请求 JSON、认证头和响应抽取；共用严格 dispatch plan 校验、超限保护及 Router 广播回退。
- [x] 增加准确率、mode accuracy、目标 precision/recall、失败数的评估库函数。
- [x] 增加 JSONL 评估命令，限制 500 条数据，只有显式带 `--confirm-live-requests` 才能访问模型服务；校验样本结构和期望计划。
- [x] 更新 README、架构设计与开发指南；明确协议适配不等于所有供应商/模型已验证。

## 未完成与边界

- 真实供应商/模型的端到端矩阵仍需在有对应服务凭据及明确数据授权的环境中运行；当前 Web 文档获取存在限制，因此只声称本地协议 stub 合约测试覆盖，不声称远端互操作已验证。
- 路由质量指标只有在目标模型、数据集、版本和运行参数一致时才可比较；必须由用户在自有环境运行评估命令。不会在本任务中调用真实模型。
- 全部完成：`gofmt`、`go test ./...`、`go test -race ./...`、`go vet ./...`、`GOOS=windows GOARCH=amd64 go test -exec=true ./plugins/classifier ./cmd/classifier-eval`、`git diff --check`。

## 验收标准

- 默认 OpenAI Chat Completions 请求和所有既有测试不回归。
- 四个协议 stub 测试均通过，且无密钥、无远程调用。
- 评估命令没有显式确认时不发送请求；样本格式校验、指标计算和广播回退行为有测试覆盖。
- 文档准确区分已测协议字段映射与尚未验证的第三方供应商互操作性。
