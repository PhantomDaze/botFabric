# 运行时 Prometheus 指标

**状态**：已完成（本地抓取验证；未接入真实 Prometheus 服务）
**开始日期**：2026-11-07
**完成日期**：2026-11-07

## 目标

把配置里早已声明、但从未实现的 `observability.metrics_enabled` 变成可用能力：为 Runtime 提供显式 opt-in 的 Prometheus 文本指标端点，覆盖事件投递、去重、路由、响应仲裁与插件启动等关键路径。默认不改变行为；配置打开但缺少监听地址时拒绝启动；不引入第三方依赖、不暴露事件载荷或凭据。

选择本切片而非继续扩展进程沙箱，原因有三：高级沙箱的新增 systemd 属性在本沙箱环境中无法端到端验证（`/run/user/1000` 只读，既有 `internal/process` 测试因此失败），而指标可离线完整验证；细粒度 egress allowlist 已因 per-user systemd 限制记录为阻塞；指标是 README 所述“可观测性治理”中唯一被配置声明却完全缺失的能力。

## 实施结果

- 新增 `internal/observability/metrics.go`：原子计数器、Prometheus 文本输出（`text/plain; version=0.0.4`）与只读 HTTP 处理器；`internal/observability/metrics_server.go` 提供独立监听、实际地址查询与优雅停机。
- 指标覆盖：事件发布/权限拒绝/去重 drop 与 forward/路由丢弃、订阅者投递成功与失败、死信、投递计划模式、分类器回退、候选回复提交/采纳/丢弃、插件启动失败。
- 接入方式保持向后兼容：`Bus`、`Router`、`Coordinator` 各新增 `SetMetrics`（`atomic.Pointer` 存储，nil 安全），`Manager.SetMetrics` 统一向下分发；未配置时所有路径维持零开销分支。
- 配置：`observability.metrics_listen_address` 新增；`metrics_enabled: true` 而地址为空时 `Config.Validate` 拒绝启动。示例配置默认关闭指标，避免示例启动额外监听端口。
- `cmd/server` 在启用时创建并启动独立指标服务，随 Runtime 上下文优雅停机；管理 API 与指标服务解耦，不要求同时启用管理 API。
- 测试：指标文本/标签输出、HTTP 方法与未知路径、动态端口服务、并发计数（race）；EventBus 发布/投递失败/权限拒绝；Router 三种计划模式与分类器回退；Coordinator 采纳/丢弃；Manager 启动失败计数；`cmd/server` 在启用指标时完整启动与停机。
- 文档：README、架构设计、开发指南补充配置、指标语义、无认证与进程内累计等边界。

## 验证

- `gofmt -l`：无输出。
- `go vet ./...`：通过。
- `go test ./...`（`internal/process` 除外）：全部通过。
- `go test -race ./internal/observability ./internal/eventbus ./internal/routing ./internal/response ./internal/runtime ./internal/config ./cmd/server`：通过。
- `GOOS=windows GOARCH=amd64 go build ./...`、`GOOS=darwin GOARCH=arm64 go build ./...`：通过。
- `internal/process` 的 3 个既有集成测试在本沙箱因 `/run/user/1000` 与 `/home` 只读而失败，与本次改动无关（基线同样失败）。
- **本机端到端验证**：以示例配置启用指标并启动 `cmd/server`，`curl 127.0.0.1:19091/metrics` 返回 200、`text/plain; version=0.0.4`，13 个指标族按预期输出（含 `action`/`mode` 标签）。
- **未做**：真实 Prometheus 服务抓取、告警规则与多实例聚合联调。

## 边界与未决事项

- 指标为进程内累计值，重启归零；多实例部署需在抓取侧按实例聚合。
- `/metrics` 无内置认证，只适合回环或可信内网；公网暴露需反向代理或后续增加 Token/网络策略。
- 仅覆盖 Runtime 内部计数，不含 HTTP 连接数、延迟直方图或插件自定义指标；`tracing_enabled` 仍是保留配置，未导出外部追踪数据。
- 独立监听而非挂载到管理 API，避免指标采集依赖管理 API 的 Bearer Token 与启用状态；若后续要求统一鉴权，可再评估合并。
