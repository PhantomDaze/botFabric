# 各功能完整仿真测试

**状态**：已完成
**开始日期**：2026-10-13
**完成日期**：2026-10-19

## 目标

为 BotFabric 各核心功能建立**端到端仿真测试**：在单进程内用可配置的模拟协议端、模拟应用端、stub LLM HTTP 服务与 miniredis，通过真实 Runtime（EventBus / ServiceGateway / Router / ResponseCoordinator / EndpointRegistry / Dedup）走完整链路，验证**跨组件协作行为**，而非各包内单元测试。

## 现状

- 每个内部包已有较完整的单元测试（dedup、router、coordinator、eventbus、onebotapp、process、runtime 等）。
- 集成/E2E 覆盖极薄：仅 `tests/e2e/core_loop_test.go`——单协议端（`mockonebot`）+ 单应用端（`reply`）+ 默认广播，**无去重、无 N:N、无仲裁竞争、无分类器入环、无 Redis、无生命周期中断**。
- 现有可复用模拟组件及其限制：
  - `plugins/protocol/mockonebot`：端点 ID 硬编码为 `protocol.mockonebot.default`，**无法多实例**，`BotID` 取 `host.BotID()`（单 Runtime 单 Bot）。
  - `plugins/application/reply`：行为固定（回 `收到：<text>`），无法模拟多应用端竞争/不回复/优先级。

## 缺口（按功能）

| 功能 | 现有覆盖层级 | 仿真缺口 |
|------|--------------|----------|
| 默认广播 1→N | 单测 + 单链 e2e | 多应用端同时收到、目标集合正确 |
| N:N（多协议端 × 多应用端） | 无 | 不同 BotID 协议端共存、端点选择 |
| 去重 drop/forward | 包内单测 | 多协议端重复消息经真实总线投递一次 / 标记重投 |
| 路由 selective/drop/priority/fallback | 包内单测 | 端到端：哪些应用端实际收到 |
| 分类器（stub LLM）入环 | 包内 stub 单测 | 分类结果实际决定投递目标集合 + 广播回退 |
| 响应仲裁 first-wins/priority/required-bot/幂等 | 包内单测 | 多应用端真实竞争 → 协议端最终发出哪条 |
| Redis 分布式去重/仲裁 | 包内 miniredis 单测 | 跨两个 Runtime 实例共享去重与仲裁 |
| 权限 deny-by-default | 包内单测 | 未声明权限的插件调用被拒并进链路 |
| TraceID 贯穿 | 单测 | 事件→服务→动作全链路同一 TraceID |
| 健康检查 / 死信 | 单测 | 故障插件上报不健康、超时进死信 |
| 生命周期 reload/unload | 包内单测 | 流程中热重载应用端后规则/订阅切换正确 |
| OneBot v11/v12 协议/应用端 | 包内 websocket 单测 | 真实协议端 ↔ 应用端 ↔ 核心全链路 |
| 外部进程插件 | 已有集成测试 | 归入仿真套件统一引用，避免重复 |

## 仿真基础设施（测试专用，新增）

全部放在 `tests/e2e/`（`package e2e_test`），仅测试编译期存在，不进入生产产物。

1. **harness（`harness_test.go`）**：`newSim(t)` 组装 `registry` / `eventbus`（含 `MemoryDeadLetterSink`）/ `gateway` / `runtime.Manager`（可注入共享 `endpoint.Registry`、`dedup.Store`）+ `BuiltinLoader` 工厂注册辅助；`t.Cleanup` 停机。
2. **`simProtocol`**：可配置协议端插件——自定义 `pluginID`、`endpointID`、`botID`、`protocol/version`；`Start` 注册 `EndpointProtocol` 端点；`Emit(raw)` 发布归一化事件（`BotID` 用自身配置，支持单 Runtime 多 Bot）；`Sent()` 返回收到的动作。
3. **`simApp`**：可配置应用端插件——自定义 `pluginID`、订阅 topic、行为函数 `func(event) *CandidateReply`（nil=不回复）；`Submit` 经响应服务提交候选回复（可带 `Priority`/`IdempotencyKey`/`TargetBotID`/延迟）；记录收到的事件。
4. **`simLLM`**：`httptest.Server`，按请求输出四种 API 格式之一的有效分类 JSON（可配置返回 `selective`/`drop`/非法计划/超时）。
5. **miniredis 辅助**：`redis.Client` + dedup/response Store 构造辅助，用于跨实例场景。

## 场景清单

- **A 广播与 N:N**：A1 单协议端→多应用端全收到；A2 双协议端双应用端广播；A3 selective 只投目标；A4 drop 无投递；A5 priority 规则覆盖 + fallback 规则生效。
- **B 去重**：B1 message_id 重复 drop；B2 fingerprint；B3 auto；B4 forward 标记 `botfabric_duplicate`；B5 窗口过期后可再投；B6 miniredis 跨两 Runtime 去重。
- **C 仲裁**：C1 first-wins 竞争；C2 priority-override 高优先级胜出；C3 幂等 key 不重复发送；C4 required-bot 定向协议端点 + fallback；C5 无回复超时不发送；C6 miniredis 跨实例仲裁。
- **D 分类器**：D1 selective 结果决定目标；D2 drop 结果不投递；D3 服务失败→广播回退；D4 四种 API 格式各跑一次端到端；D5 计划含候选外目标→广播回退；D6 服务超过 Router deadline → 取消请求并广播回退。
- **E 协议/应用端**：E1 OneBot v11/v12 协议端 × 应用端 4 种组合全链路；E2 server 模式缺 event关联的同群 fallback；E3 client 模式远端应用完整往返。
- **F 生命周期**：F1 运行中依赖卸载拒绝；F2 stop 后不再接收；F3 卸载路由后恢复广播；F4 路由规则热重载切换目标。
- **G 权限/可观测**：G1 清单/策略双重约束拒绝订阅；G2 静态策略拒绝动作调用且无协议发送；G3 TraceID 贯穿；G4 handler 错误与处理器超时进死信；G5 管理 API 鉴权/死信脱敏；G6 健康检查。

## 实施步骤

1. 建立 harness 与 `simProtocol` / `simApp`，用最快场景（A1）自证可用。
2. 按 A→G 逐组补齐场景，每组跑通后再进下一组。
3. D 组搭建 `simLLM`，确保默认不加载分类器、不访问真实网络。
4. 更新 `docs/03-开发指南.md` 第 8 节测试覆盖描述与 README（如涉及）。
5. 全量 `go test ./...`、`go test -race ./tests/...`、`go vet ./...`、`gofmt`、`git diff --check`。

## 进度（滚动更新）

- [x] 基础设施：`tests/e2e/harness_test.go`（`simHarness`/`simEnv`/策略渲染/等待辅助）、`simplugins_test.go`（可配置 `simProtocol` 多协议端点 / `simApp` 竞争·延迟·不回复·重复提交）、`simrouting_test.go`（路由与分类器路由声明）。
- [x] A 组（广播/N:N/selective/drop/priority+fallback）：5 场景通过。
- [x] B 组（去重 message_id/fingerprint/auto/forward/窗口过期/Redis 跨实例）：6 场景通过。
- [x] C 组（first-wins/priority-override/幂等键/required-bot+fallback/无回复/Redis 跨实例同一 eventID 仲裁）：7 测试通过。
- [x] D 组（stub LLM 入环：selective/drop/失败回退/四格式/候选外目标回退/超时回退）：6 场景（含四格式子测试）通过。
- [x] E 组（真实 OneBot v11/v12 协议端↔核心↔应用端↔协调器↔协议动作：4 种协议版本组合；应用端 server/client；缺 event关联同群 fallback）：3 测试通过（E1 含 4 个子测试）。
- [x] F 组（依赖卸载拒绝、停止后取消订阅、卸载路由恢复广播、热重载切换路由目标）：4 测试通过。
- [x] G 组（deny-by-default 订阅/调用、TraceID、死信（处理器错误与超时）、健康检查、管理 API 鉴权脱敏）：7 测试通过。
- [x] 收尾：race + 全量验证 + 开发指南更新 + 计划归档（2026-10-19 完成）。

## 关键发现（写码时确认，供后续参考）

- `EventBus.Publish` **同步**等待全部订阅者处理器返回，故 `Emit` 返回后事件投递与 first-wins 仲裁结果已确定；仅 priority-override（走 `time.AfterFunc` flush）与超时类需轮询。
- 单 Runtime 只能有一个 `protocol.qq.action` 提供者（`startOrder` 拒绝重复服务提供者），故 N:N 用**一个 `simProtocol` 挂多个协议端点**表达，而非多个协议插件。
- 去重器由 Runtime 用**首个启用 dedup 的插件**的配置装到总线上，全局生效。
- 权限策略必须在构造 EventBus/Gateway 前确定，故 harness 采用"先声明插件、后统一 start"。

- C6 现已验证真实跨实例竞争：在测试仿真里显式设置两个 Runtime 接收同一 `eventID`，共享 Redis `CandidateStore` 由 `TryWin` 原子仲裁；断言两个协议端动作总数恰为 1。这验证候选存储的跨 Runtime 原子性，但生产系统的事件 ID 仍由本地 EventBus 生成，测试中的共享 ID 属于受控注入，不等于生产事件复制/广播实现。
- E 组新增 `onebot.Plugin.Addr()`，与应用端 `Adapter.Addr()` 对称，返回实际监听地址，便于动态端口集成测试；Stop 后清空 listener，运行时接口以外不增加外部暴露能力。

## 未决 / 待办

- （无）本轮范围内未遗留未解决问题；跨语言 SDK E2E 与真实模型评估见上文"未做与边界"。

## 验证

- `go test ./tests/...` 与 `go test ./...`：全部通过（含 `internal/process` 真实集成测试）。
- `go test -race ./tests/...` 与全仓 `go test -race ./...`：无数据竞争。
- `go vet ./...`、`gofmt -l`、`git diff --check`：通过。
- 所有仿真场景**不访问真实网络/真实凭据**（分类器仅连 `httptest` 本地回环）。

## 未做与边界

- Python / TypeScript SDK 的跨语言端到端仿真**不纳入本轮**：SDK 有各自语言侧测试（握手、事件/服务通信、Health/Shutdown），如需跨语言 E2E 另开切片。
- 外部进程插件/systemd/cgroup 等真实宿主隔离继续由 `internal/process` 的集成测试覆盖，本轮在 Go Runtime 仿真中不伪造内核隔离证据。
- 分类器仿真只验证协议字段映射与投递决策，**不**主张任何真实供应商/模型互操作已验证（沿用既有边界）。
- C6/B6 的"跨 Runtime"为受控注入同一事件 ID + 共享 miniredis store，验证 Redis 仲裁/去重原子性；生产系统尚无跨实例事件复制/广播能力。
