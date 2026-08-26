# BotFabric N:N 路由与响应仲裁

**状态**：已完成  
**开始日期**：2026-08-25  
**完成日期**：2026-08-26  
**范围**：端点模型、跨协议端去重、可治理路由、响应仲裁、指定机器人回复、应用端 OneBot 双向接入

## 已确认决策

1. 整体设计后再分阶段实现，实现按切片推进。
2. 去重判定：协议 `message_id` 优先 + 指纹兜底，可配置切换；本轮内存存储，预留分布式接口。
3. 竞速响应：`first_wins` 与 `priority_override` 两种都支持，由路由规则选择。
4. 指定回复机器人：应用端只生成回复内容，中间层解析目标协议端连接发送。
5. 应用端 OneBot 方向：服务端（应用端连入）与客户端（中间层主动连远端应用端）两种都支持，按配置可选 `both`。
6. LLM 分类器：只定接口 + mock 实现，不接真实大模型 API。
7. 去重/竞速状态：预留分布式 Store 接口，本轮只实现内存版。
8. 契约元数据：直接扩展公共结构体字段（BotID、EndpointID、EventID、IdempotencyKey、RequiredReplyBot）。
9. 路由规则配置：作为插件 Manifest 的 `routing` 段，复用现有加载/重载机制。
10. 预设规则库：4 个核心预设，附测试。
11. 现有示例插件：同步改造接入去重/竞速/CandidateReply 流程。

## 实现切片

- [x] R1 端点模型 + 契约扩展：新增 contract 字段/类型、EndpointRegistry；协议/应用插件注册端点；默认路由=广播，行为不变。
- [x] R2 去重子系统：Deduplicator + Store 接口 + MemoryStore；EventBus 可选接入。
- [x] R3 路由核心 + 默认规则 + 预设 1：Router/DispatchPlan/RouteClassifier 接口 + mock 分类器；Manifest.Routing 解析与注册；preset-broadcast。
- [x] R4 响应协调器 + CandidateReply：`botfabric.response` 服务、first_wins（同步）/priority_override（异步）、目标解析、幂等；改造 reply/mockonebot/onebot handleAction 路由。
- [x] R5 预设 2/3/4 + 规则：regex 选择、competition、required-bot（+forward 去重）；多端点 mockonebot。
- [x] R6 OneBot 应用端适配器：server/client/both、事件推送、动作回收→CandidateReply、AppDeliverySink。
- [x] R7 LLM 分类器接口 + mock 插件：`RouteClassifier` 与 `RouteClassifierRegistrar` 提升至 `pkg/contract`，mock 实现只依赖公共契约。
- [x] R8 文档对齐 + 归档：更新 01/02/03 文档；归档本计划文档。

## 完成内容

- 公共契约扩展：`Event` 增加 `EndpointID`、`RequiredReplyBot`、`ResponsePolicy`；`SendMessageRequest` 增加 `BotID`、`EndpointID`、`EventID`、`IdempotencyKey`；`Manifest` 增加 `Routing *RoutingSpec`；新增 `endpoint.go`、`routing.go`、`sink.go`。
- `internal/endpoint`：线程安全端点注册表，区分 protocol/application 端点，提供 `Register/Unregister/ByBotID/ByKind/UnregisterOwned`，深拷贝保护，owner 校验。
- `internal/dedup`：`Store` 接口（`Claim(key, ttl)`）+ `MemoryStore`；`Deduplicator` 支持 `message_id`/`fingerprint`/`auto`，`drop`/`forward`，TTL 窗口。
- `internal/routing`：`Router.Plan` 按 priority 降序匹配，首个匹配非 fallback 规则胜出，否则 fallback，否则默认广播；`RouteClassifier` 接口独立超时 + 失败降级；规则与分类器按 owner 注册/卸载。
- `internal/response`：核心服务 `botfabric.response`（`Submit`/`Cancel`）；first_wins 原子 winner 抢占同步发送；priority_override 超时窗口收集最高优先级异步发送；目标解析按 RequiredReplyBot/BotID/EndpointID/FallbackBots；`IdempotencyKey` 保证每事件至多发送一次。
- `internal/eventbus`：Publish 流程接入可选 Deduplicator、Router.Plan、response hook、EventSink 投递；selective 计划按 owner 过滤进程内订阅者；默认无路由/去重时仍为全广播。
- `internal/onebotapp`：应用端 OneBot v11/v12 适配器，server/client/both 三种模式；事件推送反归一化；`send_group_msg`/`send_message` 回收转 CandidateReply；`__botfabric_event_id`/echo 关联；每条连接注册为 application 端点；实现 `EventSink` 供 Dispatcher 推送。
- `internal/runtime`：装配共享 EndpointRegistry、Router、Coordinator；启动注册 `botfabric.response` 核心服务；EventBus 注入 Router 与 response hook；Manifest.Routing 注册到 Router；停止/卸载清理 services/subscriptions/endpoints/routing rules/classifiers；核心 response 服务不纳入插件拓扑。
- 插件改造：`onebot` 连接保存 BotID、注册 protocol 端点、按 BotID/EndpointID 选连接；`mockonebot` 多端点 + 按 BotID/EndpointID 路由并记录目标端点；`reply` 改为提交 CandidateReply 调 `botfabric.response/Submit`。
- 路由预设：`plugins/routing` 提供 broadcast、regex、competition、required-bot 四个预设。
- 分类器：`plugins/classifier` mock 实现经 `RouteClassifierRegistrar` 注册。
- 内置注册：`plugins/builtin/register.go` 注册 mockonebot、reply、onebot、routing preset、classifier mock。

## 验证

以下命令全部通过：

```text
go test ./...
go test -race ./...
go vet ./...
```

新增测试覆盖：端点注册表与生命周期、去重 drop/forward 与 auto 策略与窗口、路由默认广播/优先级/正则/selective/drop/Header/fallback/classifier 与规则注销、响应协调器 first-wins/priority-override/required-reply-bot/幂等/race 安全、EventBus sink 投递、OneBot v11/v12 应用端往返与错误 token、routing preset 集成、内置 factory 注册。

现有 `tests/e2e/core_loop_test.go` 断言不变仍通过（回归）。

## 延后范围

- 外部进程插件完整 transport（当前仅握手骨架）。
- 真实 LLM 分类器 API（当前仅接口 + mock）。
- 分布式去重/响应 Store（如 Redis；当前仅内存，接口已预留）。
- Python / TypeScript SDK。
- 应用端在缺少 `__botfabric_event_id`/echo 时的最近未决事件 fallback 关联。
- OneBot 应用端重连、心跳与长期连接治理。
- 高级沙箱与性能基准。
