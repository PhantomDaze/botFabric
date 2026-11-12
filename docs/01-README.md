# BotFabric

> 当前已具备可运行的 Go 核心运行时与 N:N 路由/响应仲裁骨架，仍处于持续完善阶段。

## 项目概述

BotFabric 是位于 QQ 协议端与应用端之间的**完全插件化机器人中间层**，支持不限数量的协议端和应用端接入（实际规模受资源与配置配额限制）。

协议端和应用端可以通过 OneBot v11/v12 接入，也可以通过 BotFabric SDK/插件协议接入。中间层统一完成标准事件归一化、端点管理、默认广播、规则路由、跨协议端去重、竞争响应、指定机器人回复、权限、生命周期与可观测性治理；具体业务仍由可选插件提供。

## 系统边界

```text
协议端 1..N ── OneBot v11/v12 ──┐
                                ▼
                         BotFabric 核心
                端点 / 去重 / 路由 / 响应仲裁
                                ▲
应用端 1..N ── OneBot 或 SDK/插件 ─┘
```

## 设计原则

1. **N:N 接入**：协议端和应用端均不设固定数量限制。
2. **默认广播**：没有匹配规则时，事件广播给所有匹配订阅者和应用端连接。
3. **可治理路由**：通过插件 Manifest 中的 RoutingSpec 配置正则、优先级、选择性广播、drop、fallback 和分类器。
4. **可选仲裁**：支持 first-wins 与 priority-override 两种候选回复采纳模式。
5. **协议无关**：内部业务只依赖 `pkg/contract`，不依赖 OneBot 私有 JSON。
6. **默认隔离**：插件只能使用清单声明并被策略授权的能力。
7. **可替换与可组合**：协议端、应用端、业务插件、分类器和存储实现可独立替换。

## 当前状态

已实现：

- Go 核心运行时、插件生命周期、依赖拓扑、安全卸载、原子重载；
- EventBus、ServiceGateway、静态权限策略、TraceID、结构化日志、健康检查；
- 公共消息模型与 Go SDK；
- OneBot v11/v12 协议端反向 WebSocket；
- 端点注册表与多协议端目标选择；
- 可选跨协议端去重：message ID、指纹、auto，drop/forward，进程内存或 Redis 共享存储；
- 路由规则：默认广播、正则过滤、selective/drop、优先级、fallback、公共 RouteClassifier 接口；
- 可选多协议真实 LLM 分类器：支持 OpenAI Chat Completions、Anthropic Messages、OpenAI Responses 和 Google Gemini `generateContent`，显式加载后使用环境变量密钥，失败时回退为候选广播；
- 响应协调器：CandidateReply、first-wins、priority-override、幂等和指定机器人回复；
- 四个路由预设：broadcast、regex、competition、required-bot；
- 应用端 OneBot v11/v12：服务端等待应用连接、主动连接远端应用、事件推送和候选回复回收；支持 `app:` 命名空间下的稳定身份、未回复事件 TTL/容量上限与单条消息读上限 `max_message_size`；
- Bearer Token 管理与诊断 API；
- 可选 Prometheus 指标端点：显式配置 `observability.metrics_enabled` 与 `metrics_listen_address` 后，Runtime 在独立只读 `/metrics` 输出事件发布/拒绝/去重/路由丢弃、投递成功/失败/死信、投递计划模式、分类器回退、候选回复采纳/丢弃与插件启动失败计数；
- 外部进程插件的 Unix Socket 双向 gRPC Session、短期会话令牌（仅约束启动握手）、事件/服务双向通信和进程组回收；插件进程只继承最小环境白名单，Socket 以 `SO_PEERCRED` 绑定启动进程，异常退出由 Runtime 回滚服务/订阅/端点并迁移为 failed；Linux 可通过 systemd transient service 使用 cgroup v2 限制整棵进程树内存，并可按 Manifest 显式启用 PrivateNetwork 网络命名空间隔离、NoNewPrivileges 提权防护、PrivateTmp 临时目录隔离、ProtectHome 家目录隐藏、ProtectSystem 系统目录只读、固定 syscall 拒绝组、地址族限制和 capability bounding set 限制；隔离请求均在宿主不支持时拒绝启动；
- Go、Python 与 TypeScript 外部进程插件 SDK；

暂未完成或仅提供骨架：

- 真实 LLM 分类器现支持 OpenAI Chat Completions、Anthropic Messages、OpenAI Responses 和 Google Gemini `generateContent` 四种 API 格式，默认仍为 OpenAI Chat Completions；兼容性以具体格式及服务端 JSON 输出能力为准，尚不代表所有供应商/模型组合均验证通过。新增离线路由质量评估工具，仍需针对目标数据集和具体模型运行评估；
- 高级沙箱仍有待完善；外部进程插件现支持 Linux opt-in 的 `private_network`、`no_new_privileges`、`private_tmp`、`protect_home`、`protect_system`、固定 syscall 拒绝组、`restrict_address_families`（限定为 `AF_UNIX`）和 `capability_bounding_set`，但 user systemd manager 只有在 user namespace 等宿主能力可用时才能应用 capability 限制；`protect_home` 完全隐藏 `/home`、`/root` 与 `/run/user`（Socket 目录不能位于其中），`protect_system` 只把 `/usr`、`/boot`、`/etc` 置为只读，二者都不是按路径白名单的文件系统沙箱；尚无细粒度 egress allowlist（`restrict_address_families` 是粗粒度地址族限制，不能按域名/IP 放行）、可写路径白名单或其他平台的等价隔离；`Router.Plan` 已提供默认广播和规则匹配的 Go 性能基准，用于本机比较，不构成跨环境性能承诺。
- 内存去重器 `Deduplicator.Check` 已提供消息 ID 与文本指纹新事件路径基准（`go test ./internal/dedup -run '^$' -bench '^BenchmarkDeduplicatorCheck' -benchmem`），仅作本机性能对比。
- 指标为进程内累计值，重启归零，多实例部署需在抓取侧聚合；`/metrics` 无内置认证，建议仅绑定回环或内网地址；`tracing_enabled` 仍为保留配置，TraceID 已贯穿事件与日志，但尚未导出外部追踪数据。

## 真实 LLM 分类器

启用需将 `classifier.real-llm` 内置 manifest 放入 Runtime 插件目录；默认运行配置不加载该插件或请求模型。API Key 经密钥管理器注入，不要写入 Manifest 或日志：

```text
BOTFABRIC_CLASSIFIER_REAL_LLM_FORMAT=openai_chat_completions # 可选，默认值
BOTFABRIC_CLASSIFIER_REAL_LLM_ENDPOINT=https://api.example/v1/chat/completions
BOTFABRIC_CLASSIFIER_REAL_LLM_MODEL=<model-name>
BOTFABRIC_CLASSIFIER_REAL_LLM_API_KEY=<secret>
```

`BOTFABRIC_CLASSIFIER_REAL_LLM_FORMAT` 可设为 `openai_chat_completions`、`anthropic_messages`、`openai_responses` 或 `google_gemini`；endpoint 必须是对应 API 的完整 URL（Gemini 可使用 API root，由 model 构造请求路径）。启用后，匹配的群消息文本和候选应用 ID 会发送至配置的模型服务。分类器拒绝无效计划和候选外目标；网络失败或响应无效时 Router 回退为广播。四种协议有本地 stub 合约测试，但兼容性还取决于具体服务和模型是否支持相应 JSON 输出能力。离线评估数据集格式及运行方式见 [03-开发指南.md](03-开发指南.md)。


| 文档 | 内容 |
|------|------|
| [../README.md](../README.md) | 项目介绍与快速开始 |
| [02-架构设计.md](02-架构设计.md) | N:N 拓扑、分层、事件/服务模型、路由与响应仲裁 |
| [03-开发指南.md](03-开发指南.md) | 契约、插件清单、路由规则、应用端接入与测试 |
| [04-生产部署.md](04-生产部署.md) | 构建、systemd/容器、配置参考、安全、升级与排障 |
| [05-使用与接入.md](05-使用与接入.md) | 协议端、应用端、路由配置、插件开发与 API 使用 |
