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
- 可选跨协议端去重：message ID、指纹、auto，drop/forward 与内存 TTL 存储；
- 路由规则：默认广播、正则过滤、selective/drop、优先级、fallback、公共 RouteClassifier 接口；
- 响应协调器：CandidateReply、first-wins、priority-override、幂等和指定机器人回复；
- 四个路由预设：broadcast、regex、competition、required-bot；
- 应用端 OneBot v11/v12：服务端等待应用连接、主动连接远端应用、事件推送和候选回复回收；
- Bearer Token 管理与诊断 API。

暂未完成或仅提供骨架：

- 外部进程插件传输层仍是握手骨架，尚未实现完整 gRPC/Unix Socket 运行链路；
- LLM 分类器目前只有公共接口和 mock 实现，不连接真实模型 API；
- 去重与响应状态虽预留可插拔 Store，当前只提供进程内存实现；
- Python/TypeScript SDK 和高级沙箱仍属于后续切片。

## 文档导航

| 文档 | 内容 |
|------|------|
| [02-架构设计.md](02-架构设计.md) | N:N 拓扑、分层、事件/服务模型、路由与响应仲裁 |
| [03-开发指南.md](03-开发指南.md) | 契约、插件清单、路由规则、应用端接入与测试 |
