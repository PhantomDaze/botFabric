# BotFabric 核心运行时骨架

**状态**：已完成  
**开始日期**：2026-08-25  
**完成日期**：2026-08-25  
**范围**：核心运行时骨架、同进程示例闭环、外部进程插件契约骨架

## 已确认决策

- Go module：`github.com/PhantomDaze/botfabric`
- EventBus 与 ServiceGateway 分离
- 同进程插件完整跑通；外部进程插件仅实现接口、Proto 契约和握手骨架
- 本期不连接真实 QQ 协议端，不实现 Python/TypeScript SDK 与 cgroup 沙箱
- 不使用 Go `plugin` 作为同进程加载机制，采用编译期工厂注册表

## 执行阶段

- [x] 阶段 0：初始化 module 与目录结构
- [x] 阶段 1：实现 `pkg/contract` 公共契约
- [x] 阶段 2：实现可观测性、配置、注册表与权限策略
- [x] 阶段 3：实现 EventBus
- [x] 阶段 4：实现 ServiceGateway
- [x] 阶段 5：实现 Runtime、同进程加载器和服务入口
- [x] 阶段 6：实现 mockonebot、reply 示例插件与端到端测试
- [x] 阶段 7：实现外部进程插件契约与握手骨架
- [x] 最终验证：`go test ./...`、`go test -race ./...`、`go vet ./...` 全部通过

## 完成内容

- 公共契约 `pkg/contract`：Event、EventBus、ServiceGateway、Plugin/Host、Manifest、PluginLoader、稳定错误码，序列化字段名已固化。
- `internal/config`：严格 YAML 解析、未知字段拒绝、Manifest 校验（含列表去重、空值拒绝、memory 格式校验）。
- `internal/policy`：静态 YAML 权限，`default: deny/allow` 语义一致，与 runtime 配置一致性校验。
- `internal/registry`：线程安全注册表，Manifest 与 ServiceDescriptor 深拷贝，防止外部修改内部状态。
- `internal/observability`：结构化日志、TraceID 生成与传播、健康检查 HTTP Handler。
- `internal/eventbus`：精确 Topic、同插件内优先级与注册顺序、跨插件并发投递、每处理器超时、panic 隔离、死信记录、停机时拒绝新事件并等待真实 handler goroutine。
- `internal/gateway`：可信 Caller（覆盖自报）、权限检查、超时、审计日志、保留业务 `ServiceError` 错误码、同时跟踪调用方与服务提供方在途调用。
- `internal/activity`：按插件隔离的在途任务跟踪器，支撑 EventBus/Gateway 优雅停机。
- `internal/runtime`：服务依赖拓扑启动、Start 真超时与回滚、Host 能力按 Manifest 边界限制、启动后校验声明服务已注册、生命周期串行化、Stop 先停新任务再等待在途再执行 Stop、Stop 超时调用 ForceStop。
- 同进程加载器：编译期工厂注册表，内置清单与配置清单语义一致校验。
- `internal/process`：外部进程插件 Proto 契约、握手状态机骨架、可信会话校验、ForceStop。
- 示例插件：`mockonebot`（归一化 + SendMessage 动作服务）、`reply`（订阅 → 调用动作），群号全程字符串避免大整数丢精度。
- `cmd/server`：配置 → 装配 → 启动 → 信号优雅停机，可测试入口拆分。
- 测试覆盖：各包独立单测 + E2E 核心闭环，`go test -race` 通过。

## 延后范围

- 真实 OneBot 协议适配器
- 外部进程插件的真实 spawn、gRPC 代码生成与事件流通信
- Python / TypeScript SDK
- cgroups v2、seccomp、网络命名空间
- 持久化消息队列、OPA、证书链与 Payload 加密
- 插件真正的动态热加载
