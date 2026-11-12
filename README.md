# BotFabric

**完全插件化的 QQ 机器人中间层** —— 协议端与应用端 N:N 接入，统一完成事件归一化、路由、跨协议端去重与响应仲裁。

BotFabric 位于 QQ 协议端（NapCat、Lagrange、LLOneBot 等 OneBot v11/v12 实现）和业务应用端之间。它不实现具体 QQ 业务，而是提供一套可治理的运行时：协议适配、端点管理、路由规则、竞争响应、权限、插件生命周期与可观测性都由中间层统一负责，业务逻辑以插件形式接入。

```text
协议端 1..N ── OneBot v11/v12 ──┐
                                ▼
                         BotFabric 核心
                 端点 / 去重 / 路由 / 响应仲裁
                                ▲
应用端 1..N ── OneBot 或 SDK/插件 ─┘
```

## 为什么需要它

- **多协议端、多应用端**：多个 QQ 号或多个协议实现可以同时接入，应用端无需与某个协议端一对一绑定。
- **默认广播 + 可治理路由**：没有规则时事件广播给所有订阅者；需要时可用正则、优先级、selective/drop、fallback 和分类器精确控制去向。
- **竞争响应**：多个应用端都可以给出候选回复，由 first-wins 或 priority-override 决定最终发送哪一条，并支持指定机器人回复。
- **跨协议端去重**：同一个 QQ 群消息可能从多个协议端重复到达，中间层按 message_id、文本指纹或 auto 策略去重。
- **插件化与隔离**：业务插件、协议端、分类器和存储实现均可替换；外部进程插件运行在 Unix Socket gRPC 会话中，可按需启用 Linux 沙箱。
- **可运维**：权限默认 deny、TraceID 贯穿、结构化日志、健康检查、Prometheus 指标、死信与管理 API、原子热重载。

## 核心能力

| 领域 | 能力 |
|------|------|
| 接入 | OneBot v11/v12 协议端反向 WebSocket；应用端 server / client / both 三种模式；进程内 Go 插件；外部进程插件（Go / Python / TypeScript SDK） |
| 事件 | 标准 `GroupMessage` 归一化、EventBus 广播、订阅过滤、死信存储 |
| 路由 | 默认广播、正则匹配、优先级、selective / drop、fallback、公共 `RouteClassifier` 接口、四种路由预设 |
| 去重 | message_id / fingerprint / auto；drop 或 forward；进程内存或 Redis 共享存储 |
| 响应 | CandidateReply、first-wins、priority-override、幂等键、指定机器人回复 |
| 治理 | deny-by-default 静态权限策略、服务网关、超时、审计、依赖拓扑、安全卸载、原子重载 |
| 安全 | 插件最小环境白名单、会话令牌、Unix Socket `SO_PEERCRED` 进程绑定；可选 PrivateNetwork、NoNewPrivileges、PrivateTmp、ProtectHome、ProtectSystem、syscall 拒绝组、AF_UNIX 地址族限制、capability bounding set |
| 可观测 | JSON/text 日志、TraceID、健康检查、Prometheus `/metrics`、管理 API（健康、插件、死信分页） |
| 存储 | 进程内内存实现；可选 Redis 共享去重与响应状态，支持多实例 |

## 快速开始

要求 Go 1.27 或更高版本。

```bash
git clone git@github.com:PhantomDaze/botFabric.git
cd botFabric

go build -o bin/botfabric ./cmd/server
./bin/botfabric config/runtime.example.yaml
```

示例配置默认不连接真实 QQ：`onebot.enabled: false` 时加载 `protocol.mockonebot` 模拟协议端，`example.reply` 示例插件会把收到的群消息回复为 `收到：<原文>`。

启用管理 API 后可以直接检查运行时状态（token 至少 16 字符）：

```yaml
management:
  enabled: true
  listen_address: 127.0.0.1:9090
  bearer_token: replace-with-at-least-16-chars
```

```bash
curl -H "Authorization: Bearer replace-with-at-least-16-chars" \
  http://127.0.0.1:9090/healthz
curl -H "Authorization: Bearer replace-with-at-least-16-chars" \
  http://127.0.0.1:9090/v1/plugins
```

## 接入真实 QQ

1. 在 `config/runtime.example.yaml` 中启用协议端：

```yaml
onebot:
  enabled: true
  listen_address: 127.0.0.1:8090
  path_v11: /onebot/v11
  path_v12: /onebot/v12
  access_token: replace-with-at-least-8-chars
```

2. 确保插件目录中存在 `config/plugins/onebot.yaml`。
3. 在 QQ 协议端中把反向 WebSocket 地址配置为 `ws://<host>:8090/onebot/v11`（或 v12），并填写相同的 access token。

应用端可以通过 OneBot 接入（`application_onebot` 的 server / client / both 模式），也可以使用 SDK 编写进程内或外部进程插件。完整步骤见 [使用与接入指南](docs/05-使用与接入.md)。

## 生产部署

生产部署的构建、systemd/容器、配置项、安全清单、多实例、升级回滚和故障排查见 [生产部署指南](docs/04-生产部署.md)。

关键注意点：

- 管理 API 与 `/metrics` 建议只绑定回环或内网；`/metrics` 无内置认证。
- `application` 应用端稳定身份必须使用保留命名空间 `app:<id>`。
- 外部进程隔离通过 `systemd-run --user` 实现，需要可用的 user systemd manager。
- 外部插件只继承最小环境白名单，入口脚本需要自行设置必要的运行时变量。

## 文档

| 文档 | 内容 |
|------|------|
| [01-README.md](docs/01-README.md) | 项目定位、能力清单、当前状态与已知边界 |
| [02-架构设计.md](docs/02-架构设计.md) | N:N 拓扑、分层、事件与服务模型、路由与响应仲裁 |
| [03-开发指南.md](docs/03-开发指南.md) | 公共契约、插件清单、路由规则、SDK 与测试 |
| [04-生产部署.md](docs/04-生产部署.md) | 构建、systemd/容器、配置参考、安全、升级与排障 |
| [05-使用与接入.md](docs/05-使用与接入.md) | 协议端、应用端、路由配置、插件开发与 API 使用 |

## 项目结构

```text
botfabric/
├── cmd/server/              # 运行时入口与装配
├── internal/
│   ├── runtime/             # 插件生命周期、依赖、暂存与原子重载
│   ├── eventbus/            # 事件广播、投递、死信
│   ├── gateway/             # 服务网关、权限、超时、审计
│   ├── endpoint/            # 协议端/应用端端点注册表
│   ├── dedup/               # 跨协议端消息去重
│   ├── routing/             # 规则求值、DispatchPlan、分类器
│   ├── response/            # CandidateReply 竞争采纳
│   ├── onebotapp/           # 应用端 OneBot v11/v12
│   ├── config/              # 严格 YAML 配置与 Manifest 校验
│   ├── policy/              # 静态权限策略
│   ├── process/             # 外部进程插件 transport 与隔离
│   └── observability/       # 日志、TraceID、指标、健康
├── pkg/contract/            # 稳定公共契约
├── plugins/                 # 内置协议、应用、分类器与路由预设
├── sdk/                     # Go / Python / TypeScript SDK
├── config/                  # 运行时、策略与插件 Manifest 示例
├── tests/e2e/               # 端到端仿真测试
└── docs/                    # 设计与运维文档
```

## 项目状态

核心运行时、N:N 路由、响应仲裁、OneBot v11/v12 接入、外部进程插件与可观测性均已可运行。当前基线在 Linux 上通过：

```bash
go build ./...
go vet ./...
go test ./...
go test -race ./...
```

已知边界与长期项（细粒度 egress allowlist、非 Linux 等价隔离、`/metrics` 认证、外部 tracing 导出等）记录在 [01-README.md](docs/01-README.md) 与 [生产化计划](docs/plan/production-hardening.md)。

## 参与开发

```bash
gofmt -w ./cmd ./internal ./pkg ./plugins ./tests ./sdk/go
go test ./...
go test -race ./...
go vet ./...
```

提交前请确保测试全绿；代码注释使用中文，commit message 使用英文。
