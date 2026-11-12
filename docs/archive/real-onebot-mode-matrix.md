# 真实 OneBot 多连接仿真矩阵

**状态**：已完成（本机回环验证；不连接真实 QQ）
**开始日期**：2026-11-08
**完成日期**：2026-11-08

## 目标

在已有 `tests/e2e/` 仿真套件之上，补齐**真实 OneBot WebSocket 编解码**下的多连接/多模式组合。既有 A-G 组主要使用进程内模拟协议端/应用端；真实 OneBot 部分（E 组）只覆盖单协议连接 × 单应用连接。本轮要验证：

- 同时存在多个协议连接（不同 BotID、v11/v12）时，事件来源、动作目标与版本编码是否正确；
- 同时存在多个真实应用连接时，广播、first-wins 与 selective 行为是否符合预期；
- `required_reply_bot` 能否跨协议连接定向并正确选择 v11/v12 动作格式；
- 应用端 `both` 模式（本地连入 + 主动连接远端）能否同时参与同一条事件的响应竞争；
- 去重是否跨真实协议连接生效；
- 应用端适配器已接入但无连接时，事件是否进入死信而非静默丢失。

## 仿真端实现

新增 `tests/e2e/simonebot_test.go`，三个可复用仿真端全部走真实 WebSocket：

1. **`simQQClient`（仿真 QQ 协议端）**：连接 `protocol/onebot` 的 v11/v12 路径，推送标准群消息，读取 `send_group_msg`/`send_message` 动作并自动回写 echo 结果，记录动作数量、文本、群号、版本与关联字段。
2. **`simAppClient`（仿真应用端）**：连接 `onebotapp` 的 v11/v12 路径，可按需声明 `__botfabric_endpoint_id` 稳定身份，接收标准事件并按事件 ID 回发候选回复，记录事件与受理确认；支持延迟回复以构造竞争顺序。
3. **`simRemoteApp`（远端应用服务端）**：以 `httptest` 提供回环 WebSocket 服务，供应用端 `client`/`both` 模式主动连接，接收事件、回发候选回复并读取受理确认。

另提供应用端适配器启动、候选等待、端点等待与消息文本解析辅助，避免各场景重复拼装。

## 场景

| 编号 | 模式 | 断言 |
|------|------|------|
| H1 | 双协议连接（v11/bot-a、v12/bot-b）× 双应用连接（v11/v12），广播 + first-wins | 事件来源与预热连接各自注册协议端点；两个应用端都收到事件；仅事件来源连接收到 1 个 v11 动作；胜出文本来自更快的应用端 |
| H2 | `required_reply_bot: bot-b` 跨连接定向 | 事件来自 bot-a，动作由 bot-b 以 v12 `send_message` 接收，bot-a 收不到动作；群号与文本正确 |
| H3 | selective 规则按稳定应用身份定向 | 目标应用收到事件并回复，非目标应用收不到事件；动作正常回到协议端 |
| H4 | 应用端 `both` 模式：本地连入 + 主动连接远端应用 | 本地与远端应用都收到同一事件并竞争；更快的本地应用胜出，最终仅 1 个动作；远端应用收到丢弃结果的受理确认 |
| H5 | 双协议连接 + 去重 drop/forward | drop 只投递首条重复消息；forward 仍投递重复消息；后续唯一消息正常到达；无动作与死信 |
| H6 | 应用端适配器已接入但无应用连接 | 投递失败进入死信，`SubscriberID=event-sink` 且关联原事件；协议端无动作 |

## 关键发现

- `protocol/onebot` 的协议端点与 BotID 在**收到首条群消息事件后**才注册；仅建立 WebSocket 连接不会产生可定向的协议端点。H2 先用 bot-b 推送一条群 200 的预热消息，再验证 `required_reply_bot` 定向；这也是实际部署中协议端首次上线后才有路由目标的既有行为。
- 应用端稳定身份（`__botfabric_endpoint_id`）会替换连接初始的动态 ID，selective 规则可以按稳定 ID 精确选择；未声明稳定身份时目标 ID 不可预测，只能广播或按连接状态选择。
- 应用端 `both` 模式的两类连接（本地 server 接受的连接、主动拨出的远端连接）共用一个 `onebotapp.Adapter` 的 `conns` 表，事件广播与响应仲裁行为一致；远端连接不做 Token 校验的职责在远端服务端，不属于本轮范围。
- 无应用连接时 `onebotapp.Adapter.Deliver` 返回哨兵错误，EventBus 记为 `event-sink` 死信；这避免事件被静默丢弃，但消费方需要区分“配置了应用端但尚未连接”与正常投递。

## 验证

- `go test ./tests/e2e -run 'TestScenarioRealOneBot' -count=3`：通过。
- `go test -race ./tests/e2e -run 'TestScenarioRealOneBot' -count=3`：通过。
- `go test ./tests/e2e -count=5`：整套仿真通过。
- `go test -race ./tests/e2e -count=1`：整套仿真通过，无数据竞争。
- `go vet ./tests/e2e`、`gofmt -l`：通过。

## 边界

- 全部使用本机回环 WebSocket 与进程内 Runtime，不连接真实 QQ、真实客户端或外部网络。
- 未覆盖：TLS/wss、真实网络抖动与重连、多机部署、外部进程插件、真实 LLM 供应商；这些仍由各自单元/集成测试与既有边界说明负责。
- H1/H2 的“双协议连接”共享同一个 `protocol/onebot` 插件（服务提供者只能有一个）；多协议**插件**受运行时不允许多个 `protocol.qq.action` 提供者限制，N:N 以同一插件的多个连接表达。
