# BotFabric 下一阶段完善

**状态**：已完成  
**开始日期**：2026-08-25  
**完成日期**：2026-08-25  
**范围**：公共消息模型与 Go SDK、OneBot v11/v12 反向 WebSocket、TCP Bearer 管理 API、安全卸载与原子重载

## 已确认决策

- 纵向切片推进，不一次铺开所有能力。
- OneBot v11 + v12，均采用反向 WebSocket。
- WebSocket 使用 `github.com/gorilla/websocket`。
- 管理 API 使用 TCP + Bearer Token，token 明文写入 YAML；默认关闭。
- v11/v12 共用 OneBot access token。
- 热重载采用新实例成功后切换，失败保留旧实例。
- 先提供 Go SDK，不做 Python/TypeScript SDK。

## 已完成

- 公共契约：`pkg/contract/message.go` 提供 `GroupMessage`、`SendMessageRequest`、`SendMessageResult` 和 JSON 辅助；ID 字段保持字符串精度。
- Go SDK：`sdk/go` 封装 Host 的 EventBus、ServiceGateway、JSON 编解码和超时辅助，不依赖 internal 包。
- OneBot v11/v12：`plugins/protocol/onebot` 提供反向 WebSocket server、Bearer 鉴权、最大消息限制、Ping/Pong、连接写锁、双版本事件归一化、`send_group_msg`/`send_message` action 映射和 echo 响应关联。
- 管理 API：`internal/management` 提供 TCP Bearer Token 只读端点 `/healthz`、`/v1/plugins`、`/v1/dead-letters`；死信响应不含 payload/headers；支持动态 listener 和优雅关闭。
- Runtime：新增 `Unload`，保护运行中依赖；新增 staging resource capture、服务/订阅替换和 `Reload`，新实例 staging 失败时保留旧实例，切换失败恢复资源。
- 配置：加入 `onebot` 与 `management` 配置，默认关闭真实 OneBot 和管理 API；示例权限允许真实 OneBot 提供动作服务。
- 文档：更新 `docs/02-架构设计.md`、`docs/03-开发指南.md`。

## 验证

以下命令全部通过：

```text
gofmt -w ./cmd ./internal ./pkg ./plugins ./sdk ./tests
go mod tidy
go test ./...
go test -race ./...
go vet ./...
```

新增测试覆盖：Go SDK、公用模型、v11/v12 事件归一化、错误 token、v11/v12 SendMessage echo 闭环、管理 API 鉴权和死信脱敏、Unload 依赖保护、Reload 成功切换与失败保留旧实例。

## 延后范围

- 外部进程真实 spawn、gRPC 代码生成与事件流通信
- Python / TypeScript SDK
- cgroups v2、seccomp、网络命名空间
- 持久化消息队列、OPA、证书链与 Payload 加密
- 管理 API 的远程 mutation 操作
- 多 OneBot 连接的高级路由、连接优先级和持久化会话
