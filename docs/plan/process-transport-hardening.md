# 外部进程 transport 路径与启动清理加固

## 状态

- 状态：已完成
- 日期：2026-09-06

## 目标

- 防止 Manifest ID 直接参与 Unix Socket 文件名，避免路径穿越和过长路径问题。
- 让同一个 transport 的并发 Start 调用串行化。
- 确保启动阶段 context 取消、进程启动失败时唤醒等待者并清理 Socket。
- 保留旧 Transport 测试兼容，同时不破坏完整 Session transport。
- 保留 protobuf EventEnvelope 旧字段编号，避免旧客户端读取新字段时发生语义错位。

## 已完成

- `internal/process/loader.go` 使用 SHA-256 摘要生成安全的 Socket 文件名前缀，并在每次 `Loader.Load` 时追加随机实例后缀，最终形如 `plugin-<16 hex>-<16 hex>.sock`；不再让同一 Manifest 的 staging/旧实例共享 Socket 路径。
- `internal/process/grpc_transport.go` 增加启动互斥锁。
- 启动早期失败路径关闭 `processDone`，避免 Connect/Handshake 等待永久阻塞。
- gRPC 握手失败继续设置错误、断开 Session、终止外部进程并清理 Socket。
- protobuf `EventEnvelope` 保留原 `correlation_id`、`trace_id`、`headers`、`payload` 字段编号，将新增字段追加到 11-15。
- wire 层服务错误转换改用 `errors.As`，保留包装后的 `ServiceError`。
- Shutdown 遇到已断开但进程已正常退出时，按正常清理完成处理，避免将正常停止误报为失败。
- `grpcSessionStream` 使用单一发送 goroutine 串行调用 gRPC `Send`；发送请求在入队和等待期间响应 context 取消，避免每次发送创建不可控的 goroutine。
- cleanup 标记入站回调关闭状态，并在清理 Socket 前最多等待 5 秒；同时关闭发送循环，避免回调或发送队列继续使用已清理的 Session。

## 验证

- `gofmt`：通过
- `go test -count=1 ./...`：通过
- `go test -count=1 -race ./...`：通过
- `go vet ./...`：通过
- `go build ./...`：通过
- `git diff --check`：通过

## 未决事项

- gRPC 底层单次 `Send` 若因 HTTP/2 流控长期阻塞，仍需依赖 RPC context 或 server.Stop 打断；当前实现已避免调用方无限等待及每次发送泄漏 goroutine。
- 外部进程 fork 出的子孙进程回收仍需独立的进程组/沙箱切片处理。
