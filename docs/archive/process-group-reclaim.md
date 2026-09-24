# 外部进程 transport 路径与启动清理加固

## 状态

- 状态：已完成
- 日期：2026-09-24

## 目标

- 防止 Manifest ID 直接参与 Unix Socket 文件名，避免路径穿越和过长路径问题。
- 让同一个 transport 的并发 Start 调用串行化。
- 确保启动阶段 context 取消、进程启动失败时唤醒等待者并清理 Socket。
- 保留旧 Transport 测试兼容，同时不破坏完整 Session transport。
- 保留 protobuf EventEnvelope 旧字段编号，避免旧客户端读取新字段时发生语义错位。
- **新增**：外部插件进程在独立进程组中运行，终止时回收整个进程组，避免 fork 出的子孙进程残留。

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

## 当前切片：进程组与子孙进程回收（2026-09-24，已完成）

### 实施结果

1. `internal/process/process_group.go`（平台无关入口）
   - `prepareProcessGroup`：为外部插件进程设置独立进程组。
   - `terminateProcessGroup`：向进程组发送信号；pgid 为 0 时由调用方回退为只终止直接子进程。
   - `sigForceKill` 由平台文件定义。
2. `internal/process/process_group_unix.go`
   - `configureProcessGroup` 设置 `SysProcAttr.Setpgid = true`。
   - `signalProcessGroup` 使用 `syscall.Kill(-pgid, SIGKILL)`，ESRCH 视为已回收。
   - `processGroupID` 从 `/proc/<pid>/stat` 读取 pgrp，避免 `Wait4` 干扰 `exec.Cmd.Wait`。
3. `internal/process/process_group_windows.go`
   - 空实现：Windows 无 POSIX 进程组，`processGroupID` 返回 0，`killProcess` 自动回退为只终止直接子进程。
4. `internal/process/grpc_transport.go`
   - `start` 中 `prepareProcessGroup(command)`，启动成功后记录 `processGroupID`。
   - `killProcess` 优先向进程组发 SIGKILL 回收整棵进程树，再对直接子进程 `Process.Kill()` 兜底。
5. `internal/process/testdata/helper/main.go`
   - `BOTFABRIC_HELPER_FORK_CHILD=1` 时 fork 一个长驻子进程（输出自身 PID 后 `select{}` 阻塞）。
   - 子进程通过 `BOTFABRIC_HELPER_CHILD=1` 区分入口，不连接 Runtime。
   - 父进程按行读取子进程 PID 后输出到 stderr，供测试解析。
6. `internal/process/grpc_transport_integration_test.go`
   - 新增 `TestGRPCTransportKillsProcessGroup`：fork 子进程 → Kill → 验证直接子进程退出、孙进程也被回收、Socket 清理。

### 遇到的问题（已解决）

- helper 初版用 `io.ReadAll` 读子进程 stdout，但子进程输出 PID 后长驻不退出，EOF 永不到来导致 helper 卡死、Connect 超时。改为 `bufio.ReadString('\n')` 读一行即返回。
- slog 文本格式把 helper stderr 包进 `message="..."`，测试解析 PID 需先剥壳再匹配前缀。

### 搁置项

- cgroups v2、seccomp、网络隔离等沙箱能力不属于本切片。
- Windows 平台不支持 POSIX 进程组，仅终止直接子进程（`processGroupID` 恒为 0 自动回退）。
- 优雅 Shutdown 仍只发给直接子进程协议帧；若插件在收到 shutdown 后自行 fork 新进程，由进程组 SIGKILL 兜底回收。

### 验证

- `go vet ./...`：通过
- `gofmt`：通过
- `go test -count=1 ./...`：通过
- `go test -count=1 -race ./...`：通过
