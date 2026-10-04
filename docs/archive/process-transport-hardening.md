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

- gRPC 底层单次 `Send` 若因 HTTP/2 流控长期阻塞，仍需依赖 RPC context 或 server.Stop 打断；当前实现已避免调用方无限等待及每次发送泄漏 goroutine（该语义已由 `TestSendCancelDuringQueue`/`TestSendBlockedThenClose` 覆盖，剩余风险为真实网络栈下的极端流控场景）。

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
- `TestGRPCTransportKillsProcessGroup` 曾出现约一半概率的抖动失败：SIGKILL 送达后子进程从内核消失需要短暂时间，测试立即用 0 信号探测会误判"仍在运行"。改为最多 2 秒轮询等待后稳定（12 次连跑全过）。

## 当前切片：gRPC Send 阻塞与取消语义验证（2026-09-24，已完成）

### 实施结果

新增 `internal/process/grpc_transport_send_test.go`，用进程内 fake `ProcessPlugin_SessionServer` 替身验证 `grpcSessionStream` 的阻塞/取消语义：

1. `TestSendCancelDuringQueue`
   - writer 卡在底层 `Send` 时，新请求只能排队；排队期间 context 取消立即返回 `context.Canceled`，不永久阻塞、不向底层流写入。
2. `TestSendBlockedThenClose`
   - 底层 `Send` 长期阻塞时，`close()`（对应 cleanup/server.Stop 路径）让发送立即返回 `errSessionClosed`。
3. `TestStaleResponseDiscarded`
   - 已取消请求在写入 pending 前返回，不留 waiter；
   - 迟到响应落在不存在的 request_id 或空 request_id 上安全丢弃；
   - 同一 request_id 的重复响应只投递一次。

fake 替身实现 `Send/Recv/Context/SendMsg/RecvMsg/SetHeader/SendHeader/SetTrailer`，通过接口断言保证与生成的 `ProcessPlugin_SessionServer` 签名一致。

### 验证

- `go vet ./...`：通过
- `gofmt`：通过
- `go test -count=1 ./...`：通过
- `go test -count=1 -race ./...`：通过
- `go build ./...`：通过

### 搁置项

- 真实 HTTP/2 网络栈下的极端流控场景（发送缓冲区完全填满）仍依赖 RPC context/server.Stop 打断，进程内替身无法完全模拟。
- cgroups v2、seccomp、网络隔离等沙箱能力不属于本切片。
- Windows 平台不支持 POSIX 进程组，仅终止直接子进程（`processGroupID` 恒为 0 自动回退）。
- 优雅 Shutdown 仍只发给直接子进程协议帧；若插件在收到 shutdown 后自行 fork 新进程，由进程组 SIGKILL 兜底回收。

### 验证

- `go vet ./...`：通过
- `gofmt`：通过
- `go test -count=1 ./...`：通过
- `go test -count=1 -race ./...`：通过

## 当前切片：Manifest Memory 资源限制接入（2026-09-24，已完成）

### 实施结果

1. `internal/process/process_group.go`
   - `applyMemoryLimit` 改为 RSS 轮询监控：200ms 采样 `/proc/<pid>/status` VmRSS，连续 5 次超限后调用回调终止进程；返回停止函数供 cleanup 结束 goroutine。
   - 瞬时峰值不触发终止，只有持续超限才杀，避免误伤正常插件。
2. `internal/process/process_group_unix.go`
   - `readProcessRSS` 解析 VmRSS 字段（KiB 转 bytes）。
3. `internal/process/process_group_windows.go`
   - `readProcessRSS` 返回错误，监控 goroutine 自然退出（Windows 无 /proc）。
4. `internal/process/grpc_transport.go`
   - `start` 启动内存监控，超限回调 `Kill()`；`stopMemoryMonitor` 存入 transport 字段，`cleanup` 中停止，不再用 defer（曾在 start 返回时立即停止导致监控失效）。
   - `memoryLimitBytes` 解析 Manifest `Limits.Memory`（B/KiB/MiB/GiB）。
5. `internal/process/testdata/helper/main.go`
   - `BOTFABRIC_HELPER_ALLOC_PROBE` 模式：分配并逐页触碰 `BOTFABRIC_HELPER_ALLOC_BYTES` 后驻留。
6. `internal/process/grpc_transport_integration_test.go`
   - `TestGRPCTransportEnforcesMemoryLimit`：64MiB 上限 + probe 触碰 512MiB → 进程约 1 秒被终止、Socket 清理；交叉验证正常握手不受 64MiB 影响。

### 方案演进（重要决策）

- **放弃 RLIMIT_AS**：实测 Go 运行时启动即需要大量虚拟地址空间（128MiB prlimit 下 `fatal error: failed to reserve page summary memory` 直接崩溃）。RLIMIT_AS 限制的是虚拟内存而非物理内存，对 Go/JVM 等运行时不可用。
- **改用 RSS 监控 + 超限终止**：读物理内存占用，对任意语言插件有效，语义与 systemd MemoryMax 类似。
- **probe 必须逐页触碰**：`make([]byte, 512MB)` 是惰性分配，不触碰则 RSS 仅 ~13MB，监控读不到超限。

### 遇到的问题（已解决）

- 初版把 `defer stopMemoryMonitor()` 放在 `start()` 里，函数返回即停止监控。改为 transport 字段 + cleanup 停止。
- 初版 probe 只分配不触碰，RSS 不增长，测试误判监控失效。

### 搁置项

- Windows 无 /proc，内存监控自动禁用（Manifest Memory 仅作配置记录）。

### 验证

- `go vet ./...`：通过
- `gofmt`：通过
- `go test -count=1 ./...`：通过
- `go test -count=1 -race ./internal/process/`：通过

## 当前切片：cgroup v2 memory.max 内核内存限制（2026-09-25，已完成）

### 实施结果

RSS 轮询只能看到直接子进程，且是事后杀。本切片在 Linux 上优先用 systemd transient service 的 `MemoryMax` 设置 cgroup v2 `memory.max`，属性在进程启动前生效并覆盖整棵进程树；不可用时回退 RSS 轮询。

1. `internal/process/cgroup_linux.go`（`//go:build linux`）
   - `startCgroupMemoryLimit`：用 `systemd-run --user` 启动 transient service，直接设置 `MemoryMax`、`MemorySwapMax=0`、`OOMPolicy=continue` 和 `KillMode=control-group`。systemd 把插件及 fork 出的所有后代放进同一 cgroup，内核在超限时 OOM kill 触发进程。
   - 插件环境写入权限为 `0600` 的 `EnvironmentFile`，避免 Session Token 出现在 `systemd-run` 命令行参数中。
   - stdout/stderr 追加到普通文件，`tailProcessLog` 轮询增量并按行转发，保持按插件 ID 记录日志的行为。
   - systemd、属性或环境文件任一前置条件失败都返回 nil，调用方回退 RSS 轮询。
2. `internal/process/cgroup_other.go`（`//go:build !linux`）：`startCgroupMemoryLimit` 恒返回 nil，`waitManagedProcess` 回退 `exec.Cmd.Wait`。
3. `internal/process/process_group.go`：`memoryEnforcer` 统一两种手段（stop 停止日志转发、reaper 终止托管服务、managed 标记进程归属）。
4. `internal/process/grpc_transport.go`：`start` 优先走 cgroup service（由它启动并托管进程），失败回退直接 exec + RSS 监控；`killProcess` 对托管服务调用 systemd stop；`waitProcess` 对托管进程用 PID 轮询（systemd 启动的进程不是 Runtime 子进程，`os.Process.Wait` 只返回 ECHILD）。

### 方案决策

- **不先启动再迁移 cgroup**：迁移前存在无约束窗口，而且用户 session scope 往往由 root 拥有，普通用户不能把其中的进程移出。systemd 在创建服务 cgroup 时直接应用 `MemoryMax`，启动及早期 fork 都受约束。
- **`OOMPolicy=continue`**：实测默认 `stop` 会在任一后代 OOM 时停止整个 transient service。`continue` 让内核只回收触发超限的进程，正常的插件主进程可继续运行。
- **`KillMode=control-group`**：停止服务时由 systemd 回收服务 cgroup 内的全部后代，避免只杀主进程留下孙进程。
- **日志用普通文件**：FIFO 在 systemd `append:` 下要么写端缺失导致插件 SIGPIPE（status=209/STDOUT），要么读端持有写端导致清理死锁。普通文件加轮询 tail 没有这些语义问题。
- **环境文件权限 `0600`**：`--setenv` 会把变量值放进 `systemd-run` 参数；EnvironmentFile 避免 Session Token 出现在进程参数中。

### 验证

- `TestGRPCTransportCgroupCoversDescendants`：64MiB 上限下 helper fork 出触碰 512MiB 的孙进程，内核 OOM 回收孙进程而主进程不受影响。
- 现有内存、进程组、握手测试在 cgroup 路径下全部通过（三次连跑）。
- `go test -count=1 -race ./internal/process/`：通过
- `go vet ./...`、`gofmt`、`go test ./...`：通过

### 搁置项

- 无用户委派 cgroup 的环境（无 systemd user manager、或 memory 控制器未委派）自动回退 RSS 轮询，只覆盖直接子进程。
- cgroup 路径依赖 `systemd-run --user`；容器内无 systemd 时同样回退。集成测试在该路径不可用时会清理插件并跳过 cgroup 专项断言，避免误触发大内存分配。

### 最终验证（2026-10-03）

- `go test -count=1 ./...`：通过
- `go test -count=1 -race ./...`：通过
- `go vet ./...`：通过
- `go build ./...`：通过
- `GOOS=windows GOARCH=amd64 go test -exec=true ./internal/process/`：通过（交叉编译验证）
- `gofmt`、`git diff --check`：通过
