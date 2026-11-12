# 生产化加固任务计划

- 状态：部分完成（验收范围内构建 / vet / 全量测试与 -race 全绿；下列长期项未实现）
- 开始：2026-11-11
- 最近验收：2026-11-12
- 目标：让 BotFabric 构建 / vet / 全量测试（含 -race）全绿，修复影响稳定性与生产部署的缺陷，补齐关键部署能力与文档。

## 范围
1. 基线：go build / go vet / go test（含 -race），记录失败。
2. 审计（5 个维度）：核心管线并发与正确性；OneBot/WebSocket 协议；进程插件与隔离安全；配置/运行时/权限；可观测性与生产部署就绪度。
3. 对抗性验证所有发现，剔除误报。
4. 分批修复已确认问题并补回归测试。
5. 最终全量验证，记录残余风险。

## 约束
- 不修改/弱化现有测试来"过关"。
- 不自动 commit。
- 修复保持现有代码风格，注释用中文。

## 最终验收结果（2026-11-12）

环境：Arch Linux，非 root（uid 1000），go1.27.3-X:nodwarf5 linux/amd64。

| 命令 | 结果 |
|------|------|
| `go build ./...` | 通过，exit 0 |
| `go vet ./...` | 通过，exit 0 |
| `go test ./... -count=1 -timeout 15m` | 全绿，exit 0；internal/process 约 12.0s，其余均 <0.6s |
| `go test -race ./... -count=1 -timeout 25m` | 全绿，无数据竞争，exit 0；tests/e2e 约 1.4s |
| `gofmt -l <changed go files>` | 无输出 |
| `git diff --check` | 无输出 |

跳过测试（均为条件性 skip，非失败）：
1. `internal/config` 的 4 个 `TestValidateManifestRejects*OnUnsupportedPlatform`：仅在非 Linux 断言，Linux 上按设计 Skip。
2. `internal/process` 的 `TestGRPCTransportCapabilityBoundingSetIsolation`：宿主 systemd --user 不允许收窄 capability bounding set（systemd-run 返回 status=218/CAPABILITIES），环境限制。

## 已完成的修复批次

### 第 1 批：协议正确性、会话生命周期与有界状态
- 数字型 `group_id` 精确保留：OneBot envelope 使用 `json.Decoder.UseNumber`，`stringParam` 支持 `json.Number`，避免大整数群号被格式化为科学计数法。`internal/onebotapp/adapter.go`。
- 外部进程会话不再在运行期按墙钟过期：`validateRequestSession` 只校验凭据与连接绑定；TTL 仅约束启动握手。`internal/process/grpc_transport.go`。
- 新增 `Loader.SetSessionTTL`（当前未接入运行时配置文件，默认 5m）。`internal/process/loader.go`。
- 应用端 `pending/recent` 增加 TTL（默认 5m）与容量上限（默认 1024），写入失败回滚登记，后台按 TTL/2 清理。`internal/onebotapp/adapter.go`。

### 第 2 批：内存有界、身份防伪与消息上限
- `MemoryStore` 惰性清理过期条目 + 默认 65536 条目上限与最早过期淘汰。`internal/dedup/dedup.go`。
- 服务入口一律使用网关签发的 `request.Caller` 作为应用身份，忽略载荷中的 `application_id`。`internal/response/coordinator.go`。
- 修复 `connection.id` 与连接表键的数据竞争：身份迁移在 `connMu` 写锁内完成，锁外读取统一走 `endpointID()` / `connTarget` 快照。`internal/onebotapp/adapter.go`。
- 应用端 WebSocket 增加 `max_message_size`（默认 4MiB，上限 64MiB），配置校验与示例接线。`internal/config/application_onebot.go`、`cmd/server/main.go`、`config/runtime.example.yaml`。

### 第 3 批：外部进程安全与故障回滚
- 外部插件异常退出经 `GRPCTransport.Done()/ExitError()` 回传 `Manager.NotifyPluginExit`，回滚服务/订阅/端点并迁移为 `failed`；死信存储有界。`internal/process/grpc_transport.go`、`internal/process/plugin.go`、`internal/process/loader.go`、`internal/runtime/manager.go`、`cmd/server/main.go`、`internal/eventbus/eventbus.go`。
- Unix Socket 使用 `SO_PEERCRED` 校验对端 PID，只接受 transport 启动的插件进程（systemd 路径为 MainPID）。`internal/process/peercred_linux.go`、`internal/process/peercred_other.go`。
- 外部插件环境改为最小白名单，不再继承 `os.Environ()`，防止 LLM API key 等宿主秘密泄露；`ExtraEnv` 作为运维扩展点。`internal/process/grpc_transport.go`。
- 补齐 `pending/recent` 有界回归断言（实现见第 1 批）。`internal/onebotapp/adapter_test.go`。

### 第 4 批：热重载原子性、稳定身份与死信治理
- 热重载使用捕获式暂存端点/分类器，切换时迁移并在切换后 swap；旧宿主 `fenced` 防止 Stop 误删新资源。`internal/runtime/staging.go`、`internal/runtime/manager.go`、`internal/routing/router.go`。
- 重载失败不再提前替换线上路由规则：`Router.ValidateRules` 仅编译，`RegisterRules` 在切换成功后执行；失败清理带超时并支持 `ForceStop`。`internal/runtime/manager.go`、`internal/routing/router.go`。
- 动作调用使用服务端固定主体 `application.onebot`（`DispatchSubject` 不序列化），应用端稳定身份限定 `app:` 命名空间且禁止覆盖其他 owner 端点；策略示例与文档同步。`pkg/contract/routing.go`、`internal/response/coordinator.go`、`internal/onebotapp/adapter.go`、`config/policy.example.yaml`、`docs/03-开发指南.md`。
- 死信存储增加默认 8MiB 字节上限、载荷 4KiB/错误 1KiB 截断、Header 白名单、最旧淘汰与 `Dropped()`；管理 API 支持 `offset/limit`（默认 100，硬上限 200，新增 `EntriesPage`）。`internal/eventbus/eventbus.go`、`internal/management/server.go`、`cmd/server/main.go`。

### 观测性
- 新增 `internal/observability`：原子计数器、Prometheus 文本输出与独立只读 `/metrics` 服务；配置 `metrics_enabled` + `metrics_listen_address` 显式启用，启用但缺地址时拒绝启动。指标覆盖事件发布/拒绝/去重/路由丢弃、投递成败/死信、投递计划模式、分类器回退、候选回复采纳/丢弃、插件启动失败。
- README / 02 / 03 已同步指标语义、无认证与进程内累计边界。

## 未完成 / 有意跳过

1. 外部插件异常退出后的自动退避重启：当前只迁移为 `failed` 并回滚资源，恢复依赖上层编排或运维，未引入自动重启策略。
2. 每外部插件独立 UID / user namespace，以及会话令牌经继承 fd 传递：属部署架构级改造，当前以 `SO_PEERCRED` PID 绑定作为同 UID 防抢占的替代。
3. 细粒度 egress allowlist、可写路径白名单、非 Linux 平台的等价隔离：`restrict_address_families` 仅粗粒度限制 `AF_UNIX`。
4. `/metrics` 认证与外部 tracing 导出：`tracing_enabled` 仍为保留配置，TraceID 只贯穿事件与日志。

## 残余风险（具体）

1. **SO_PEERCRED 精确 PID 匹配**：若插件入口是 fork 子进程连接的包装脚本（非 `exec`），连接 PID 与启动 PID 不一致会被拒绝，插件无法握手；需改为入口直连，或后续增加 cgroup 成员校验回退。
2. **环境白名单可能破坏依赖宿主变量的插件**：`LD_LIBRARY_PATH`、`PYTHONPATH`、代理变量等不再继承；`GRPCTransportConfig.ExtraEnv` 存在但尚未从运行时配置文件接线，运维当前无法通过配置补充。
3. **热重载后失败通知的进程插件**：重载产生的新 `runtime.managedPlugin` 包装正常，但异常退出回调仍由 `process.Loader` 的 failureHandler 驱动；若未来新增其他 `Transport` 实现，必须同时实现 `Done()/ExitError()`，否则不会触发回滚。
4. **去重淘汰牺牲一次判定**：条目达到 65536 上限时淘汰最早过期键，极端高基数场景可能放行一次重复事件；这是有界内存换取的可用性取舍。
5. **死信淘汰不可观测**：`Dropped()` 已公开但未接入 `/metrics`，容量打满后的丢弃只能通过管理 API 查询或代码观测。
6. **会话令牌仍写入 systemd EnvironmentFile**：文件权限 0600 且位于私有目录，但同 UID 进程可读；`SO_PEERCRED` 已阻断其冒用连接，令牌本身仍不是独立进程边界。
7. **`Loader.SetSessionTTL` 未接入配置**：当前固定默认 5m；运行期已不受 TTL 影响，但如需调整握手窗口，需要新增配置字段并接线。
8. **指标无认证、无持久化**：绑定 `0.0.0.0` 即对网络暴露只读指标；多实例需在抓取侧聚合，重启归零，且缺少延迟直方图。
9. **应用端稳定身份改用 `app:` 命名空间**：旧部署若用裸 `self_id` 或插件 ID 作为稳定 ID，将被静默忽略并退化为每连接新 ID；升级说明应提示改为 `app:<id>`。

## 验证备注
- 验收为只读执行 + 计划/README 文档更新；未执行 git commit/reset/checkout。
- 未删除测试、未新增 `t.Skip`、未放宽既有断言。
- 未发现调试残留或提交密钥；diff 中的 `secret`/`token` 字样均为测试哨兵值。
