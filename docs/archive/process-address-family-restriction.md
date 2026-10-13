# Linux 外部进程插件地址族限制

**状态**：已完成（本机端到端验证通过）
**开始日期**：2026-10-13
**完成日期**：2026-10-13

## 目标

为 Linux `runtime: process` 插件提供显式 opt-in 的 systemd `RestrictAddressFamilies=` 限制，映射到固定值 `AF_UNIX`，使插件只能创建 Unix 域套接字、无法创建 TCP/UDP 等网络套接字。默认不改变行为；非 Linux 平台拒绝配置；隔离属性不能应用时拒绝启动，不回退到无隔离启动。

选择本切片而非 README 列出的细粒度 egress allowlist，是因为实测确认 `IPAddressAllow=`/`IPAddressDeny=` 仅适用于 system service，而 `RestrictAddressFamilies=` 在 per-user service 上可用并可端到端验证。

## 执行结果

- 公共契约 `contract.Limits` 增加布尔开关 `RestrictAddressFamilies`，与既有 `private_network`/`restrict_syscalls` 等保持同构。
- 严格 YAML 解析与校验：in_process runtime 与非 Linux 平台拒绝该配置；把重复的隔离条件提取为 `requestsProcessIsolation` 辅助函数。
- Linux 启动路径映射为 systemd `RestrictAddressFamilies=AF_UNIX`（固定值，不开放任意地址族列表或 systemd 参数，沿用 `restrict_syscalls` 的“固定、经审慎选择的值”原则）；非 Linux 保持 fail-closed。
- 顺带收敛启动参数：新增第 6 个隔离标志后位置布尔参数已过多且易错，改为 `processIsolation` 结构体承载，`grpc_transport.go` 新增 `isolationFromManifest` 与 `requested()`；`cgroup_linux.go`/`cgroup_other.go`/测试同步更新。
- 测试：参数构造（`TestStartTransientServiceRestrictsAddressFamilies`）、fail-closed 表格新增用例、配置解析与校验用例、集成测试 `TestGRPCTransportRestrictAddressFamiliesIsolation`（helper 新增 `BOTFABRIC_HELPER_PROBE_ADDRESS_FAMILY` 探测）。
- 文档：README、架构设计、开发指南补充配置字段、语义与限制边界。

## 验证

- `go build ./...`：通过。
- `go test ./...`：通过。
- `go test -race ./internal/process ./internal/config ./internal/runtime`：通过。
- `go vet ./...`：通过。
- `GOOS=windows GOARCH=amd64`、`GOOS=darwin GOARCH=arm64` 交叉编译测试：通过。
- `git diff --check`：通过。
- **本机端到端验证**：`TestGRPCTransportRestrictAddressFamiliesIsolation` 真实通过（非跳过）——插件在 `RestrictAddressFamilies=AF_UNIX` 下启动、Unix Socket Session 握手成功，且 helper 创建 TCP 套接字被内核拒绝（`address family probe: blocked`）。另有独立 `systemd-run` 探针确认同宿主上 `AF_INET` 被拒、`AF_UNIX` 可用。

## 边界与未决事项

- 粗粒度地址族限制，不是 egress allowlist：不能按域名/IP 放行个别目标，与 `private_network`、未来 egress allowlist 互补而非替代。
- 需要 Unix Socket 之外地址族的插件不适用。
- 细粒度 egress allowlist 仍受 per-user systemd 不支持 `IPAddressAllow/Deny` 限制（见 `process-egress-allowlist.md`），需在具备可验证强制机制的环境重新设计。
