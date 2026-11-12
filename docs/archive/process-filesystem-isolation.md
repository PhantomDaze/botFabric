# Linux 外部进程插件文件系统隔离

**状态**：已完成（属性构造与宿主强制验证通过；Runtime 端到端用例在本沙箱跳过）
**开始日期**：2026-11-07
**完成日期**：2026-11-08

## 目标

继续外部进程插件沙箱方向，补齐 README 中“高级沙箱”缺少的文件系统维度：为 Linux `runtime: process` 插件提供显式 opt-in 的 `protect_home` 与 `protect_system`，分别映射到 systemd `ProtectHome=yes` 与 `ProtectSystem=full`。默认行为不变；非 Linux 或非外部进程 runtime 拒绝配置；属性不可用时 fail-closed，不降级为无隔离启动。

选择固定值而非开放任意 systemd 属性，沿用 `restrict_address_families`（固定 `AF_UNIX`）与 `restrict_syscalls`（固定拒绝组）的审慎取值原则：`ProtectHome=yes` 给出“家目录完全不可见”的强语义，`ProtectSystem=full` 只读 `/usr`、`/boot`、`/etc` 而保留插件工作目录可写；不以 `strict` 让整个根文件系统只读，避免破坏常规插件。

## 实施结果

- 公共契约 `contract.Limits` 新增 `ProtectHome`、`ProtectSystem`；严格 YAML 解码支持 `limits.protect_home`、`limits.protect_system`，并纳入 `requestsProcessIsolation` 的 runtime/平台校验。
- `processIsolation` 增加对应字段，`isolationFromManifest` 与 `requested()` 同步；Linux 启动参数追加 `ProtectHome=yes`、`ProtectSystem=full`，非 Linux 保持 fail-closed。
- `ProtectHome=yes` 会隐藏 `/home`、`/root`、`/run/user`，而插件需要通过 Runtime 监听的 Unix Socket 连接；因此在启动前校验 Socket 目录（含符号链接解析结果）不得位于这三个路径下，与既有 `private_tmp` Socket 路径校验复用路径解析逻辑。插件入口位于家目录时会在 systemd exec 阶段失败，同样 fail-closed。
- 测试：参数构造（`TestStartTransientServiceProtectsFilesystem`）、fail-closed 表格新增两项、Manifest 解析与 in-process/非 Linux 拒绝、Socket 路径拒绝与符号链接用例、宿主 systemd 强制探针（`TestSystemdFilesystemProtectionProbe`：`/home` 不可读、`/usr` 只读）、完整 Runtime 集成用例（连接 Session 并观察 helper 探针日志）。
- helper 新增 `BOTFABRIC_HELPER_PROBE_FILESYSTEM` 探针；文档更新 README、架构设计、开发指南与当前限制。

## 验证

- `gofmt -l`：无输出；`go vet ./...`：通过。
- `go test ./internal/process -run 'TestStartTransientServiceProtectsFilesystem|TestStartCgroupIsolationFailsClosed|TestSystemdFilesystemProtectionProbe|TestValidateProtectHome|TestStartProtectHome'`：通过。
- `go test ./...`（`internal/process` 除外）：全部通过。
- `go test -race`（新增隔离测试与 config）：通过。
- 交叉编译 Windows amd64、macOS arm64：通过。
- **宿主强制验证**：直接 `systemd-run --user --wait --pipe -p ProtectHome=yes` 下 `ls /home` 返回权限不足；`-p ProtectSystem=full` 下 `/usr`、`/etc` 挂载变为 `ro`；`TestSystemdFilesystemProtectionProbe` 在本机真实通过。
- **Runtime 端到端用例在本沙箱跳过**：`TestGRPCTransportFilesystemIsolation` 走 `systemd-run --no-block`，本沙箱的非阻塞模式连接用户管理器私有 socket 返回 `No data available`；这是环境限制，既有的地址族集成测试同样跳过。参数构造、fail-closed、路径校验与 systemd 强制语义均有独立测试覆盖。
- `internal/process` 仍有 3 个既有 `privateTmp` 用例因 `/run/user/1000`、`/home` 只读失败，与本次改动无关（基线同样失败）。

## 边界与未决事项

- 粗粒度文件系统保护，不是按路径的可读/可写白名单：`protect_home` 隐藏整个家目录与 `/run/user`，`protect_system` 仅只读系统目录；插件仍可写入 `/opt`、`/srv`、`/var` 与工作目录。
- 需要读取家目录配置或把插件安装在 `/home` 下的部署不适用 `protect_home`；Socket 目录位于 `/home`、`/root`、`/run/user` 时会在启动前被拒绝。
- 仍不提供细粒度 egress allowlist、可写路径白名单或其他平台等价隔离；这些继续留在 README 的未完成项中。
