# Linux 外部进程插件 PrivateTmp 隔离

**状态**：已完成
**开始日期**：2026-10-11
**完成日期**：2026-10-11

## 目标与安全语义

- 新增显式 opt-in 的 Manifest `limits.private_tmp`，默认 false。
- Linux `runtime: process` 通过 systemd transient service 设置 `PrivateTmp=yes`，令插件的 `/tmp` 与 `/var/tmp` 视图隔离。
- systemd 或 PrivateTmp 属性不可用时 fail-closed；不得裸 exec 或退回 RSS 监控并以无隔离状态启动。memory-only 的 RSS fallback 保持不变。
- 非 Linux 平台在 Manifest 校验和进程启动层拒绝该设置。
- PrivateTmp 改变 `/tmp`、`/var/tmp` 的挂载视图。Runtime Unix Socket 位于任一目录或其子目录时，插件可能无法访问宿主 listener，因此启动前拒绝此类 Socket 路径，并解析目录符号链接避免绕过。
- 未声明 PrivateTmp 的插件维持既有行为。

## 实施内容

1. 在契约和严格 Manifest 解码中增加 `PrivateTmp`，加入 runtime/platform 校验和测试。
2. 将配置传至 systemd 启动路径，按需增加 `PrivateTmp=yes`；在任何 isolation request 失败时 fail-closed，保留 memory-only RSS fallback。
3. 为 `/tmp`、`/var/tmp` Socket 路径及 symlink 绕过增加测试；扩展 fake systemd 参数与失败清理测试。
4. 扩展 helper 和 Linux systemd 集成测试，验证插件能在私有 `/tmp` 写临时文件、看不到宿主 `/tmp` 与 `/var/tmp` 标记，同时 Unix Socket Session 可连接。
5. 更新 README、架构设计与开发指南，记录启用方式及 Socket 路径约束。

## 实施结果

- `contract.Limits.PrivateTmp` 和严格 manifest YAML 解码支持 `limits.private_tmp`；校验仅允许 Linux 的 `runtime: process`，非 Linux 配置和启动层均拒绝。
- systemd transient service 按需设置 `PrivateTmp=yes`；PrivateTmp 与 PrivateNetwork、NoNewPrivileges、memory limit 可组合。所有隔离请求失败时 fail-closed，memory-only 仍保留 RSS fallback。
- PrivateTmp 模式下 systemd 环境文件和日志目录优先放入 `XDG_RUNTIME_DIR`，否则使用 `$HOME`；若两者均不可用或解析后位于 `/tmp`/`/var/tmp`，启动失败，避免托管文件被私有挂载遮蔽。
- Runtime Unix Socket 路径位于 `/tmp` 或 `/var/tmp`（含子目录）时在启动前拒绝，并检查目录 symlink 目标。
- 假 systemd 参数测试覆盖单独 PrivateTmp、属性组合和失败清理；socket 路径测试覆盖两个临时目录、边界相似路径与 symlink。
- 真实 systemd 集成测试验证 `/tmp` 和 `/var/tmp` 中的宿主标记不可见、插件可分别向两处写入，并能通过位于 `/dev/shm` 的 Unix Socket 完成 Session 握手。
- README、架构设计和开发指南记录 `private_tmp` 的 opt-in 用法、边界和 Unix Socket 目录约束。

## 验证

- `gofmt`：通过。
- `go test -count=1 ./internal/config ./internal/process`：通过，真实 PrivateTmp systemd 集成测试通过。
- `go test -count=1 ./...`：通过。
- `go test -count=1 -race ./internal/process ./internal/config`：通过。
- `go vet ./...`：通过。
- `GOOS=windows GOARCH=amd64 go test -exec=true ./internal/process ./internal/config`：通过。
- `git diff --check`：通过。

## 未决事项

- 无；systemd user manager、`/dev/shm` 或临时目录权限不可用的环境会跳过真实集成测试，但参数生成和 fail-closed 单元测试仍强制执行。
