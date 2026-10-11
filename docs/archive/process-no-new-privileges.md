# Linux 外部进程插件禁止新增特权

**状态**：已完成
**完成日期**：2026-10-11
**范围**：在现有 systemd transient service 启动路径增加显式 opt-in 的 `NoNewPrivileges=yes`，禁止插件及其后代通过 setuid/setgid/setcap exec 获得新增特权。不默认改变进程行为，不实现 seccomp、capability 白名单、UID 隔离或 egress 策略。

## 安全语义

- 新增 Manifest `limits.no_new_privileges` 布尔字段，默认 `false`。
- 仅 Linux 的 `runtime: process` 可请求；其他 runtime/平台在 manifest 校验时拒绝，避免声明但未生效。
- 请求时必须通过 systemd transient service 设置 `NoNewPrivileges=yes`；缺少 systemd/user manager 或属性不支持时启动失败，不允许裸 exec/RSS 回退。
- 对未请求 NNP 的插件保持现有启动路径不变；和 `private_network`、memory limit 可组合，且不应因单独 NNP 请求而强制附加 memory.max。

## 实施步骤

1. 扩展 `contract.Limits` 与严格 Manifest YAML 解码、运行时/平台校验。
2. 将 NNP 要求传给 systemd 启动逻辑，组合 private network 与 memory 属性；NNP 启动失败 fail-closed，memory-only 失败保留 RSS 回退。
3. 添加 manifest、参数生成、NNP fail-closed、memory-only 回退及 option composition 测试。
4. 更新 README、架构设计、开发指南，并注明它不阻止已有特权、不限制网络或系统调用。
5. 全量测试、race、vet、Windows 交叉编译和 diff 检查通过后归档本计划。

## 实施结果

- `contract.Limits.NoNewPrivileges` 与严格 YAML manifest 解码现支持 `limits.no_new_privileges`，默认 false；校验仅允许 Linux 的 `runtime: process` 使用。
- Linux transient service 在请求时加 `NoNewPrivileges=yes`。NNP 单独使用时不附加 `MemoryMax`；与 `PrivateNetwork`、memory limit 组合时按请求叠加属性。
- NNP 或网络隔离启动失败时 fail-closed，memory-only 仍回退 RSS；非 Linux 平台在 Manifest 和平台启动层均拒绝 NNP。
- 假 systemd-run 测试覆盖属性参数、独立 NNP、不隐式开启内存/网络限制、失败资源清理、fail-closed 与旧 memory 回退语义。
- 真实 systemd 集成测试启用 `NoNewPrivileges=yes` 和 `PrivateNetwork=yes`，通过 `/proc/<pid>/status` 断言 `NoNewPrivs: 1`，并验证 helper 无法连接宿主 loopback listener；Unix Socket Session 仍可工作。
- README、架构设计与开发指南说明 opt-in 行为与 NNP 的安全边界。

## 验证

- `go test ./internal/config ./internal/process`：通过。
- `go test -count=1 ./internal/process -run 'TestGRPCTransportPrivateNetworkIsolation|TestStartTransientServiceNoNewPrivilegesWithoutMemoryLimit|TestStartTransientServiceRequestsIsolationWithoutMemoryLimit|TestStartCgroupIsolationFailsClosed|TestStartCgroupMemoryLimitKeepsMemoryFallback' -v`：通过。
- `go test ./...`：通过。
- `go test -race ./internal/process ./internal/config`：通过。
- `go vet ./...`：通过。
- `GOOS=windows GOARCH=amd64 go test -exec=true ./internal/process ./internal/config`：通过。
- `gofmt`、`git diff --check`：通过。

## 安全边界

- NNP 仅阻止 exec 转换获得新特权，不会撤销已有 UID、capabilities 或其他权限。
- NNP 不等同 seccomp，不限制系统调用，也不限制网络访问；seccomp、capability bounding set、独立 UID 与策略化 egress 留待后续切片。
- 此切片只修改进程限制契约、配置校验、Linux process transport、相关测试和三份用户文档，没有覆盖其他工作树文件。
