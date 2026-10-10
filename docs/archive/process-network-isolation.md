# Linux 外部进程插件网络隔离

**状态**：已完成
**完成日期**：2026-10-09
**范围**：在现有 systemd transient service/cgroup v2 托管路径上增加显式 opt-in 的 PrivateNetwork 网络命名空间隔离；不默认改变插件网络行为，不实现任意 egress allowlist、seccomp 或非 Linux 隔离。

## 实施结果

- `contract.Limits.PrivateNetwork` 和严格 YAML manifest 解码现支持 `limits.private_network`，默认为 false；Manifest 校验仅允许 Linux 的 `runtime: process` 使用。
- Linux 在 systemd transient service 启动参数中设置 `PrivateNetwork=yes`。当隔离请求遇到 systemd、user manager 或 namespace 错误时，transport 返回启动错误并清理 Socket，不会裸 exec 降级；普通 memory-only 限制失败仍沿用 RSS 回退。
- 非 Linux 启动层也显式返回隔离不可用；配置校验在非 Linux 目标拒绝该设置。
- 增加假的 systemd-run 参数、fail-closed 清理、memory-only 回退和真实 systemd 网络隔离集成测试。真实集成测试已通过，helper 在私有网络中成功经 Unix Socket 连接 Runtime，同时无法连接宿主 loopback listener。
- README、架构设计、开发指南已说明 opt-in 配置、隔离影响与当前边界。

## 验证

- `go test ./internal/config ./internal/process`：通过。
- `go test -count=1 ./internal/process -run 'TestGRPCTransportPrivateNetworkIsolation|TestStartTransientServiceRequestsPrivateNetworkWithoutMemoryLimit|TestStartCgroupMemoryLimit' -v`：通过，真实 `PrivateNetwork` 隔离集成测试通过。
- `go test ./...`：通过。
- `go test -race ./internal/process ./internal/config`：通过。
- `go vet ./...`：通过。
- `GOOS=windows GOARCH=amd64 go test -exec=true ./internal/process ./internal/config`：通过。
- `gofmt`、`git diff --check`：通过。

## 实施步骤

1. 扩展 `contract.Limits`、严格 YAML manifest 解码和校验，增加 `PrivateNetwork`。
2. 将 network-isolation 要求传入 `startCgroupMemoryLimit` / systemd transient service 启动逻辑；启动失败时返回可诊断错误并清理临时目录，不回退无隔离路径。
3. 保持 memory-only 的既有回退语义与不声明隔离的默认行为。
4. 添加配置校验、systemd 参数/启动路径以及不可用时 fail-closed 的测试；覆盖启动失败资源清理。
5. 更新 README、架构设计和开发指南，说明网络命名空间的影响和 opt-in 用法。
6. 完成后将本文归档至 `docs/archive/process-network-isolation.md`。

## 风险与边界

- `PrivateNetwork` 为插件创建独立网络命名空间，隔离宿主网络接口及宿主 loopback；插件自身 loopback 仍存在，但宿主接口和服务不可直接访问。该能力不提供细粒度出站 allowlist；需要访问的外部服务代理、按域名 egress policy 和 seccomp 留待独立切片。
- 系统级 systemd 可接受 `PrivateNetwork=yes`；此宿主已实际验证 user transient service 启动和访问阻断。其他宿主的 systemd、内核 namespace 或权限可能不同，opt-in 插件在此类失败下 fail-closed。
- 当前工作树已存在与本切片无关的分类器、benchmark 和文档改动；未覆盖或提交这些既有改动。
