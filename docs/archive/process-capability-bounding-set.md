# Linux 外部进程插件 capability bounding set

**状态**：已完成（本地实现与测试；当前宿主 user manager 拒绝实际收窄，集成探针按条件跳过）
**开始日期**：2026-10-12
**完成日期**：2026-10-13

## 目标

为 Linux `runtime: process` 插件提供显式 opt-in 的 systemd `CapabilityBoundingSet=` 配置，允许 Manifest 限定插件进程及其后代在 exec 时可保留的 capability 集合。默认不改变行为；非 Linux 平台拒绝配置；隔离属性不能应用时拒绝启动，不回退到无隔离启动。

## 执行结果

- 公共契约 `contract.Limits` 增加 `CapabilityBoundingSet []string`；由于该字段使 `Limits` 不可比较，内置加载器的清单一致性检查由 `!=` 改为 `reflect.DeepEqual`。
- 严格 YAML 解析与校验：`capability_bounding_set` 仅允许 `CAP_*` 大写名称、拒绝空名与重复；`nil` 表示未配置（不设置属性），显式空列表表示空 bounding set；in_process runtime 与非 Linux 平台拒绝该配置。
- Linux 启动路径将集合映射为 systemd `CapabilityBoundingSet=`，非 Linux 保持 fail-closed；`isolationRequested` 统一判定是否需要 systemd transient service。
- 修正启动等待逻辑：`waitServiceMainPID` 不再仅看到 `MainPID` 就返回，而是确认 PID 存活（`kill(pid, 0)`，容忍 `EPERM`）；systemd 异步启动后立即失败的服务不再被当作成功启动。
- 测试：`TestStartTransientServiceCapabilityBoundingSet`（参数构造，含空集合）、`TestWaitServiceMainPIDWaitsForProcessAfterTransientUnitCreated`、`TestStartCgroupIsolationFailsClosed`（新增 capability 用例）、配置解析/校验用例、`TestGRPCTransportCapabilityBoundingSetIsolation`（同步 `systemd-run --wait` 探针读取 `CapBnd`）。
- 文档：README、架构设计、开发指南补充配置字段、语义（bounding set 仅限制 exec 可获 capability 上界，不撤销已有 capability）与宿主依赖（user manager 需非特权 user namespace 支持）。

## 验证

- `go build ./...`：通过。
- `go test ./...`：通过。
- `go test -race ./internal/process ./internal/config ./internal/runtime`：通过。
- `go vet ./...`：通过。
- `GOOS=windows GOARCH=amd64 go test -exec=true ./internal/process ./internal/config`：通过。
- `git diff --check`：通过。

## 边界与未决事项

- 当前宿主 user systemd manager 返回 `218/CAPABILITIES`，无法实际收窄 capability；`TestGRPCTransportCapabilityBoundingSetIsolation` 在该条件下按设计 skip，未在真机验证能力确实生效。需要在允许该属性的环境（非特权 user namespace 可用）重新运行以获得端到端证据。
- 仅限制 capability bounding set，不是独立 UID、文件系统沙箱或网络策略，也不自动撤销进程已有的有效 capability。
- 不硬编码内核 capability 名单：格式校验拦截常见错误，未知名称由 systemd 在启动时 fail-closed 拒绝。
