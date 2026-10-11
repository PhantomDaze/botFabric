# 外部进程插件 syscall 过滤

**状态**：已完成
**开始日期**：2026-10-11
**完成日期**：2026-10-11

## 目标

为 Linux 外部进程插件增加一个独立 opt-in 的系统调用过滤限制，使用 systemd `SystemCallFilter=` 施加固定、文档化的拒绝组；不允许 Manifest 透传任意 systemd 属性或系统调用表达式。宿主不支持或无法应用过滤时拒绝启动，不静默降级。

## 执行计划

1. 在公共 Manifest `limits` 和严格 YAML 解析/校验中增加 `restrict_syscalls` 布尔字段；仅允许 Linux `process` runtime 使用。
2. 将该设置接入 Linux transient service 启动路径，并纳入“需隔离时 fail-closed”的逻辑；非 Linux 显式请求时拒绝启动。
3. 为 systemd 参数组装、manifest 解析/校验和非 Linux 行为补充测试；运行相关测试及完整 Go 测试、race、vet。
4. 更新架构、开发指南和 README，说明固定过滤组、兼容性边界及不等同完整沙箱。

## 验收标准

- 未声明时行为不变；声明后 systemd service 必须应用固定 syscall filter。
- systemd 不可用或启动失败时插件不能无过滤启动。
- in-process 和非 Linux 配置被拒绝。
- 测试与文档准确限定过滤范围，不宣称 syscall allowlist 或完整沙箱。

## 验证结果

- `go test ./...`：通过；最后补入真实集成探测后，再运行完整测试确认。
- `go test -race ./...`：通过；最后补入真实集成探测后，再运行竞态测试确认。
- `go vet ./...`：通过；最后补入真实集成探测后，再运行静态检查确认。
- 定向集成测试已通过：systemd transient service 启动 helper 后，`/proc/<pid>/status` 的 `Seccomp_filters` 数量高于测试进程基线，证明 systemd 额外安装了过滤器。
- `systemd-analyze syscall-filter` 已确认固定类别在当前 systemd 中可识别。
- `git diff --check`：通过。

## 待记录事项

- syscall 拒绝类别固定为 `@clock`、`@debug`、`@module`、`@mount`、`@obsolete`、`@raw-io`、`@reboot`、`@swap`、`@cpu-emulation`；Manifest 不接受自定义 systemd 参数或 syscall expression。
- 拒绝组可能影响依赖这些调用的插件；这是 opt-in 限制，启用前需进行目标运行时兼容性验证。
- 此功能不提供 syscall allowlist、细粒度网络策略、capability 白名单或完整沙箱。
