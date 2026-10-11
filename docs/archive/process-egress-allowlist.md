# Linux 外部进程插件 egress allowlist

**状态**：已调查，因宿主能力限制未实施
**开始日期**：2026-10-11
**结束日期**：2026-10-11

## 目标

为 Linux 外部进程插件提供显式 opt-in 的 CIDR egress allowlist，沿用 systemd transient service 管理路径。默认行为不变；隔离请求不支持或无法应用时拒绝启动，不裸 exec 降级。能力边界需明确说明，避免将 IP allowlist 描述为域名策略或完整沙箱。

## 执行计划

1. 检查现有进程隔离、Manifest 解码/校验及测试路径，确定宿主可用的强制机制。
2. 扩展公共契约和严格 Manifest 配置，验证 CIDR、平台与 runtime 约束。
3. 将网络 allowlist 接入 Linux 启动隔离路径，确保不可用时 fail-closed，并覆盖地址绕过与启动失败资源清理。
4. 更新架构、开发指南和 README，说明语义、配置及限制。
5. 运行格式化、相关测试、全量测试、race、vet 和交叉编译检查。

## 调查结论

- 当前隔离路径通过 `systemd-run --user` 启动 user transient service。
- 本机 systemd 262 的 `systemd.resource-control(5)` 明确说明 `IPAddressAllow=` / `IPAddressDeny=` 当前只适用于 system service，不适用于 per-user service。
- 本机执行 `systemd-run --user --wait --collect --property=IPAddressDeny=any /bin/true` 返回成功，但这不构成属性实际限制网络流量的证据；为避免产生无效的安全承诺，不增加配置字段，也不接受此实现。
- 本次只运行了只读能力探测及命令验证，未更改沙箱代码或用户配置。

## 后续方向

- 需在有可验证强制机制（例如由受信任 system manager 托管，或独立网络策略执行器）的环境中重新设计；user service 当前不支持 systemd IP 过滤。
- 继续其他明确未完任务前，优先按 README 检查真实分类器兼容矩阵与路由质量评估；项目当前集成是 OpenAI-compatible，任何更换为 Claude API 的改动需先明确授权。
