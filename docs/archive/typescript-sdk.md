# TypeScript 外部进程插件 SDK

**状态**：已完成  
**开始日期**：2026-10-03  
**完成日期**：2026-10-04

## 目标与范围

新增 Node.js/TypeScript 外部进程插件 SDK，使用 Runtime 注入的 Unix Socket gRPC 双向 Session，保持与 `proto/botfabric/v1/process.proto`、Go/Python transport 的 Struct 帧 wire 格式兼容。不实现同进程插件、Windows transport、高级沙箱或生成代码。

## 实施结果

- 新增 `sdk/typescript` npm 包，声明 Node.js >=20，使用 `@grpc/grpc-js`；Struct protobuf wire 编解码不要求消费者生成绑定。
- `SessionClient` 支持 Runtime 握手、事件订阅与 ACK、远程服务、Host Publish/Invoke、Health/Shutdown、超时与关闭清理。
- 入站回调独立执行，可从回调内再次调用 Host；可选 `AbortSignal` 在关闭或流断开时触发。
- 新增最小插件文档和 Unix Socket 回环测试，覆盖握手、双向调用、Base64 payload、事件 ACK、Health/Shutdown、回调反向调用 Host 与 timeout/close。
- 更新 README、架构设计和开发指南，将 TypeScript SDK 从后续切片移至已实现范围。

## 验证记录

- Node.js 26.10.0 / npm 12.1.0：`npm run build`、`npm run typecheck`、`npm test`（3 项通过）、`npm pack --dry-run` 均通过。
- `git diff --check` 通过；仓库根目录 `go test ./...` 全部通过。
- 未覆盖 Node.js 20 最低版本及其他 Unix 平台矩阵；Windows transport 不在范围内。

## 完成后限制

- 仅支持 Node.js 外部进程与 Unix Socket；不提供同进程插件、Windows transport 或高级沙箱。
- LLM 真实模型 API、高级沙箱和性能基准仍是后续项目切片。
