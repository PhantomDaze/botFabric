# Python 外部进程插件 SDK

## 状态

- 状态：已完成
- 日期：2026-10-03

## 目标

为 Python 外部进程插件提供轻量 SDK，封装 Unix Socket 双向 gRPC Session 的连接、身份握手和常用事件/服务帧，减少插件作者直接处理协议细节的负担。

## 实施计划

1. 对照 Go SDK、ProcessPlugin proto 和 Go transport 的 wire 语义，确定 Python SDK 的公开接口与依赖。
2. 新增最小可安装 Python 包及 Session client，使用动态 Struct 流避免要求消费者生成项目专属 protobuf 代码。
3. 为握手、请求/响应、取消/关闭和错误路径添加可重复的测试，并补充 Python 插件接入文档。
4. 运行 Python 测试、格式/静态检查（若项目已有对应工具）及 Go 回归测试，记录未覆盖的平台边界。

## 范围约束

- 本切片只覆盖外部进程 gRPC Session；不实现 Python 同进程插件装载、沙箱或 TypeScript SDK。
- 依赖不可用或 Session 语义无法安全封装时，不静默降级，需在计划中记录原因。

## 验证记录

- 实现：`sdk/python/botfabric` 异步 SessionClient，支持 Runtime 握手、事件订阅和 ACK、远程服务提供、Host Publish/Invoke、Health/Shutdown、payload Base64 编解码；所有入站帧由独立任务处理并受 close 清理管理，避免回调中的 Host 调用阻塞唯一读循环。
- 增加 pip 可安装的 `pyproject.toml`、依赖约束、最小插件示例和接入说明。
- 回环互操作测试覆盖 gRPC/Unix Socket 双向流与 Struct 编码，并验证事件、Health 和 Shutdown 回调可发起 Host 调用而不死锁，以及 Shutdown ACK 后 `serve_forever()` 正常返回。
- `python -m unittest discover -s sdk/python/tests -v`：通过（4 项）。
- `ruff check`、`ruff format --check`、`compileall`：通过。
- Go 回归：`go test -count=1 ./...`、`go test -count=1 -race ./...`、`go vet ./...`、`go build ./...`：通过。
- 限制：本轮在 Python 3.14 环境验证；Python 3.11/3.12 矩阵尚未运行。SDK 仅支持 Unix Socket，本身依赖 asyncio。
