# 外部进程 transport Socket 实例隔离与热重载并发安全

## 状态

- 状态：已完成
- 日期：2026-09-19

## 目标

- 同一个 Manifest 的不同外部进程实例使用不同 Unix Socket 路径。
- staging reload 启动新实例时不解除旧实例的 Socket 地址，避免旧实例、新实例和清理流程互相影响。
- 并发 Reload 同一个插件时串行执行，避免多个 staging 实例交错 cutover。
- 保持旧 Transport 接口和现有生命周期行为兼容。

## 实施计划

1. 为每次 `Loader.Load` 生成安全、短路径的实例级 Socket 文件名，并由 `managedPlugin` 保存完整路径。
2. 启动时使用保存的实例路径，不再按 Manifest ID 重新计算稳定路径。
3. 为 Manager 增加 Reload 串行化保护；失败 staging 只清理自己的 transport。
4. 增加同一 Manifest 多实例并行启动、Socket 路径隔离和并发 Reload 测试。
5. 运行 gofmt、全量测试、race、vet、build 和 diff check。

## 验证

- `gofmt`：通过
- `go test -count=1 ./...`：通过
- `go test -count=1 -race ./...`：通过
- `go vet ./...`：通过
- `go build ./...`：通过
- `git diff --check`：通过
## 未决事项

- 进程组及 fork 子孙进程回收仍不在本任务范围。
- 不引入跨进程锁；实例 Socket 唯一性由随机实例后缀和 transport 自有 inode 校验共同保证。
