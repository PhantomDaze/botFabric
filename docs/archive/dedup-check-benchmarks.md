# 去重检查性能基准

**状态**：已完成
**完成日期**：2026-10-09
**范围**：为内存 `Deduplicator.Check` 建立本机 Go 性能基线，覆盖消息 ID 与文本指纹策略的新事件路径；未改运行时逻辑，不新增依赖，不设置绝对性能门槛。

## 实施内容

- 新增 `internal/dedup/dedup_bench_test.go`。
- `message-id-miss` 为每次迭代生成固定长度、唯一的消息 ID；`fingerprint-miss` 为每次迭代生成固定长度、唯一的消息文本，使每次 Check 都占用新键。
- 事件 payload 编码、Deduplicator JSON 解析、键构造及内存 Claim 均在计时范围内。
- README、架构文档、开发指南已列出此基准；基准结果仅用于相同环境的比较。

## 本机参考采样

环境：Linux/amd64，Intel Core i5-12400F。命令：

```sh
go test ./internal/dedup -run '^$' -bench '^BenchmarkDeduplicatorCheck' -benchmem -count=1
```

| 基准 | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| 消息 ID，新键 | 1716 | 659 | 8 |
| 文本指纹，新键 | 2414 | 1210 | 14 |

此单机采样受编译器、运行时、CPU 和系统负载影响，不是跨机器性能承诺或硬性阈值。

## 验证

- `go test ./internal/dedup -run '^$' -bench '^BenchmarkDeduplicatorCheck' -benchmem`：通过。
- `go test ./...`：通过。
- `go vet ./...`：通过。
- `git diff --check`：通过。

## 后续范围

重复命中路径、并发争用、Redis store、EventBus 全链路吞吐及生产算法优化不在本切片内。
