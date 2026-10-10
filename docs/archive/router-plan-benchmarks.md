# 路由计划性能基准

**状态**：已完成
**完成日期**：2026-10-09
**范围**：为 `internal/routing.Router.Plan` 增加可复现的 Go benchmark，覆盖默认广播和规则匹配热路径；未改运行时行为，未新增依赖或绝对性能门槛。

## 实施内容

- 新增 `internal/routing/router_bench_test.go`。
- 默认广播覆盖 1、16、128 个候选端；规则路径覆盖单条 Topic 规则和 32 条优先级正则规则。
- 每项基准准备阶段在计时外完成，报告分配，并将结果保存在 package 级变量中。
- README、架构设计和开发指南已说明 Router.Plan 有本机比较基准，并继续标注高级沙箱为未完成。开发指南的旧“LLM 只有接口和 mock”表述也已更新为真实分类器现状。

## 本机参考采样

环境：Linux/amd64，Intel Core i5-12400F。命令：

```sh
go test ./internal/routing -run '^$' -bench '^BenchmarkRouterPlan' -benchmem -count=1
```

结果：

| 基准 | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| 默认广播，1 个候选 | 116.4 | 40 | 2 |
| 默认广播，16 个候选 | 328.3 | 280 | 2 |
| 默认广播，128 个候选 | 689.2 | 2328 | 2 |
| 单条 Topic 规则 | 1241 | 713 | 5 |
| 32 条优先级正则规则 | 35820 | 14001 | 38 |

这是单机基线采样，受运行环境与负载影响，不能作为跨机器的性能承诺或阈值。

## 验证

- `go test ./internal/routing -run '^$' -bench '^BenchmarkRouterPlan' -benchmem`：通过。
- `go test ./...`：通过。
- `go vet ./...`：通过。
- `git diff --check`：通过。

## 后续范围

并发争用、端到端吞吐、跨平台硬件比较以及 Router 算法优化不在本切片内；应在定义代表性负载和测量环境后另行规划。
