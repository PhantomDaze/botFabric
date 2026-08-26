# BotFabric 分布式 Store（Redis）

**状态**：已完成  
**开始日期**：2026-08-26  
**完成日期**：2026-08-26  
**范围**：去重与响应状态 Store 的 Redis 共享实现，解除单进程内存的横向扩展瓶颈；进程死亡兜底清理

## 背景

N:N 路由与响应仲裁已落地，但去重与响应状态仅有进程内内存实现。多实例部署时两个实例各自独立判定，无法跨实例共享主事件资格与 winner，且内存实现依赖 `time.AfterFunc` 触发 `Release`，进程崩溃时定时器不触发，状态泄漏。本任务为两个 Store 接口补齐 Redis 实现，多实例共享同一 Redis 即可跨实例一致，并以 key TTL 兜底进程死亡。

## 已确认决策

1. Redis 方案：`github.com/redis/go-redis/v9`（生产）+ `github.com/alicebob/miniredis/v2`（纯 Go 嵌入式测试，无需真实 Redis 实例，CI 友好）。
2. liveness 缺口：Redis 实现写入时自带 key TTL（进程死亡兜底），不扩展 CandidateStore 接口；TTL 由 Redis 实现内部管理。
3. dedup Redis：`SET key NX EX` 原子首占 + TTL 自动过期，消除内存实现的时钟依赖与周期清理需求。
4. response Redis：每事件一个 hash key + Lua 脚本保证 Claim/TryWin 原子，每次状态变更刷新 key TTL。
5. 装配：`internal/store` 集中 Redis 连接与 key 命名，dedup 与 response 共享同一 client；main.go 按 `redis.enabled` 创建并注入，未启用回退内存。
6. Redis 不可用时本轮不自动降级内存（启用即要求可用）；Claim/TryWin 出错保守返回 false。

## 实现切片

- [x] D1 配置 + 依赖：`internal/config/redis.go`（RedisConfig + Validate + Prefix）+ config.go 接入 + 示例 + 单测；go.mod/go.sum 加 go-redis/miniredis。
- [x] D2 store 封装 + dedup Redis：`internal/store/redis.go`（连接封装、Ping 探活）+ `internal/dedup/redis.go`（SET NX EX）+ miniredis 测试。
- [x] D3 response Redis：`internal/response/redis_store.go`（hash + Lua 脚本 + key TTL 兜底）+ miniredis 测试。
- [x] D4 装配 + 注入：manager.go 加 `dedupStore` 字段 + `SetDedupStore` + `refreshDeduplicator` 改用注入 store；main.go 按 config 创建 Redis/内存 store 注入 dedup 与 `coordinator.SetStore`，shutdown 关闭 client；修复 runtime.example.yaml 的 key_prefix YAML 引号。
- [x] D5 文档：更新 01/02/03 文档分布式 Store 描述与限制；归档本计划文档。

## 完成内容

- `internal/config/redis.go`：`RedisConfig`（enabled/address/password/db/key_prefix）+ `Validate`（enabled 时 address 非空、db 非负）+ `Prefix`（空值缺省 `botfabric:`）。
- `internal/store/redis.go`：`Config` + `Client`（封装 go-redis，`New` 带 Ping 探活、`Prefix`/`Redis`/`Close`），dedup 与 response 共享同一 client 与 key 命名空间。
- `internal/dedup/redis.go`：实现 `dedup.Store`，`Claim` 用 `SET prefix:dedup:<key> <unix> NX EX <seconds>`，NX 原子首占、EX 自动过期；TTL 向下取整到秒且 <1s 按 1s；出错保守返回 false。
- `internal/response/redis_store.go`：实现 `CandidateStore`，每事件 `prefix:resp:<eventID>` hash，字段 `winner`/`claim:<key>`；`claimScript`/`tryWinScript` 用 `redis.NewScript` 保证原子；每次状态变更 `EXPIRE` 刷新 key TTL（默认 10min，`DefaultEventTTL`）；`Winner`/`ReleaseWinner`/`Release` 对应 HExists/HDel/Del。
- `internal/runtime/manager.go`：`Manager` 增 `dedupStore dedup.Store` 字段 + `SetDedupStore`；`refreshDeduplicator` 改为优先用注入的共享 store，无注入回退 `NewMemoryStore`。
- `cmd/server/main.go`：`redis.enabled` 时 `store.New` 连接（Ping 探活失败即致命）、`dedup.NewRedisStore`→`SetDedupStore`、`response.NewRedisStore`→`coordinator.SetStore`；错误路径关闭 client；shutdown 段关闭 redisClient。
- `config/runtime.example.yaml`：新增 redis 配置段（key_prefix 值加引号避免 YAML 冒号歧义）。
- go.mod/go.sum：新增 `github.com/redis/go-redis/v9 v9.22.0`、`github.com/alicebob/miniredis/v2 v2.38.0` 及传递依赖。

## 验证

以下命令全部通过：

```text
go build ./...
go test ./...
go test -race ./...
go vet ./...
```

新增测试覆盖：
- config Redis：默认禁用、enabled 要求 address、db 非负、prefix 解析与缺省。
- dedup Redis（miniredis）：原子首占、TTL 过期后可重新 Claim、亚秒 TTL 取整、空参数拒绝、写入 unix 时间戳。
- response Redis（miniredis）：Claim 幂等与被 winner 阻塞、TryWin 仅一次、ReleaseWinner 保留 claims、Release 清空全部、key TTL 过期兜底（短 TTL + FastForward）、空参数拒绝。

现有内存实现与协调器/去重测试回归不变（Redis 关闭时走内存）；E2E 回归通过。不依赖真实 Redis 实例（测试用 miniredis）。

## 取舍与遗留

- **TTL 精度**：Redis `EX` 最小秒级，dedup 窗口 <1s 按 1s 处理（窗口通常 ≥1s，可接受）。
- **response key TTL 为粗粒度兜底**：用配置默认 TTL（10min，≥ Coordinator 实际 expire），不精确等于每事件 timeout；Coordinator 正常时仍调 Release，TTL 仅在进程死亡时兜底，不影响正确性。
- **不扩展 CandidateStore 接口**：保持接口稳定，TTL 由 Redis 实现内部管理。
- **Redis 不可用不降级**：本轮启用即要求可用，Claim/TryWin 出错保守返回 false；自动降级内存策略留待后续配置项。
- **连接池/超时调优**：go-redis 默认连接池够用；高级调优（池大小、DialTimeout 等）留待后续配置项。
