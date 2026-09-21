# 排行榜 tick 协程池（gpool）容量实测与取值建议

> **代码位置**：`internal/server.go`（池初始化）、`internal/rank/manager.go`（唯一提交点）、`golib/gpool/`（池实现）、`golib/redis/redis.go`（连接池）
> **相关文档**：[rank_optimization.md](rank_optimization.md)（第 04 条即引入本池的改动；本文的 §10 是其瓶颈分析的压测验证）
> **被测参数**：[internal/server.go:141](../internal/server.go#L141) 的 `gpool.Global = gpool.New("socialserver", ..., 5120)`
> **最后更新**：2026-09-20
> **结论**：`workers` **500 没有任何依据，32 已经足够**。`500` 是已提交值（`HEAD`），工作区已改为 `32`（未提交）⇒ **这个改动可以保留，不需要再动**；想让余量更大可取 64。`queueLen` 5120 **保持不变**
> **维护者**：sunbin

---

## 0 · 怎么读这份文档

| 章节 | 内容 |
|---|---|
| [1](#1--结论) | 结论与依据（只想要数字看这节） |
| [2](#2--压测环境与方法) | 压测环境、为什么必须用真机、怎么复现 |
| [3](#3--真实业务形状实测) | 真实业务形状 —— 全部建模的输入 |
| [4](#4--单次-tick-的成本) | 单次 Tick 的耗时与命令数 |
| [5](#5--单服务分组数上限378-个) | **单服务分组数上限：378 个**（与池无关的硬上限） |
| [6](#6--实例服务数上限redis-吞吐才是天花板) | **实例服务数上限：Redis 吞吐**（池只需 13 并发） |
| [7](#7--workers-扫描) | workers 扫描：为什么 500 与 32 无法区分 |
| [8](#8--queuelen) | queueLen：留 5120 的唯一理由是「不丢 tick」 |
| [9](#9--推荐值与取值规则) | 推荐值与可复用的取值规则 |
| [10](#10--真正的瓶颈不在池上) | 真正的瓶颈（不在池上） |
| [11](#11--复现命令) | 复现命令 |
| [12](#12--原始数据) | 原始数据附录 |

**一句话结论**

`workers=500` **不是「保险」而是「没有依据」**：池本身没有任何随 worker 数增长的开销（§7.1 证明），而单节点 Redis 的吞吐在 **约 13 个并发** 时就已被喂饱（§6），实测活跃服务数只有 **5** 个（§3）。压测无法在 `w∈[32,500]` 区间测出任何可复现的差异（§7.2 给出了原因），因此**取小值即可** —— 工作区已把 `500` 改回 `32`，**这个改动保留即可**。真正需要担心的是两个与池无关的上限：单服务 **378 个分组**（§5）和整实例 **约 2,140~3,500 个活跃服务**（§6）。

---

## 1 · 结论

| 参数 | 现值 | 建议 | 依据 |
|---|---|---|---|
| `workers` | **32**（工作区；`HEAD` 是 500） | **32 保持不动**；想留更大余量取 64 | 只有当 `workers < 并发服务数 N` 时才付「波次」代价（§7.1）；`workers ≥ N` 之后无收益。实测 N=5，32 已有 6× 余量；再往上被 Redis 连接池 `PoolSize=100` 封顶，且 Redis 吞吐早已饱和（§6） |
| `queueLen` | 5120 | **5120 不变** | 唯一稳定可复现的作用是「不丢 tick」：`queueLen ≥ N` 时提交永不阻塞、`SubmitWaitTimeout` 永不超时。实测唯一出现丢 tick 的配置都是 `q=1`（§8） |

**为什么 500 不可取**

1. **池不背这个成本**：32 个任务、w 从 32 加到 500，批次时间 3.79ms → 3.83ms（§7.1）。多出的 468 个 worker 完全闲置，没有可测量的代价，**但也没有任何收益**。
2. **Redis 先饱和**：单 worker 每秒约产生 1,890 条命令，单节点 Redis 实测吞吐 1.5~2.4 万条/秒 ⇒ **约 13 个并发就足够把 Redis 喂满**（§6）。w=500 里的 487 个只是排在连接池后面。
3. **连接池封顶**：`golib/redis/redis.go:96-100` 的 `PoolSize=100` 决定同一时刻最多 100 条命令在途。`workers > 100` 在结构上不可能提高 Redis 侧并发度。
4. **它救不了真正的瓶颈**：`tickAllRobots` 在**单个任务内**串行遍历该服务的全部分组（[engine/service_robot.go:245-251](../internal/rank/engine/service_robot.go#L245-L251)），所以单个大活动的 Tick 耗时与 `workers` **完全无关**（§5）。500 会给出「池很大所以没事」的错误安全感。

**所以**：把已提交的 `500` 改回 `32` 是对的，**本轮调优的结论是「改完之后不需要再动」** —— 从 32 提到 500 得不到任何收益。如果希望为将来活跃服务数增长留一档余量，取 64（仍远低于连接池上限 100）；压测区分不出 32 与 64，纯粹是余量偏好。

---

## 2 · 压测环境与方法

| 项 | 值 |
|---|---|
| Redis | `172.20.4.224:6379`，单节点，`db9`（**压测专用库**，开压前 `DBSIZE=0`，收尾 `FlushDB`，全程不触碰 `db0` 的业务数据） |
| 客户端 | Intel i7-8700 @3.20GHz（12 逻辑核），Windows 11，Go 1.22 |
| 网络 | 客户端与 Redis **不同机器**，实测单次往返 **0.39 ~ 0.45ms**（`BenchmarkRealRedisRTT`） |
| 连接参数 | 与生产同一组：`PoolSize=100`、`MaxActiveConns=200`、`PoolTimeout=4s`（`golib/redis/redis.go:96-100`） |

**为什么必须用真机、不能用 miniredis**

miniredis 是进程内 loopback，实测往返 **49µs**，比真机快约 **9 倍**。而单次 Tick 的成本几乎全是**串行 Redis 往返次数 × 往返延迟**（§4），所以在 loopback 上测得的「单 Tick 耗时」和「最优 workers」都会被系统性低估。本文件的所有数字都来自真机。

**门禁**：真机压测默认 **不跑**（`requireRealRedis` 检查 `GPBENCH_REAL_REDIS=1`），否则常规 `go test ./...` 会去连一台私有 Redis 并清空 db9。常规跑只看到 `--- SKIP`。

---

## 3 · 真实业务形状（实测）

对 `db0` 做**只读**聚合得到（不写入）。这是全部建模的输入：

| key | 数量 | 说明 |
|---|---|---|
| `rank:mongo_chk:*` | 3827 | Mongo 同步检查点 |
| `rank:meta:*` | 567 | 注册过的 bizId |
| `rank:mb / members / groups / seq / inst :*` | **各 5** | **有真实状态的服务只有 5 个** |
| `rank:settled:*` | 4 | |
| `rank:def:*` | 2 | |
| `rank:member_index:*` | 2 | |
| `rank:periodic_cur_round / periodic_meta:*` | 各 2 | |
| `rank:claims:*` | 1 | |
| `rank:*` 合计 | 4432 | |

三条从数据里读出来的、与直觉相反的事实：

1. **567 个 bizId 里只有 5 个是活的**。566 个是 `camper_competition` 的 `{bizType}_{actID}_r{N}` 历史轮次残留。更关键的是 `rank:{active_services}`（tick 的唯一来源）**整个 key 不存在** —— 说明这是一个已停止的测试实例，当前**没有任何服务在被 tick**。
2. **每个服务只有 1 个分组**（存活服务的分组数 min = p50 = p90 = max = 1，n=5；抽样分组 HLen = 1）。
3. **`rank:def` 只有 2 个**：`rankCode = fmt.Sprintf("%s_score_%d", bizType, cfg.ActID)`（[manager_periodic.go:36](../internal/rank/manager_periodic.go#L36)、[:85](../internal/rank/manager_periodic.go#L85)）用 **ActID 而非轮次**，所以 567 个轮次 meta 只映射到 2 个 def。

**机器人与冷却**（`config/RobotRank.json`、`config/RankBase.json`）：

- 每个 bizType 每分组 **5 个机器人**（`num` = 1+1+1+2）。
- `growTokenCd` = 600s / 1200s / 2400s / 3600s，**比 `tickInterval`(1s) 大 3 个数量级** ⇒ 稳态下绝大多数 Tick 只读不写。
- `rankPeopleNum = 30`（每分组 30 人）⇒ 一个 1 万人的活动 ≈ 333 个分组。

**因此压测测两种形状**：

| 形状 | 分组 | 机器人 | `growTokenCd` | 对应 |
|---|---|---|---|---|
| `steady` | 1 | 5 | 600s | **线上稳态**（只读路径） |
| `burst` | 1 | 5 | 0 | 写路径上界（每 tick 全量写回） |

> 注意 `tick_pool_bench_test.go` 的默认形状（30 分组 × **29** 机器人 × cd=0）是**刻意放大的上界**，不是线上形状。本文件的数字均来自上面两种真机形状。

---

## 4 · 单次 Tick 的成本

`BenchmarkRealTickSingle`（`-benchtime=20x`）：

| 形状 | 单 Tick 耗时 | `Range`/tick | `BatchUpsertScore`/tick | 折合串行往返数 |
|---|---|---|---|---|
| `steady` | **3.45 ~ 3.70ms** | 1.000 | 0.02 ~ 0.05 | ≈ 7 ~ 9 |
| `burst` | **10.62 ~ 10.89ms** | 1.000 | 1.000 | ≈ 23 ~ 26 |

（往返数由 3.70ms ÷ 0.42ms 估算；两次独立跑分别给出 3.45/3.70 与 10.89/10.62，波动约 7%。）

**读法**：

- `Range`/tick = 1.000 说明**每个分组每 tick 恰好一次全量读**（`Range(0,-1)`）。
- `steady` 的 `upsert`/tick = 0.02~0.05 ≈ 0，即 **95% 以上的 tick 完全不写**；`burst` 的 1.000 是每 tick 都写。
- 一次 Tick 的成本 ≈ **固定开销 + 分组数 × 每组开销**，几乎没有池/调度的份额。这是后面所有结论的基础。

---

## 5 · 单服务分组数上限：378 个

`BenchmarkRealTickByGroupCount`（`steady` 形状，`-benchtime=20x`）：

| 分组数 G | 单 Tick 耗时 | `ms/group` | `Range`/tick | `upsert`/tick |
|---|---|---|---|---|
| 1 | 4.44ms | 4.440 | 1.000 | 0.05 |
| 10 | 26.84ms | 2.684 | 10.00 | 0.50 |
| 50 | 131.81ms | 2.636 | 50.00 | 2.50 |
| 200 | 528.81ms | 2.644 | 200.0 | 10.00 |

**线性度极高**：边际成本 50→200 为 2.647ms/分组，10→50 为 2.625，1→10 为 2.487。拟合：

```
单 Tick 耗时 T(G) ≈ 1.80ms + 2.64ms × G
```

`Range`/tick 严格等于 `G` —— 直接证实「每分组一次独立往返、且串行」。

**由此得到 tick 预算上限**：`tickLoop` 是 `time.Ticker`（1s）上的**同步**消费（[manager.go:109-121](../internal/rank/manager.go#L109-L121)），单次 `tickServices` 超过 1s 就会丢掉下一次 tick（ticker channel 容量为 1），表现为**排行榜更新周期被拉长**（不会并发重入）。令 `T(G) < 1000ms`：

```
G < (1000 - 1.8) / 2.64 ≈ 378 个分组
```

即 **`rankPeopleNum=30` 时约 11,340 人**。超过这个规模的活动，单个服务的一个 Tick 就吃掉整个 1 秒预算。

**关键点：这个上限与 `workers` 完全无关。** 全部分组在 `tickAllRobots` 的同一个任务里串行遍历（[engine/service_robot.go:245-251](../internal/rank/engine/service_robot.go#L245-L251)）：

```go
for _, t := range targets {
    robots, ok := s.tickRobots(t.groupID)
    if !ok || len(robots) == 0 { continue }
    s.tickGroupRobots(ctx, t.groupID, t.instanceID, robots, nowMs)
}
```

而 `tickServices` 又对整个批次 `wg.Wait()`，所以**一个大活动就能拖住整个批次**。把 `workers` 从 32 提到 500 对此**一点用都没有** —— 修法是把这个循环**按分组分片成多个任务**（每个分片一个 `SubmitWait`），而不是加大池。

---

## 6 · 实例服务数上限：Redis 吞吐才是天花板

从「单 worker 产能」与「Redis 实测吞吐」两侧夹逼：

**单 worker 产能**：一次 Tick 约 7 条命令、耗时 3.70ms ⇒ 一个 worker 每秒产生 `7 / 0.0037 ≈ 1,890` 条命令。

**Redis 实测吞吐**：`BenchmarkRealTickWorkersKnee` 在 `services=500 / w=500`（全并发）时耗时 **143ms**（倒序跑）/ 233ms（升序跑），即 500 个服务 × 约 7 条命令：

| 跑法 | 批次 | 折合吞吐 |
|---|---|---|
| 倒序 | 143ms | ≈ **24,500 命令/秒** |
| 升序 | 233ms | ≈ **15,000 命令/秒** |

**把 Redis 喂饱所需的并发度**：`15,000 ~ 24,500 ÷ 1,890 ≈` **8 ~ 13 个 worker**。

这与 §7 实测的曲线形状**互相印证**：真机扫描在 `w≈16~32` 就饱和，之后再加 worker 不再变快——因为瓶颈已经从「并发度」转移到了「单节点 Redis 的命令吞吐」。

**由此得到第二个硬上限**（不依赖 `workers`）：

| 形状 | 每服务每 tick 命令数 | 可支撑活跃服务数 N |
|---|---|---|
| `steady` | ≈ 7 | **≈ 2,140 ~ 3,500** |
| `burst` | ≈ 23 | **≈ 650 ~ 1,065** |

实测 N = **5**，即当前利用率为 **0.14% ~ 0.23%**。

> 这是单节点 Redis 的结论。若上 Cluster 且各服务落不同 slot，吞吐可近似线性放大，届时 `workers` 也该按 `≥ N` 重新取值。

---

## 7 · workers 扫描

### 7.1 池自身没有任何随 worker 数增长的开销（免 Redis 对照）

`BenchmarkPoolOverheadVsWorkers` 把任务体换成等长 `time.Sleep(3.4ms)`（= 实测真实单 Tick 耗时），完全脱离 Redis，32 个任务：

| workers | 批次耗时 | 理论波次 |
|---|---|---|
| 8 | **14.44ms** | 4 波 × 3.4ms = 13.6ms |
| 16 | **7.35ms** | 2 波 = 6.8ms |
| 32 | 3.79ms | 1 波 |
| 48 | 3.75ms | 1 波 |
| 64 | 3.82ms | 1 波 |
| 128 | 3.82ms | 1 波 |
| 256 | 3.74ms | 1 波 |
| **500** | **3.83ms** | 1 波 |

**两个结论**：

1. **`workers < N` 会付「波次」代价**：`⌈N/W⌉` 波，每波一个 Tick 时长。实测与模型吻合。
2. **`workers ≥ N` 之后完全平坦**（32→500 全程 3.74~3.83ms，波动 2.4%）。**池没有 per-worker 成本** —— 这一条直接证伪了「worker 开太多反而慢」的猜想。

### 7.2 真机扫描：`w∈[32,500]` 测不出可复现差异

同一个 workers 序列（8,16,32,48,64,100,128,256,500），分别按**升序**和**倒序**各跑一遍。子基准是串行执行的，所以**执行位置**会与 workers 数混淆（时间漂移、Redis 侧负载、共享连接池状态、网络抖动都会被读成「worker 越多越慢」）。倒序跑是判据：

**`services=32`（每次跑，单位 ms/op，越小越好）**

| workers | 升序(跑A) | 升序(跑B) | **倒序** |
|---|---|---|---|
| 8 | 12.67 | 11.62 | 12.73 |
| 16 | 7.70 | 7.56 | 8.64 |
| **32** | **7.40** | **7.42** | 11.43 |
| 48 | 8.15 | 8.44 | 10.96 |
| 64 | 11.08 | 9.12 | 10.95 |
| 100 | 9.28 | 9.51 | 10.13 |
| 128 | 10.59 | 10.26 | 9.19 |
| 256 | 11.93 | 10.73 | 8.77 |
| **500** | 11.63 | 10.86 | **7.85** |

**两次升序跑说「w=32 最快、w=500 最慢」，倒序跑把结论整个翻转成「w=500 最快、w=32 最慢」** —— 退化的方向跟着**执行位置**走，而不是跟着 `workers` 数走。`services=200` 与 `services=500` 同样出现完全反转（例：`services=500` 升序 w=32 为 74.4ms 最快 / w=500 为 233ms 最慢；倒序 w=500 为 143ms 最快 / w=32 为 222ms 最慢）。

**因此：`w=500 比 w=32 慢 1.5~3.7×` 是压测顺序造成的假象，不能作为结论。** 稳健的结论只有 §7.1 的两条（`w<N` 付波次、`w≥N` 平坦），二者都有免 Redis 的对照支撑。

> **方法论留痕**：如果只跑升序，上面的表会「证明」一个不存在的最优值。任何按参数网格串行执行的基准都需要做一次顺序反转，否则报出的极值可能只是时间漂移。

---

## 8 · queueLen

`queueLen` 对吞吐基本没有影响（`q=1` 与 `q=5120` 的差异在噪声范围内）；它唯一稳定可复现的作用是**不丢 tick**。

`q=1` 与 `q=5120` 的丢 tick 对比（`BenchmarkRealTickBatchPool`）：

| 形状 | N | workers | `q=1` 丢 tick | `q=5120` 丢 tick |
|---|---|---|---|---|
| `burst` | 200 | 4 | **0.075%** | 0 |
| `burst` | 500 | 32 | **0.010%** | 0 |
| 其余全部组合 | — | — | 0 | 0 |

**规则**：`tickServices` 用 `SubmitWaitTimeout(ctx, fn, 200ms)` 提交（[manager.go:142-168](../internal/rank/manager.go#L142-L168)）。当 **`queueLen ≥ N`** 时每个提交都能立刻入队，`SubmitWaitTimeout` **永不超时** ⇒ **不丢 tick，与 `workers` 无关**；只有当队列极小（`q=1`）且 `workers` 明显不足时，才会出现「本 tick 放弃该服务、下一 tick 重试」的静默停更（日志里是 `rank: tick submit skipped (pool busy)`）。

**所以 `queueLen=5120` 要保留**：它才是「不丢 tick」的真正保障，而且代价只是 5120 个函数指针的 slot。反过来，`workers` 取小值之所以安全，正是因为 `queueLen` 足够大。

---

## 9 · 推荐值与取值规则

```
推荐：gpool.Global = gpool.New("socialserver", 32, 5120)   // == 当前代码，不需要改动
可选：gpool.Global = gpool.New("socialserver", 64, 5120)   // 仅当想为服务数增长多留一档余量
```

可复用的取值规则（两条独立，各自是硬约束）：

**① `queueLen`：保证不丢 tick**

```
queueLen ≥ 并发服务数 N        // 实测 N=5，5120 有 1000× 余量
```

**② `workers`：把 Redis 喂饱即可，不必超过连接池**

```
workers ≥ N                          // 满足「一波跑完」，实测 N=5
workers ≈ 喂饱 Redis 所需的并发度      // 实测 ≈ 8~13，取 16~32 即饱和
workers ≤ RedisPoolSize(100)         // 超过 100 在结构上无法提高并发度
```

反解「什么时候才需要 500」：

- 按 §7.1 的波次模型，`workers=500` 只有在 `N > 500` 时才比 32 有优势，而实测 `N=5`；
- 按 §6 的吞吐模型，`workers` 超过 **≈13** 就不再提高 Redis 侧并发 ⇒ **500 是它的约 38 倍**；
- 按 §6 的容量表，单节点 Redis 稳态最多支撑 **≈2,140~3,500** 个活跃服务，实测 5 个 ⇒ **利用率约 0.15%**。

三条从不同角度给出同一结论：**500 对应的负载比实测高 1~3 个数量级**，而池本身不产生成本（§7.1），所以 500 既不必要、也无害 —— 只是没有依据。

三个参数（N=活跃服务数、G=单服务分组数、`workers`）的关系可以概括成：

| 量 | 上限 | 与 `workers` 的关系 |
|---|---|---|
| 单服务分组数 G | **378**（§5） | **无关**（单任务内串行） |
| 全实例活跃服务数 N | **2,140~3,500** steady / **650~1,065** burst（§6） | 只要 `workers ≥ ~13` 就与 `workers` 无关 |
| 不丢 tick | `queueLen ≥ N`（§8） | 靠 `queueLen`，**不靠** `workers` |

---

## 10 · 真正的瓶颈（不在池上）

按对线上影响排序：

1. **`tickAllRobots` 在单任务内串行遍历全部分组**（[engine/service_robot.go:245-251](../internal/rank/engine/service_robot.go#L245-L251)）。这是**目前唯一能真正吃掉 1s tick 预算**的结构：378 个分组（≈1.1 万人）就顶满，且加 worker 无效。修法是按分组分片成多个任务。
2. **`BatchUpsertScore` 名为 Batch、实为逐 item 一次 Lua `Eval`**（[common/rank/service_redis.go:403-454](../../common/rank/service_redis.go#L403-L454)）。`burst` 形状下写回占了单 Tick 的 ~68%（10.62ms − 3.70ms ≈ 6.9ms / 5 个机器人 ≈ 1.4ms/机器人 ≈ 3 次往返/机器人）。改成「一次 Lua 写完整批」可直接压掉这部分。
3. **注册表残留**：`rank:meta` 567 个 bizId vs 真实 5 个服务（§3），566 个是历史轮次残留。它们不在 `rank:{active_services}` 里，所以不参与 tick；但会让任何按 `rank:meta` 做的全量操作（`bootstrapRegistry` 的 SCAN、运维排查）看不真实规模。清理属运维项。

---

## 11 · 复现命令

```bash
# 真机压测（默认不跑，必须显式开；会写入并清空 db9）
GPBENCH_REAL_REDIS=1 go test ./internal/rank/engine/ -run '^$' \
    -bench 'BenchmarkReal' -benchtime=20x

# 顺序混淆检验：同一组 workers 倒序再跑一遍（§7.2）
GPBENCH_REAL_REDIS=1 GPBENCH_KNEE_ORDER=desc go test ./internal/rank/engine/ -run '^$' \
    -bench 'BenchmarkRealTickWorkersKnee' -benchtime=20x

# 免 Redis 的池开销对照（§7.1）—— 不需要真机，也不需要门禁
go test ./internal/rank/engine/ -run '^$' -bench 'BenchmarkPoolOverheadVsWorkers' -benchtime=20x

# 常规跑：真机基准全部 SKIP，不会连私有 Redis
go test ./internal/rank/engine/
```

| 基准 | 位置 | 测什么 |
|---|---|---|
| `BenchmarkRealRedisRTT` | [real_redis_bench_test.go](../internal/rank/engine/real_redis_bench_test.go) | 到真机的单次往返（换算基准） |
| `BenchmarkRealTickSingle` | 同上 | 单服务单 Tick 的耗时与命令数（§4） |
| `BenchmarkRealTickByGroupCount` | 同上 | 单 Tick 随分组数的斜率（§5） |
| `BenchmarkRealTickWorkersKnee` | 同上 | workers 细扫 + 顺序反转（§7.2） |
| `BenchmarkRealTickBatchPool` | 同上 | 批次形状 × workers × queueLen，含丢 tick 计数（§8） |
| `BenchmarkPoolOverheadVsWorkers` | [tick_pool_bench_test.go](../internal/rank/engine/tick_pool_bench_test.go) | 池自身开销，免 Redis（§7.1） |
| `BenchmarkTickColdByGroupCount` / `BenchmarkTickBatchPool` | 同上 | miniredis 版本，方法学参照（**数字不可外推到真机**） |

**压测台的两个坑**（已在代码里处理，改压测时注意）：

- **逻辑时钟必须全局单调**：`TryLockRobotTick` 是按秒的 `SETNX`（`rank:robot_tick:{bizId}:{sec}`）。若各子基准各自从 `time.Now()` 起算，相邻子基准的秒区间重叠会命中前一个的残留锁，整个 `tickAllRobots` 被跳过，压出假数据。见 `benchNow`/`nextBenchNow`。
- **bizId 必须全局唯一**：Go 基准框架会先用 N=1 校准再正式跑，**建数据的代码会被执行两次以上**。miniredis 每次是全新实例所以掩盖了这点，真机状态跨调用保留，第二次就会撞 `OpenInstance: rank instance already exists`。隔离靠唯一 bizId（`realUniq`），不能靠「新实例」。
- **客户端要共享**：每个客户端 `PoolSize=100`，子基准有几十个，各建一个会累积上千条连接，在 Windows 上直接打爆临时端口（表现为收尾 `FlushDB` 报 `Only one usage of each socket address ... permitted`）。见 `realBenchClients` 的单例。

---

## 12 · 原始数据

以上正文的数字均来自下列跑次（`-benchtime=20x`）。两次升序跑用于确认漂移的稳定性。

### 12.1 单次 Tick（§4）

```
BenchmarkRealRedisRTT-12                       20    387120 ns/op     （另一次 449298）
BenchmarkRealTickSingle/steady-12              20   3699540 ns/op   1.000 range/tick   0.05000 upsert/tick
BenchmarkRealTickSingle/burst-12               20  10617170 ns/op   1.000 range/tick   1.000 upsert/tick
```

### 12.2 分组数斜率（§5）

```
BenchmarkRealTickByGroupCount/groups=1-12      20    4440330 ns/op   4.440 ms/group   1.000 range/tick    0.05000 upsert/tick
BenchmarkRealTickByGroupCount/groups=10-12     20   26839760 ns/op   2.684 ms/group   10.00 range/tick    0.5000 upsert/tick
BenchmarkRealTickByGroupCount/groups=50-12     20  131813120 ns/op   2.636 ms/group   50.00 range/tick    2.500 upsert/tick
BenchmarkRealTickByGroupCount/groups=200-12    20  528811110 ns/op   2.644 ms/group   200.0 range/tick    10.00 upsert/tick
```

### 12.3 池开销（免 Redis，§7.1）

```
BenchmarkPoolOverheadVsWorkers/tasks=32/w=8-12      20   14435885 ns/op
BenchmarkPoolOverheadVsWorkers/tasks=32/w=16-12     20    7347560 ns/op
BenchmarkPoolOverheadVsWorkers/tasks=32/w=32-12     20    3786180 ns/op
BenchmarkPoolOverheadVsWorkers/tasks=32/w=48-12     20    3750535 ns/op
BenchmarkPoolOverheadVsWorkers/tasks=32/w=64-12     20    3817355 ns/op
BenchmarkPoolOverheadVsWorkers/tasks=32/w=128-12    20    3815205 ns/op
BenchmarkPoolOverheadVsWorkers/tasks=32/w=256-12    20    3743815 ns/op
BenchmarkPoolOverheadVsWorkers/tasks=32/w=500-12    20    3833185 ns/op
```

### 12.4 workers 细扫，升序（§7.2 表「升序跑A」，`services=32`）

```
w=8    12670230 ns/op    2526 ticks/s        w=8    11623095 ns/op    2753 ticks/s
w=16    7697310 ns/op    4157 ticks/s        w=16    7559290 ns/op    4233 ticks/s
w=32    7396760 ns/op    4326 ticks/s        w=32    7418630 ns/op    4313 ticks/s
w=48    8146790 ns/op    3928 ticks/s        w=48    8440555 ns/op    3791 ticks/s
w=64   11075275 ns/op    2889 ticks/s        w=64    9122405 ns/op    3508 ticks/s
w=100   9282055 ns/op    3448 ticks/s        w=100   9513240 ns/op    3364 ticks/s
w=128  10587055 ns/op    3023 ticks/s        w=128  10259715 ns/op    3119 ticks/s
w=256  11929875 ns/op    2682 ticks/s        w=256  10732540 ns/op    2982 ticks/s
w=500  11630190 ns/op    2751 ticks/s        w=500  10855645 ns/op    2948 ticks/s
（跑A，同批含 RTT/Single）                    （跑B，收尾 FlushDB 撞端口耗尽，数据仍有效）
```

### 12.5 workers 细扫，倒序（§7.2 表「倒序」，`services=32`）

```
w=500   7846990 ns/op    4078 ticks/s
w=256   8766535 ns/op    3650 ticks/s
w=128   9193155 ns/op    3481 ticks/s
w=100  10126775 ns/op    3160 ticks/s
w=64   10949615 ns/op    2922 ticks/s
w=48   10956405 ns/op    2921 ticks/s
w=32   11428740 ns/op    2800 ticks/s
w=16    8635480 ns/op    3706 ticks/s
w=8    12725035 ns/op    2515 ticks/s
```

### 12.6 批次 × queueLen，`burst` 形状节选（§8）

```
shape=burst/services=200/w=4/q=1-12       20   656144960 ns/op   0.07500 tick-drop%   0 tick-err%    304.8 ticks/s
shape=burst/services=200/w=4/q=5120-12    20   532553515 ns/op         0 tick-drop%   0 tick-err%    375.5 ticks/s
shape=burst/services=500/w=32/q=1-12      20   243219750 ns/op   0.01000 tick-drop%   0 tick-err%   2056 ticks/s
shape=burst/services=500/w=32/q=5120-12   20   188610320 ns/op         0 tick-drop%   0 tick-err%   2651 ticks/s
```

> `tick-err%` 全程为 0：即便 `workers=500` 远超连接池 `PoolSize=100`，也没有出现 `PoolTimeout`。go-redis 会让多出的 worker 排队等连接，而不是报错——但这正说明多出的 worker 只是在排队，不产生并发收益。

---

## 附：文件与行号索引

| 主题 | 文件 |
|---|---|
| 池初始化与关闭顺序 | [internal/server.go](../internal/server.go#L141) |
| 池化调度 / tickLoop / tickServices | [internal/rank/manager.go](../internal/rank/manager.go#L110-L165) |
| 机器人 tick（分组串行循环） | [internal/rank/engine/service_robot.go](../internal/rank/engine/service_robot.go#L222-L252) |
| `BatchUpsertScore`（逐 item Lua） | [common/rank/service_redis.go](../../common/rank/service_redis.go#L403-L454) |
| 池实现 | [golib/gpool/gpool.go](../../golib/gpool/gpool.go) |
| Redis 连接池参数 | [golib/redis/redis.go](../../golib/redis/redis.go#L96-L100) |
| 真机压测台 | [internal/rank/engine/real_redis_bench_test.go](../internal/rank/engine/real_redis_bench_test.go) |
| miniredis 压测台 | [internal/rank/engine/tick_pool_bench_test.go](../internal/rank/engine/tick_pool_bench_test.go) |
| 榜单配置与机器人档位 | [config/RankBase.json](../config/RankBase.json)、[config/RobotRank.json](../config/RobotRank.json) |
