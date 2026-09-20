# 排行榜引擎优化：问题、方案与实施状态

> **代码位置**：`internal/rank/`、`internal/taskpool/`、`common/rank/`、`internal/handler/rank.go`、`internal/server.go`
> **最后更新**：2026-09-18
> **约束**：不改变现有存储结构和存储结果
> **文档沿革**：本文经三轮核对与重写，过程记录见 git log（`git log --oneline -- docs/rank_optimization.md`），正文只保留结论

---

## 0 · 怎么读这份文档

| 章节 | 内容 |
|---|---|
| [1](#1--两条基础原则) | 两条基础原则 —— 后文所有"该不该做"的判定依据 |
| [2](#2--key-生命周期台账) | Key 生命周期台账 —— 17 个在用 key 的现状、缺口与读写路径（全文唯一一份） |
| [3](#3--13-条优化逐条说明) | 13 条优化：**原问题 → 方案 → 状态 → 落点 → 测试** |
| [4](#4--缺陷与待办全部已修复) | 缺陷与待办：全部已修复 |
| [5](#5--测试用例总览) | 测试用例总览 |
| [6](#6--上线方式一个-pr全量开启) | 上线方式：一个 PR、全量开启（含**与线上实例的兼容性核对**、**存量永久 key 的一次性迁移**、**上线操作清单**） |
| [7](#7--审计中发现但不在-13-条内的问题) | 审计中发现、但不在 13 条内的问题 |

**状态图例**：✅ 已实现 · ⚠️ 部分实现 · ❌ 未实现

**一句话结论**

13 条主线**全部已实现**，且每一条都有对应测试。**「不允许存在永久 key」这一簇（缺陷 1 / 2 / 3 / 4 / 6 / 7 / 8）已全部落地并全量开启**（无灰度、无开关）；§4.2 的待办 A / C / E / G-d2 / G-d3 也一并完成。§2 台账的 17 个 key 现在没有任何一个的 TTL 一栏为空。

剩下的只有两项**明确不在本轮范围**的工作：待办 G 的负缓存 + 版本戳（需架构改动），以及 `periodic/handler.go` 中已与 `Settle` 重叠的 `setSettledTTLForRound`（可删，属清理而非缺口）。

---

## 1 · 两条基础原则

### 1.1 原则一：Redis 只是缓存 —— 不允许存在永久 key

**三条硬规则，无例外**

1. **每个 key 在写入时必须带 TTL。** 任何 `Set` / `HSet` / `SAdd` / `SetNX` / `Restore*` 的落点，都要在同一次往返里把过期时间设上。不存在「这个 key 需要常驻，所以不设 TTL」的例外——常驻需求用「滑动 TTL + 写时续期」表达（第 3 条）。
2. **每个 key 必须有一条「单调用可恢复」的路径。** 判定标准：把这个 key 删掉，下次读它时能否**不依赖任何其它 Redis key** 自行恢复。做不到的 key 不允许设 TTL。
3. **TTL 必须覆盖两种 key 形态**（这是第 1 条能落地的前提）

| key 形态 | 举例 | TTL 规则 | 是否需要续期 |
|---|---|---|---|
| **活动数据**（有 `effectiveSettleAt()`） | meta / groups / members / claims / inst / mb / seq / settled / robots / robot_infos | 绝对过期时刻 = `effectiveSettleAt() + SettledCacheTTL`（2 周），**与设置时机无关** | ❌ 写一次即可，重复写同一个绝对值是幂等的 |
| **非活动数据**（没有天然结束时刻） | `rank:def`、注册表 `rank:{active_services}`、`rank:member_index` | **滑动 TTL**：每次写刷新到 `now + ColdDataTTL` | ✅ 活跃期被持续续期；连续一个 TTL 时长无写入才算冷掉 |

`ColdDataTTL = 7 天`（[defines.go:73](../../common/rank/defines.go)），与既有的 `rank:member_index` TTL 取同一个值，避免多一个需要解释的魔数。它表达的是「静默多久才算真的冷掉」——取值宁大勿小：回收本身是安全的（权威源在 MongoDB），值偏大的代价只是多占几天内存，偏小才是风险。

**没有第三种形态。** 遇到一个 key 说不清属于哪一类，说明它的生命周期没设计完，而不是它需要豁免。

**「设 TTL」与「加恢复」是一体两面，只做一半都会出问题**

- **只设 TTL 不做恢复** → 到期后功能直接失效。最典型的是 `rank:def`：`OpenInstance` 先查 `Exists(rank:def)`，不存在就返回 `ErrDefinitionNotFound`（[service_redis.go:189](../../common/rank/service_redis.go)），即**新分组无法创建、活动彻底不可玩**。
- **只做恢复不设 TTL** → 未走结算/删除流程的活动永久占内存。这是当前的主要缺口：见 §2 台账「结算后 TTL」一列。

### 1.2 原则二：正常逻辑不得扫库

**「扫库」指任何 O(全库 key 数) 的命令**（`KEYS` / `SCAN` / 全量 `HGETALL` / 全量 `SMEMBERS`）。代价与**目标数据量无关**，只与**实例总 key 数**有关，因此无法通过清理业务数据来控制。

**全量审计结果：整个排行榜子系统只有一处扫库，已在第 01 条修复。**

| 位置 | 命令 | 原频率 | 现状 |
|---|---|---|---|
| `syncFromRedis` | `m.rdb.Keys(prefix + "*}")` | 每 30 秒 | 改为「启动一次 `SCAN` + 之后查注册表」，稳态归零 |

推论：**任何新功能都不得引入扫库。** 需要「按前缀找 key」时，改为维护显式集合（第 01 条的注册表就是这个模式），或依赖已有的 MongoDB 权威查询（`LoadAllRankConfigs` 走的是带索引的 `Find`）。

### 1.3 合规的代价是零：写 + EXPIRE 放进同一 pipeline

| 写法 | 往返 |
|---|---|
| 现状：裸 `HSet` / `SAdd` / `Set` | 1 |
| 朴素改法：写 + 单独一次 `Expire` | **2**（这才是"加 TTL 很贵"这个印象的来源） |
| **合规写法：pipeline 里写 + `Expire`** | **1**（与现状相同） |

```go
// ttlFor 是纯内存计算（只读 s.config 的时间字段），不产生 IO
pipe := st.rdb.Pipeline()
pipe.HSet(ctx, key, field, value)
pipe.Expire(ctx, key, ttlFor(activityEnd))
_, _ = pipe.Exec(ctx)
```

三点说明：① **不需要原子性，所以用 `Pipeline()` 而非 `TxPipeline()`**——`HSET` 与 `EXPIRE` 之间没有必须同时生效的关系，即使 `EXPIRE` 丢了，key 只是比预期活得更久，**下次写入会补上**；失败方向是"多留数据"而不是"错删数据"。② 有限期活动重复设同一个绝对值是幂等的，不必判断"是否首次写入"（判断本身反而多一次往返）。③ 所以第 02 条的活跃期 TTL 与缺陷 6 的结算后 TTL 都**不需要以性能为由打折扣**。

已落地：[store.go:345](../internal/rank/engine/store.go#L345) `registerActive` 与 [store.go:371](../internal/rank/engine/store.go#L371) `unregisterActive` 就是这个形状（`SAdd`/`SRem` + `Expire` 一次往返）。

### 1.4 可恢复性：为什么给任何 key 设 TTL 都不会丢数据

| key 类别 | 权威源 | 恢复代价 |
|---|---|---|
| 11 个活动数据 key（meta / groups / members / claims / inst / mb / seq / settled / robots / robot_infos / mongo_chk） | MongoDB 的 7 个集合 | 一次 Mongo 查询（`recoverGroupData` / `Load*` 已有） |
| `rank:def` | **只有 engine 内存**——MongoDB `CT_RANK_CONFIG` 存的 `engine.Config` 不含 `RankName` / `ScoreOrder` / `TieBreakPolicy` / `MaxQuerySize` | **零 IO**：注册期用 `engine.WithRankDef` 存一份原始定义，`rankDefProvider` 在 miss 时原样写回（缺陷 1 的修法） |
| `rank:member_index` | engine 内存 `memberGroup`（`WarmUp` 时从 Mongo 载入） | 零 Redis/Mongo 访问（第 08 条） |
| 3 个锁 key | 无（瞬时状态） | 无需恢复；丢失即"锁被释放"，`TryLock*` 本身 fail-open |
| `rank:mongo_chk` | 无（哨兵） | 无需恢复；丢失只是重查一次 Mongo |
| `rank:claims` | MongoDB `CT_RANK_CLAIM` | `AtomicClaim` 已有 Mongo 回退（`GetClaim` / `SaveClaimIfNotExists`）——**所以 claims 的 TTL 不可能导致重复发奖** |

**结论：Redis 里的每一个字节都能从 MongoDB（或 engine 内存）重算，所以给任何 key 设 TTL 都不会丢数据。**

真正需要额外小心的**不是"能不能恢复"，而是"恢复是否零成本"**——因为恢复发生在读路径上：

- `rank:def` 的恢复必须**零 IO**，否则每次 miss 都要打 Mongo，而 `OpenInstance` 在写路径上（缺陷 1）。而且它的恢复源**只能是内存**：Redis 是 `RankName` / `ScoreOrder` / `TieBreakPolicy` / `MaxQuerySize` 这四个字段的唯一住所，`rank:def` 一到期，Mongo 里那份 `engine.Config` 拼不出等价的定义。所以恢复依赖"注册期捕获的原始定义还在进程内"，而这条依赖之所以成立，是因为**只要还有活着的 `engine.Service`，那份定义就一定在内存里**；服务被删时 `engineServices` 一并删除，provider 立刻查不到，不会把 GM 删掉的定义复活。
- `rank:member_index` 的恢复**不能争写锁**，否则 GM 批量查询会与 `UpsertScore` 抢锁（待办 G）。

**判定工具（「删 key 测试」）**：要判断某个 key 能不能设 TTL，直接 `DEL` 它，看系统能否自愈。不能自愈的 key 不允许设 TTL——但也不允许长期无 TTL，那属于"恢复路径还没做完"，是待办而不是豁免。

---

## 2 · Key 生命周期台账

### 2.1 TTL 现状（17 个在用 key）

13 个数据 key（`rank:max_score` 死代码已删除）+ 1 个注册表 + 3 个锁 key。**17 个全部带 TTL，无缺口。**

「活跃期 TTL」一列的写法规约：**绝对** = 过期时刻恒为 `effectiveSettleAt() + SettledCacheTTL`，与设置时机无关；**滑动** = 每次写入刷新到 `now + ColdDataTTL`。

| key | 活跃期 TTL | 结算后 TTL（一次性 / 周期） | 权威源 → 恢复路径 | 缺口 |
|---|---|---|---|---|
| `rank:def:{rankCode}` | ✅ 7 天（`SetEX(ColdDataTTL)`，写入时起算） | ✅ / ✅ 7 天 | **只有 engine 内存** → `rankDefProvider` 零 IO 重建 | 无（缺陷 1） |
| `rank:inst:{instanceID}` | ✅ 绝对 | ✅ / ✅ 2 周 | `CT_RANK_INST` → `LoadGroupInst` | 无（缺陷 2 + 6） |
| `rank:mb:{instanceID}` | ✅ 绝对 | ✅ / ✅ 2 周 | `CT_RANK_SCORE` → `recoverGroupData` | 无（缺陷 2 + 6） |
| `rank:seq:{instanceID}` | ✅ 绝对 | ✅ / ✅ 2 周 | `CT_RANK_SCORE` → `RestoreMembers` 推进到 `max(sequence)` | 无（缺陷 2 + 6） |
| `rank:settled:{instanceID}` | — | ✅ / ✅ 2 周 | `CT_RANK_SETTLED` → `LoadGroupSettled` | 无（缺陷 4 + 6） |
| `rank:meta:{bizId}` | ✅ 绝对 | ✅ / ✅ 2 周 | `CT_RANK_GROUP` → `ensureLoaded` 重算 `nextGroupID` | 无（缺陷 2 + 6） |
| `rank:groups:{bizId}` | ✅ 绝对 | ✅ / ✅ 2 周 | `CT_RANK_GROUP` → `LoadGroups` | 无（缺陷 2 + 6） |
| `rank:members:{bizId}` | ✅ 绝对 | ✅ / ✅ 2 周 | `CT_RANK_MEMBER` → `GetMember` | 无（缺陷 2 + 6） |
| `rank:claims:{bizId}` | ✅ 绝对 | ✅ / ✅ 2 周 | `CT_RANK_CLAIM` → `AtomicClaim` 回退 `GetClaim` | 无（缺陷 2 + 6） |
| `rank:robots:{bizId}:{gid}` | ✅ 绝对 | ✅ / ✅ 2 周 | `CT_RANK_ROBOT` → `LoadRobots` | 无（缺陷 2 + 6） |
| `rank:robot_infos:{bizId}:{gid}` | ✅ 绝对 | ✅ / ✅ 2 周 | 同上 | 无（缺陷 2 + 6） |
| `rank:member_index:{uid}` | ✅ 7 天（`Track` 每次写入续期） | ✅ / ✅ 7 天 | engine 内存 → `rebuildMemberIndex` | 无（第 08 条） |
| `rank:mongo_chk:{bizId}` | ✅ 10 分钟（`SetNX`，不会缩短已有 TTL） | ✅ / ✅ 2 周 | 无需（哨兵本身是缓存） | 无（待办 J） |
| `rank:settle:{bizId}:{gid}` | ✅ 10 分钟 | ✅ / ✅ | 无需（锁） | 无 |
| `rank:robot_tick:{bizId}:{sec}` | ✅ 3 秒 | ✅ / ✅ | 无需（锁） | 无 |
| `rank:periodic_advance:{lk}:r{n}` | ✅ 1 分钟 | ✅ / ✅ | 无需（锁） | 无 |
| `rank:{active_services}` | ✅ 7 天（滑动） | ✅ / ✅ 7 天（滑动） | `CT_RANK_GROUP` → `syncFromMongo` → `NewService` → `SaveActivityTimes` → `SAdd` | 无（第 01 条 + 缺陷 8） |

**三条口径**

1. **活跃期**：13 个数据 key 全部带 TTL。活动数据的过期时刻一律是**绝对值** `effectiveSettleAt() + SettledCacheTTL`——同一个活动下所有 key、所有写入点算出的都是同一个时刻，因此重复写幂等，既不会续期也不会缩短。
2. **结算后**：一次性活动与周期轮次走同一条路——`Settle` 收集本轮所有已结算分组，统一 `ExpireInstance`（inst/mb/seq/settled 四 key）+ `ExpireLiveData`（meta/groups/members/claims/mongo_chk + 每分组 robots/robot_infos）。周期轮次另经 `CleanupLiveData`，该函数**保留固定 2 周**语义不动（轮次清理发生在结算后一个周期，改用 `ttlFor(roundClose)` 会让总保留期从 21 天缩到 14 天，那是真实的功能退化）。
3. **合规率**：17/17。

> **本轮实测发现的最后一个缺口（已修复）**：活跃期写路径上曾有 `rank:inst` / `rank:mb` / `rank:seq` 三个永久 key，且都在最热的得分写入上。
> - `common/rank.BatchUpsertScore` 更新实例元数据用的是**裸 `SET`**，而 `SET` 会连带清掉 key 上已有的过期时间——`OpenInstance` 刚给 `rank:inst` 设的保留期，被紧随其后的第一次得分写入抹掉。该分支几乎每写必进（`LastScoreUpdate` 每次得分都在推进），所以稳态下必然发生。
> - `rank:mb` / `rank:seq` 由 upsert 的 Lua 用 `HSET`/`INCR` 现建，建的那一刻 `EXPIRE` 会被静默丢弃（key 还不存在），脚本本身又无从设 TTL。
>
> 修法：把那条裸 `SET` 换成同一次 pipeline 内的「`Set(inst, ttl)` + `Expire(mb)` + `Expire(seq)`」，三者共用一个绝对过期时刻，往返数与原来相同。覆盖性成立：Lua 重建 mb/seq 必然伴随至少一个成员 `HGET` 未命中 ⇒ `newCount > 0` ⇒ 该分支必然执行，不存在缝隙。回归测试 `TestBatchUpsertScoreKeepsInstanceTTL`（`common/rank`，单元）与 `TestActivePeriodWritePathLeavesNoPermanentKey`（`engine`，端到端全键扫描）。

### 2.2 写入 / 恢复 / 删除路径

约定：① "删除"只统计显式 `Del`/`SRem`/覆盖式重建，自然 TTL 过期不算；② 锁 key 无独立"恢复"语义（`SetNX` 本身即"不存在则创建"）；③ 注册表 key 已是代码事实（不再是设计草案）。

**分组 / 成员 / 结算数据（`common/rank` 层）**

| key | 新增（首次写入） | 恢复（miss 后重建） | 删除（显式路径） |
|---|---|---|---|
| `rank:def` | `RegisterRank`（[service_redis.go:192](../../common/rank/service_redis.go)），`SetEX(ColdDataTTL)`；6 处调用点收敛为单一构造器 `rankDefFor()`：`registerEngine`、`syncFromRedis`、`syncFromMongo`、`applyRankConfigDoc`、`registerSubService`/`replaceSubService` | **`RedisService.GetRank` 内建零 IO 重建（缺陷 1）**：miss 时向注入的 `rankDefProvider` 取回构造期由 `WithRankDef` 捕获的原始定义，原样 `RegisterRank` 写回再重读。provider 为 nil 时行为与改造前完全一致 | `DeleteRankDef`（[service_redis.go:717](../../common/rank/service_redis.go)），调用方 `Service.Cleanup()`、`Manager.forceCleanupOrphan`，均只在 `RemoveService` 链路上 |
| `rank:inst` | `OpenInstance`（`SetNX` + `TTLForActivityEnd`）；另有两个带 TTL 的写入点：`BatchUpsertScore` 的元数据更新分支（同 pipeline 内 `Set(ttl)` + 补 mb/seq），以及 `RestoreInstance` 之后的 `ExpireInstance` | 冷启动 `RestoreInstance`（`ensureLoaded`）；运行时 `UpsertScore` 检测 `rank:mb` 不存在 → `recoverGroupData` → `RestoreInstance`。两条恢复路径都在末尾统一 `ExpireInstance(backfillTTL())` | `DeleteInstance`（一次调用删 inst/mb/seq/settled），调用方同上；结算后走 `ExpireInstance` 设 TTL 而非删除，**一次性活动与周期轮次同路径** |
| `rank:mb` | `BatchUpsertScore`（Lua），由 `UpsertScore` 每次调用；TTL 由紧随其后的元数据 pipeline 补设（Lua 内建不了——`EXPIRE` 作用在不存在的 key 上会被丢弃） | `RestoreMembers` 强写全部成员分数 | 同 `rank:inst` |
| `rank:seq` | 同 `rank:mb`（Lua 内维护自增序号） | `RestoreMembers` 恢复分数后把计数器推进到 `max(sequence)`（同函数内完成） | 同 `rank:inst` |
| `rank:settled` | `SettleInstance`（`SetEX(SettledCacheTTL)`，[service_redis.go:367](../../common/rank/service_redis.go)），由 `Settle()` 调用 | ① 冷启动 `RestoreSettled`（[store.go:435](../internal/rank/engine/store.go#L435)）强写快照，并在同一处 `SetEX(..., backfillTTL())` 把 TTL 一起设上（修前是裸 `Set`，会把结算时设的 TTL 抹掉）；② 历史查询 miss 时 `LoadGroupSettledCached` 用 `backfillTTL()` 回填（[store.go:1018](../internal/rank/engine/store.go#L1018)、[:1035](../internal/rank/engine/store.go#L1035)）——三条路径的 TTL 语义统一到「≥ 14 天」，互不缩短（缺陷 4） | 同 `rank:inst` |

**业务层数据（`internal/rank/engine` 层）**

| key | 新增（首次写入） | 恢复（miss 后重建） | 删除（显式路径） |
|---|---|---|---|
| `rank:meta` | `SaveActivityTimes`（[store.go:275](../internal/rank/engine/store.go#L275)）在 `NewService` 构造时调用一次（6 处注册路径的公共收口），同一 pipeline 内 `Expire(writeTTL())`；随后 `NextGroupID` / `IncrRealCount` 持续追加字段 | `RestoreNextGroupID` 把 `nextGroupID` 修正到 `≥ max(现有分组ID)`，防止 Redis 清空后新分组 ID 与旧分组冲突；三个时间字段无独立恢复函数，靠 `SaveActivityTimes` 幂等重写覆盖 | `CleanupAll`（[store.go:671](../internal/rank/engine/store.go#L671)），调用方 `Service.Cleanup()` 与 `forceCleanupOrphan` |
| `rank:groups` | `SaveGroup`，由 `ensureGroupLocked` 新建分组时调用 | `LoadGroups` miss 时从 `CT_RANK_GROUP` 批量回填 | `CleanupAll` |
| `rank:members` | `SetMember`，由 `UpsertScore` 新成员入组时调用 | `GetMember`/`GetAllMembers` miss 时回填；查不到写负向哨兵 `nullCacheEntry`（"记住查过没有"） | `CleanupAll` |
| `rank:claims` | `AtomicClaim`（Lua 首次领奖原子写入）；`SetClaim` 供补偿/GM 路径 | `AtomicClaim` 内建"Redis 未命中但 Mongo 有记录"分支：判定首次领取后反查 `CT_RANK_CLAIM`，命中则回写；真首次则 `SaveClaimIfNotExists` 落库防重复 | `CleanupAll` |
| `rank:robots` | `SaveRobots`，由 `spawnRobotsForGroup` 在首位真实玩家入组时调用；`tickAllRobots` 增量更新 | `LoadRobots` miss 时从 `CT_RANK_ROBOT` 批量回填 | `CleanupAll`（逐分组） |
| `rank:robot_infos` | `SaveUsedInfoIDs`（`SAdd`），调用方同 `rank:robots` | **无 Mongo 权威源**——miss 时返回空集合，靠后续机器人生成重新累积；属"可重建"而非"精确还原" | `CleanupAll`（逐分组） |
| `rank:mongo_chk` | `setMongoChecked`（`SetNX(key, "1", mongoCheckedTTL)`，[store.go:92](../internal/rank/engine/store.go#L92)），仅在确认"Mongo 也确实无数据"时才写。**必须是 `SetNX` 而不是 `SetEX`**：`SET` 会连带清掉 key 上已有的过期时间，把 `ExpireLiveData` 设的 2 周削成 10 分钟（待办 J） | 不需要——它本身就是缓存哨兵；到期后下次读重新判定 | 无独立删除路径；清理该 bizId 时顺带处理，否则自然过期 |

**编排层（`internal/rank` 层）**

| key | 新增 / 恢复 | 删除 |
|---|---|---|
| `rank:member_index` | `Track`（`SAdd` + `Expire(ColdDataTTL)`，[member_index.go:53](../internal/rank/member_index.go#L53)），由 `onMemberJoin` 回调在成员首次入组时触发。**恢复**走独立路径 `rebuildMemberIndex`，遍历该用户参与的所有 `engine.Service` 内存状态重建，权威源是 engine 内存而非 MongoDB（全表唯一例外） | `RemoveUserEntries`（[member_index.go:116](../internal/rank/member_index.go#L116)，按 512 分块 pipeline，待办 G-d2），调用方 `RemoveService` / `forceCleanupOrphan` / `subscribeDeleteEvents` / `syncFromMongo` |
| `rank:{active_services}` | `Store.registerActive`（`SAdd` + `Expire(ColdDataTTL)`，[store.go:345](../internal/rank/engine/store.go#L345)），meta key 创建时同函数调用；启动期由 `bootstrapRegistry` 批量播种 | `Store.unregisterActive`（`SMEMBERS` + 前缀筛选 + `SRem` + `Expire`，[store.go:371](../internal/rank/engine/store.go#L371)），meta key 删除时同函数调用；成员级由 `syncFromRedis` 按 deadline 惰性剔除 |
| 3 个锁 key | `TryLockSettle` / `TryLockRobotTick` / 内联 `SetNX`，`SetNX` 本身即新增即恢复 | 无显式删除，TTL 自然过期（锁语义的正常设计） |

---

## 3 · 13 条优化逐条说明

### 00 · 建立 socialserver 全局协程池（基础设施） ✅

**原问题**

全仓无限量 `go func()`，缺少统一的有界执行层，四处散落：

| 位置 | 形态 | 风险 |
|---|---|---|
| `warmUpAllServices` | 每个 Service 一个 goroutine，无上限 | Service 上千时瞬时上千 goroutine + Redis/Mongo 并发 |
| `syncFromMongo` | `go localNewSvc.WarmUp(...)` | 散落，无追踪 |
| `periodic/advanceRound` | `go svc.WarmUp` + 局部 `warmupSem` | 各写各的信号量，无全局约束 |
| `periodic/advanceRound` | `time.AfterFunc` | 未纳入生命周期（见第 07 条） |

`tickLoop` / `syncLoop` / 2×`subscribe` 是常驻守护循环，不属于池的适用范围，保持现状。

`golib/gpool` 不能直接复用：无完成通知（而 `tickServices` 必须等全部 Tick 结束才能跑周期活动）、无非阻塞投递（慢 worker 会阻塞 dispatch 循环）、无关闭语义、worker 内无 panic 保护。

**方案**

单一全局池，任务的重要性差异通过**三个提交入口**表达，而不是通过池的隔离：

| 入口 | 语义 | 适用任务 |
|---|---|---|
| `Submit` | 非阻塞，队满返回 `ErrPoolFull` | WarmUp、重建等可丢弃/可重试任务 |
| `SubmitWait` | 阻塞直到入队，不丢弃 | **批内**单个 Service 的 tick / 批量 WarmUp |
| `SubmitWaitTimeout` | 阻塞至多 d，超时返回 `ErrPoolFull` | **常驻 ticker 循环提交批次**，跳过优于阻塞 |

第三个入口是必需的：`time.Ticker` 的 channel 容量为 1，消费不及时会**静默丢弃** tick，所以常驻循环不能用无界 `SubmitWait`。跳过一个 tick 是安全的（排行榜状态由 Redis 决定，`Tick`/`Settle` 不维护跨轮次内存增量），但**必须记 warn 日志**——否则池长期饱和会表现为"排行榜悄悄停更"，比崩溃更难发现。

**完成语义由调用方持有**（池只管有界执行，批量等待用 `sync.WaitGroup` 组合，避免池级 `Wait()` 误等无关任务）；提交失败/超时必须 `wg.Done()` 配平，否则 `wg.Wait()` 永久阻塞。

**三项配套硬约束**

1. **panic 保护**：worker 内 `recover`。池化之后尤其重要——原先单任务 panic 是"进程崩溃"（可观测），池化后会退化为"任务挂起"（难排查）。
2. **关闭顺序**：池必须先于 Redis/MongoDB 关闭并 drain 完成，否则残留任务会操作已关闭的连接。
3. **池内任务不得再阻塞等待池内任务**，除非 worker 数**严格大于**可并发阻塞的批次数。这是必须写进代码注释的不变量。

**已采纳的加固**：文档给出的替代做法是"外层批次不经过池，`tickLoop` 直接同步调用 `tickServices`，只把每个 Service 的 `Tick` 提交进池"——这样嵌套彻底消失，代价是批次本身占着一个常驻 goroutine，而它本来就占着 `tickLoop` 的 goroutine。**代码采用的就是这一版**（[manager.go:142](../internal/rank/manager.go#L142)）。

**状态**：✅ 已实现

**落点**：[pool.go](../internal/taskpool/pool.go)（`New` / `Submit` / `SubmitWait` / `SubmitWaitTimeout` / `Close` / `Global`）；[server.go:141](../internal/server.go#L141) 初始化（32 worker / 4096 队列）、[server.go:173](../internal/server.go#L173) 关闭（先于 `queue.Shutdown`）；[manager.go:138](../internal/rank/manager.go#L138) `tickServices`、[manager.go:900](../internal/rank/manager.go#L900) `warmUpAllServices`；[handler.go](../internal/rank/periodic/handler.go) 删除局部 `warmupSem`

**测试**：`pool_test.go` 7 例 + `manager_taskpool_test.go` 4 例（见 §5）

**剩余**：`tickSubmitTimeout` 只约束"单次提交"，不约束批次总量 —— 见 §7 观察 1

---

### 01 · 取消周期全库扫描，改「启动 SCAN 建表 + 之后查注册表」 ✅

**原问题**

`syncFromRedis` 用 `m.rdb.Keys("rank:meta:{*}")` 每 30 秒扫全库，两个问题：

1. **阻塞**：`KEYS` 在 Redis 单线程命令循环里一次性遍历全部 key，期间该节点上所有命令排队（包括其它业务的读写）。
2. **集群下是功能性 bug**：go-redis 对 keyless 命令走 `hashtag.RandomSlot()`，即 `KEYS` 只命中**一个随机 master**，其余分片的 `rank:meta` 永远扫不到。

先厘清 `SCAN` 的成本结构再谈频率：`SCAN` 不阻塞，但 `MATCH` **不减少**工作量——仍游走整个 keyspace，只是服务端过滤后才返回。成本取决于**实例总 key 数**，与有多少个 `rank:meta` 无关。单次迭代往返次数 ≈ 全库 key 数 / COUNT。按 100 万 key、10 节点、30s 频率估算 ≈ 670 次扫描往返/秒。

**关键判断：既然发现的本质是「启动期一次」，周期扫描就没有必要存在。**

`syncFromRedis` 只在「Redis 有、MongoDB 没有」时才有价值，而这个状态只可能由**进程崩溃于 Mongo 异步写落盘之前**产生：

| 场景 | 谁负责恢复 | 是否需要扫描 |
|---|---|---|
| 运行期新活动创建 | pub/sub（`rank:create`）实时通知所有节点 | ❌ |
| pub/sub 漏消息 | 源节点已写过 MongoDB → `syncFromMongo` 补 | ❌ |
| 进程崩溃、Mongo 写入未落盘 | 只有 Redis 有数据 | ✅ **仅此一种** |

所以它的本质是**启动期的崩溃恢复手段**，不是需要 30s 轮询的热路径。

**方案**

两段式：① 每个节点启动后各自做一次 `SCAN` 把注册表建起来（`SADD` 幂等，失败不置位、下轮重试）；② 之后每轮只 `SMEMBERS` 注册表 + **纯本地**按 deadline 过滤，陈旧成员批量 `SREM`。

**成员格式里带截止时间，修剪不需要任何额外 Redis 命令**

```text
key:    rank:{active_services}       // 单 SET，hash tag 固定单一 slot
        TTL = ColdDataTTL（滑动；每次 SAdd / SRem 后刷新）
member: {bizId}:{deadlineMillis}     // 如 balloon_1:1760000000000
```

`deadline = settleAt + SettledCacheTTL`（正是第 02 条推导出的绝对过期时刻）。修剪因此变成**纯本地过滤**：`SMEMBERS` 返回后直接在内存里筛掉 `deadline < now` 的成员，攒够一批发一次 `SREM`。每次迭代的额外命令数：`SMEMBERS` 1 条 +（有陈旧成员时）`SREM` 1 条。

若改成对每个成员发 `EXISTS rank:meta:{bizId}`，命令数就是 O(活跃服务数) × 2/分钟——**比它取代的扫描还贵**。

**注册表只挂两个维护点（这是方案可行的关键）**

| 时机 | 落点 | 说明 |
|---|---|---|
| 创建 | `Store.SaveActivityTimes` → `SAdd` | meta key 的**唯一**创建入口，由 `engine.NewService` 构造时调用，而 `NewService` 是全部 6 处注册路径的公共收口 |
| 删除 | `Store.CleanupAll` → `SRem` | meta key 的**唯一**删除出口 |
| 过期 | meta 的 TTL 到期后 meta 自动消失 | 无法在写入时预知，由 ② 惰性修剪收敛 |

这三处同时也是注册表 **key 自身 TTL** 的续期点：`SAdd` / `SRem` 之后各补一次 `Expire(ColdDataTTL)`（放进同一个 pipeline，零额外往返）。因为续期与成员增删同处，**不存在"改了成员忘了续期"的路径**——与「不存在"改了 meta 忘了改注册表"」是同一个论证。

**注册表必须能自愈（三种不一致场景）**

| 不一致 | 后果 | 兜底 |
|---|---|---|
| 漏 `SADD` | 该服务不被 `syncFromRedis` 发现 | pub/sub（`rank:create`）实时通知；`syncFromMongo` 重建时经 `NewService` → 再次 `SADD` |
| 漏 `SRem` | 注册表残留幽灵 bizId | ② 惰性修剪，每 30 秒收敛一次 |
| meta 因 TTL 到期消失 | 同上 | 同上，靠 deadline 到点剔除 |

**Redis 被整体清空**：注册表与 meta 同时消失，`registryBootstrapped` 已为 true 故不再 `SCAN`，但 `syncFromMongo` 会从 MongoDB 重建全部内存服务，每个都经 `SaveActivityTimes` → `SADD`，注册表自动重建。**此场景不需要 `SCAN`**——因为「Redis 有、Mongo 没有」在清空后不可能存在。

**原方案的三处错误（已修正）**

| 原方案 | 问题 | 现方案 |
|---|---|---|
| `SetNX rank:{registry_ready}` 哨兵 | 节点若在补种中途硬崩溃，哨兵永久留在 Redis，此后**没有任何节点会再补种**，注册表保持为空 → `syncFromRedis` 静默失效（不报错，只是再也发现不了服务） | 无哨兵，靠进程内 `registryBootstrapped atomic.Bool`；每节点各自补种，无单点 |
| 两阶段发布 + 存量迁移 | 需要协调发布顺序 | 无。旧代码创建的活动会被本节点 `syncFromMongo` 重建时自动补 `SADD`；`bootstrapRegistry` 覆盖「Redis 有、Mongo 没有」的存量 |
| 对每个成员发 `EXISTS` 探测来修剪 | 命令数 O(活跃服务数) | deadline 编进成员，修剪降为纯本地过滤 |

**注册表规模**：成员含周期轮次的每个轮次，而轮次推进后旧轮的 meta 保留 2 周，所以大小 ≈「2 周内的轮次数 + 一次性活动数」，有界，不随运行时间无限增长。

**状态**：✅ 已实现（含缺陷 8 的注册表 key 滑动 TTL）

**落点**：[manager.go:592](../internal/rank/manager.go#L592) `syncFromRedis`、[648](../internal/rank/manager.go#L648) `registerFromBizId`、[736](../internal/rank/manager.go#L736) `bootstrapRegistry`、[773](../internal/rank/manager.go#L773) `registryDeadline`、[788](../internal/rank/manager.go#L788) `parseRegistryMember`；[store.go:275](../internal/rank/engine/store.go#L275) `SaveActivityTimes` → `registerActive`、[345](../internal/rank/engine/store.go#L345) `registerActive`、[371](../internal/rank/engine/store.go#L371) `unregisterActive`、[671](../internal/rank/engine/store.go#L671) `CleanupAll` → `unregisterActive`；`golib/redis/redis_op_scan.go` 新增 `ScanAll`（集群走 `ForEachMaster`，单机直接 `Scan`）

**测试**：`registry_test.go` 4 例（成员解析 / 周期轮次 bizId / 非法成员 / `activityTimes` 缺失时 deadline 退化）

**剩余**：注册表写入与剪枝（`registerActive` / `unregisterActive` / `syncFromRedis`）**无法单元测试** —— 见 §7 观察 3

---

### 02 · tickAllRobots 加软缓存（TTL 2s）+ 活跃期 Redis key TTL 设计 ✅

**原问题**

每秒，在抢到 `TryLockRobotTick` 的节点上，每个有机器人的 Service 调用 `LoadGroups()` + 每组 `LoadRobots()` + 每组 `Range(0, -1)`。设 S 个 Service、每 Service 平均 M 组：每秒 **S×(1+M+M) 次 Redis 调用**。

**其中 `Range(0,-1)` 是最大的、也是原方案完全漏掉的一项**：`tickGroupRobots` 每组每秒做一次**全量 ZSET 读**，却只用其中两个标量（榜一分数、真实玩家榜一分数）。分组 ZSET 大小有上界 = `RankPeopleNum` + 组内机器人数（`ensureGroupLocked` 只在 `totalCount() < RankPeopleNum` 时复用分组），所以"有界"，但"有界"不等于"免费"——它仍是 S×M 次全量读。

**方案**

**① 软缓存（Cache-Aside + 2 秒有效期），只加在 `tickAllRobots` 路径**

| 缓存 | 覆盖 | 说明 |
|---|---|---|
| `tickGroups()` | 整份未结算分组列表 | **必须是整份列表**，按 `groupID` 取值满足不了 `tickAllRobots`；**必须判断有效期**，只判 `!= nil` 会让缓存永不过期，该节点从此看不到其他节点新建的分组 |
| `tickRobots(groupID)` | 按分组的机器人列表 | 缓存持有 `*robotState` 指针，原地修改直接反映到缓存，不必在 `SaveRobots` 后显式回写；`spawnRobotsForGroup` 后显式失效 |
| `tickGroupScores()` | 榜一 / 真实玩家榜一分数 | 把唯一未被覆盖的 `Range(0,-1)` 也纳入同一套 Cache-Aside（采用"减少读次"方向），≤2s 陈旧只影响 `calcGrowTarget` 万分比 clamp 的细微扰动 |

**不加在 `ensureGroupLocked`（UpsertScore 路径）上**：那是多节点并发写路径，节点 B 分配新成员时必须实时读 Redis 才能感知节点 A 刚建的分组，加缓存会导致重复分组。

**TTL 不能拉长**：`tickRobotScore` **不是时间的纯函数**——它是有状态的逐 tick 随机游走，依赖 `robot.Score` / `LastGrowAt` / `PendingScore` / `PendingStartAt`，并按 `GrowTokenCdMs` / `OvertakeIntervalMs` 决定这一步是否增长。喂给它越陈旧的状态，越会算错增长节拍。所以机器人状态的陈旧窗口**不能由 TTL 控制**，靠 Cache-Aside 写回压到最小；TTL 只用来覆盖「锁在节点间切换」这一场景。

**收益量级要说清楚（不要夸大）**：`tickGroupsCacheTTL = 2s` 而 tick 周期 1s，所以每个 key 每 2 秒至少回源一次，稳态降幅约 **2×**，不是「接近 0」。

| 调用 | 现状 | 缓存后 | 降幅 |
|---|---|---|---|
| `LoadGroups()` | 1/s | 0.5/s | 2× |
| `LoadRobots(groupID)` × M | M/s | M/2/s | 2× |
| `Range(instanceID, 0, -1)` × M | M/s | M/2/s | 2×（本轮新增覆盖） |

**② 活跃期 TTL = 距活动结束 + 结算保留期**

```go
// 与第 06 条共用同一个 effectiveSettleAt()：activityEnd = GameEndTime > 0 ? GameEndTime : CloseTime
// 两者若不一致，保留期就会算错（GameEndTime < CloseTime 的活动会提前过期）
ttl := time.Until(time.UnixMilli(activityEnd)) + commonrank.SettledCacheTTL
```

这个式子的关键性质是**绝对过期时刻与设置时机无关**：设在 `t0`，过期于 `t0 + (end - t0) + R = end + R`。由此得到两个好处：① 只需在活动创建/首次写入时设一次，不给 tick 热路径增加 `EXPIRE` 调用（理由是"幂等且无信息量"，不是性能——按 §1.3，`HSet` + `Expire` 同 pipeline 只算一次往返）；② 与 `CleanupLiveData` 方向一致：活跃期 TTL 的到期时刻恰好是 `end + 2周`，`CleanupLiveData` 之后把它重置为完整 2 周只是略微延后，**永不提前**。

若按原方案「活动周期 + 1 天 buffer」，`T+1d` 就会删掉本该保留到 `T+14d` 的历史数据，构成功能性退化。

**`rank:meta` 必须用同一个 TTL 一起过期**，否则活动结束 2 周后会出现「meta 存在、分组数据已空」的**空心服务**：`syncFromRedis` 据此注册一个没有分组数据的服务，`tryRecoverPeriodicFromRedis` 还会尝试从中降级恢复周期轮次。

**「常驻活动」这一形态不存在**：`settleAt == 0` 时 `Tick` 的守卫恒为假，`CloseTime=0 && GameEndTime=0` 的活动**在第一个 tick（≤1 秒）就被结算掉**，不是"长期存活"。所以没有"需要不设 TTL 的活动"。

| 情况 | 处理 |
|---|---|
| `effectiveSettleAt() > 0`（正常配置） | 走绝对过期公式，设一次 |
| `effectiveSettleAt() == 0`（非法配置，待办 K） | TTL 退化为 `ColdDataTTL` 滑动值兜底 |

**真正需要滑动 TTL 的不是活动数据**，而是没有天然结束时刻的非活动 key（`rank:def` / 注册表 / `rank:member_index`）。它们的续期点都**不在 tick 热路径上**，所以本条"降低每秒 Redis 调用"的目标不受影响。

**这是全文唯一不可逆的改动**

其余所有改动最坏结果是"慢"或"多一次读"，错了可以回滚；**只有这条算错就是删数据，且删掉不可恢复**（2 周保留期内的历史查询直接失效，`bootstrapRegistry` 也扫不回已过期的 key）。

因此落地时把它拆成了两步：先只打日志观察（`logPlannedActiveTTL`），确认 `expireAt` 恒等于 `activityEnd + SettledCacheTTL` 且不早于 `CleanupLiveData` 设的时刻，再打开真正的 `Expire`。**两步现已合并上线**——随「缺陷 3（读路径回填也带 TTL）」一起打开，因为只开写路径 TTL 而不开回填 TTL，读路径会把 key 重新写成永久的，两半必须同批。

诊断日志**保留**：它现在是线上核对 `expireAt` 的手段，而不是灰度开关。判断依据是「TTL 由谁设」——写路径用 `writeTTL()`，读/重设路径用 `backfillTTL()`，两个具名函数，调用方不需要挑分支。

**状态**：✅ 已实现（含活跃期 TTL，已全量开启）

| 子项 | 状态 | 落点 |
|---|---|---|
| 三项软缓存（groups / robots / scores） | ✅ 已实现 | [service_robot.go:34](../internal/rank/engine/service_robot.go#L34) `tickGroups`、[58](../internal/rank/engine/service_robot.go#L58) `tickRobots`、[94](../internal/rank/engine/service_robot.go#L94) `tickGroupScores`、[84](../internal/rank/engine/service_robot.go#L84) `invalidateRobotsCache` |
| TTL 公式 `ttlFor` + 下限版 `backfillTTLFor`（含缺陷 7 的退化分支） | ✅ 已实现 | [store.go:300](../internal/rank/engine/store.go#L300) `ttlFor`、`backfillTTLFor` |
| 活跃期 TTL 真正 `Expire` | ✅ **已开启** | 写路径：[store.go:145](../internal/rank/engine/store.go#L145) `writeTTL()`；读/重设路径：[store.go:120](../internal/rank/engine/store.go#L120) `backfillTTL()`。诊断日志 [store.go:335](../internal/rank/engine/store.go#L335) `logPlannedActiveTTL` 保留 |

**注意：不能用「`ttl <= 0` 就跳过 `Expire`」作为保护**——跳过等于把 key 留成永久的，与原则一直接冲突（详见缺陷 7）。正确做法是给一个极短 TTL（1 分钟）让它被回收；数据本来就在 MongoDB 里，删掉不影响正确性。

**测试**：`service_tick_cache_test.go` 7 例（三项缓存的命中 / 过期回源 / 失效）+ `store_ttl_test.go`（TTL 公式的绝对性、`activityEnd` 缺失退化、永不返回非正数）+ `store_write_ttl_test.go`（`backfillTTL >= writeTTL`、恒定 `>= SettledCacheTTL` 下限）+ `active_period_ttl_test.go`（活跃期写路径端到端全键扫描，见 §2.1 末注）+ `service_redis_miniredis_test.go`（`common/rank` 侧写入即带 TTL）

---

### 03 · ensureGroupInstance 加带有效期的内存标记 ✅

**原问题**

每次 `UpsertScore` 都调用 `ensureGroupInstance`，后者做一次 Redis `GetInstance`。实例创建后这个 GET 是纯冗余的，却发生在最热的写路径上。

**方案**：不使用永久 boolean 标记，而是记录**上次向 Redis 确认的时间**，超过有效期（60s）后回源重新确认；再叠加显式清理，让可控制的路径立即生效：

```go
type groupInstanceState struct{ verifiedAt int64 }
const instanceVerifyInterval = 60 * 1000 // 毫秒
```

**为什么用有效期而非永久标记 + 穷举删除路径**：`CleanupLiveData` 的 TTL 过期、Redis 清空、以及未来新增的删除逻辑都不需要穷举——60s 内自动自愈。

| 方案 | 每写一次分的 GetInstance 调用 |
|---|---|
| 现状 | 1 次（每次都调） |
| 永久标记 | 0 次，但需穷举所有删除路径，有残留风险 |
| **有效期标记（采用）** | 平均 1/60 次，且自动自愈 |

`instanceStates` 是**正向缓存**（只记录"已确认存在"），不产生误判：标记有效 → 跳过 GET 安全；标记无效/缺失 → 回源确认（与现状完全一致）；其他节点创建的实例 → 首次访问回源后写入标记。

**显式清理**：`CleanupLiveData` 在处理完 `ExpireInstance` 后立即 `invalidateInstanceState`，不必等 60s（[service.go:1103](../internal/rank/engine/service.go#L1103)）。`RemoveService` 场景无需调用——Service 本身会随之从内存摘除。

**状态**：✅ 已实现

**落点**：[group.go:92](../internal/rank/engine/group.go#L92) `ensureGroupInstance`、[87](../internal/rank/engine/group.go#L87) `instanceVerifyInterval`、[131](../internal/rank/engine/group.go#L131) `markInstanceVerified`、[143](../internal/rank/engine/group.go#L143) `invalidateInstanceState`

**测试**：`service_tick_cache_test.go` 4 例（有效期内跳过 / 过期后回源 / 不存在时创建并标记 / `CleanupLiveData` 失效标记）

---

### 04 · tickServices 改用全局协程池并发执行 ✅

**原问题**：`tickServices` 串行遍历所有 Service 的 `Tick()`，每次含 Redis RTT。20 个 Service × 5ms/次 = 100ms 累计延迟，最后几个 Service 的结算检测被推迟。

**方案**：接入第 00 条的全局池，**不自建临时 worker**。tick 属不可丢弃任务，但提交用 `SubmitWaitTimeout` 而非无界 `SubmitWait`（见第 00 条的 ticker 丢 tick 约束）；调用方持有 `sync.WaitGroup` 控制批次完成语义，`wg.Wait()` 之后才跑 `tickPeriodicActivities`。

两点实现细节：① `tickServices` 遍历的是 `m.services`（`RankBizService` 包装），而 `warmUpAllServices` 遍历 `m.engineServices`（元素是 `*engine.Service`）——两者是**不同**的快照，不能共用一个泛型快照函数；② 并发度不等于 Service 数：批次内每个 Service 只提交一次，所以同一 Service 的多个 tick 不会并发，但**不同 Service 会同时打 Redis**——这才是接入池真正的收益，也意味着 Redis 连接池大小要与 worker 数（32）匹配，否则收益会被连接等待吃掉。

**状态**：✅ 已实现

**落点**：[manager.go:138](../internal/rank/manager.go#L138)，`tickSubmitTimeout = 200ms`（[manager.go:33](../internal/rank/manager.go#L33)）

**测试**：`manager_taskpool_test.go` 3 例

**剩余**：见 §7 观察 1

---

### 05 · warmUpAllServices 改用全局协程池 ✅

**原问题**：`warmUpAllServices` 为每个 Service 启动一个 goroutine，100 个 Service 同时调用 `ensureLoaded`，每个触发 Redis HGetAll + MongoDB 查询，瞬时并发远超连接池容量。同一问题还散落在 `syncFromMongo` 的单点 `go localNewSvc.WarmUp` 与 `periodic/advanceRound` 的 `go svc.WarmUp` + 局部 `warmupSem`。

**方案**：与第 04 条同款做法，接入同一个池。**唯一差异是提交方式**：批量 WarmUp 因为要 `wg.Wait()` 汇总，用 `SubmitWait`；单点 WarmUp 用非阻塞 `Submit`（池饱和时主动丢弃，保护 tick，丢掉的由后续懒加载兜底）。`periodic/handler.go` 的局部 `warmupSem` 因此可以删除，由全局池统一约束。

**状态**：✅ 已实现

**落点**：[manager.go:900](../internal/rank/manager.go#L900) `warmUpAllServices`；`syncFromMongo` 的替换分支改为 `Submit`；[handler.go:289](../internal/rank/periodic/handler.go#L289) 改为 `Submit`，删除 `warmupSem`

**测试**：`manager_taskpool_test.go` 1 例（池小于批次仍全量执行）

---

### 06 · Tick/Settle 增加结算短路（配置变更自动失效） ✅

**先修正原稿的两处错误论断**

**❌ 论断一：「`IsSettled()` 在每次 tick（每秒）都被调用」——不成立。** `Tick()` 从不调用 `IsSettled()`。真正的每秒开销来自 `Tick` 与 `Settle` **各自**的一次 `LoadGroups()`：`Tick` 在 `now >= settleAt` 后每秒一次 HGETALL；`Settle` 无条件读，且在**所有分组已结算**时完全浪费。这个状态会持续到 2 周保留期结束，是主要成本。`IsSettled()` 的热调用点不是 tick，而是**每次查询**（`ResolveEngineService` / `ListServices` / GM 查询）。

**❌ 论断二：「已结算的活动被重新开放——当前不存在这条路径」——不成立，链路完整可走通。**

| 步骤 | 位置 | 行为 |
|---|---|---|
| 1 | `handleUpdateRankConfig` | GM 更新配置，把 `CloseTime` 推到未来 |
| 2 | `UpdateService` → `svc.UpdateConfig(cfg)` | **无任何已结算校验**，纯字段合并 |
| 3 | `canUpdateScore` | `now < CloseTime` → 重新返回 `InstanceStateOpen` |
| 4 | `UpsertScore` | 新玩家写入被接受 |
| 5 | `ensureGroupLocked` | 已结算分组不再复用（只挑 `GroupStateOpen`），**新建一个 Open 分组** |
| 6 | `IsSettled()` | 内存里同时有旧已结算分组和新 Open 分组 → 重新返回 **false** |

**因此 sticky `atomic.Bool` 会产生真实 bug**：标志一旦为 `true` 就永久短路，步骤 5 新建的分组从此不再被 tick/结算——机器人不增长，到点不结算，该分组永远停在 Open，**其中玩家的奖励永久丢失**。

**方案：记录「已按哪个 settleAt 完成结算」，而不是「是否已结算」**

把标志从 bool 换成 int64，存**观察到的 `settleAt`**。配置变更（GM 推后/提前 `CloseTime`）会使 `settledAt != settleAt`，短路自动失效，**不需要在 `UpdateConfig` 里写任何重置代码**。

**必改-1：短路必须自己加闸门，不能假设"结算后不会再有新分组"**

原稿的论证是单向的——它只证明了"活动结束后写入被拒"，却把结论推广成了"结算之后不可能有新分组"。代码里有两个反例：

| 反例 | 说明 |
|---|---|
| **① 时间比较方向不一致** | `canUpdateScore` 用 `now > GameEndTime`（**严格大于**），`Tick` 用 `now >= settleAt`。`GameEndTime` 是独立于 `CloseTime` 的 GM 字段。当 `0 < GameEndTime < CloseTime` 时，`now == GameEndTime` 这一毫秒内 `Tick` 已结算，而 `UpsertScore` 仍被接受 → 新建 Open 分组。窗口仅 1ms，但 `UpsertScore` 是高并发热路径，长期必然命中 |
| **② GM 提前手动结算** | `handler/rank.go:264` 直接调 `svc.Settle(ctx)`，**无任何时间校验**。该路径的唯一调用方是 GM 后台的「结算」按钮。GM 在 `now < settleAt` 时结算，`settledAt` 置位；此后 `canUpdateScore` 仍是 `Open`，玩家继续进榜并**继续创建新分组** |

两者后果相同且不可逆：新分组永远停在 `GroupStateOpen` → 永不 `CloseInstance`、永不 `SettleInstance` → **该分组玩家的成绩不进任何快照，奖励永久丢失**。更隐蔽的是 `IsSettled()` 同步短路返回 true，`ResolveEngineService` 于是把查询路由到历史 MongoDB 路径——**玩家连自己参与过这个活动都查不到**。

**修复**：把置位条件换成**与写入门禁同一个谓词**，而不是假设不变量成立：

```go
if s.canUpdateScore(time.Now().UnixMilli()) != rank.InstanceStateOpen {
    s.settledAt.Store(settleAt)
}
```

这样短路区间恰好等于「活动对写入关闭之后」，与要优化的区间（活动结束后仍 tick 的那 2 周）完全重合，**收益不变**。`IsSettled()` 的短路必须用同一条件，否则仍会把活跃分组误判为历史。

> 顺带建议：统一 `canUpdateScore` 的 `now > GameEndTime` 与 `Tick` 的 `now >= settleAt`（差一个等号）。但**统一后这道闸门仍要保留**——它能同时挡住 GM 提前结算这条路径，而那是纯时间比较挡不住的。

**必改-4：`settledAt.Store` 必须放在分组循环之后**

若落进某个分组的成功分支（`TryLockSettle` 为真的分支），则抢锁失败的节点永远置不上位，短路只在赢得锁的那个节点生效——**其余 N-1 个节点每秒 2 次 HGETALL 原样保留**，收益被摊薄到 1/N。正确落点是 `for` 循环之后：抢锁失败走 `continue`，循环结束后统一到达置位点。安全性由 `canUpdateScore` 闸门保证。

**`Tick` 与 `Settle` 都必须短路**：`Settle` 有三个调用点——`Tick`、GM 手动结算、周期轮次推进——只在 `Tick` 里短路会漏掉后两者。

| 场景 | `settledAt == settleAt`? | 行为 |
|---|---|---|
| 活动进行中 | 否（`settledAt=0`） | 与现状一致 |
| 已结算、配置未变 | 是 | 短路，跳过 2 次 HGETALL/s |
| 已结算 → GM 推后 `CloseTime` | 否（`settleAt` 变大） | 短路失效，新分组正常结算 ✅ |
| 已结算 → GM 提前 `CloseTime` | 否 | 短路失效，重走一次幂等结算（无害） |
| **GM 提前手动结算** | 否（闸门拦住，不置位） | 与现状一致 ✅ |
| **`now == GameEndTime` 边界那 1ms 建的新分组** | 否（同上） | 与现状一致 ✅ |
| 进程重启 | 否（`settledAt=0`） | 每个已结算 Service 多一次 `LoadGroups`（幂等，仅一次） |

**状态**：✅ 已实现

**落点**：[service.go:80](../internal/rank/engine/service.go#L80) `settledAt atomic.Int64`、[437](../internal/rank/engine/service.go#L437) `effectiveSettleAt`、[440](../internal/rank/engine/service.go#L440) `Tick`、[731](../internal/rank/engine/service.go#L731) `Settle`（短路在 `LoadGroups` 之前）、[827](../internal/rank/engine/service.go#L827) 置位点、[948](../internal/rank/engine/service.go#L948) `IsSettled`

**测试**：`service_settle_test.go` 7 例（Tick 短路后不再碰分组 / 写入未关闭时不钉死（必改-1）/ 闸门在循环后生效（必改-4）/ `IsSettled` 快路径 / GM 推后 `CloseTime` 自动失效 / `Settle` 短路 / `settleAt<=0` 跳过）

---

### 07 · time.AfterFunc cleanup 纳入 Handler 生命周期管理 ✅

**原问题**：`advanceRound` 用 `time.AfterFunc(cycleDelay, svc.CleanupLiveData)` 延迟清理历史轮次数据，timer 未被追踪：① Server 关闭后 timer 到期，`CleanupLiveData` 调用已关闭的 Redis 连接；② Server 在 timer 到期前重启，清理永远不执行（`CleanupHistoricalRounds` 已有兜底）。

**方案**：`Handler` 维护 `timers []*time.Timer`（受 `mu` 保护），`Clear()` 时 `Stop()` 所有 timer。timer 触发后从列表中移除（`untrackTimer`），避免列表无限增长。

**状态**：✅ 已实现

**落点**：[handler.go:47](../internal/rank/periodic/handler.go#L47) `timers`、[89](../internal/rank/periodic/handler.go#L89) `Clear`、[101](../internal/rank/periodic/handler.go#L101) `trackTimer`、[108](../internal/rank/periodic/handler.go#L108) `untrackTimer`、[246](../internal/rank/periodic/handler.go#L246) 调度点

**测试**：`handler_timer_test.go` 2 例（`Clear` 停掉已追踪 timer / `untrackTimer` 移除已触发 timer）

---

### 08 · MemberIndex 键过期策略与清理链路 ✅

**现状核验：TTL 已实现**（`memberIndexTTL = 7 天`，`Track` 时 `SAdd` + `Expire`）。但**TTL 不是"滑动"的**：`Track` 只在**入榜**和懒重建时被调用，没有任何周期性刷新，所以 7 天是从**最后一次入榜**起算。

**原稿「不推荐 TTL」的两条理由均不成立**

| 原稿理由 | 核对结果 |
|---|---|
| 「TTL 会导致活跃用户的历史记录被误清」 | ❌ 用户的活跃活动在每次入榜时都刷新 TTL；只有 7 天内无任何入榜行为的条目才过期，而这类条目对唯一的下游用途（GM 查询）价值本就低，且可重建 |
| 「Redis 重启后数据无法恢复」 | ❌ 已有懒重建 `rebuildMemberIndex`，遍历各 engine service 的内存 `memberGroup` 重建。`memberGroup` 常驻内存，重建不产生任何 Redis/MongoDB 访问（但代价不止于此，见 (d3)） |

TTL 的实际影响面很小：**唯一的生产消费者是 GM 查询**。所以过期不是数据丢失，只是"下次 GM 查询多一次纯内存扫描"。

**原稿的 ① 和 ② 其实已经实现，不是新增工作**

| 原稿条目 | 实际情况 |
|---|---|
| ② WarmUp 重建 | **已实现**：`ensureLoaded` 加载完 `memberGroup` 后对全部成员回调 `onMemberJoin`，而它正是 `Track` 的挂载点 |
| ① 活动删除时显式清理 | **已实现**：`RemoveUserEntries` 已存在并被 4 处调用 |

**真正待修的缺陷**

| 编号 | 问题 | 状态 |
|---|---|---|
| (a) | `RemoveByKey` 在生产环境是 no-op（`if rdb != nil { return }`），3 处调用点误以为已清理 | ✅ **已删除**（连同 3 处调用点） |
| (b) | 清理存在隐式顺序依赖且未封装：`RemoveUserEntries` 必须早于 `CleanupAll`（后者会 `Del` 掉 `rank:members`），顺序靠调用方手写维护 | ✅ **已封装为 `cleanupServiceData`** |
| (c) | `GetAllMembers()` 失败时静默跳过、无日志 | ✅ **已加 warn 日志** |
| (d) | `CleanupAll` 不清理 `rank:member_index`（它无法自行处理：`Store` 只持有 `bizId`，而索引条目编码是 `{bizType}:{actID}:{groupID}`，周期轮次后缀 `_r{N}` 让 bizId 不可逆） | ✅ 已修但**方式不是"补进 `CleanupAll`"**（那做不到）：改为把顺序约束固化成不变量——`Service.Cleanup()` 只允许被 `cleanupServiceData`（先 `RemoveUserEntries` 再 `Cleanup`）与 `forceCleanupOrphan` 两条同形状路径调用，并加了正反两个测试钉死。剩余风险由 7 天 TTL 兜底 |
| (d2) | `RemoveUserEntries` 逐条 `SRem`，无分块无 pipeline。`members` 是**整个活动**的成员表，10 万人活动即 10 万次串行往返 | ✅ 已修：按 `removeUserEntriesChunk = 512` 分块 pipeline，10 万用户从 ~10 万次往返降到 ~200 次。每用户 key 不同，无法合并成单条 `SRem`，只能靠 pipeline 降往返 |
| (d3) | 懒重建既无负缓存（miss 不被记住，重复全扫 N 个服务），`GetMemberGroupID` 又取的是 `s.mu.Lock()` **写锁**，与 `UpsertScore` 直接争锁 | ✅ **写锁部分已修**：`GetMemberGroupID` 只读 `s.memberGroup`、不调 `ensureLoaded`，改 `RLock` 是语义等价的一行修复（已核查无调用方依赖写锁语义），GM 批量查询不再与 `UpsertScore` 串行化。**负缓存未做**——它需要"用户无索引"的版本戳，架构改动较大，另立项 |

**TTL 使 (b)(c)(d) 从正确性问题降级为及时性问题**：索引键 7 天后必然过期，所以任何清理失败最多留下 7 天的陈旧索引条目，之后自动消失，且 GM 查询在 miss 时还能懒重建。因此它们**不阻塞上线**，属清理质量改进。这也是 (d) 选择"固化调用顺序"而不是"想办法让 `CleanupAll` 直接删索引"的理由：后者要先把不可逆的 bizId 编码问题解决掉，而收益只是把 7 天的兜底窗口缩短到即时。

**方案（已完成部分）**：统一清理入口，把顺序固定在一个地方；删除 `RemoveByKey` 及其调用点。

```go
// cleanupServiceData 按固定顺序清理一个服务的全部痕迹。顺序不可调换。
func (m *Manager) cleanupServiceData(svc *engine.Service, bizType BizType, actID int32) {
    switch members, err := svc.GetAllMembers(); {
    case err != nil:
        zaplog.LoggerSugar.Warnf("... (索引由 7 天 TTL 兜底)", ...)
    case len(members) > 0:
        m.memberIndex.RemoveUserEntries(bizType, actID, members)
    }
    svc.Cleanup() // 内部 CleanupAll 会删掉 rank:members，必须在其之前完成索引清理
}
```

顺带修掉了待办 B：`syncFromMongo` 此前**完全漏掉了 `svc.Cleanup()`**，导致孤儿实例与文档残留。

**状态**：✅ 已实现（(a)(b)(c)(d)(d2)(d3-写锁) 全部完成；仅 (d3-负缓存) 另立项）

**落点**：[manager.go:414](../internal/rank/manager.go#L414) `cleanupServiceData`；`RemoveService` / `subscribeDeleteEvents` / `syncFromMongo` 三处调用点统一；[member_index.go](../internal/rank/member_index.go) 删除 `RemoveByKey`、`RemoveUserEntries` 分块 pipeline、`removeUserEntriesChunk`；[service.go:1117](../internal/rank/engine/service.go#L1117) `GetMemberGroupID` 改读锁

**测试**：`manager_sync_test.go` 6 例（含 `TestCleanupServiceDataRemovesIndexBeforeCleanup` / `TestCleanupBeforeIndexRemovalLosesTheIndex` 一正一反钉死顺序不变量、`TestForceCleanupOrphanRemovesIndexBeforeCleanup`、无成员时仍必须无条件执行 `svc.Cleanup()`）；`member_index_miniredis_test.go` 5 例（跨分块不漏、不误删其它活动、空输入 no-op、索引键已被 TTL 回收时无害、`Track` 每次续期）；`service_lookup_test.go` `TestGetMemberGroupIDDoesNotRequireExclusiveLock`

---

### 09 · syncFromMongo 由「删除」改为「修复」 ✅

**原问题**

`syncFromMongo` 每 30 秒把「在 `m.engineServices` 但不在 MongoDB」的服务判定为已删除并清掉。但「不在 MongoDB」有两种完全不同的成因：

| 成因 | 应该做什么 |
|---|---|
| `dao.SaveRankConfig` 的异步写被丢弃（`maxRetries=1`，熔断打开 30s 后任务永久丢失） | **修复**——把内存中的 config 写回 MongoDB |
| GM 在本节点离线期间删除（`subscribeDeleteEvents` 漏了广播） | **删除** |

原稿的「保护窗口」方案（`registeredAt` + 60s）只覆盖第一种成因的**首次**判定，而且只是把删除**延后 60 秒**——写队列持续积压超过 60s 时误删照旧发生。它还引入了一个新字段和一个魔法常量。

**更严重的是：猜错的后果会自我延续。** 以 `tryRecoverPeriodicFromRedis` 为例：

```text
崩溃于 Mongo 写入落盘前
  → syncFromRedis 从 rank:meta 恢复服务（该路径不写 Mongo）
  → 30s 后 syncFromMongo 发现 Mongo 无此服务 → 删除
  → 下一次 syncFromRedis 再次恢复 → 再被删除 → 每 30 秒循环一次
```

`applyRankConfigDoc` 也有同样的形状：它从广播注册服务但**不写 MongoDB**，完全依赖广播源的写入落盘。

**方案：按「活动是否仍在进行」决定修复还是删除**（判别依据用 `cfg` 里已有的数据，不新增任何字段）

```go
if activityStillOpen(cfg, now) {
    // 活动仍在进行：MongoDB 缺失只可能是异步写被丢弃 → 修复，绝不删除
    m.dao.SaveRankConfig(key, cfg)
    continue
}
// 活动已结束且 MongoDB 无记录：视为已删除，走统一清理
m.cleanupServiceData(svc, BizType(cfg.BizType), cfg.ActID)
```

| 场景 | `CloseTime` 状态 | 行为 | 正确性 |
|---|---|---|---|
| 进行中的活动，写被丢弃 | `now < CloseTime` | 修复 | ✅ 活动不被误杀，且修复本身可重试 |
| 进行中的活动，`tryRecoverPeriodicFromRedis` 恢复 | `now < CloseTime` | 修复 | ✅ 终止振荡循环 |
| GM 删除（本节点漏广播） | 视活动状态 | 已结束 → 删除；仍在进行 → 先修复，删除由下次广播兜底 | ✅ |
| 活动超过 7 天（内存管理性剔除） | `now >= CloseTime` | 删除 | ✅ 与原行为一致 |
| 无结束时间的常驻活动 | `CloseTime == 0` | 修复 | ✅ 常驻活动不应被剔除 |

**关键的取舍原则：删除是不可逆的，修复是幂等且自愈的。** 若修复写本身也被队列丢弃，下一轮 `syncFromMongo`（30 秒后）会发现它仍不在 Mongo，于是**再次修复**——天然重试，无需额外机制。反之，误删之后没有任何机制能恢复（除启动期的 `bootstrapRegistry` 扫描，而进程不重启就不会再跑）。在信息不足以区分两种成因时，选择**可恢复的那一侧**。

**已知副作用**：若 GM 删除了一个**尚未结束**的活动，而某节点漏掉了 `rank:delete` 广播，本方案会把该活动**写回 MongoDB**（复活）。这是「优先可恢复」的必然代价。规避需要新增带 TTL 的删除墓碑键，与「不改变现有存储结构」的约束冲突，因此接受该副作用，并要求 `rank:delete` 广播的可靠性由重连订阅保障（`subscribeDeleteEvents` 已有重连逻辑）。

**状态**：✅ 已实现

**落点**：[manager.go:1000](../internal/rank/manager.go#L1000) `activityStillOpen`、[manager.go:1005](../internal/rank/manager.go#L1005) 附近的 `syncFromMongo` 修复/删除分支

**测试**：`manager_sync_test.go` 2 例（`activityStillOpen` 的 5 种场景 / 清理入口行为）

---

### 10 · ListGroupRank / GetMemberRank 提取 resolveSettled ✅

**原问题**：两个方法各含约 30 行相同的"检查内存 settledGroup → 检查 Redis group 状态 → loadAndCacheSettled"逻辑。

**方案**：提取私有方法 `resolveSettled(groupID, cached)`，两处复用。抽取时核对了原两处加锁位置的细微差异（`cached` 由调用方持锁时读出后传入，避免在方法内重复加锁）。

**状态**：✅ 已实现 — [service.go:588](../internal/rank/engine/service.go#L588)

**测试**：`service_lookup_test.go` 3 例（命中缓存不回源 / 未知分组返回 nil / 回退到内存分组缓存）

---

### 11 · Settle() 减少 cloneSnapshots 调用次数（3 次 → 1 次） ✅

**原问题**：同一次结算中 `members` 被 `cloneSnapshots` 三次，产生三份相同副本。

**方案**：`settledGroup` 与 `SaveSettled` 共享同一份 clone。前提已核对：`results` 返回给调用方只读、`settledGroup` 是内存缓存只读、`SaveSettled` 是异步队列 + 只读的 `json.Marshal`，都不修改 slice。

**状态**：✅ 已实现 — [snapshot.go:5](../internal/rank/engine/snapshot.go#L5)

**测试**：`service_lookup_test.go` 1 例（验证三处共享同一份快照）

---

### 12 · findTier / robotAvatarInfo 改为 map 查找 ✅

**原问题**：`findTier(tierID)` 和 `robotAvatarInfo(robot)` 在每次机器人 tick（每秒 × 每机器人）时线性扫描配置数组。

**方案**：`NewService` 时预建 `tierMap map[int32]*RobotTierCfg` 与 `infoMap map[int64]*RobotInfoEntry`，O(1) 替换 O(N)。构造后只读（`RobotTiers`/`RobotInfos` 当前不会被 `UpdateConfig` 修改），因此无需加锁。

**状态**：✅ 已实现 — [service.go:116](../internal/rank/engine/service.go#L116) `buildRobotLookupMaps`、[service_robot.go:302](../internal/rank/engine/service_robot.go#L302) `robotAvatarInfo`、[315](../internal/rank/engine/service_robot.go#L315) `findTier`

**测试**：`service_lookup_test.go` 2 例（命中 / 未命中）

---

### 13 · WarmUp 加原子快路径 ✅

**原稿目标**：`ensureLoaded` 改用 `sync.Once`，首次 load 的 IO 不占 `s.mu`。

**核对结论：原稿的 `sync.Once` 方案零收益，已否决。**

**为什么零收益**：14 处调用点**全部在持有 `s.mu` 时**调用 `ensureLoaded`（每一处都是 `s.mu.Lock(); s.ensureLoaded()`）。把 `loaded bool` 换成 `sync.Once` 后，IO 依然运行在调用方已持有的 `s.mu` 内——**锁的持有时间不变，收益为 0**。原稿「其他 goroutine 在 `Do` 返回后立即继续，无需持有 `s.mu`」的前提是调用方不持锁，而当前所有调用方都持锁。

**第二个约束**：状态安装必须留在锁内。`ensureLoaded` 填充的 `s.groups` / `s.memberGroup` / `s.nextGroupID` 都受 `s.mu` 保护并被多个读路径直接访问，把整个函数体塞进 `Once.Do` 会让这些字段在**无锁**下被写，构成数据竞争。因此两阶段改造虽然收益真实，但需改动全部读路径的加锁边界，**风险与收益不匹配，本轮不做**。

**顺带修正一处错误注释**：`ensureLoaded` 原注释称「并在 Redis 被清理后完整恢复」，这不成立——`loaded` 没有任何重置点，置 true 后永不重载。它的"一次性"语义其实**已经等价于 `sync.Once`**。真正的 Redis 丢失恢复是按分组进行的 `recoverGroupData`。

**采用第三种做法：`WarmUp` 加一条原子快路径（1 行、零风险）**

`warmUpAllServices` 是 `WarmUp` 的唯一调用方，而 `WarmUp` 是**唯一自己获取 `s.mu` 的函数**（其余调用点的加锁由外层负责）。所以只要把早退提到加锁之前，就能在不触碰其他 13 处的前提下消掉稳态开销：

```go
func (s *Service) WarmUp(ctx context.Context) {
    if s.loaded.Load() { return }   // 原子快路径：稳态下不再进 s.mu
    s.mu.Lock()
    defer s.mu.Unlock()
    s.ensureLoaded()
}
```

`loaded` 从 `bool` 升级为 `atomic.Bool`（**全量替换**，不是与 `bool` 并存；`loaded` 只在 2 处出现）。

**收益量级要说清楚，不要夸大**：现状是每服务每 30 秒一次写锁获取，抢到锁后只做一次 `if s.loaded { return }`。**消除的是"获取"而非"持有"**——省下的是每次 `Lock()` 的原子操作与排队，以及与 `UpsertScore` 的争抢机会。规模是 N/30 次/秒的写锁获取，对一个每秒被 `UpsertScore` 获取成百上千次的锁来说**不显著**。它的价值在于**成本几乎为零且方向正确**。

**状态**：✅ 已实现 — [service.go:327](../internal/rank/engine/service.go#L327)

**测试**：`service_lookup_test.go` 1 例（已加载时 `WarmUp` 不取 `s.mu`）

---

## 4 · 缺陷与待办：全部已修复

### 4.1 缺陷 3 / 6 / 1 / 4（+ 2 / 7 / 8）：「不允许存在永久 key」这一簇 ✅

**全部已落地并全量开启**（无灰度、无开关）。下面保留每个缺陷的「问题」与「为什么不能拆」作为记录，修法一栏写的是**实际落地的方案**——与本轮开工前的草案有几处实质差异，差异都写在各自小节里。

这四个缺陷必须**一起**做，不能拆开——它们是同一个不变量的不同侧面。

**为什么不能拆**

| 拆法 | 后果 |
|---|---|
| 只做缺陷 3（回填补 TTL），不做缺陷 6 | 一次性活动的 11 个 key 依然永久残留 |
| 只做缺陷 6，不做缺陷 3 | 结算后设的 TTL 会被任何一次「到期后读取」的回填**重新写成永久** —— TTL 在最该生效的历史查询场景下完全失效 |
| 做 3 + 6，不做缺陷 1 | `rank:def` 是唯一「设了 TTL 就会出故障」的 key（新分组建不出来） |
| 做 3 + 6 + 1，不做缺陷 4 | `rank:settled` 的 TTL 会被冷恢复的 `RestoreSettled` 抹掉（而它恰恰在 key 已不存在时被调用） |

#### 缺陷 3（**最关键**）：所有懒加载回填都用 `HSet`，不带 TTL

这是原则一在**实施层面**最大的坑：第 02 条把 TTL 设对了，但只要有**任何一次读发生在 TTL 到期之后**，回填就会把 key 重新写成永久的。

| 回填点 | 代码（修后位置） | 修前的回填方式 | 修后 |
|---|---|---|---|
| `LoadGroups` | [store.go:200](../internal/rank/engine/store.go#L200) | `HSet` ×N，**无 TTL** | ✅ `backfill` |
| `GetMember` | [store.go:488](../internal/rank/engine/store.go#L488) | `HSet`，**无 TTL** | ✅ `backfill` |
| `GetAllMembers` | [store.go:534](../internal/rank/engine/store.go#L534) | `HSet` ×N，**无 TTL** | ✅ `backfill` |
| `LoadRobots` | [store.go:583](../internal/rank/engine/store.go#L583) | `HSet` ×N，**无 TTL** | ✅ `backfill` |
| `GetClaim`（含负缓存哨兵） | [store.go:944](../internal/rank/engine/store.go#L944)、[:952](../internal/rank/engine/store.go#L952) | `HSet`，**无 TTL** | ✅ `backfill`（哨兵也必须有 TTL——漏了它照样是永久 key） |
| `AtomicClaim` | [store.go:840](../internal/rank/engine/store.go#L840) | Lua 强写，**无 TTL** | ✅ Go 侧 `Expire(backfillTTL())`，不改 Lua |
| `RestoreSettled` | [store.go:444](../internal/rank/engine/store.go#L444) | `Set`，**无 TTL**（见缺陷 4） | ✅ `SetEX(..., backfillTTL())` |
| `LoadGroupSettledCached` | [store.go:1018](../internal/rank/engine/store.go#L1018)、[:1035](../internal/rank/engine/store.go#L1035) | `SetEX(SettledCacheTTL)` ✅ **修前唯一正确的写法** | ✅ 改用 `backfillTTL()` |

时间线（以 `rank:groups` 为例）：

```text
T        活动结束 → CleanupLiveData → EXPIRE rank:groups 2周
T+14d    key 到期，Redis 自动删除                ← TTL 生效
T+14d+ε  GM 查历史 → LoadGroups → Redis 空 → 查 Mongo（永久保留）→ 命中
         → HSet 回填 rank:groups               ← 但没有 EXPIRE！
T+∞      这个 key 永远存在了。且每查一个过期活动就多一个永久 key
```

**而这些恰好就是历史查询的目标活动**，所以不修这一条，第 02 条的 TTL 在最重要的场景下完全失效。

**修法（实际落地，与草案有实质差异）**：把所有回填点统一改为「回填 + 补 TTL」，抽出一个助手 `Store.backfill`：

```go
// store.go —— 回填 Redis 后立即把生命周期补上。
// 顺序必须是先写数据再 EXPIRE：若先 EXPIRE 后 HSet，EXPIRE 作用在不存在的 key 上会被丢弃。
func (st *Store) backfill(key string, write func()) {
    if !st.available() { return }
    write()
    _, err := st.rdb.Expire(key, st.backfillTTL())
    // err → warn
}
func (st *Store) writeTTL() time.Duration    { return ttlFor(st.activityEndMs()) }
func (st *Store) backfillTTL() time.Duration { return backfillTTLFor(st.activityEndMs()) }
```

**差异一：读路径不能用裸 `ttlFor`，必须有 14 天下限。** 草案写的是 `Expire(key, ttlFor(...))`，那是错的：`ttlFor` 对「活动结束已超过 14 天」返回 1 分钟的钳制值，于是一次历史查询会把 key 写成 1 分钟后过期 ⇒ 下次再 miss 再打 Mongo ⇒ **反复震荡**。`backfillTTLFor` 在 `ttlFor` 之上取 `max(_, SettledCacheTTL)`，同时满足三个方向：

| 要求 | 论证 |
|---|---|
| 永不出现永久 key | `>= SettledCacheTTL = 14d > 0`，任何分支都不返回非正数 |
| 绝不在保留期结束前删除 | 活动还在未来时 `ttlFor` 就是精确剩余时长，`max` 原样保留（与写入路径的绝对值一致 ⇒ 幂等）；已过绝对到期时刻时下限给出「从此刻起 14d」，严格长于剩余保留期 |
| 不让读路径打 Mongo | 下限消除上面说的震荡 |

**「写路径用 `writeTTL()`，读/重设路径用 `backfillTTL()`」是一条硬规则**——两个具名函数，调用方不需要挑分支，也就没机会挑错。

**差异二：`activityEnd` 由闭包注入 Store，且必须是原子读。** `Store` 原本只有 `bizId`，拿不到活动结束时刻。用闭包 `activityEnd func() int64`（直接传 `Service.effectiveSettleAt`）而不是快照值，使 GM 改配置后立即生效、结构上不可能陈旧；对应地 `Service.activityEnd` 从普通字段改成 `atomic.Int64`——回填发生在 `s.mu` 之外，与持锁的 `UpdateConfig` 并发，直接读 `s.config` 就是一处新的无锁竞争读。

**兼容性红利**：非活动上下文的 9 处 Store（历史查询 / 孤儿清理 / 周期元数据读 / claim 兜底）传 `nil` 闭包，`activityEndMs()` 返回 0 ⇒ `backfillTTLFor(0) == SettledCacheTTL`，与它们原本手工设的 2 周常量逐位相同。行为变化只发生在拿到真 `activityEnd` 的那一个 Store，审查面因此收窄。

**差异三：`AtomicClaim` 用 Go 侧 `Expire` 而不是改 Lua。** 改脚本会变更 ARGV 契约，且 miniredis 不校验跨 slot 语义——改 Lua 的风险只有生产 Cluster 才暴露。

**改造后 7 个回填站点**：`LoadGroups`、`GetMember`、`GetAllMembers`、`LoadRobots`、`GetClaim`（含负缓存哨兵）、`AtomicClaim`（3 处）、`LoadGroupSettledCached`。同时**删掉了 3 处手工 `Expire(..., SettledCacheTTL)`**（`periodic/handler.go`）：它们排在 `backfill` 之后会把算出的更长 TTL（活动还剩 3 个月时 ≈104 天）**削回 14 天**，「不提前删除」的论证依赖删掉它们。

**测试**：`store_miniredis_test.go`（`TestBackfillAfterExpiryRecreatesBoundedKey` 等，见 §5）——第 3 步同时证明「不会重建为永久 key」（`TTL != 0`）与「不会退化成 1 分钟震荡」（下限生效）。

**另一条恢复路径的例外**：`RestoreMembers` / `RestoreInstance` 是用 Lua 写的**强写**路径（`rank:seq` 由脚本内 `INCR` 维护），补 TTL 要么改脚本追加 `EXPIRE`、要么由 Go 侧在脚本返回后补一次。选的是后者：engine 侧在两条恢复路径（`ensureLoaded` 冷启动、`recoverGroupData` 运行时）的末尾各补一次 `ExpireInstance(backfillTTL())`，覆盖 inst/mb/seq/settled **四个** key，不必改 `common/rank` 的接口签名。这两处注释里写死了「必须用 `backfillTTL()` 而不是 `writeTTL()`」——恢复可能发生在活动结束很久之后，用 `writeTTL()` 会算出 1 分钟钳制值，把刚恢复的数据立刻删掉。

#### 缺陷 6（**「永久 key」的主要来源**）：一次性活动结算后没有任何 TTL 设置点

`Service.Settle()` 在结算一个分组时依次做 `CloseInstance` / `SettleInstance` / `SaveGroup` / `SaveSettled`（Mongo）/ `SaveRankInst`（Mongo）——**一次 `Expire` 都没有**。

而 TTL 的两个设置点**当时**都只挂在周期路径上：

| 设置点 | 覆盖 key | 修前的调用者 | 修前覆盖一次性活动？ |
|---|---|---|---|
| `Store.CleanupLiveData`（[store.go:667](../internal/rank/engine/store.go#L667)） | meta / groups / members / claims / mongo_chk / robots / robot_infos | 只有 `periodic/handler.go` 的 `time.AfterFunc` 与 `CleanupHistoricalRounds` | ❌ 只有周期 |
| `RedisService.ExpireInstance`（[service_redis.go:708](../../common/rank/service_redis.go)） | inst / mb / seq / settled | 只有 `Service.CleanupLiveData` | ❌ 只有周期 |

修后这两个设置点各自多了一批调用者：`ExpireLiveData` 由 `Settle` 直接调用（下文的 `settledGroups` 循环），`ExpireInstance` 除 `CleanupLiveData` 外还由 `Settle` 与两条恢复路径（`ensureLoaded` 冷启动、`recoverGroupData` 运行时）调用。

**后果**：一次性活动走完 `Tick` → `Settle()` 之后，它的 11 个 key **全部无 TTL，永久残留**。唯一的回收路径是 GM 手动 `RemoveService` → `Cleanup()` → `CleanupAll`——而那是**连 MongoDB 一起删的硬删除**，不是生命周期管理。GM 不删，key 就永远在，且每结算一个一次性活动就永久多 11 个 key。

**这比缺陷 2 严重**：活跃期的 key 至少在活动结束时有机会被清掉，而这里的 key 已经走完了全部业务流程。

**修法（实际落地）**：`Store.ExpireLiveData(groups []*Group, ttl time.Duration)` 是 `CleanupLiveData` 的「按传入 TTL 版本」，两者共用同一份 key 集合定义；`CleanupLiveData` 改为转调它并固定传 `settledDataRetentionTTL`。

**统一的是实现，不是 TTL 值**——周期路径**必须**继续用固定 14 天：轮次清理发生在结算后一个周期，改用 `ttlFor(roundClose)` 会让轮次数据总保留期从 21 天缩到 14 天，那是真实的功能退化。这条写进了 `CleanupLiveData` 的注释。

`Settle()` 里改为**先收集、循环之后统一设 TTL**：

```go
// engine/service.go —— Settle() 的 per-group 循环
settledGroups := make([]*Group, 0, len(groups))
for _, group := range groups {
    if group == nil { continue }
    if group.State == GroupStateSettled {                       // ← 崩溃窗口兜底
        settledGroups = append(settledGroups, group); continue
    }
    // …原有逻辑；两条 `group.State = GroupStateSettled` 赋值点（正常结算、
    //   ErrInstanceNotFound 分支）各自 append
}
if len(settledGroups) > 0 {
    ttl := s.store.backfillTTL()
    for _, g := range settledGroups {
        instanceID := s.groupInstanceID(g.GroupID)
        if err := s.rankService.ExpireInstance(ctx, instanceID, ttl); err != nil {
            zaplog.LoggerSugar.Warnf("rank engine: settle expire instance %s: %v", instanceID, err)
        }
        s.invalidateInstanceState(instanceID)
    }
    s.store.ExpireLiveData(settledGroups, ttl)
}
```

用 `backfillTTL()` 而不是 `writeTTL()`：结算可能发生在活动结束很久之后（GM 手动结算、节点重启后补结算），`writeTTL()` 会算出 1 分钟钳制值，把刚结算的数据立刻删掉。

**这段的三个易错点，每一个都有对应测试**：

1. **必须收集「已 settled」的分组**，不能只收集本轮新结算的。循环对 `State == GroupStateSettled` 的分组直接 `continue`，而上一次结算在 `SaveGroup` 之后、设 TTL 之前崩溃的话，那批 key 就永远补不上——`TestSettleTTLCoversAlreadySettledGroups` 守这一点。
2. **一份 TTL 逻辑要覆盖两个赋值点**（正常结算 + `ErrInstanceNotFound` 分支）。`ErrInstanceNotFound` 恰恰是最容易留永久 key 的路径：Redis 里的实例已过期消失，Mongo 里却还有数据，这条分支什么都不做的话 key 就烂在那里。
3. **绝不调用 `Service.CleanupLiveData()`**。那会把**尚未结算**的分组也按 14 天设 TTL——是数据丢失级错误。

循环末尾原有的 `settledAt` 闸门不动（见「必改-1 / 必改-4」）。

三点与草案不同，都是必须的：

- **收集「本来就已经是 settled」的分组**：循环对它们直接 `continue`，只收集本轮新结算的那批，会让「上一轮崩在设 TTL 之前」的那批 key **永远补不上 TTL**——恰恰是永久 key 最容易残留的窗口。
- **一份 TTL 逻辑同时覆盖两个赋值点**（正常结算 + `ErrInstanceNotFound`）。后者（`CloseInstance` 时发现实例已不在）是最容易留永久 key 的路径，草案只写了前者。
- **用 `backfillTTL()` 而非裸 `ttlFor(settleAt)`**：结算可能晚于 `settleAt` 很久，裸 `ttlFor` 会算出 1 分钟钳制值，把刚结算的数据立刻删掉。重复设 TTL 永不会缩短（绝对时刻幂等 + 下限）。

**绝不调用 `Service.CleanupLiveData()`**——那会把服务下**尚未结算**的分组也按 14 天设 TTL，是数据丢失级错误。

**测试**：`settle_ttl_test.go`（11 个 key 的 TTL 断言 + 预置 `State==Settled` 但无 TTL 的分组必须被补上 + `TestSettleTTLNeverShortensOnRepeat` + `TestCleanupLiveDataKeepsConstantRetention`）

#### 缺陷 1：`rank:def` 是「有 TTL 就会出故障」的唯一一个 key

`rank:def` 由 `RegisterRank` 用 `Set` 写入，**无 TTL**；6 处生产调用点全在注册路径上。它**可以**设 TTL，但**恢复延迟是致命的**：

```text
t0      rank:def 到期消失
t0+ε    玩家 upsert → ensureGroupLocked → OpenInstance → Exists(rank:def)=false
        → ErrDefinitionNotFound → 新分组建不出来
t0+Δ    syncLoop 下一轮（Δ 最坏 30s）才 RegisterRank 重建
```

**注意 `Exists` 不经过 `GetRank`**。`OpenInstance` 查的是 `Exists(rank:def)`，只包一层 `GetRank` 修不到它——必须**同时**把 `OpenInstance` 的存在性检查换成 `GetRank`（附带好处：把 `Exists` + `SetNX` 两次往返压成一次读）：

```go
// common/rank/service_redis.go —— OpenInstance 开头
// 原：exists, err := s.rdb.Exists(GetRankDefKey(rankCode))
if _, err := s.GetRank(ctx, instance.RankCode); err != nil {
    return err   // ErrDefinitionNotFound 原样透出
}
```

**修法（实际落地）**：恢复钩子做在 `RedisService.GetRank` **内部**，而不是包一层装饰器。

**这一条是调研推翻的结论，草案的写法行不通**：草案设想在 `engine.Service` 外面包一层 `GetRank`，但 `BatchUpsertScore`（[service_redis.go:455](../../common/rank/service_redis.go)）与 `loadMembersWithDef` 都在 `RedisService` **内部**直接调 `s.GetRank`。装饰器拦不住它们——定义一过期，**每个玩家的每次得分写入都会失败**。`TestBatchUpsertScoreSurvivesExpiredDef` 就是否决该方案的那条断言。

```go
// common/rank —— 附加式 API，provider 为 nil 时行为与改造前逐位相同
type rankDefProvider func(rankCode string) (Rank, bool)
func (s *RedisService) SetRankDefProvider(p rankDefProvider)

func (s *RedisService) GetRank(ctx context.Context, rankCode string) (*Rank, error) {
    base, err := s.getRankRaw(rankCode)              // 从原 GetRank 抽出的纯读
    if err == nil { return base, nil }
    if err != ErrDefinitionNotFound || s.rankDefProvider == nil { return nil, err }
    def, ok := s.rankDefProvider(rankCode)
    if !ok { return nil, ErrDefinitionNotFound }     // 定义已被删除，不许复活
    if regErr := s.RegisterRank(ctx, def); regErr != nil { /* warn */; return nil, ErrDefinitionNotFound }
    return s.getRankRaw(rankCode)
}
```

**恢复源只能是内存，这是与草案的第二个实质差异。** 草案写 `s.rankDefFromConfig()`——**有损**。`RankConfigDoc.Config` 是 `engine.Config`，**不含** `RankName` / `ScoreOrder` / `TieBreakPolicy` / `MaxQuerySize`，Redis 是这四个字段的唯一住所，`rank:def` 一到期，Mongo 里那份配置拼不出等价定义。正确做法是在**构造期捕获原始定义**，注册与恢复共用同一个构造器，保证逐字节相同：

```go
// engine —— WithRankDef(def) 把原始定义存进 Service，RankDef() 取回；不写 Mongo、不改存储结构
// manager —— 6 个注册点收敛成单一构造器，注册与恢复共用
func rankDefFor(bizType BizType, cfg engine.Config) commonrank.Rank {
    return commonrank.Rank{
        RankCode: cfg.RankCode, RankName: fmt.Sprintf("%s_rank_%d", bizType, cfg.ActID),
        ScoreOrder: commonrank.ScoreOrderDesc, TieBreakPolicy: commonrank.TieBreakPolicyFirstEnter,
        CreateTime: cfg.OpenTime, UpdateTime: cfg.OpenTime,
    }
}
// provider 注入点：InitGlobalManager 扫 engineServices（持 RLock）
func (m *Manager) rankDefFromMemory(rankCode string) (commonrank.Rank, bool) { ... }
```

**刻意不额外维护 `rankCode → Service` map**：服务被删时 `engineServices` 一并删除，provider 立即查不到，不会把 GM 删掉的 `rank:def` **复活**；另建 map 就必须在 `RemoveService` / `subscribeDeleteEvents` 三处手工同步，漏一处就是删不掉的定义。线性扫描只在 miss 时发生。

**TTL 表达**（与草案不同：草案写 24 小时滑动，且假设「注册路径每 30 秒走一遍、TTL 被顶回」——**该假设不成立**）：

| 项 | 实际取值 | 说明 |
|---|---|---|
| TTL | `ColdDataTTL` = **7 天** | 定义不是活动数据，没有天然结束时刻，走原则一的「滑动 TTL」形态；复用 `rank:member_index` 的同一个常量，不新增魔数 |
| 续期 | **只有 `RegisterRank` 时刷新** | 调研核实：`syncFromMongo` 对已存在服务走 `continue`，`registerFromBizId` 同样只处理不存在的服务 ⇒ **不存在周期性续期路径**。草案「TTL 被每 30 秒顶回」的论证是错的 |
| 回收 | 连续 7 天无注册 | 即「该 rankCode 已不再被任何配置引用」 |
| 恢复 | `GetRank` miss → provider 零 IO 重建 | 恢复写入与注册写入逐字节相同（同一个 `rankDefFor`、同样的 `json.Marshal` 输入 ⇒ 同样的输出），因此不可能随代码演进漂移 |

**「先加恢复、再开 TTL」的代码顺序被保留**：恢复能力的测试独立通过之后，才把 `Set` 换成 `SetEX(ColdDataTTL)`。两者同批上线（用户已明确不灰度），但顺序上先有恢复、后开 TTL，这样 P6a 那批测试的通过不依赖 P6b。

**测试**（`common/rank/service_redis_miniredis_test.go`）：`TestGetRankRecoversExpiredDefinition`、`TestGetRankWithoutProviderKeepsNotFound`、`TestGetRankDoesNotResurrectUnknownRankCode`、**`TestBatchUpsertScoreSurvivesExpiredDef`**（否决装饰器方案的关键一条）、`TestRegisterRankSetsColdDataTTL`、`TestGetRankReestablishesExpiredDefTTL`（守住二阶故障：恢复不带 TTL ⇒ 第二轮永远无 key）、`TestOpenInstanceRecoversExpiredDef`、`TestOpenInstanceStillRejectsMissingDef`。另有 `internal/rank/manager_def_recovery_test.go` 4 例。

#### 缺陷 4：`rank:settled` 的 TTL 是「半个」的

同一个 key 有 3 个写入者，TTL 行为曾经不一致：

| 写入者 | 代码 | 修前 | 修后 |
|---|---|---|---|
| `SettleInstance`（结算时） | [service_redis.go:367](../../common/rank/service_redis.go) | ❌ `Set`，无 TTL | ✅ `SetEX(GetRankSettledKey, snapData, SettledCacheTTL)` |
| `RestoreSettled`（冷恢复） | [store.go:435](../internal/rank/engine/store.go#L435) | ❌ `Set`，无 TTL | ✅ `st.backfill(settledKey, ...)` → `Expire(backfillTTL())` |
| `LoadGroupSettledCached`（历史查询回填） | [store.go:1012](../internal/rank/engine/store.go#L1012) | ✅ 已有 TTL | ✅ `st.backfillTTL()`（由固定 2 周改为随活动结束时刻走） |

它之所以看起来「有 2 周 TTL」，纯粹是因为 `ExpireInstance` 在结算后顺带给它设了一次。但**任何一个 `RestoreSettled` 都会把 TTL 抹掉**，而 `RestoreSettled` 恰恰是在 key 已不存在时被调用的——即缺陷 3 时间线的 `T+14d+ε` 那一步。

**顺带修正**：`rank:settled` 的「设置者」是 `ExpireInstance`（经 `Service.CleanupLiveData`），**不是** `SaveSettled`（后者只写 MongoDB）。

**为什么 `LoadGroupSettledCached` 用的是 `backfillTTL()` 而不是沿用固定 2 周**：它同时是**唯一的自愈入口**——`rank:settled` 丢失（被提前删、被 `RestoreSettled` 抹过 TTL、节点故障）时由它从 Mongo 重建。既然是重建，就该和首次写入同一口径；沿用固定的 2 周会在活动还剩 3 个月时把 TTL 削到 14 天，之后每次 miss 都回源 Mongo。`backfillTTL()` 的下限恰好也是 14 天，所以「不缩短既有保留期」这条没有代价。

#### 落地顺序

三条缺陷不是并列的补丁，而是同一个不变量的三段；实际落地顺序如下（**全部已完成**）：

```text
缺陷 3（backfill 助手 + writeTTL/backfillTTL 两个具名函数）
    │
    ├─→ 缺陷 6（ExpireLiveData 统一实现 + Settle 收集 settledGroups 后统一设 TTL）
    ├─→ 缺陷 4（三个 rank:settled 写入方统一到 >14d 语义）
    ├─→ 待办 J（setMongoChecked 的 SetEX → SetNX）—— 与缺陷 3/6 同批，不可分
    └─→ 第 02 条的其余写点（SaveActivityTimes / SaveGroup / SetMember / ... 各补 Expire）
              ↓
        缺陷 1（rank:def 恢复能力 + ColdDataTTL）—— 独立改动包
```

前置约束（落地时遵守、后续维护也必须遵守）：

- **缺陷 3 与第 02 条的写路径 TTL 必须同批**：只加写路径 TTL 不加回填 TTL ⇒ 读路径把 key 重新写成永久；只加回填 TTL 不加写路径 TTL ⇒ 活跃期 key 仍永久。
- **缺陷 6 的 `ExpireLiveData` 与缺陷 3 一起做**——它才是「不允许存在永久 key」的主要落点。
- **待办 J 与缺陷 3/6 不可分**：`setMongoChecked` 的 `SetEX(10min)` 会把同一批 key 的 TTL 从 2 周削成 10 分钟，改完 TTL 方案但不改它等于把缺陷 3 的成果抵消掉。
- **缺陷 1 必须晚于以上全部**：它开的 TTL 落在最热路径上（每次得分写入都会读 `rank:def`），只有在 Redis 侧的 TTL 语义已经稳定之后才谈得上「定义可以被回收」。

### 4.2 待办清单（本轮全部处理完毕）

**A / C / E / G-d2 / G-d3(写锁) / J 六项已随本轮落地**，只剩 G 的负缓存一项明确留到下一轮。

| 项 | 问题 | 状态 | 落地方式 |
|---|---|---|---|
| A | `applyRankConfigDoc` 与 `registerSubService` 注册服务时**不写 MongoDB**，完全依赖广播源写入落盘；写入被丢弃时与 `syncFromMongo` 形成振荡 | ✅ 已处理 | 真正的缺口只有「逻辑配置 doc 缺失」一处：`applyRankConfigDoc` 在 `NewService` 成功后补 `m.dao.SaveRankConfig(key, cfg)`（幂等 upsert，失败只记日志）；`tryRecoverPeriodicFromRedis` 在逻辑 doc 缺失时补 `SaveRankConfigWithPeriodic(logicalKey, overallCfg, state.ToSavedState())`。**`registerSubService` 刻意不补**：它注册的是**某一轮**的 round cfg，写进去会覆盖逻辑配置、破坏 `syncFromMongo` 的 `Config`/`Periodic` 约定——补在这里是把振荡换成永久错配 |
| C | 持锁期间做 IO：`GetMemberRankEntries` 在 `RLock` 内做 N 次 Redis 往返；`ListServices` 在 `RLock` 内做 N 次 `IsSettled`；`registerEngine` 在写锁内做 Redis+Mongo IO | ✅ 已处理 | 抽出 `snapshotServices()`（[manager.go:224](../internal/rank/manager.go#L224)）：**锁内只取切片快照，锁外遍历**。`registerEngine` 照抄 `registerFromBizId` 的既有范式——`RegisterRank`/`NewService`/`SaveRankConfig` 全移到锁外，锁内只做二次检查 + 插入，**重复分支返回已插入的那个 Service 而不是自己造的副本**（返回副本会让内存分组状态从那一刻起分叉） |
| E | `CleanupAll` 删除的 key 集合不含 `rank:member_index` | ✅ 已处理（改为约束不变量） | 按字面补进 `CleanupAll` **做不到也不该做**：`Store` 只有 `bizId`，索引条目编码 `{bizType}:{actID}:{groupID}` 在带 `_r{N}` 轮次后缀时不可逆。落点改为把调用关系固化成不变量 + 注释（[service.go:1048](../internal/rank/engine/service.go#L1048)）：`Service.Cleanup()` 只允许被 `cleanupServiceData`（先 `RemoveUserEntries` 再 `Cleanup`）或 `forceCleanupOrphan` 调用；索引条目最终由 `Track` 写入时的 `ColdDataTTL` 兜底 |
| G-d2 | `RemoveUserEntries` 逐条 `SRem`、无分块无 pipeline（10 万人活动 = 10 万次串行往返） | ✅ 已处理 | 按 `removeUserEntriesChunk = 512` 分块 pipeline（[member_index.go:107](../internal/rank/member_index.go#L107)）。每用户 key 不同、无法合并成一条 `SRem`，但往返次数从 100k 降到 ~200 |
| G-d3 | `GetMemberGroupID` 取写锁，GM 批量查询与 `UpsertScore` 直接争锁 | ✅ 已处理（换锁） | `s.mu.Lock()` → `s.mu.RLock()`（[service.go:1117](../internal/rank/engine/service.go#L1117)）。已核查无调用方依赖写锁语义，是**语义等价的一行修复** |
| G-d3(负缓存) | `rebuildMemberIndex` 无负缓存，GM 批量查 U 用户 × N 服务 = U×N 次锁 | ⏳ **留到下一轮** | 需要「用户无索引」的版本戳，属架构改动，本轮不做 |
| J | `setMongoChecked` 的 `SetEX(10min)` 会把同 bizId 其它 key 的 2 周 TTL **缩短**为 10 分钟 | ✅ 已处理 | `SetEX` → `SetNX`（[store.go:92](../internal/rank/engine/store.go#L92)）：`SET` 会连带清掉 key 上已有的 TTL，`SETNX` 只在 key 不存在时写入，**永不缩短**。修前不可达（只在 Redis 与 Mongo 双空时调用），但本轮把 TTL 语义改成滑动之后就可能变为可达 |

**已解决的待办**（保留编号以便对照历史）：B（`syncFromMongo` 漏 `svc.Cleanup()`）→ 第 08 条；D（`RemoveByKey` no-op）→ 第 08 条；F（无界 `SubmitWait` 丢 tick）→ 第 00 条；H（TTL 灰度）→ 已取消灰度、直接全开；I（`rank:max_score` 死代码）→ 已删除；K（配置时间字段无校验）→ 见下。

**关于 K 的落地**：`handleCreateRankConfig` 现在校验 `CloseTime <= 0 && GameEndTime <= 0` 直接拒绝（[rank.go:452](../internal/handler/rank.go#L452)）；同时 `Tick` 对 `settleAt <= 0` 显式跳过并告警（[service.go:440](../internal/rank/engine/service.go#L440)），不依赖 `settledAt` 零值的巧合短路。

**另有一处可选清理未做**：`periodic/handler.go:308-322` 的 `setSettledTTLForRound` 与 `Settle` 的 `ExpireInstance` 在本轮之后语义重叠，删掉可把设 TTL 的点从 4 处降到 2 处。不影响正确性（重复设 TTL 因绝对时刻幂等而永不缩短），留作独立小提交。

---

## 5 · 测试用例总览

除特别标注的 `common/rank` 侧用例外，全部位于 `socialserver/internal/`；`socialserver` 的 `go test ./...` 与 `common/rank` 的 `go test ./rank/...` 均全绿。

| 优化项 | 测试文件 | 用例 | 断言要点 |
|---|---|---|---|
| 00 | `taskpool/pool_test.go` | `TestPoolExecutesSubmittedTask` | 提交的任务确实被执行 |
| 00 | 〃 | `TestPoolSubmitReturnsErrPoolFullWhenSaturated` | `Submit` 队满返回 `ErrPoolFull`（不阻塞） |
| 00 | 〃 | `TestPoolSubmitWaitBlocksUntilRoomThenSucceeds` | `SubmitWait` 阻塞至有位置后成功 |
| 00 | 〃 | `TestPoolSubmitWaitTimeoutExpiresWhenSaturated` | `SubmitWaitTimeout` 超时返回而非永久阻塞 |
| 00 | 〃 | `TestPoolWorkerSurvivesPanickingTask` | 单任务 panic 不导致 worker 退出 |
| 00 | 〃 | `TestPoolCloseDrainsQueueThenRejectsNewSubmissions` | `Close` 先 drain 再拒绝新任务 |
| 00 | 〃 | `TestPoolCloseRespectsContextDeadline` | `Close` 尊重 ctx 截止时间 |
| 00 / 01 | `rank/registry_test.go` | `TestParseRegistryMember` | `{bizId}:{deadline}` 解析 |
| 00 / 01 | 〃 | `TestParseRegistryMemberPeriodicRoundBizId` | `_r{N}` 后缀的 bizId 仍能正确切分 |
| 00 / 01 | 〃 | `TestParseRegistryMemberInvalid` | 非法成员判定为 stale |
| 00 / 01 | 〃 | `TestRegistryDeadlineDegradesWhenActivityTimesMissing` | meta 缺失时 deadline 退化，不被立即判 stale |
| 02 | `rank/engine/service_tick_cache_test.go` | `TestTickGroupsCacheHitReturnsCachedSliceWithoutStoreCall` | 有效期内命中不回源 |
| 02 | 〃 | `TestTickGroupsCacheExpiredFallsBackToStore` | 过期后回源（**必改-2**：「缓存永不过期」会回归失败） |
| 02 | 〃 | `TestTickRobotsCacheHitReturnsCachedSlice` | robots 缓存命中 |
| 02 | 〃 | `TestTickRobotsCacheExpiredFallsBackToStore` | robots 缓存过期回源 |
| 02 | 〃 | `TestInvalidateRobotsCacheForcesReload` | 显式失效后立即回源 |
| 02 | 〃 | `TestSpawnRobotsForGroupInvalidatesRobotsCache` | 生成机器人后立即失效缓存 |
| 02 | 〃 | `TestTickGroupScoresCachesRangeResultWithinTTL` | `Range(0,-1)` 纳入缓存（原方案最大遗漏项） |
| 02 | `rank/engine/store_ttl_test.go` | `TestTTLForAbsoluteExpiryIsIndependentOfSetTime` | 绝对过期时刻 == `end + SettledCacheTTL`，与设置时机无关 |
| 02 / 缺陷 7 | 〃 | `TestTTLForDegradesToColdDataWhenActivityEndMissing` | `activityEnd<=0` 退化为 `ColdDataTTL` |
| 02 / 缺陷 7 | 〃 | `TestTTLForNeverReturnsNonPositive` | 超出保留期时返回 1 分钟，**永不返回非正数**（不得靠"跳过"保护） |
| 02 / 06 | 〃 | `TestEffectiveSettleAtPrefersGameEndTime` | `GameEndTime` 优先，缺失才退化 `CloseTime` |
| 03 | `rank/engine/service_tick_cache_test.go` | `TestEnsureGroupInstanceSkipsGetInstanceWithinVerifyInterval` | 有效期内跳过 `GetInstance` |
| 03 | 〃 | `TestEnsureGroupInstanceReVerifiesAfterIntervalExpires` | 过期后回源确认（自愈） |
| 03 | 〃 | `TestEnsureGroupInstanceCreatesAndMarksVerifiedWhenMissing` | 不存在时创建并写入标记 |
| 03 | 〃 | `TestCleanupLiveDataInvalidatesInstanceState` | `ExpireInstance` 后显式失效标记 |
| 04 / 05 | `rank/manager_taskpool_test.go` | `TestTickServicesRunsAllViaPoolEvenWhenPoolSmallerThanBatch` | 池小于批次仍全量执行、不死锁 |
| 04 | 〃 | `TestTickServicesSkipsWhenPoolSaturatedPastTimeout` | 池饱和超时后跳过该 service，且有界返回（**待办 F**） |
| 04 | 〃 | `TestTickServicesNoServicesStillRunsPeriodic` | 无 service 时仍走周期活动 |
| 05 | 〃 | `TestWarmUpAllServicesRunsAllViaPoolEvenWhenPoolSmallerThanBatch` | 批量 WarmUp 同上 |
| 06 | `rank/engine/service_settle_test.go` | `TestTickShortCircuitsOnceSettled` | Tick 短路后不再处理任何分组 |
| 06 / 必改-1 | 〃 | `TestSettleDoesNotStickBeforeWritesClosed` | GM 提前结算时**不得**钉死 `settledAt` |
| 06 / 必改-4 | 〃 | `TestSettleGateFiresAfterLoopRegardlessOfPerGroupOutcome` | 所有分组 `continue` 时置位仍生效 |
| 06 | 〃 | `TestIsSettledFastPath` | 短路命中时 `IsSettled` 直接 true |
| 06 | 〃 | `TestGMExtendingCloseTimeAutoInvalidatesShortCircuit` | GM 推后 `CloseTime` → 短路零代码自动失效 |
| 06 | 〃 | `TestSettleShortCircuitsAfterGateSet` | `Settle` 短路在 `LoadGroups` 之前，返回 nil |
| 待办 K | 〃 | `TestTickSkipsWhenSettleAtIsZero` | `settleAt<=0` 时既不清算也不 tick 机器人 |
| 07 | `rank/periodic/handler_timer_test.go` | `TestHandlerClearStopsTrackedTimers` | `Clear` 停掉所有已追踪 timer |
| 07 | 〃 | `TestUntrackTimerRemovesFiredTimer` | 已触发的 timer 被移出追踪列表 |
| 08 / 09 | `rank/manager_sync_test.go` | `TestActivityStillOpen` | 5 种活动状态下的修复/删除判据 |
| 08 | 〃 | `TestCleanupServiceDataAlwaysRunsCleanupEvenWithoutMembers` | 无成员时仍无条件执行 `svc.Cleanup()` |
| 08 | `rank/member_index_test.go` | 5 例 | `Track`/`Lookup`/幂等/多条目/返回副本（含 `RemoveByKey` 删除后的回归面） |
| 10 | `rank/engine/service_lookup_test.go` | `TestResolveSettledCacheHitReturnsWithoutLookup` | 命中缓存不回源 |
| 10 | 〃 | `TestResolveSettledMissWithNoKnownGroupReturnsNil` | 未知分组返回 nil |
| 10 | 〃 | `TestResolveSettledMissFallsBackToInMemoryGroupCache` | 回退到内存分组缓存 |
| 11 | 〃 | `TestSettleSharesSingleClonedSnapshotAcrossResultsAndCache` | 三处共享同一份 clone |
| 12 | 〃 | `TestFindTierUsesMapLookup` / `TestRobotAvatarInfoUsesMapLookup` | 命中 / 未命中 |
| 13 | 〃 | `TestWarmUpFastPathSkipsMutexWhenAlreadyLoaded` | 已加载时 `WarmUp` 不取 `s.mu` |
| 待办 K | `handler/rank_test.go` | `TestHandleCreateRankConfigRejectsMissingCloseAndGameEndTime` | 两个时间字段都缺省 → `InvalidArgument` |
| 待办 K | 〃 | `TestHandleCreateRankConfigAllowsGameEndTimeOnly` | 只给 `GameEndTime` → 放行 |
| 缺陷 3 | `rank/engine/store_miniredis_test.go` | **`TestBackfillAfterExpiryRecreatesBoundedKey`** | **最关键的一条**：回填 → 快进 15 天（key 与负缓存一起到期）→ 再回填；断言 `TTL > 0` 且 `== SettledCacheTTL`。同时证明「不会重建为永久 key」与「不会退化成 1 分钟震荡」 |
| 缺陷 3 | 〃 | `TestLoadGroupsBackfillCarriesTTL` | `LoadGroups` 回填带 TTL |
| 缺陷 3 | 〃 | `TestGetAllMembersBackfillCarriesTTL` | `GetAllMembers` 回填带 TTL |
| 缺陷 3 | 〃 | `TestLoadRobotsBackfillCarriesTTL` | `LoadRobots` 回填带 TTL |
| 缺陷 3 | 〃 | `TestGetClaimBackfillAndNegativeCacheBothCarryTTL` | `GetClaim` 的**负缓存哨兵**也带 TTL（漏了它照样是永久 key） |
| 缺陷 3 | 〃 | `TestAtomicClaimSetsTTL` | `AtomicClaim` 三个分支（`661`/`687`/`698`）全部设上 TTL |
| 缺陷 3 | 〃 | `TestBackfillWritesBeforeExpire` | 顺序断言：`EXPIRE` 作用在不存在的 key 上会被静默丢弃，必须先写后 EXPIRE |
| 第 02 条 | `rank/engine/store_write_ttl_test.go` | `TestAllActiveWritePathsLeaveBoundedTTL` | 活跃期全部写点（`SaveActivityTimes`/`SaveGroup`/`SetMember`/`IncrRealCount`/`NextGroupID`/`SaveRobots`/`SaveUsedInfoIDs`/`SetClaim`）逐个断言 `TTL > 0` |
| 第 02 条 | 〃 | `TestActiveWriteTTLTracksActivityEnd` | TTL 跟随活动结束时刻，不是固定常量 |
| 第 02 条 | 〃 | `TestRepeatedActiveWriteIsIdempotent` | 重复写回到**同一个绝对过期时刻**（不续期、不重算） |
| 第 02 条 | 〃 | `TestPastActivityWriteStillSetsPositiveTTL` | 已结束活动写入仍为正数（1 分钟夹紧值，非 0） |
| 第 02 条 | 〃 | `TestBackfillTTLNeverShorterThanActiveWriteTTL` | 读路径 TTL 恒 ≥ 写路径 TTL（下限不变量的属性测试） |
| 第 02 条 / 缺陷 1 | `rank/engine/active_period_ttl_test.go` | **`TestActivePeriodWritePathLeavesNoPermanentKey`** | 走**真实 `RedisService`（miniredis 后端）**的活跃期写路径：5 次 `UpsertScore` 之后**全键扫描** `mr.Keys()`，断言**每一个**存在的 key 都有 TTL；再点名 `rank:inst`/`rank:mb`/`rank:seq`；最后用 `FastForward(30m)` 断言幂等。全键扫描是刻意的——点名只能证明「我想到的那几个 key 没问题」，而这条原则要的是「不存在永久 key」 |
| 缺陷 4 | `rank/engine/settle_ttl_test.go` | `TestSettleSetsTTLOnAllLiveKeys` | `Settle` 后 11 个 key（meta/groups/members/claims/mongo_chk + 每分组 robots/robot_infos）`TTL ≈ SettledCacheTTL` |
| 缺陷 6 | 〃 | `TestSettleTTLCoversAlreadySettledGroups` | **崩溃窗口兜底**：预置一个 `State==Settled` 但无 TTL 的分组，`Settle` 必须把它也补上（循环对已结算分组是 `continue` 的，只收集本轮新结算的会漏掉这批） |
| 缺陷 6 | 〃 | `TestSettleTTLUsesFloorNotClampForLongPastActivity` | 久远活动走 `backfillTTL()` 的下限，不是 `ttlFor` 的 1 分钟夹紧值 |
| 缺陷 6 | 〃 | `TestSettleDoesNotTouchGroupsHeldByOtherNodes` | 别的节点持有的分组不被误设 TTL |
| 缺陷 6 | 〃 | `TestCleanupLiveDataKeepsConstantRetention` | 锁死「**实现统一、TTL 值不统一**」：周期路径必须继续用固定 14 天，改用 `ttlFor(roundClose)` 会让轮次数据总保留期从 21 天缩到 14 天，是真实功能退化 |
| 缺陷 6 | 〃 | `TestSettleTTLNeverShortensOnRepeat` | GM 把时间改早后重复 `Settle`，TTL 仍 `>= 14d` |
| 待办 J | 〃 | `TestSetMongoCheckedDoesNotShortenExistingTTL` | `SetNX` 不得覆盖同 bizId 已有的 2 周 TTL |
| 缺陷 1 | `rank/manager_def_recovery_test.go` | `TestRankDefFromMemoryReturnsInjectedDef` | provider 能从 `engineServices` 取回构造期捕获的定义 |
| 缺陷 1 | 〃 | `TestRankDefFromMemorySkipsServiceWithoutDef` | 未注入定义的服务不被误当作恢复源 |
| 缺陷 1 | 〃 | **`TestRankDefFromMemoryDoesNotResurrectRemovedService`** | 服务被删后 provider 立即查不到 ⇒ **不会把 GM 删掉的 `rank:def` 复活**。这是「不额外维护 `rankCode → Service` map」这个决定的守卫 |
| 缺陷 1 | 〃 | `TestUpsertScoreSurvivesExpiredDef` | 端到端：`Del` 掉定义后 `UpsertScore` 仍成功 |
| 缺陷 1 / 4 | `common/rank/service_redis_miniredis_test.go` | 15 例（新增） | 见下方「`common/rank` 侧」 |
| 待办 E | `rank/manager_sync_test.go` | `TestCleanupServiceDataRemovesIndexBeforeCleanup` | 固化不变量：`RemoveUserEntries` 必须在 `Cleanup` 之前 |
| 待办 E | 〃 | `TestCleanupBeforeIndexRemovalLosesTheIndex` | **反向用例**：顺序反了就静默丢索引——它证明上一条不是同义反复 |
| 待办 E | 〃 | `TestForceCleanupOrphanRemovesIndexBeforeCleanup` | 孤儿清理路径同形状 |
| 待办 C | `rank/manager_lock_test.go` | **`TestGetMemberRankEntriesDoesNotHoldLockDuringIO`** | 用卡在 channel 上的 fake 把「IO 进行中」拉长，断言此刻 `m.mu` 仍可被**独占**获取（用写锁而非读锁：读锁与其他读者共存，证明不了锁已释放）。若哪天有人把 IO 挪回锁内，测试会**超时失败**而不是安静地慢下去 |
| 待办 C | 〃 | `TestConcurrentRegisterEngineReturnsSingleInstance` | 8 协程并发 `registerEngine`，断言**所有调用方拿到同一个指针**。返回各自造的副本是最容易犯的错：内存分组状态从那一刻起分叉，玩家会随机落到两份互不可见的状态上 |
| 待办 C | 〃 | `TestRegisterEngineSecondCallIsNoop` | 已存在时走快速路径：`Del` 掉 `rank:def` 后二次调用不得重新写出来 |
| 待办 G-d2 | `rank/member_index_miniredis_test.go` | `TestRemoveUserEntriesRemovesAllAcrossChunks` | 成员数跨过 2 个分块边界仍全删 |
| 待办 G-d2 | 〃 | `TestRemoveUserEntriesKeepsOtherActivities` | 只删目标活动的条目（同一用户可同时参加多个活动） |
| 待办 G-d2 | 〃 | `TestRemoveUserEntriesEmptyIsNoop` | 空输入不发起任何 Redis 命令、不 panic |
| 待办 G-d2 | 〃 | `TestRemoveUserEntriesMissingKeyIsHarmless` | 索引 key 已被 TTL 回收时不报错 |
| 待办 G-d2 | 〃 | `TestTrackRefreshesTTLEveryCall` | 索引条目本身不会成为永久 key（`Track` 写时带 `ColdDataTTL`——这是待办 E 的兜底） |
| 待办 G-d3 | `rank/engine/service_lookup_test.go` | `TestGetMemberGroupIDDoesNotRequireExclusiveLock` | 换锁后语义等价且不再与写操作争用 |

**`common/rank` 侧新增（`service_redis_miniredis_test.go`，15 例）**

| 分组 | 用例 | 断言要点 |
|---|---|---|
| 缺陷 2 | `TestOpenInstanceSetsBoundedTTL` / `TestOpenInstancePrefersGameEndTime` / `TestOpenInstanceRepeatDoesNotDisturbExisting` / `TestOpenInstancePastActivityStillSetsPositiveTTL` | `OpenInstance` 的 `SetNX` 带 TTL；`GameEndTime` 优先；重复开实例不扰动已有 TTL；过期活动仍为正数 |
| 缺陷 7 | `TestTTLForActivityEndNeverReturnsZero` / `TestSettleAtOfPrefersGameEndTime` | `TTLForActivityEnd` 对 `activityEnd ∈ {0, -1, now-30d, now-1s, now+1h, now+90d}` 恒为正；`settleAtOf` 三分支 |
| 缺陷 1 | `TestGetRankRecoversExpiredDefinition` / `TestGetRankWithoutProviderKeepsNotFound` / `TestGetRankDoesNotResurrectUnknownRankCode` / **`TestBatchUpsertScoreSurvivesExpiredDef`** / `TestRegisterRankSetsColdDataTTL` / `TestGetRankReestablishesExpiredDefTTL` / `TestOpenInstanceRecoversExpiredDef` / `TestOpenInstanceStillRejectsMissingDef` | 见 §4.1 缺陷 1 |
| 缺陷 2（热路径） | **`TestBatchUpsertScoreKeepsInstanceTTL`** | 见 §2.1 的「本轮实测发现的最后一个缺口」 |

**运维工具侧（`cmd/rankttl`，5 例；见 §6.2）**

| 用例 | 断言要点 |
|---|---|
| `TestClassifyCoversEveryRealKey` | 14 类数据 key 全部用 `common/redis` 的**真实构造函数**生成（而非手写字面量）后归类与 TTL 正确。key 格式一变本用例就失败，而不是让工具静默漏掉或错配一整类 key |
| `TestClassifyLocksAreSkipped` | `rank:settle:` / `rank:robot_tick:` 必须被识别为「跳过」。这不是洁癖：被当成数据 key 会让工具给一把本该 30 秒消失的锁加上 14 天，比不修更糟 |
| `TestClassifyNearMissPrefixes` | 锁死两组近似前缀：`rank:settled:`（数据）vs `rank:settle:`（锁）；`rank:robots:` / `rank:robot_infos:`（数据）vs `rank:robot_tick:`（锁）。这是工具唯一一处「写错就会伤害生产」的地方 |
| `TestClassifyUnknown` | 未登记的前缀落进「未识别」而不是被硬塞进某一类——对新增 key 类型报告而不猜 |
| `TestUnknownPrefixBuckets` | 未识别 key 的桶名可读且有界，否则报告会刷屏 |

**测试基建的变化**：本轮引入了 `github.com/alicebob/miniredis/v2`（test-only）作为 Redis 替身。`golib/redis.NewRedis` 走 `redis.NewUniversalClient`，单地址 ⇒ 普通 client ⇒ miniredis 是**可直接替换**的替身，生产代码不需要为测试留任何 API。`Store` 的 `dao` 由 `*DAO` 放宽为 `storeMongo` 接口（约 14 个调用点机械替换、无语义变化，生产代码只用 `*DAO`）——这是「为测试改生产代码」，也是缺陷 3 那条最关键断言的**唯一**写法。

**仍无法覆盖的部分**见 §7 观察 3。需要注意 miniredis 的两个陷阱：`SMEMBERS` 对不存在的 key **返回错误**（真实 Redis 返回空集），必须先 `Exists` 守卫；`mr.TTL` 对「无 TTL」和「key 不存在」**都返回 0**，断言「有 TTL」前必须先断言 key 存在。

---

## 6 · 上线方式：一个 PR、全量开启

**不做灰度，新功能直接全开。** 因此本文档此前描述的「阶段一观察 / 阶段二执行」两段式上线方案**已取消**——`SaveActivityTimes` 里的 `Expire` 是直接生效的，`logPlannedActiveTTL` 那行日志保留下来，用途从「阶段一的验收依据」变成「线上核对 expireAt 是否等于预期绝对时刻」的常规诊断。

`rank` 相关的全部改动合并为**一个 PR**，内部落地顺序如下：

```text
00（基础设施 taskpool）→ 01 → 04/05（接入池）→ 06 → 09 → 08
    → 02/03（写路径 TTL + 回填 TTL，同批不可拆）
    → 缺陷 4 / 6 / 待办 J（与 02/03 同批）
    → 缺陷 1（rank:def 恢复能力 + ColdDataTTL）
    → 10/11/12/13 → 待办 A / C / E / G-d2 / G-d3
```

其中只有以下三组是**硬绑定**（顺序不可调换、也不可拆成不同 PR）：

| 组 | 组成 | 为什么不可拆 |
|---|---|---|
| ① | 第 02 条写路径 TTL + 缺陷 3 回填 TTL | 同一个不变量的两半。只加写路径 ⇒ 读路径把 key 重新写成永久；只加回填 ⇒ 活跃期 key 仍永久 |
| ② | 缺陷 3 + 缺陷 6 + 待办 J | 缺陷 6 是「永久 key」的主要落点；待办 J 不改则 `setMongoChecked` 的 10 分钟 `SetEX` 会把缺陷 3 的成果抵消掉 |
| ③ | 缺陷 1 的**恢复能力**先于它的 **TTL** | 代码顺序上先有恢复、后开 TTL。虽然同批上线（不灰度），但恢复能力的测试必须独立通过，P6a 那批测试的通过不依赖 P6b |

08 排在 09 之后：09 的修复分支复用了 08 的 `cleanupServiceData`。13 与任何一条都无耦合，1 行改动，可随时插入。

**上线的验收清单**（第 02 条是全文唯一不可逆的改动）

1. 大活动（`RankPeopleNum` 打满）删档后，验证 `rank:mb` / `rank:seq` 在保留期内仍可读；
2. 给「`ttl` 退化为 1 分钟的分组数」加指标——它是配置异常（待办 K）的运行时信号；
3. 确认待办 J 已一并修复（`setMongoChecked` 的 10 分钟 TTL 不得覆盖同 bizId 的 2 周 TTL）；
4. 线上抽查 `rank:def` 的 `TTL` 为 7 天，且**到期后**首个得分写入能触发恢复（`logPlannedActiveTTL` 与恢复告警是这条的观测点）。

### 6.1 与线上正在运行的实例兼容吗

**兼容，且不需要任何数据迁移。** 逐条核对如下。

| 维度 | 结论 | 依据 |
|---|---|---|
| 存储结构 | **零变化** | 全部 key 名、JSON/HASH 编码、Mongo 文档结构逐字节相同；本批 diff 没有一处改 value 编码。唯一签名变化 `NewStore(..., activityEnd func() int64)` 是包内构造器（10 个调用点全在 socialserver 内），不构成跨进程契约 |
| 读旧数据 | 走同一份逻辑 | 新代码把原 `GetRank` 拆成 `getRankRaw` + 恢复钩子，Redis 命中时两者是同一条路径 |
| `rank:def` 恢复钩子 | 升级瞬间不会被触发 | 钩子只在 `ErrDefinitionNotFound` 时介入；线上 `rank:def` 存在就命中，provider 完全不参与，所以「恢复能力」本身不构成升级风险 |
| pub/sub | 载荷不变 | `rank:create` 广播的 `RankConfigDoc` 与线上版本逐字段相同 |
| `rank:max_score` | **没有孤儿 key** | 该前缀全仓只有定义、无任何生产者（缺陷 5 死代码），线上 Redis 里不存在这批 key，删除定义不留残留 |
| 滚动发布 | 安全 | 发现主路径是 `syncFromMongo`（Mongo 权威源），新旧节点都有，不依赖注册表；新节点靠 `bootstrapRegistry` 启动时 SCAN `rank:meta:{*}*` 一次建表，拿到与旧代码 `syncFromRedis` SCAN 完全相同的数据集；旧节点继续 SCAN，不认识注册表也不受影响 |
| 回滚 | 见下方「回滚注意」 | 发现路径回滚安全，但 `rank:def` 的 TTL 是一处有界风险 |

**回滚注意**：旧代码的 `syncFromRedis` 用 SCAN `rank:meta:*`，不依赖注册表，所以回滚后活动发现正常；残留的 `rank:{active_services}` 自身带滑动 `ColdDataTTL`，会自过期。唯一的风险在 `rank:def`——它现在有 7 天 TTL，而旧代码没有恢复钩子，因此**回滚后若连续运行超过 7 天且不重启**，定义会过期导致该活动每次得分写入返回 `ErrDefinitionNotFound`。进程重启会让旧代码对不存在服务重新 `RegisterRank`（裸 `SET`，永久）从而自愈，所以暴露窗口 = 回滚后 >7 天不重启。要绝对安全，回滚前手工 `PERSIST rank:def:*`。

### 6.2 存量永久 key 的收敛与一次性迁移

**收敛是「增量全自动、存量不自动」**，这一点必须先说清楚，否则会误以为升级即完成。

新写的 key 一定带 TTL，但**启动时并不会统一给旧 key 补 TTL**——读路径是纯命中，不写任何东西（`LoadGroups` 在 `len(raw) > 0` 时直接返回，[store.go:172](../internal/rank/engine/store.go#L172)；`ensureLoaded` 的 `if mbExists` 分支跳过 restore，`needBackfill` 也为假）。因此收敛分四类：

| 类 | 触发时机 | 依据 |
|---|---|---|
| `rank:def` / `rank:meta` / `rank:{active_services}` | 新节点**首次启动**一次 | `syncFromMongo` 对不存在的服务调 `RegisterRank`；`NewService` → `SaveActivityTimes`（[store.go:275](../internal/rank/engine/store.go#L275)）设 meta TTL 并 `registerActive`（[store.go:345](../internal/rank/engine/store.go#L345)） |
| `rank:inst` / `rank:mb` / `rank:seq` 与 `groups` / `members` / `robots` / `claims` 等 | 活跃活动**下一次写入**（秒级） | `BatchUpsertScore` 的 pipeline、`SaveGroup` / `SetMember` / `SetClaim` / `SaveRobots` |
| 已结算活动的全部 key | **重启后第一次 Tick** 一次 | `Tick` 的闸门是 `settledAt.Load() == settleAt`（[service.go:454](../internal/rank/engine/service.go#L454)），重启后 `settledAt` 归零即放行 → 走到 `LoadGroups` → 收集 `State == Settled` 的分组 → `ExpireInstance` + `ExpireLiveData`（[service.go:819](../internal/rank/engine/service.go#L819)）。这正是缺陷 6 的「崩溃窗口兜底」在起作用 |
| **关闭超过 7 天的活动** | **永不收敛** | `syncFromMongo`（[manager.go:1038](../internal/rank/manager.go#L1038)）与 `registerFromBizId`（[manager.go:682](../internal/rank/manager.go#L682)）都有 `now > CloseTime + 7*86400000` 的跳过分支，这些活动不进 `engineServices` ⇒ 没有 Service ⇒ 不 Tick ⇒ 不 Settle ⇒ 线上那批永久 key 原样留着 |

第四类是**唯一真缺口**：不影响正确性（新代码从不读这些冷活动的 Redis），但「不允许存在永久 key」这条原则的**存量部分拿不到**——新产生的 key 全部有界，存量 key 永远无界，且随活动个数无限增长。

**一次性迁移工具：`cmd/rankttl`**

```bash
cd socialserver && go build -o ../bin/rankttl ./cmd/rankttl
cd ../bin && ./rankttl           # dry-run：只报告将要改动的 key 数，不写入
cd ../bin && ./rankttl -apply    # 实际写入
```

可执行文件必须与 `.devops.yaml` 同目录（`yamlcfg.LoadYamlCfg` 是按可执行文件所在目录找配置的，与 socialserver 一致），这样 Redis 密码不必出现在命令行里。

**安全性由一条规则保证：只处理 `PTTL == -1`（永不过期）的 key。** 已带 TTL 的 key 一律不碰，所以它在结构上不可能缩短任何既有 TTL；活跃活动的 key 要么已带 TTL，要么下一次写入会按绝对过期时刻重设，两种情况下工具的结果都会被后续写入覆盖，不会造成提前删除。

分类表（`cmd/rankttl/main.go` 的 `classes`）全部引用 `common/redis` 的导出常量而不是字面量，并有两组必须显式登记为「跳过」的锁 key：

| key | 补的 TTL | 说明 |
|---|---|---|
| `rank:def` / `rank:member_index` / `rank:{active_services}` | `ColdDataTTL`（7 天） | 与 `RegisterRank` 的 `SetEX`、Manager 的 `memberIndexTTL` 一致 |
| 其余 13 类活动数据 key | `SettledCacheTTL`（14 天） | 与 `Store.ExpireLiveData`（[store.go:637](../internal/rank/engine/store.go#L637)）的 key 集合逐项对齐 |
| `rank:settle:{bizId}:{gid}` / `rank:robot_tick:{bizId}:{sec}` | **跳过** | 锁 key，自带短 TTL 且可能正被运行中的活动持有。必须显式登记：一旦被当成数据 key，工具会给一把本该 30 秒消失的锁加上 14 天，比不修更糟 |
| 未识别的前缀 | **跳过 + 打印** | 工具对新增 key 类型的态度是报告而不是猜 |

两个易错点已被测试锁死（`cmd/rankttl/main_test.go`）：`rank:settled:`（数据）与 `rank:settle:`（锁）只差一个字符；`rank:robots:` / `rank:robot_infos:`（数据）与 `rank:robot_tick:`（锁）同理。`HasPrefix` 恰好都不会串，但改动分类表前必须重新核对——这是本工具唯一一处「写错就会伤害生产」的地方。

迁移与部署的先后无硬性要求：工具只碰无 TTL 的 key，与正常运行时的写入不冲突，可以部署前跑、也可以部署后跑。建议部署后跑一次（此时新 key 已带 TTL，工具报告的待补数量就是纯粹的存量）。

### 6.3 上线操作清单

**不是「换掉可执行程序」一件事，但要做的事很少：一条只读查询、一次工具执行，外加知会运营两个展示变化。**

#### 6.3.1 只重新构建、部署一个二进制

| 组件 | 是否重发 | 依据 |
|---|---|---|
| `socialserver` | **是（唯一）** | `common` 在 `go.mod` 里是源码依赖（`replace common => ../common`），会被**编译进** socialserver，不需要单独部署 `common` |
| `gameserver` / `globalserver` / `payserver` / `robot` / `gm` | 否 | 对 rank 的唯一接触面是 4 个 S2S 接口，proto 与签名零改动（`git diff HEAD -- '*.pb.go'` 为空） |
| `gm` 前端 | 否 | 建榜表单的 `closeTime` 必填早于本轮改动 |

不影响部署的项**逐条确认过为空**：无新增配置项（`git diff HEAD` 在 `*config*` / `*yaml*` / `conf/` / `release/` 上为空；`internal/server.go` 只多了一处 32 协程池的初始化与收尾）、无 Redis 结构变更、无 Mongo schema 迁移、无灰度开关。

#### 6.3.2 部署前：一条只读查询

**这一步不建议跳过。** `Tick` 在 `settleAt == 0` 时的行为发生了**反转**：

- HEAD：`settleAt = 0` ⇒ `now < settleAt` 为假 ⇒ 继续往下走 ⇒ **第一次 Tick 就把整个活动结算掉**（活动实质上 1 秒内死亡）；
- 新代码：识别出这一情形后打警告并 `return nil` ⇒ **永不结算，也永不对机器人 tick**，且因为 `syncFromMongo` 的 7 天跳过以 `cfg.CloseTime > 0` 为条件（[manager.go:1038](../internal/rank/manager.go#L1038)），这些活动**仍会被载入内存**，于是每秒刷一条警告。

[handler/rank.go:451](../internal/handler/rank.go#L451) 新增的校验只拦**新建**，拦不住存量：

```js
// 字段名是小写的。engine.Config 没有 bson 标签，序列化结果实测为
// config.closetime / config.gameendtime，而不是 closeTime。
db.rank_config.find({ "config.closetime": 0, "config.gameendtime": 0 }, { _id: 1 })
```

**存量里大概率一条都没有**，原因是两道前端闸门：GM 前端直到 `d9028af`（2026-08-24「新增周期性排行榜类型支持」）才加上 `closeTime` 非空校验；且活动 `endTime` 为 0 时自动填充得到空串（`tsToDatetimeLocalCst` 对 0 返回 `''`），同样会被前端拦下。只有那之前创建的榜理论上可能是 0 值。

- **查到**：先给这些活动补一个真实时间（`db.rank_config.updateOne(...)`）再上线，或确认它们本就是废弃数据；
- **查不到（大概率）**：这一步到此结束，可以直接上线。

#### 6.3.3 部署后：跑一次 `cmd/rankttl`

见 §6.2——存量永久 key 不自动收敛，必须手工跑一次（先 dry-run 看报告，再 `-apply`）。建议放在部署后：此时新产生的 key 已带 TTL，报告里的待补数量就是纯粹的存量。

#### 6.3.4 两个 GM 侧可见的行为变化

| 变化 | 影响面 | 依据 |
|---|---|---|
| `Settle` 第二次调用返回空列表 | **仅展示**。奖励发放走 `S2SGetRewardUsers` → `ResolveEngineService` → `GetHistoricalRewardUsers`（Mongo 历史路径），**不受影响** | §7 观察 2 |
| 建榜必须给 `closeTime` 或 `gameEndTime`，否则返回 `InvalidArgument` | GM 表单本来就必填，实际不会触发——这条是给我方脚本/旁路调用兜底的 | [handler/rank.go:451](../internal/handler/rank.go#L451) |

知会运营即可，不需要改代码。

#### 6.3.5 滚动发布与回滚

**滚动发布安全。** 新老节点可混跑：老节点继续靠 SCAN `rank:meta:*` 发现（§3 第 01 条），新节点靠一次性 `bootstrapRegistry` 初始化，两者得到的是**同一份服务集合**；`rank:create` 的 pub/sub 载荷未变（逐字节相同）。每个新节点启动时会做一次 `SCAN`，属**预期现象**，不是故障。

**回滚有一处有界风险。** 新代码给 `rank:def` 加了 7 天 TTL（缺陷 1），而老代码**没有恢复钩子**。若回滚后连续运行超过 7 天且期间不重启，定义过期会让该活动的**每次得分写入**返回 `ErrDefinitionNotFound`。

两种处理，任选其一：

- **最省事**：回滚后 7 天内重启一次即自愈——老代码对不存在的服务会重新 `RegisterRank`，那时用的是裸 `SET`，写了就是永久 key；
- **最彻底**：回滚前先把定义钉住（Cluster 下需逐 master 执行，`redis-cli --scan` 只扫单节点）：

  ```bash
  redis-cli --scan --pattern 'rank:def:*' | xargs -r -n1 redis-cli persist
  ```

#### 6.3.6 构建环境

[go.mod](../go.mod) 新增了 `github.com/alicebob/miniredis/v2 v2.36.1`。**只有测试用它，生产二进制不含该模块**；但构建机需要能解析它——若构建走离线私有 proxy，上线前先确认可达。

---

## 7 · 审计中发现、但不在 13 条内的问题

按严重程度排列。观察 1 与观察 2 **仍未修复**（都不阻塞上线，但需要产品/GM 侧决策或独立立项）；观察 3 的前半段已随本轮引入 miniredis 解决。

### 观察 1 · `tickSubmitTimeout` 只约束单次提交，不约束批次总量

`tickServices` 对**每个** Service 依次调用 `SubmitWaitTimeout(ctx, fn, 200ms)`（[manager.go:142](../internal/rank/manager.go#L142)）。这个超时约束的是**单次入队等待**，而批次总量是 `200ms × N`：

```text
池持续饱和（例如 Redis 大面积超时，每个 Tick 都卡在 RTT 上）
  → service #1 等待 200ms 超时跳过
  → service #2 等待 200ms 超时跳过
  → ... N 个 service 串行等待 = 200ms × N
```

`tickServices` 跑在 `tickLoop` 的消费 goroutine 上，所以 `tickLoop` 会被拖住 `200ms × N`。`time.Ticker` 的 channel 容量为 1，期间只保留一个待处理 tick，其余被静默丢弃——即**文档原方案要解决的"ticker 丢 tick"问题只被部分缓解**：从"无界阻塞"变成"有界但仍可能很长"。

触发条件苛刻（池需持续饱和 200ms×N，且当前 worker=32 / queue=4096），所以不阻塞上线。但如果要彻底消除，有两个方向：

- **给整个批次加一个截止时间**（例如整批预算 500ms，超预算后剩余 service 直接跳过本轮），这也正是文档 §00 原始建议的形状（"批次提交用带超时的非阻塞提交"）；
- 或者接受：Redis 大面积超时时本来也无法推进榜单，此时"跳过一个 tick"与"慢一个 tick"没有实质差别——**但必须有可观测手段**，否则表现为"排行榜悄悄停更"。

**未覆盖测试**：需要模拟"池长时间饱和 + 多个 service"，现有 `TestTickServicesSkipsWhenPoolSaturatedPastTimeout` 只用了 2 个 service。

### 观察 2 · `Settle` 短路返回 `nil, nil`，GM 的结算结果展示会变空

`Settle` 短路时 `return nil, nil`（[service.go:731](../internal/rank/engine/service.go#L731)），调用方 `handleSettle` 用 `for gid, snapshots := range results`（nil map 迭代 0 次）得到一个空的 `Groups` 列表。

**影响面已核实为"仅展示"**：GM 的奖励发放走的是 `S2SGetRewardUsers` → `ResolveEngineService` → 历史路径 `GetHistoricalRewardUsers`（读 MongoDB），**不依赖 `Settle` 的返回值**。所以 GM 后台第二次点「结算」会看到空列表，但不会少发奖励。

**这是一处行为变更**，需与 GM 前端确认可接受：活动正常结束（`Tick` 自动结算并置位）之后，GM 再点「结算」拿到的是空列表而不是已结算快照。若前端依赖该列表做展示，应改为短路时回读历史快照。

### 观察 3 · 测试基建：Redis 替身已就位，注册表路径仍待补测

**这一观察的前半段已过时，后半段仍然成立。**

本轮引入了 `github.com/alicebob/miniredis/v2`（test-only），「仓库内没有 Redis 测试替身」不再成立。`golib/redis.NewRedis` 走 `redis.NewUniversalClient`，**单地址 ⇒ 普通 client** ⇒ miniredis 是可直接替换的替身，生产代码不需要为测试留任何 API；`Store.dao` 也由 `*DAO` 放宽为 `storeMongo` 接口。TTL 的全部写路径/回填路径现在都有 miniredis 级的 key 断言（见 §5）。

**但第 01 条的注册表路径仍未被直接覆盖**：`registerActive` / `unregisterActive` / `syncFromRedis` 的剪枝 / `bootstrapRegistry` 依旧只有两个纯函数（`parseRegistryMember` / `registryDeadline`）与 `manager_sync_test.go` 的状态判据被覆盖。而第 01 条恰恰是本次改动里**集群正确性风险最高**的一条（`KEYS` → 注册表是一处行为替换，且带 TTL 续期）。

**建议（独立立项）**：现在替身已经有了，补测的成本从「先建基建」降到「照 §5 里 `member_index_miniredis_test.go` 的形状写几例」。在此之前，第 01 条仍只能靠代码审查与线上观察。

需要提醒的是：**miniredis 是单节点，不校验 hash tag 与 CROSSSLOT**。涉及多 key 的 Lua（`BatchUpsertScore`、`RestoreMembers`）与「`rank:{active_services}` 单 slot」这类不变量本地全绿**也不能证明生产 Cluster 可用**，新增/改动 Lua 的键空间必须人工核对同 slot。这也是 `AtomicClaim` 选 Go 侧 `Expire` 而不是改 Lua 的原因。另外 `instanceVerifyInterval`(60s) 这类基于 Go 侧 `time.Now()` 的缓存无法用 `FastForward` 推进（后者只动 Redis 虚拟时钟），跨进程锁（`TryLockSettle` / `TryLockRobotTick` / periodic 分布式锁）也无法单测。

---

## 附：文件与行号索引

| 主题 | 文件 |
|---|---|
| 全局协程池 | [internal/taskpool/pool.go](../internal/taskpool/pool.go) |
| 服务注册 / 池化调度 / 注册表 | [internal/rank/manager.go](../internal/rank/manager.go) |
| 结算短路 / 软缓存 / TTL 公式 | [internal/rank/engine/service.go](../internal/rank/engine/service.go)、[store.go](../internal/rank/engine/store.go) |
| 机器人 tick 缓存 | [internal/rank/engine/service_robot.go](../internal/rank/engine/service_robot.go) |
| 实例正向缓存 | [internal/rank/engine/group.go](../internal/rank/engine/group.go) |
| 周期轮次 timer 生命周期 | [internal/rank/periodic/handler.go](../internal/rank/periodic/handler.go) |
| 成员索引 | [internal/rank/member_index.go](../internal/rank/member_index.go) |
| 集群安全的 SCAN | [golib/redis/redis_op_scan.go](../../golib/redis/redis_op_scan.go) |
| key 定义 | [common/redis/defines_rank.go](../../common/redis/defines_rank.go)、[common/rank/defines.go](../../common/rank/defines.go) |
| 池初始化与关闭顺序 | [internal/server.go](../internal/server.go) |
