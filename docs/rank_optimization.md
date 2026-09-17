# 排行榜引擎优化建议

> 文件：`internal/rank/`，`internal/server.go`
> 日期：2026-09-16
> 约束：不改变现有存储结构和存储结果
> 修订：2026-09-17（见文末「修订记录」）

---

## 概览

| # | 标题 | 文件 | 优先级 |
|---|---|---|---|
| 00 | 建立 socialserver 全局协程池（基础设施） | `internal/taskpool/`（新增），`server.go` | 🔴 高 |
| 01 | syncFromRedis 改「启动 SCAN 建表 + 之后查注册表」，取消周期扫描 | `manager.go`, `store.go`, `golib/redis` | 🔴 高 |
| 02 | tickAllRobots 加分组缓存（TTL 2s）+ 收敛 `Range(0,-1)` + 活跃期 Redis key TTL 设计 | `service_robot.go`, `store.go` | 🔴 高 |
| 03 | ensureGroupInstance 加带有效期的内存标记 | `group.go`, `service.go` | 🔴 高 |
| 04 | tickServices 改用全局协程池并发执行 | `manager.go` | 🔴 高 |
| 05 | warmUpAllServices 改用全局协程池 | `manager.go` | 🔴 高 |
| 06 | Tick/Settle 增加结算短路（配置变更自动失效） | `service.go` | 🟡 中 |
| 07 | time.AfterFunc cleanup 纳入生命周期管理 | `periodic/handler.go` | 🟡 中 |
| 08 | MemberIndex 键过期策略（现状核验 + TTL + 清理链路缺陷） | `member_index.go`, `manager.go` | 🟡 中 |
| 09 | syncFromMongo 由「删除」改为「修复」 | `manager.go` | 🟡 中 |
| 10 | ListGroupRank/GetMemberRank 提取 resolveSettled | `service.go` | 🟢 质量 |
| 11 | Settle() 减少 cloneSnapshots 调用次数 | `service.go` | 🟢 质量 |
| 12 | findTier/robotAvatarInfo 改为 map 查找 | `service_robot.go` | 🟢 质量 |
| 13 | WarmUp 加原子快路径（`sync.Once` 与两阶段改造均不做） | `service.go` | 🟢 质量 |

**本轮已重写**：00（补充 ticker 背压约束与池内不可再等池的硬约束）、01（改为「启动 SCAN 建表 + 注册表」，取消周期扫描）、02（改为整份分组列表缓存 + 2 秒有效期 + `Range(0,-1)` 收敛；活跃期 TTL 覆盖全部 13 个 key、回填补 TTL、走灰度）、06（`settledAt` 比较 + `canUpdateScore` 闸门，修正两处论断）、08（现状核验 + 清理链路缺陷 (a)–(d3)）、09（删除改为修复）、13（改为 `WarmUp` 原子快路径）；新增「基础原则与全量盘点」一节（两条原则 + 17 个 key 的 TTL/恢复台账 + 全仓扫库审计）

**待办**：本轮另发现 10 项问题（A–J），见文末「修订记录」，不在原 13 条内

**基础原则**：见下一节「基础原则与全量盘点」——① Redis 只是缓存，所有 key 都必须有 TTL **且**有恢复路径；② 正常逻辑不得扫库。这两条是全文的判定依据。

---

**🔴 上线前必改（2026-09-17 二次复核新增）**

以下 6 项是复核第 02、06 条方案时发现的**方案自身缺陷 + 与基础原则冲突处**，不是原代码的既有问题——即"照本文档实施反而会引入 bug"。必须先修正再落地：

| # | 问题 | 触发条件 | 后果 | 落点 |
|---|---|---|---|---|
| 必改-1 | 第 06 条论证所依赖的「结算之后不可能再有新分组产生」**与代码不符** | ① `canUpdateScore` 用 `now > GameEndTime`（严格），`Tick` 用 `now >= settleAt`，`0 < GameEndTime < CloseTime` 时存在 1ms 窗口；② `Settle` 的**唯一外部调用方是 GM 后台**，无任何时间闸门 | 短路后新分组**永不结算** → 玩家奖励永久丢失；且 `IsSettled()` 同步短路，查询被路由到历史 MongoDB 路径，该分组**完全不可见** | 第 06 条 |
| 必改-2 | 第 02 条缓存 sketch 的 `groupsCacheExpiry` **只写不读**，缓存永不过期 | 同一节点长期持有 robot tick 锁（锁 key 每秒一把，抢占结果可能长期不变） | 其他节点后续创建的分组在该节点**永远不被 tick** | 第 02 条 |
| 必改-3 | 第 02 条缓存 API 形状与真实调用点不符 | — | `getGroup(groupID)` 是按分组取值，而 `tickAllRobots` 需要的是**整份未结算分组列表**，按现 sketch 落不了地 | 第 02 条 |
| 必改-4 | 第 06 条未定义 `settledAt.Store` 的**落点** | — | 若放在成功分支内，则只有抢到 `TryLockSettle` 的 1/N 节点受益，其余节点每秒 2 次 HGETALL 原样保留 | 第 06 条 |
| 必改-5 | 第 02 条的 TTL 会被**懒加载回填**永久抵消 | TTL 到期后发生任意一次读（`LoadGroups` / `GetMember` / `GetAllMembers` / `LoadRobots` / `RestoreSettled`） | 回填用 `HSet`/`Set` 且**不带 TTL** → key 变永久。而这些正是历史查询的目标活动，**等于 TTL 在最该生效的场景失效** | 缺陷 3 |
| 必改-6 | `rank:def` **不能只设 TTL 不加懒恢复** | 设了 TTL 且到期 | `OpenInstance` 依赖 `rank:def` 存在否则返 `ErrDefinitionNotFound` → **新分组建不出来，活动不可玩**，最坏 30 秒窗口（等 `syncLoop` 重建）。必须配 `GetRank` miss → 从内存 `s.config` 零 IO 重建 | 缺陷 1 |

**结论**：必改-1 是全文唯一会**静默吞掉玩家奖励**的问题，必须最先处理；必改-5 是唯一会让**第 02 条整体失效**的问题。

---

实施顺序建议：**00（基础设施）→ 01 → 04/05（接入池）→ 06 → 09 → 08 → 02/03 → 其余**
（08 提到 09 之后：09 的修复分支要复用 08 的 `cleanupServiceData`；13 与任何一条都无耦合，1 行改动，可随时插入 → 见第 13 条）

**落地上须先做的四件事**：① 第 06 条加 `canUpdateScore` 闸门（必改-1，全文唯一会静默吞奖励的问题）；② 第 02 条缓存改为「整份分组列表 + 2 秒有效期」（必改-2、必改-3，两者是同一段代码）；③ 缺陷 3 的回填补 TTL 必须与第 02 条**同批上线**（必改-5，否则 TTL 在历史查询场景下完全失效）；④ 第 02 条活跃期 TTL 是全文唯一**不可逆**改动，必须先灰度（见该条「⚠️ 唯一不可逆的改动」与待办 H）。另：`rank:def` 在补上懒恢复前**不得设 TTL**（必改-6）。

---

## 基础原则与全量盘点（2026-09-17 补充）

本节的两条原则是全文所有条目的判定依据；任何条目若与本节冲突，以本节为准。

### 原则一：Redis 只是缓存 —— 所有排行榜 key 都必须有 TTL，且必须有恢复路径

这两件事是一体两面，**只做一半都会出问题**：

- **只设 TTL 不做恢复** → 到期后功能直接失效。最典型的例子是 `rank:def`：`OpenInstance` 先 `Exists(rank:def)`，不存在就返回 `ErrDefinitionNotFound`（[service_redis.go:189-195](../../common/rank/service_redis.go)）——即**新分组无法创建**，活动彻底不可玩。
- **只做恢复不设 TTL** → 未走结算/删除流程的活动永久占内存。这正是当前现状（见下表「活跃期」一列）。

**全量 key 台账**（14 个数据 key + 3 个锁 key，其中 1 个数据 key 是死代码 → 13 个在用）

| key | 活跃期 TTL | 结算后 TTL | 恢复路径（Mongo 权威源） | 缺口 |
|---|---|---|---|---|
| `rank:def:{rankCode}` | ❌ 无（`Set`） | ❌ 无 | 6 处 `RegisterRank` 从配置重建（最长 30s 延迟） | ⚠️ **缺陷 1** |
| `rank:inst:{instanceID}` | ❌ 无（`SetNX` TTL=0） | ✅ 2 周（`ExpireInstance`） | `LoadGroupInst` ← `CT_RANK_INST` | ⚠️ 缺陷 2 |
| `rank:mb:{instanceID}` | ❌ 无 | ✅ 2 周（同上） | `recoverGroupData` ← `CT_RANK_SCORE` | ⚠️ 缺陷 2 |
| `rank:seq:{instanceID}` | ❌ 无 | ✅ 2 周（同上） | `RestoreMembers` 推进到 `max(sequence)`（Lua） | ⚠️ 缺陷 2 |
| `rank:settled:{instanceID}` | — | ⚠️ 部分（见缺陷 4） | `LoadGroupSettled` ← `CT_RANK_SETTLED` | ⚠️ **缺陷 4** |
| `rank:meta:{bizId}` | ❌ 无 | ✅ 2 周（`CleanupLiveData`） | `ensureLoaded` 从 `CT_RANK_GROUP` 重算 `nextGroupID` | ⚠️ 缺陷 2 |
| `rank:groups:{bizId}` | ❌ 无 | ✅ 2 周（同上） | `LoadGroups` ← `CT_RANK_GROUP` | ⚠️ 缺陷 2 |
| `rank:members:{bizId}` | ❌ 无 | ✅ 2 周（同上） | `GetMember` ← `CT_RANK_MEMBER` | ⚠️ 缺陷 2 |
| `rank:claims:{bizId}` | ❌ 无 | ✅ 2 周（同上） | `AtomicClaim` 回退 `GetClaim`/`SaveClaimIfNotExists` ← `CT_RANK_CLAIM` | ⚠️ 缺陷 2 |
| `rank:robots:{bizId}:{gid}` | ❌ 无 | ✅ 2 周（同上） | `LoadRobots` ← `CT_RANK_ROBOT` | ⚠️ 缺陷 2 |
| `rank:robot_infos:{bizId}:{gid}` | ❌ 无 | ✅ 2 周（同上） | 同上 | ⚠️ 缺陷 2 |
| `rank:member_index:{uid}` | ✅ 7 天 | ✅ 7 天 | `rebuildMemberIndex` ← engine 内存 | 无（第 08 条） |
| `rank:mongo_chk:{bizId}` | ✅ 10 分钟 | ✅ 2 周 | 无需（哨兵本身是缓存） | 无 |
| `rank:max_score:{bizId}` | — | — | — | ⚠️ **缺陷 5（死代码）** |
| `rank:settle:{bizId}:{gid}` | ✅ 10 分钟 | ✅ | 无需（锁） | 无 |
| `rank:robot_tick:{bizId}:{sec}` | ✅ 3 秒 | ✅ | 无需（锁） | 无 |
| `rank:periodic_advance:{lk}:r{n}` | ✅ 1 分钟 | ✅ | 无需（锁） | 无 |

**结论**：13 个在用数据 key 中，**活跃期有 TTL 的只有 2 个**（`rank:member_index`、`rank:mongo_chk`）。其余 11 个在活动存续期间**永不过期**，一旦活动未走 `Settle`→`CleanupLiveData` 或 `RemoveService`，key 永久残留。第 02 条的「活跃期 TTL」正是补这一块缺口，但它的现表**只列了 9 个 key**，漏掉 `rank:def` / `rank:inst` / `rank:mb` / `rank:seq`，且 `rank:settled` 行的「设置者」写错了（见缺陷 4）。第 02 条已按本节的写法修正。

---

#### 缺陷 1：`rank:def` 是「有 TTL 就会出故障」的唯一一个 key

`rank:def` 由 `RegisterRank` 用 `Set` 写入，**无 TTL**（[service_redis.go:164](../../common/rank/service_redis.go)）；6 处生产调用点全在注册路径上（`registerEngine` / `registerSubService` / `syncFromRedis` / `syncFromMongo` / `applyRankConfigDoc` / periodic handler）。

它**可以**设 TTL——因为 MongoDB 的 `CT_RANK_CONFIG` 是权威源，注册路径能从配置重建。但**恢复延迟是致命的**：

```
t0      rank:def 到期消失
t0+ε    玩家 upsert → ensureGroupLocked → OpenInstance → Exists(rank:def)=false
        → ErrDefinitionNotFound → 新分组建不出来
t0+Δ    syncLoop 下一轮（Δ 最坏 30s）才 RegisterRank 重建
```

也就是说，**只要给 `rank:def` 设 TTL 而不加读路径懒恢复，就会周期性出现「活动在但玩家进不去」的故障**，窗口最坏 30 秒。

**修法**：`GetRank` miss 时按需重建，而 `engine.Service` 内存里就有 `s.config`，所以这是**零 IO 恢复**：

```go
// common/rank/service_redis.go —— 交给 engine 层注册一个恢复回调，
// 或让 engine.Service.GetRank 包装一层：miss → RegisterRank(s.config 转换出的 Rank) → 重读
func (s *Service) GetRank(ctx context.Context, rankCode string) (*rank.Rank, error) {
    def, err := s.rankService.GetRank(ctx, rankCode)
    if err == nil {
        return def, nil
    }
    if !errors.Is(err, rank.ErrDefinitionNotFound) {
        return nil, err
    }
    // 零 IO：定义就在 s.config 里
    _ = s.rankService.RegisterRank(ctx, s.rankDefFromConfig())
    return s.rankService.GetRank(ctx, rankCode)
}
```

**在补上这个懒恢复之前，`rank:def` 不要设 TTL。** 这是「原则一要求设 TTL」与「设了会故障」的唯一冲突点，必须成对落地。

---

#### 缺陷 2：11 个 key 活跃期无 TTL —— 由第 02 条统一补，但必须覆盖到全部 11 个

第 02 条已给出公式（`ttl = time.Until(effectiveSettleAt()) + SettledCacheTTL`，绝对过期时刻 = `end + 2 周`，与设置时机无关）。需要补的是**覆盖面**：

| 现有覆盖机制 | 覆盖的 key | 活跃期是否已覆盖 |
|---|---|---|
| `Store.CleanupLiveData`（[store.go:403-413](../../socialserver/internal/rank/engine/store.go)） | meta / groups / members / claims / mongo_chk / robots / robot_infos | ❌ 只在结算后调用 |
| `RedisService.ExpireInstance`（[service_redis.go:632-635](../../common/rank/service_redis.go)） | inst / mb / seq / settled（**一次调用覆盖 4 个**） | ❌ 只在结算后调用 |
| `registerEngine` / `RegisterRank` | def | ❌ 从未设 TTL |

`ExpireInstance` 已经是「一次调用设 4 个 key」的现成入口，活跃期复用它对每个 group 调一次即可，无需新增函数。加上 `Store.CleanupLiveData` 的 7 个，再加 `def`（须先做缺陷 1），11 个 key 全覆盖。

---

#### 缺陷 3（**最关键**）：所有懒加载回填都用 `HSet`，不带 TTL —— 会把第 02 条的 TTL 永久抵消

这是原则一在**实施层面**最大的坑：第 02 条把 TTL 设对了，但只要有**任何一次读发生在 TTL 到期之后**，回填就会把 key 重新写成永久的。

| 回填点 | 代码 | 回填方式 |
|---|---|---|
| `LoadGroups` | [store.go:99-102](../../socialserver/internal/rank/engine/store.go) | `HSet` ×N，**无 TTL** |
| `GetMember` | [store.go:258](../../socialserver/internal/rank/engine/store.go) | `HSet`，**无 TTL** |
| `GetAllMembers` | [store.go:301-303](../../socialserver/internal/rank/engine/store.go) | `HSet` ×N，**无 TTL** |
| `LoadRobots` | [store.go:347-350](../../socialserver/internal/rank/engine/store.go) | `HSet` ×N，**无 TTL** |
| `RestoreSettled` | [store.go:217](../../socialserver/internal/rank/engine/store.go) | `Set`，**无 TTL**（见缺陷 4） |
| `LoadGroupSettledCached` | [store.go:757](../../socialserver/internal/rank/engine/store.go) | `SetEX(SettledCacheTTL)` ✅ **唯一正确的写法** |

时间线（以 `rank:groups` 为例）：

```
T        活动结束 → CleanupLiveData → EXPIRE rank:groups 2周
T+14d    key 到期，Redis 自动删除                ← TTL 生效
T+14d+ε  GM 查历史 → LoadGroups → Redis 空 → 查 Mongo（永久保留）→ 命中
         → HSet 回填 rank:groups               ← 但没有 EXPIRE！
T+∞      这个 key 永远存在了。且每查一个过期活动就多一个永久 key
```

**也就是说，第 02 条的 TTL 在「过期后仍被读取」的活动上完全失效**，而这些恰好就是历史查询的目标活动。不修这一条，原则一在第 02 条上落不了地。

**修法**：把所有回填点统一改为「回填 + 补 TTL」，抽出一个小助手，照 `LoadGroupSettledCached` 的写法：

```go
// Store —— 回填 Redis 后立即重设绝对过期时刻（与第 02 条同一个公式）
// 顺序必须是先写数据再 EXPIRE：若先 EXPIRE 后 HSet，EXPIRE 作用在不存在的 key 上会被丢弃。
func (st *Store) backfill(key string, write func()) {
    write()
    if ttl := st.remainingRetention(); ttl > 0 {
        st.rdb.Expire(key, ttl)
    }
}

// remainingRetention = (effectiveSettleAt + SettledCacheTTL) - now
// 活动仍在进行时该值为「距结束 + 2 周」，与第 02 条活跃期 TTL 完全一致，
// 因此活跃期回填与结算后回填可以共用同一个函数，不需要分支。
```

`Expire` 的 TTL 用 `time.Until(activityEnd) + SettledCacheTTL`，**与第 02 条派生出的公式是同一个式子**，所以活跃期与结算后的回填天然一致，无需区分两条路径。`ttl <= 0` 时跳过（此时本应已过期，`Expire` 收到非正数会立即删除 key——比不设更危险）。

**注意 `RestoreMembers` 是例外**：它已经是用 Lua 写的恢复路径，`rank:seq` 由脚本内 `SET`/`INCR` 维护，补 TTL 需要在脚本里追加 `EXPIRE`，或在脚本返回后由 Go 侧补一次 `Expire`（后者更简单，且恢复是低频路径，多一次往返可接受）。

---

#### 缺陷 4：`rank:settled` 的 TTL 是「半个」的

同一个 key 有 3 个写入者，TTL 行为不一致：

| 写入者 | 代码 | TTL |
|---|---|---|
| `SettleInstance`（结算时） | [service_redis.go:308](../../common/rank/service_redis.go) | ❌ `Set`，无 TTL |
| `RestoreSettled`（冷恢复） | [store.go:217](../../socialserver/internal/rank/engine/store.go) | ❌ `Set`，无 TTL |
| `LoadGroupSettledCached`（历史查询回填） | [store.go:757](../../socialserver/internal/rank/engine/store.go) | ✅ `SetEX(SettledCacheTTL)` |

它之所以看起来「有 2 周 TTL」，纯粹是因为 `ExpireInstance` 在结算后顺带给它设了一次（[service_redis.go:635](../../common/rank/service_redis.go)）。但**任何一个 `RestoreSettled` 都会把 TTL 抹掉**，而 `RestoreSettled` 恰恰是在 key 已不存在时被调用的（[service.go:620](../../socialserver/internal/rank/engine/service.go)、`625`）——即缺陷 3 时间线的 `T+14d+ε` 那一步。修法与缺陷 3 相同（统一走 `backfill`）。

**顺带修正第 02 条的台账**：`rank:settled` 的活跃期/结算后 TTL 与「设置者」三处都要改——设置者是 `ExpireInstance`（通过 `Service.CleanupLiveData`），不是 `SaveSettled`（后者只写 MongoDB）。

---

#### 缺陷 5：`rank:max_score` 是死代码

`RankMaxScoreKeyPrefix` / `GetRankMaxScoreKey` 只在 [defines_rank.go:88-123](../../common/redis/defines_rank.go) 自身出现，**全仓无任何生产者与消费者**。它不是「漏了 TTL」，而是「漏了实现」——真实玩家最高分的功能从未接入。建议删除定义，或明确标注为预留；不要把它算进 TTL 台账（已在台账中标注）。

---

#### 补充核查：`setMongoChecked` 会把 2 周 TTL 缩短为 10 分钟（当前不可达，记录备查）

`setMongoChecked` 用 `SetEX(mongo_chk, 10min)`（[store.go:42](../../socialserver/internal/rank/engine/store.go)），而 `CleanupLiveData` 用 `Expire(2周)`（[store.go:407](../../socialserver/internal/rank/engine/store.go)）。`SetEX` 会**覆盖** TTL，方向是缩短。

已核查为**当前不可达**：`setMongoChecked` 只在「Redis 空且 Mongo 也空」时调用（[store.go:94-97](../../socialserver/internal/rank/engine/store.go)、`297-299`），而 `CleanupLiveData` 作用于「已有分组数据」的 bizId，两者不会命中同一个 bizId 的同一时刻。

**但一旦第 02 条引入滑动刷新、或第 01 条的注册表修剪改变调用时机，它就会变成可达的**（TTL 从 2 周被压回 10 分钟 → `rank:groups` 被提前删除）。修法是把 `setMongoChecked` 也走缺陷 3 的 `backfill`，或把它的 TTL 改为「与同 bizId 其它 key 取同一个值」。**只要动了 TTL 方案，这条必须一起改。**

### 原则二：正常逻辑不得扫库

**「扫库」指任何 O(全库 key 数) 的命令**（`KEYS` / `SCAN` / `HGETALL` 全量、`SMEMBERS` 全量等按 key 空间规模增长的操作）。这类操作的代价与**目标数据量无关**，只与**实例总 key 数**有关，因此无法通过清理业务数据来控制。

**全量审计结果：整个排行榜子系统只有一处扫库，且已在第 01 条修复。**

| 位置 | 命令 | 当前频率 | 状态 |
|---|---|---|---|
| `syncFromRedis`（[manager.go:469](../../socialserver/internal/rank/manager.go)） | `m.rdb.Keys(prefix + "*}")` | **每 30 秒**（`syncLoop`） | 第 01 条改为「启动一次 SCAN + 之后查注册表」 |

审计方法：全仓 grep `\.Keys\(` / `\.Scan\(` / `\.Iterator\(`，除 `golib/redis` 自身的实现外，**只有 `manager.go:469` 一处**。

**两点必须强调**：

1. **当前用的是 `KEYS`，比 `SCAN` 严重一个量级**。`SCAN` 是游标式增量返回，不阻塞 Redis；`KEYS` 在 Redis 单线程命令循环里一次性遍历全部 key，期间**该实例上所有命令排队**（包括其它业务的读写）。所以第 01 条是双重修复：既把它移出稳态，也把唯一一次执行从 `KEYS` 换成 `SCAN`。
2. **bootstrap 必须是一次性的、且失败可重试**。第 01 条的 sketch 用 `m.registryBootstrapped` 原子标记控制，失败不置位以便下一轮重试——这个结构保证了「扫库只可能发生在启动后的头几轮，且总次数有界」。**不要把它改回 `syncLoop` 里的无条件调用**，那等于把周期扫描重新引入。

**推论：任何新功能都不得引入扫库。** 需要「按前缀找 key」时，改为维护一个显式集合（第 01 条的注册表就是这个模式：写入方 `SADD`、读取方 `SMEMBERS`），或依赖已有的 MongoDB 权威查询（`LoadAllRankConfigs` 走的是带索引的 `Find`，不是 key 空间扫描）。

---

## 🔴 高优先级

### 00 · 建立 socialserver 全局协程池（基础设施）

**文件**：新增 `internal/taskpool/`，`internal/server.go`，`internal/rank/manager.go`，`internal/rank/periodic/handler.go`

**问题**

当前无限量地 `go func()`，缺少统一的有界执行层：

| 位置 | 形态 | 风险 |
|---|---|---|
| `manager.go:633` `warmUpAllServices` | 每个 Service 一个 goroutine，无上限 | Service 上千时瞬时上千 goroutine + Redis/Mongo 并发 |
| `manager.go:804` | `go localNewSvc.WarmUp(...)` | 散落，无追踪 |
| `periodic/handler.go:257` | `go svc.WarmUp` + 局部 `warmupSem` | 各写各的信号量，无全局约束 |
| `periodic/handler.go:216` | `time.AfterFunc` | 未纳入生命周期（见第 07 条） |

`manager.go:88-91` 的 tickLoop/syncLoop/2×subscribe 是常驻守护循环，不属于池的适用范围，保持现状。

**方案：单一全局池，优先级由提交 API 表达**

不做多池拆分，用一个全局池承载全部任务。任务的重要性差异通过**两个提交入口**表达，而不是通过池的隔离：

| 提交方式 | 语义 | 适用任务 |
|---|---|---|
| `SubmitWait` | 阻塞直到入队，不丢弃 | tick 等秒级时效、不可丢弃的任务 |
| `Submit` | 非阻塞，队满返回 `ErrPoolFull` | WarmUp、重建等可丢弃/可重试的任务 |

这样既保持单池的简单性，又避免了"WarmUp 洪水挤占队列导致 tick 任务被丢"——tick 走 `SubmitWait` 保证一定被执行，WarmUp 走 `Submit` 在池饱和时被主动丢弃（丢掉的 WarmUp 由后续懒加载兜底，无正确性影响）。

**为什么不直接复用 `golib/gpool`**

| 缺口 | 说明 |
|---|---|
| 无完成通知 | `Push` 是 fire-and-forget，没有 WaitGroup 或完成信号，而 tickServices 必须等全部 Tick 结束才能跑 `tickPeriodicActivities` |
| 非阻塞投递缺失 | `dispatch` 按 `offset % nLen` 轮询投递，慢 worker 的 channel 堆满时会阻塞整个 dispatch 循环 |
| 无关闭语义 | `Release()` 只停 worker，不 drain 队列，无法配合 `server.OnClose` 顺序 |
| 无 panic 保护 | worker 内未 recover |

补齐这四项的改动量超过新建一个约 80 行的池，且会改变 `gpool` 现有调用方的行为。若倾向复用，可作为后续统一收敛的方向（把 `gpool` 升级并迁移所有调用方），但不是本次上线的必要路径。

**API**

```go
package taskpool

type Pool struct { /* ... */ }

func New(name string, workers, queueLen int) *Pool

// Submit 非阻塞提交，队列满返回 ErrPoolFull
func (p *Pool) Submit(fn func()) error

// SubmitWait 阻塞直到任务入队（保证不丢失），池已关闭返回 ErrPoolClosed
func (p *Pool) SubmitWait(ctx context.Context, fn func()) error

// Close 停止接收 → drain 队列 → 等待 worker 退出
func (p *Pool) Close(ctx context.Context) error

// Global 进程级单例，server.OnInit 初始化
var Global *Pool
```

**完成语义由调用方持有**：池只管有界执行，批量等待由调用方用 `sync.WaitGroup` 组合，避免池级 `Wait()` 误等无关任务：

```go
var wg sync.WaitGroup
for _, svc := range svcs {
    s := svc
    wg.Add(1)
    if err := taskpool.Global.SubmitWait(ctx, func() {
        defer wg.Done()
        tickOne(s)
    }); err != nil {
        wg.Done() // 提交失败也要配平，否则 Wait 永久阻塞
        zaplog.LoggerSugar.Errorf("rank tick submit failed: %v", err)
    }
}
wg.Wait()
```

**panic 保护**：worker 内 `recover`，避免单个任务 panic 导致 worker 退出、后续任务永久堆积。这一点在池化之后尤其重要——原先是"进程崩溃"（可观测），池化后若 worker 静默退出会退化为"任务挂起"（难排查）。

**初始化与关闭**

```go
// server.OnInit
taskpool.Global = taskpool.New("socialserver", 32, 4096)

// server.OnClose —— 必须在 redis/mongodb 关闭之前
taskpool.Global.Close(ctx)  // drain 完成后不再有任务持有 Redis 连接
```

关闭顺序很关键：池必须先于 Redis/MongoDB 关闭并 drain 完成，否则残留任务会操作已关闭的连接（与第 07 条同类问题）。

**单池的权衡**：worker 需要按"tick + WarmUp 并发上限之和"来配（建议 32 起），因为没有池级隔离，一次轮次切换的 WarmUp 洪峰会占用较多 worker 使 tick 排队。缓解手段有三：worker 数留足、WarmUp 走非阻塞 `Submit` 不堆积队列、tick 走 `SubmitWait` 保证不被丢弃。

**补充：常驻 ticker 循环不能用 `SubmitWait` 无界阻塞（必须遵守）**

上一条"tick 走 `SubmitWait` 保证不被丢弃"只适用于**批内**的每个 Service 任务，不适用于常驻循环对批次本身的提交。`tickServices` 的直接调用者是 [tickLoop](../../socialserver/internal/rank/manager.go)（[manager.go:95](../../socialserver/internal/rank/manager.go)，注意函数名是 `tickServices` 而非 `tickProcess`），它运行在 `time.Ticker` 上，而 Go 的 ticker channel 容量为 1、消费不及时会**静默丢弃** tick：

```go
// ❌ 错误：SubmitWait 无界阻塞会让 ticker 丢 tick
for range ticker.C {
    taskpool.Global.SubmitWait(ctx, func() { m.tickServices(ctx, now) })  // 池饱和时阻塞在此
}

// ✅ 正确：批次提交用带超时的非阻塞提交，提交失败本轮跳过
for range ticker.C {
    nowMs := now.UnixMilli()
    if err := taskpool.Global.SubmitWaitTimeout(ctx, func() { m.tickServices(ctx, nowMs) }, 500*time.Millisecond); err != nil {
        zaplog.LoggerSugar.Warnf("rank tick skipped, pool busy: %v", err)
        continue  // 跳过本轮，下一 tick 补上（tick 是幂等的：状态由 Redis 决定）
    }
}
```

注意 `nowMs` 必须在提交时按值捕获（如上），否则批次任务读到的可能已不是提交时那一秒。

因此 API 需要第三个提交入口：

| 提交方式 | 语义 | 适用任务 |
|---|---|---|
| `SubmitWait` | 阻塞直到入队，不丢弃 | **批内**单个 Service 的 tick |
| `SubmitWaitTimeout` | 阻塞至多 d，超时返回 `ErrPoolFull` | **常驻循环提交批次**，跳过优于阻塞 |
| `Submit` | 非阻塞，队满返回 `ErrPoolFull` | WarmUp、重建等可丢弃/可重试的任务 |

跳过一个 tick 是安全的：排行榜状态由 Redis 的 group 数据决定，`tickServices` 及其下游的 `Tick`/`Settle` 都不维护跨轮次的内存增量状态，下一轮 tick 会基于最新状态重新计算。**但必须记 warn 日志**——否则池长期饱和会表现为"排行榜悄悄停更"，比崩溃更难发现。同理 `syncLoop`（[manager.go:108](../../socialserver/internal/rank/manager.go)）在 tick 饱和时也应记为可观测事件，而不是无声跳过。

**⚠️ 硬约束：池内任务不得再阻塞等待池内任务，除非 worker 数严格大于可并发阻塞的批次数**

第 04/05 条会把 `tickServices` / `warmUpAllServices` **整体**提交进池，而它们内部又用 `SubmitWait` + `wg.Wait()` 提交每个 Service 的子任务。这构成嵌套：若同一时刻有 W 个外层批次任务占满了全部 W 个 worker，且它们都在 `wg.Wait()` 上等待子任务，而子任务还排在队列里等 worker —— **死锁**。

当前参数（worker=32、外层并发只有 `tickLoop` 与 `tickPeriodicActivities` 两个来源）是安全的，根因是 **32 ≫ 2**。因此这条不是"现在没问题"，而是一条必须写进代码注释的不变量：

> pool worker 数必须 **严格大于**「可能同时阻塞在 `wg.Wait()` 上的批次数」。调小 worker 或新增一个会自行向池内提交批次任务的调用方时，必须重新核算。

若不希望依赖这条不变量，替代做法是外层批次**不经过池**（`tickLoop` 直接同步调用 `tickServices`，只把每个 Service 的 `Tick` 提交进池）。这样嵌套消失，代价是批次本身占着一个常驻 goroutine——而它本来就占着 `tickLoop` 的 goroutine，实际没有额外成本，是更稳的选择。

| 优点 | 缺点 |
|---|---|
| goroutine 数量全局有界，与 Service 数、活动数解耦 | 新增基础设施包，需统一改造现有散落的 goroutine |
| 单池结构简单，无需为每类任务单独调参与维护 | tick 与 WarmUp 共享 worker，WarmUp 洪峰会延迟 tick（靠 worker 余量缓解） |
| panic 统一兜底，避免池化后"任务挂起"取代"进程崩溃" | 队满策略需按任务性质区分（批内阻塞提交、批次限时提交、WarmUp 可拒绝），调用方需正确选用 |
| 生命周期统一，关闭顺序可控 | **存在池内嵌套：需维持 worker 数 ≫ 并发批次数，否则死锁**（建议改为外层不过池以彻底消除） |
| 常驻循环限时提交，ticker 不再因池饱和而静默丢 tick | 批次超时跳过时需有日志/指标，否则"停更"不可见 |

---

### 01 · syncFromRedis 改「启动 SCAN 建表 + 之后查注册表」，取消周期扫描

**文件**：`manager.go`，`golib/redis`（新增 `ScanAll`）

**问题**

`syncFromRedis` 使用 `m.rdb.Keys(prefix + "*}")`（[manager.go:469](../../socialserver/internal/rank/manager.go)）扫描全库：

1. **阻塞**：`KEYS` 在 Redis 单线程上一次性遍历全部 key，key 多时阻塞数百毫秒到数秒，期间该节点上所有 Redis 命令排队。
2. **集群下是功能性 bug**：go-redis 对 `cmdFirstKeyPos == 0` 的 keyless 命令走 `hashtag.RandomSlot()`（`osscluster.go:1990`），即 `KEYS` 只命中**一个随机 master**，其余分片的 `rank:meta` 永远扫不到。

**频率核实**（回答"扫库频率会不会带来性能问题"）

先厘清 `SCAN` 的成本结构，再谈频率：

| 事实 | 说明 |
|---|---|
| `SCAN` 不阻塞 Redis | 游标式增量返回，单次只走一小段，这是它相对 `KEYS` 的唯一但关键优势 |
| `SCAN` 的**总功仍是 O(全库 key 数)** | `MATCH` 参数**不减少**工作量——仍然游走整个 keyspace，只是服务端过滤后才返回。成本取决于**这个 Redis 实例的总 key 数**，而不是有多少个 `rank:meta` |
| 单次迭代往返次数 ≈ 全库 key 数 / COUNT | `COUNT=500` 时：10 万 key → 200 次；100 万 key → 2000 次；500 万 key → 10000 次 |

**所以真正的问题不是 `SCAN` 本身，而是当前把它挂在 30s 的循环上。** `syncFromRedis` 与 `syncFromMongo` 共用 `syncLoop` 的 `syncInterval = 30 * time.Second`（[manager.go:22](../../socialserver/internal/rank/manager.go)），即每节点 2 次/分钟：

| 全库 key 数 | 现状：30s 一次（每节点） | 降频：600s 一次 | 本方案：仅启动一次 |
|---|---|---|---|
| 10 万 | 6.7 次往返/秒 | 0.33 次往返/秒 | **0（稳态）** |
| 100 万 | 67 次往返/秒 | 3.3 次往返/秒 | **0（稳态）** |
| 500 万 | 333 次往返/秒 | 16.7 次往返/秒 | **0（稳态）** |

不阻塞 Redis，但按 100 万 key、10 节点算，30s 频率意味着约 670 次往返/秒的纯扫描开销。降频只是把常数变小；**把扫描限制在启动期、稳态改走注册表，才能把它直接归零**。

**关键判断：既然发现的本质是「启动期一次」，周期扫描就没有必要存在。**

`syncFromRedis` 只在「Redis 有、MongoDB 没有」时才有价值，而这个状态只可能由 **进程崩溃于 Mongo 异步写落盘之前** 产生。除此之外的所有场景都不需要它：

| 场景 | 谁负责恢复 | 是否需要扫描 |
|---|---|---|
| 运行期新活动创建 | pub/sub（`rank:create`）实时通知所有节点 | ❌ |
| pub/sub 漏消息 | 源节点已写过 MongoDB → `syncFromMongo` 补 | ❌ |
| 进程崩溃、Mongo 写入未落盘 | 只有 Redis 有数据 | ✅ **仅此一种** |
| `tryRecoverPeriodicFromRedis` 降级路径恢复的周期服务 | 该路径不写 Mongo（见第 09 条，属待修的振荡问题） | — |

「崩溃于写入落盘前」这个窗口只在**进程重启时**需要处理——进程一旦存活，后续创建都走 pub/sub。因此 `syncFromRedis` 的本质是**启动期的崩溃恢复手段**，不是需要 30s 轮询的热路径。

**方案：首次 SCAN 建表，之后维护注册表，彻底取消周期扫描**

采用两段式：**启动时用 SCAN 把注册表建起来，之后所有发现走注册表**。这样 `KEYS` 的阻塞问题和周期扫描的聚合成本一起消除。

**成员格式里带截止时间，修剪不需要任何额外 Redis 命令**

注册表的难点是「meta 因 TTL 到期消失后，成员会变成幽灵」。朴素做法是每次迭代对每个成员发 `EXISTS rank:meta:{bizId}`——但那样每次迭代的命令数就是 O(活跃服务数)，1 万活跃服务下每 30 秒 2 万条命令，**比它取代的扫描还贵**。

正确做法是把截止时间编进成员本身：

```
key:    rank:{active_services}       // 单 SET，hash tag 固定单一 slot
member: {bizId}:{deadlineMillis}     // 如 balloon_1:1760000000000
```

`deadline = settleAt + SettledCacheTTL`（正是第 02 条推导出的绝对过期时刻；常驻活动用远期哨兵值表示永不过期）。于是修剪变成**纯本地过滤**：`SMEMBERS` 返回后直接在内存里筛掉 `deadline < now` 的成员，攒够一批发一次 `SREM`。每次迭代的额外 Redis 命令数：`SMEMBERS` 1 条 +（有陈旧成员时）`SREM` 1 条。

```go
func (m *Manager) syncFromRedis(ctx context.Context) {
    // ① 每个节点启动后做一次：SCAN + SADD，幂等
    if !m.registryBootstrapped {
        if m.bootstrapRegistry(ctx) {
            m.registryBootstrapped = true
        }   // 失败则不置位，下次迭代重试
    }

    // ② 读注册表 + 本地修剪：O(活跃服务数) 命令，O(1) 往返
    members, err := m.rdb.SMembers(ctx, rediskeys.RankActiveServicesKey)
    now := time.Now().UnixMilli()
    var live, stale []string
    for _, mem := range members {
        bizId, deadline, ok := parseRegistryMember(mem)
        if !ok || (deadline > 0 && deadline < now) {
            stale = append(stale, mem)   // 格式非法或已过期
            continue
        }
        live = append(live, bizId)
    }
    if len(stale) > 0 {
        m.rdb.SRem(ctx, rediskeys.RankActiveServicesKey, stale...)
    }

    // ③ live 里的 bizId 走原有注册流程（LoadActivityTimes 仍会校验 meta 是否真在）
}

func (m *Manager) bootstrapRegistry(ctx context.Context) bool {
    keys, err := m.rdb.ScanAll(ctx, "rank:meta:{*}", 500)  // 见下：仍需 ScanAll
    if err != nil {
        return false
    }
    // 逐个读 meta 里的 closeTime/gameEndTime 算出 deadline 后 SADD
    for _, key := range keys {
        bizId := extractBizId(key)
        deadline := m.registryDeadline(ctx, bizId)
        m.rdb.SAdd(ctx, rediskeys.RankActiveServicesKey, fmt.Sprintf("%s:%d", bizId, deadline))
    }
    return true
}
```

**陈旧 deadline 会不会误删有效成员？——不会造成实际损失**

GM 通过 `UpdateConfig` 把 `CloseTime` 推后时，注册表里的 deadline 仍是旧值（偏早），修剪会提前把它移除。但这没有危害，因为：

`UpdateService` → `SaveRankConfig` 会把配置**写入 MongoDB**（[manager.go:270](../../socialserver/internal/rank/manager.go)），所以这类服务由 `syncFromMongo` 负责注册，**不依赖注册表发现**。反过来说，注册表发现能力真正不可替代的场景是「Redis 有、Mongo 没有」——而这种服务的配置从未经 GM 成功更新过（`UpdateService` 必然写 Mongo），因此它的 deadline 一定是准的。两者互补，无需在 `UpdateConfig` 里加第三个维护点。

`golib` 仍需新增 `ScanAll(ctx, pattern, count) ([]string, error)`——`UniversalClient` 接口不含 `ForEachMaster`，需类型断言到 `*redis.ClusterClient`（集群）或直接 `Scan`（单机）。它现在只服务于 bootstrap，但集群正确性同样必要（`KEYS`/`SCAN` 在集群下都只命中一个随机 master，见上文）：

```go
// golib/redis 新增
func (r *Redis) ScanAll(ctx context.Context, pattern string, count int64) ([]string, error) {
    if cc, ok := r.client.(*goredis.ClusterClient); ok {
        var mu sync.Mutex
        var all []string
        err := cc.ForEachMaster(ctx, func(ctx context.Context, c *goredis.Client) error {
            keys, err := scanNode(ctx, c, pattern, count)
            if err != nil { return err }
            mu.Lock(); all = append(all, keys...); mu.Unlock()
            return nil
        })
        return all, err
    }
    return scanNode(ctx, r.client, pattern, count)  // 单机：普通 SCAN
}
```

**注册表只挂两个维护点（这是本方案可行的关键）**

原本担心「8 处写路径都要同步维护 SADD/SREM」，但核对代码后发现 meta key 的创建与删除各自只有一个入口，把注册表挂在同一处即可天然同步：

| 时机 | 落点 | 说明 |
|---|---|---|
| 创建 | `Store.SaveActivityTimes`（[store.go:164](../../socialserver/internal/rank/engine/store.go)）→ `SAdd` | meta key 的**唯一**创建入口。它由 `engine.NewService` 在构造时调用（[service.go:50](../../socialserver/internal/rank/engine/service.go)），而 `NewService` 正是全部 **6 个** `engine.NewService` 调用点（`registerEngine` [manager.go:179](../../socialserver/internal/rank/manager.go) / `syncFromRedis` [560](../../socialserver/internal/rank/manager.go) / `syncFromMongo` [880](../../socialserver/internal/rank/manager.go) / `applyRankConfigDoc` [1024](../../socialserver/internal/rank/manager.go) / `registerSubService` / `replaceSubService`）的公共收口 |
| 删除 | `Store.CleanupAll`（[store.go:435](../../socialserver/internal/rank/engine/store.go)）→ `SRem` | meta key 的**唯一**删除出口 |
| 过期 | `Store.CleanupLiveData` 设置的 TTL（[store.go:403](../../socialserver/internal/rank/engine/store.go)）到期后 meta 自动消失 | 无法在写入时预知，由 ③ 惰性修剪清理 |

因为创建/删除都在 `Store` 内部、与 meta key 自身的生命周期同函数，**不存在"改了 meta 忘了改注册表"的路径**。`store.go:137/156` 的 `HIncrBy`（realCount / nextGroupID）虽然也会创建 meta hash，但只在 `NewService` 之后发生，此时注册表已 SADD。

**注册表必须能自愈：三种不一致场景的兜底**

| 不一致 | 后果 | 兜底 |
|---|---|---|
| 漏 `SADD`（写注册表失败） | 该服务不被 `syncFromRedis` 发现 | pub/sub（`rank:create`）实时通知；`syncFromMongo` 重建时经 `NewService` → `SaveActivityTimes` → 再次 SADD |
| 漏 `SRem` | 注册表残留幽灵 bizId | ② 惰性修剪：`deadline < now` 时批量 `SRem`，每 30 秒收敛一次 |
| meta 因 TTL 到期消失 | 同上（幽灵） | 同上，同样靠 deadline 到点剔除。周期轮次推进后旧轮 bizId 也走这条路径 |

**Redis 被整体清空**：注册表与 meta 同时消失（两者都在同一 Redis），`registryBootstrapped` 已为 true 故不再 SCAN，但 `syncFromMongo` 会从 MongoDB 重建全部内存服务，每个都经 `NewService` → `SaveActivityTimes` → SADD，注册表自动重建。**此场景下不需要 SCAN**——因为「Redis 有、Mongo 没有」在清空后不可能存在。

**bootstrap 不需要 SetNX 哨兵，也不需要两阶段发布**

每个节点启动时各自 SCAN 一次即可：`SADD` 幂等，N 个节点重复 bootstrap 的结果完全一致。这样去掉了原方案的两个隐患：

| 原方案 | 问题 | 本方案 |
|---|---|---|
| `SetNX rank:{registry_ready}` 哨兵 | 节点若在补种中途硬崩溃，哨兵永久留在 Redis，此后**没有任何节点会再补种**，注册表保持为空 → `syncFromRedis` 静默失效（不报错，只是再也发现不了服务） | 无哨兵，靠进程内 `registryBootstrapped` 标志；每个节点各自补种，无单点 |
| 两阶段发布 + 存量迁移 | 需要协调发布顺序 | 无。旧代码创建的活动虽未写入注册表，但会被本节点的 `syncFromMongo` 重建时经 `NewService` 自动补 SADD；`bootstrapRegistry` 则覆盖「Redis 有、Mongo 没有」的存量 |
| 两个 key 不同 hash tag | `rank:{active_services}` 与 `rank:{registry_ready}` 落在不同 slot，无原子性（本就不需要） | 只剩一个 key |

**性能对比**

| 方案 | 稳态成本 | Redis 是否阻塞 | 集群正确性 |
|---|---|---|---|
| 现状（`KEYS` 每 30s） | O(全库 key 数) × 2/分钟，**阻塞** | ❌ 阻塞单线程 | ❌ 只命中随机 master |
| SCAN + 降频 10 分钟 | O(全库 key 数) / 600s | ✅ | ✅（需 `ForEachMaster`） |
| **SCAN 一次 + 注册表（本方案）** | **O(活跃服务数) 命令 + O(1) 往返**（`SMEMBERS` + 批量 `SREM`），稳态零扫描 | ✅ | ✅ |

按 100 万 key、1 万活跃服务估算，稳态从约 670 次扫描往返/秒降到 **2 次往返/分钟**（`SMEMBERS` 与偶发 `SREM`），命令数仅 O(活跃服务数)。这才是真正的量级改善——降频只是把常数变小，注册表是把复杂度从 O(全库) 换成 O(活跃)。

**注册表规模**

成员是 `{bizId}:{deadline}`，含周期轮次的每个轮次。由于轮次推进后旧轮的 meta 保留 2 周（`CleanupLiveData`），注册表大小 ≈ 「2 周内的轮次数 + 一次性活动数」，有界，不会随运行时间无限增长。因为陈旧成员只靠 deadline 本地判断即可剔除，`SMEMBERS` 的全量返回量也被同一上界约束。

| 优点 | 缺点 |
|---|---|
| 稳态成本从 O(全库 key 数) 降到 O(活跃服务数)，彻底消除周期扫描 | 新增 1 个注册表 key（**增量新增**：不改动任何现有 key 的结构或内容，现有 key 与查询结果完全不变） |
| 维护点只有 2 个且都在 `Store` 内，与 meta key 生命周期同函数，不存在漏改路径 | 引入 meta 与注册表的一致性问题，需惰性修剪兜底（每 30s 收敛） |
| 无哨兵、无两阶段发布、无存量迁移 | 惰性修剪需要全量 `SMEMBERS`，大小受上表上界约束；剔除是纯本地过滤，只多发一次批量 `SREM` |
| 修复集群下只命中随机单节点的功能性 bug | `golib` 需新增 `ScanAll`（bootstrap 用，含一次类型断言） |
| 每个节点独立 bootstrap，无单点故障 | bootstrap 仍是全库 SCAN，但只在启动时执行一次 |

---

### 02 · tickAllRobots 对 groups/robots 加内存缓存（TTL ≈ 2s）+ 活跃期 Redis key TTL 设计

**文件**：`service_robot.go`

**问题**

每秒，在抢到 `TryLockRobotTick` 的节点上，每个有机器人的 Service 调用：
- `store.LoadGroups()` — HGetAll
- 每组 `store.LoadRobots(groupID)` — HGetAll
- `rankService.Range(0, -1)` — ZREVRANGE

设 S 个 Service、每 Service 平均 M 组：每秒产生 **S×(1+M+M) 次 Redis 调用**。

**方案**

在 Service 内部对 groups 和 robots 采用 **Cache-Aside** 模式，按 groupID 细粒度回源：

- 内存命中 → 直接返回，不走 Redis
- 内存未命中（首次 / 其他节点新建的分组）→ **立即**回源 Redis，回写缓存后返回
- 本节点写操作（SaveGroup/SaveRobots）→ 同步更新缓存，并刷新对应 Redis key 的 TTL
- TTL（≈ 2s）兜底：定期淘汰长期不访问的分组，防止内存无限增长

**Redis key TTL 分层设计（与既有结算保留策略对齐）**

先摸清现状——现有设计**已经有 TTL，但只在结算时统一设置**：

| key | 活跃期 | 结算后 | 设置者 |
|---|---|---|---|
| `rank:def:{rankCode}` | 无 | 无 | `RegisterRank`（**须先做缺陷 1 的懒恢复才能设 TTL**） |
| `rank:inst:{instanceID}` | 无 | 2 周 | `ExpireInstance`（经 `Service.CleanupLiveData`） |
| `rank:mb:{instanceID}` | 无 | 2 周 | 同上 |
| `rank:seq:{instanceID}` | 无 | 2 周 | 同上 |
| `rank:settled:{instanceID}` | 无 | **2 周（但会被 `RestoreSettled` 抹掉，见缺陷 4）** | `ExpireInstance`，**不是** `SaveSettled`（后者只写 Mongo） |
| `rank:meta:{bizId}` | 无 | 2 周 | `CleanupLiveData`（[store.go:403](../../socialserver/internal/rank/engine/store.go)） |
| `rank:groups:{bizId}` | 无 | 2 周 | 同上 |
| `rank:members:{bizId}` | 无 | 2 周 | 同上 |
| `rank:claims:{bizId}` | 无 | 2 周 | 同上 |
| `rank:robots:{bizId}:{gid}` | 无 | 2 周 | 同上 |
| `rank:robot_infos:{bizId}:{gid}` | 无 | 2 周 | 同上 |
| `rank:mongo_checked:{bizId}` | 10 分钟 | 2 周 | `SetEX` / `CleanupLiveData`（**`SetEX` 会把 2 周压回 10 分钟**，见文首「补充核查」） |
| `rank:member_index:{uid}` | 7 天 | 7 天 | `MemberIndex.Track`（见第 08 条） |

**⚠️ 本表覆盖 13 个数据 key，缺一不可**（`rank:max_score` 是死代码，已从台账剔除，见缺陷 5）。原表只列了 7 个 `CleanupLiveData` 覆盖的 key + `rank:settled` + `rank:member_index`，漏掉 `rank:def` / `rank:inst` / `rank:mb` / `rank:seq`——这 4 个同样在活跃期永不过期，必须一并纳入。完整盘点与恢复路径见文首「[基础原则与全量盘点](#基础原则与全量盘点2026-09-17-补充)」。

**⚠️ 只设 TTL 不够：所有懒加载回填都必须补 TTL（缺陷 3）**

本条把 TTL 设对之后，只要有**一次读发生在 TTL 到期之后**，`LoadGroups` / `GetMember` / `GetAllMembers` / `LoadRobots` / `RestoreSettled` 的 `HSet` 回填就会把 key 重新写成**永久的**（全部代码位置见缺陷 3）。而这些恰好是历史查询的目标活动，所以**不修缺陷 3，本条的 TTL 在最重要的场景下完全失效**。修法与本节 TTL 公式共用同一个助手，见缺陷 3。

`SettledCacheTTL = 14 * 24 * time.Hour`（[common/rank/defines.go:67](../../common/rank/defines.go)）。

**⚠️ 活跃期 TTL 必须与这个 2 周保留期对齐，否则会提前销毁历史数据。**

若按原方案「活动周期 + 1 天 buffer」，时间线会冲突：

```
T        活动结束 → Settle() → CleanupLiveData 设 2 周 TTL（到期 T+14d）
T+1d     ← 本条 TTL 到期，groups/robots 被删除
T+14d    ← 原本的保留期结束
```

T+1d 到 T+14d 之间历史查询读不到分组数据，构成功能性退化。

**修正后的公式：活跃期 TTL = 距活动结束 + 结算保留期**

```go
// activityEnd 必须与第 06 条的 effectiveSettleAt() 取同一个值：
//   activityEnd = GameEndTime > 0 ? GameEndTime : CloseTime
// 两者若不一致，保留期就会算错（GameEndTime < CloseTime 的活动会提前过期）。
activityEnd := s.effectiveSettleAt()
// 绝对过期时刻 = activityEnd + settledDataRetentionTTL
ttl := time.Until(activityEnd) + commonrank.SettledCacheTTL
rdb.Expire(ctx, groupsKey, ttl)
```

这个式子的关键性质是**绝对过期时刻与设置时机无关**：设在 `t0`，过期于 `t0 + (end - t0) + R = end + R`。由此得到两个好处：

1. **只需在活动创建/首次写入时设一次，之后无需刷新** → 不给 tick 热路径增加任何 EXPIRE 调用。这一点很重要：本条优化的目的是降低每秒 Redis 调用，若改成每次 `SaveGroup`/`SaveRobots` 都刷新 TTL，等于把省下的 HGETALL 又用 EXPIRE 还回去。
2. **与 `CleanupLiveData` 方向一致**：活跃期 TTL 的到期时刻恰好是 `end + 2周`，`CleanupLiveData` 之后把它重置为完整 2 周只是略微延后，永不提前。

**⚠️ 这是全文唯一不可逆的改动 —— 必须灰度，不能直接上**

前 13 条里所有改动的最坏结果是"慢"或"多一次读"，错了可以回滚；**只有这一条算错就是删数据，且删掉不可恢复**（2 周保留期内的历史查询直接失效，`bootstrapRegistry` 也扫不回已过期的 key）。因此上线的第一步**不是** `Expire`，而是打日志观察：

```go
// 阶段一（只观察，不生效）：跑满一个完整活动周期
zaplog.LoggerSugar.Infof("rank ttl plan bizId=%s activityEnd=%d ttl=%s expireAt=%d",
    bizId, activityEnd, ttl, time.Now().Add(ttl).UnixMilli())
```

验收标准：对**已结束**的活动，日志里的 `expireAt` 必须等于 `activityEnd + SettledCacheTTL`，且不早于 `CleanupLiveData` 设的时刻。确认无误后再打开开关。可选加一层保护：`ttl <= 0` 时跳过 `Expire`（此时 key 本应已过期，不设比误删安全——`Expire` 收到非正数会**立即删除** key）。

**`rank:meta` 必须用同一个 TTL 一起过期（否则出现「空心服务」）**

`syncFromRedis` 通过注册表（见第 01 条）发现服务，而注册表成员正是 bizId。若只给 groups/robots 设 TTL 而 meta 不设，活动结束 2 周后会出现「meta 存在、分组数据已空」的状态：`syncFromRedis` 据此注册一个没有分组数据的服务，`tryRecoverPeriodicFromRedis` 还会尝试从中降级恢复周期轮次。因此**同一 bizId 下的全部 key 共享同一个 TTL，一起过期**；meta 过期后注册表成员由第 01 条的惰性修剪移除。

meta 一起过期是安全的：MongoDB 才是权威数据源，`syncFromMongo` 可重建服务定义；`syncFromRedis` 只承担「崩溃且未落盘」的恢复，而那种场景下活动刚结束不久，TTL 远未到期。

**常驻活动（无固定结束时间）**

无法计算自然结束点。原方案的「30 天滑动刷新」会给热路径带来持续开销，两种选择：

| 选择 | 说明 |
|---|---|
| **不设 TTL**（推荐） | 常驻活动按定义应长期存活，终止依赖 `DeleteAllByBizId`（已有显式删除）。放弃「防无限增长」这层兜底，换取热路径零开销 |
| 30 天滑动刷新 | 刷新点必须放在已有的 `syncLoop`（30s 一次，非热路径），**不能**放在 `SaveGroup`/`SaveRobots` |

若选滑动方案，务必确认刷新点不在 tick 路径上，否则每秒一次的 EXPIRE 会抵消本条优化的收益。

**必改-2 / 必改-3：缓存必须是「整份分组列表 + 2 秒有效期」**

`tickAllRobots` 的真实形状（[service_robot.go:108-137](../../socialserver/internal/rank/engine/service_robot.go)）是**先拿整份分组列表筛出 `targets`，再逐组 `LoadRobots` + `Range`**。因此缓存要暴露的是列表，而不是单分组查询：

```go
const tickGroupsCacheTTL = 2 * time.Second

// tickGroups 返回本轮需要 tick 的分组列表。
// 只服务 tickAllRobots —— UpsertScore 路径不得使用（见下「缓存范围说明」）。
func (s *Service) tickGroups() ([]*Group, bool) {
    now := time.Now()
    s.cacheMu.RLock()
    // 必改-2：必须判断有效期。原 sketch 只判 groupsCache != nil，
    // 导致缓存一旦填充便永不过期 —— 该节点永远看不到其他节点新建的分组。
    if s.groupsCache != nil && now.Before(s.groupsCacheExpiry) {
        groups := s.groupsCache
        s.cacheMu.RUnlock()
        return groups, true
    }
    s.cacheMu.RUnlock()

    groups, err := s.store.LoadGroups()
    if err != nil {
        return nil, false // 调用方回退到 s.groups（与现状一致）
    }
    s.cacheMu.Lock()
    s.groupsCache = groups
    s.groupsCacheExpiry = now.Add(tickGroupsCacheTTL)
    s.cacheMu.Unlock()
    return groups, true
}
```

**为什么有效期判断（必改-2）不能省**

本节下方「多节点分析」的原论证是「持锁期间其他节点创建新分组 → 新分组没有机器人 → 2s 内 tick 不到无影响」。这个论证默认了 **2s 后一定能看到**——而原 sketch 的实现里缓存永不过期，所以是**永远看不到**。

而「新分组没有机器人」这个前提也不总是成立：分组由 `ensureGroupLocked`（[group.go:16-51](../../socialserver/internal/rank/engine/group.go)）在任何节点的 `UpsertScore` 上创建，机器人则由首位真实玩家入组时生成（[service.go:404](../../socialserver/internal/rank/engine/service.go) 附近的 `needSpawnRobots` 分支）。**其他节点建组并生成机器人后，本节点若缓存不过期，该组在整个活动期间都不会被 tick** —— 机器人积分静止，榜单失真。

**缓存 TTL 的量化：稳态是 ~2× 而不是「接近 0」**

原优点表写「Redis ops 从 O(S×M)/s 降至接近 0（缓存命中时）」，这与 `TTL = 2s`、tick 间隔 = 1s 的取值不符：每个 key 每 2 秒至少回源一次，所以每个 key 的读取量是 **1/2 而不是 0**。逐项拆开：

| 调用 | 现状 | TTL=2s 缓存后 | 降幅 |
|---|---|---|---|
| `LoadGroups()` | 1/s | 0.5/s | 2× |
| `LoadRobots(groupID)` × M | M/s | M/2/s | 2× |
| `Range(instanceID, 0, -1)` × M | M/s | **M/s（未覆盖）** | **无** |

所以本条的净收益是 **~2×，而且只作用于前两项**（见下节）。

**为什么不能靠「延长 TTL」来逼近 0**

`tickRobotScore`（[robot.go:128](../../socialserver/internal/rank/engine/robot.go)）**不是时间的纯函数**——它是有状态的逐 tick 随机游走，依赖 `robot.Score`、`LastGrowAt`、`PendingScore`、`PendingStartAt`，并按 `GrowTokenCdMs` / `OvertakeIntervalMs` 决定这一步是否增长。喂给它越陈旧的状态，越会算错增长节拍。因此：

- 机器人状态的陈旧窗口**不能由 TTL 控制**，正确做法是靠 Cache-Aside 提升：本节点 `tickGroupRobots` 写回后同步更新自己的缓存（持锁期间唯一的写者就是自己，缓存天然准确）；
- TTL 只用来覆盖「锁在节点间切换」这一场景（A 缓存 → B 持锁改写 → 2s 内 A 取回锁）。窗口本来就 ≤ 一个 tick 的量级，2s 是合适的；
- **把 TTL 拉长到 10s+ 会让 A 用 10 秒前的状态去推 10 秒的增长，且随机序列被压缩**，机器人积分曲线随之改变。

**第 02 条遗漏的最大一项：`Range(0, -1)`**

`tickGroupRobots` 每组每秒做一次**全量 ZSET 读**（[service_robot.go:143](../../socialserver/internal/rank/engine/service_robot.go)），却只用其中两个标量：

```go
allSnapshots, err := s.rankService.Range(ctx, instanceID, 0, -1)  // ← 整组读回
firstScore := allSnapshots[0].Score                              // ① 榜一分数
// ② 循环找第一个非机器人成员的分数 → realFirstScore
```

它是上表三个 M 项里的两项之一，且缓存方案（只覆盖 groups + robots）对它无效。两个可行方向，择一即可：

| 方向 | 做法 | 代价 |
|---|---|---|
| **缩小读量** | `firstScore` 改用 `ZREVRANGE key 0 0`；`realFirstScore` 用 `ZREVRANGEBYSCORE` 分页找首个非机器人成员 | `realFirstScore` 最坏要翻过组内全部机器人，需给分页加上限 |
| **减少读次** | 把 `firstScore` / `realFirstScore` 也纳入同一套 2s Cache-Aside（它们只用于 `calcGrowTarget` 的万分比 clamp，2s 陈旧只影响目标分的细微扰动） | 与 robots 缓存同样的陈旧窗口 |

> 补充一个有助于定量的边界：`ensureGroupLocked` 只在 `group.totalCount() < s.config.RankPeopleNum` 时复用分组（[group.go:27](../../socialserver/internal/rank/engine/group.go)），所以**单个分组的 ZSET 大小有上界 = `RankPeopleNum` + 组内机器人数**。`Range` 的返回量因此有界，但「有界」不等于「免费」——它仍是 S×M 次全量读。这一点应写进文档而不是留白。

**缓存范围说明**

此缓存**仅加在 `tickAllRobots` 路径**，不加在 `ensureGroupLocked`（UpsertScore 路径）上。

`ensureGroupLocked` 是多节点并发写路径，节点 B 在分配新成员时必须实时读 Redis 才能感知节点 A 刚建的分组，加缓存会导致重复分组。分组在节点间的同步依赖 Redis 本身：任何节点的下一次 `ensureGroupLocked` 直接从 Redis 读，自然感知新分组。

**多节点分析**

`TryLockRobotTick` 是 Redis SetNX 分布锁，key 形如 `rank:robot_tick:{bizId}:{nowMs/1000}`（[store.go:626-637](../../socialserver/internal/rank/engine/store.go)）——**锁粒度是「每服务每秒一把」，TTL 为 3 秒**（不是原稿写的 1s；TTL 只用于兜底清理，互斥性由 key 里的秒数保证）。因此同一秒内只有一个节点执行 `tickAllRobots`：

| 场景 | 行为 |
|---|---|
| 持锁节点读自己的缓存 | 缓存由自己的 `SaveRobots` 写回维护（Cache-Aside 提升），实时准确 |
| 持锁节点切换（A → B） | B 缓存为空或已过期，首次 tick 回源 Redis，完全正确 |
| 持锁期间其他节点创建新分组 | 依赖 2s 有效期后回源才能看到 —— **有效期判断不可省（必改-2）** |
| 其他节点在本节点两次 tick 之间改写机器人 | 锁已换手，本节点缓存陈旧 ≤ 2s；由 TTL 兜底恢复 |

**⚠️ 分布锁是 fail-open 的，不能当成强保证**

`TryLockRobotTick` 与 `TryLockSettle` 在 Redis 出错时**都返回 `true`**（[store.go:634](../../socialserver/internal/rank/engine/store.go)、[store.go:618](../../socialserver/internal/rank/engine/store.go)，注释即「treat as unlocked on error so robots still progress」）。所以「任意时刻只有一个节点执行」**只在 Redis 健康时成立**；Redis 抖动期间所有节点会同时 tick。

这一取舍本身是合理的——不退让就是整个榜单停更。但要在文档里写清它带来的是什么：Redis 抖动窗口内机器人积分会被多节点重复推进（结果仍被 `BatchUpsertScore` 覆盖为各自算出的值，不会翻倍，但曲线会抖）。缓存不改变这个性质，只是让每个节点用各自的内存快照去算。

| 优点 | 缺点 |
|---|---|
| `LoadGroups` 与 `LoadRobots` 的读取量各降 ~2×（TTL=2s / tick=1s） | 持锁节点切换时新节点首次 tick 仍走 Redis（正确行为） |
| 多节点安全：分布锁（健康时）保证单节点执行，缓存不跨节点共享 | 新建分组在 ≤2s 内对 robot tick 不可见 |
| 机器人状态陈旧窗口由 Cache-Aside 写回压到最小，不依赖 TTL | **`Range(0,-1)` 未被覆盖，仍是主要成本**（见上节） |
| — | 缓存 TTL 不能随意拉长：`tickRobotScore` 是有状态随机游走，喂陈旧状态会算错增长节拍 |

---

### 03 · ensureGroupInstance 加带有效期的内存标记，跳过冗余 Redis GET

**文件**：`group.go`，`service.go`

**问题**

每次 `UpsertScore` 都调用 `ensureGroupInstance`，后者做一次 Redis `GetInstance`。实例创建后这个 GET 是纯冗余的，却发生在最热的写路径上。

**实例 key 的生命周期**

| 操作 | 语义 | 调用方 | Service 是否还在内存 |
|---|---|---|---|
| `CloseInstance` | 封榜（seal），**key 保留** | `Settle()` | ✅ 在 |
| `DeleteInstance` | 删除 4 个关联 key | `Cleanup()` | ❌ 已从 map 摘除 / 从未加载 |
| `ExpireInstance` | 设置 TTL | `CleanupLiveData()` | ✅ **仍在内存** |

**核心风险：实例被删除/过期后，内存标记必须同步失效**

| 路径 | 场景 | 处理 |
|---|---|---|
| `RemoveService` → `Cleanup()` | Service 同时从 `m.services`/`m.engineServices` 摘除 | ✅ 标记随 Service 一起 GC，无需处理 |
| `forceCleanupOrphan` | Service 从不在内存 | ✅ 无标记 |
| `CleanupLiveData()` | 周期排行榜历史轮次，实例 key 2 周后 TTL 过期，**Service 仍在内存** | ⚠️ 需显式清理标记 |
| Redis 清空 | — | ⚠️ 需自动自愈 |

**方案：标记带验证时间戳，到期回源确认（与 02 同款 Cache-Aside 思路）**

不使用永久 boolean 标记，而是记录上次向 Redis 确认的时间，超过有效期后回源重新确认：

```go
type groupInstanceState struct {
    instanceOK bool
    verifiedAt int64 // 上次向 Redis 确认的时间
}

const instanceVerifyInterval = 60 // 秒

func (s *Service) ensureGroupInstance(ctx context.Context, instanceID string, groupID int32, now int64) error {
    // 1. 内存确认过且在有效期内 → 跳过
    if st, ok := s.instanceStates[instanceID]; ok &&
        st.instanceOK && now-st.verifiedAt < instanceVerifyInterval {
        return nil
    }
    // 2. 回源 Redis 重新确认（首次 / 已过期）
    inst, err := s.rankService.GetInstance(ctx, instanceID)
    if err == nil {
        s.instanceStates[instanceID] = groupInstanceState{instanceOK: true, verifiedAt: now}
        _ = s.store.SaveRankInst(groupID, *inst)
        return nil
    }
    if err != rank.ErrInstanceNotFound {
        return err
    }
    // 3. 确实不存在 → 创建
    newInst := rank.RankInstance{ /* ... */ }
    if openErr := s.rankService.OpenInstance(ctx, newInst); openErr != nil {
        return openErr
    }
    _ = s.store.SaveRankInst(groupID, newInst)
    s.instanceStates[instanceID] = groupInstanceState{instanceOK: true, verifiedAt: now}
    return nil
}
```

再叠加显式清理，让可控制的路径立即生效、不必等 60s：

```go
// CleanupLiveData / Cleanup 中，处理完 ExpireInstance 后
delete(s.instanceStates, instanceID)
```

**为什么用有效期而非永久标记 + 穷举删除路径**：不需要穷举所有删除路径（`CleanupLiveData` 的 TTL 过期、Redis 清空、以及未来新增的删除逻辑），60s 内自动自愈。

| 方案 | 每写一次分的 GetInstance 调用 |
|---|---|
| 现状 | 1 次（每次都调） |
| 永久标记 | 0 次，但需穷举所有删除路径，有残留风险 |
| **有效期标记（推荐）** | 平均 1/60 次，且自动自愈 |

**多节点分析**

`instanceStates` 是**正向缓存**（只记录"已确认存在"），不产生误判：
- 标记有效：本节点已确认该实例存在，跳过 GET 安全
- 标记无效/不存在：回源 Redis 确认（与现在完全一致）
- 其他节点创建的实例：本节点首次访问时回源 Redis，查到后写入标记

| 优点 | 缺点 |
|---|---|
| 每分组平均 60s 才进一次 Redis，写分路径基本无 RTT | 标记过期时仍有一次 GetInstance（可接受） |
| 删除/过期/Redis 清空均可在 60s 内自愈，无需穷举路径 | 实例删除后最多 60s 内可能误判为存在（`CleanupLiveData` 场景已叠加显式清理消除） |
| 多节点安全：正向缓存不产生误判 | — |

---

### 04 · tickServices 改用全局协程池并发执行

**文件**：`manager.go`

**问题**

`tickServices` 串行遍历所有 Service 的 `Tick()`，每次含 Redis RTT。20 个 Service × 5ms/次 = 100ms 累计延迟，最后几个 Service 的结算检测被推迟。

**方案：接入第 00 条的全局池（`SubmitWait`）**

不再自建临时 worker，直接复用全局池。tick 属不可丢弃任务，用 `SubmitWait`；调用方持有 `sync.WaitGroup` 控制批次完成语义：

```go
func (m *Manager) tickServices(ctx context.Context, now int64) {
    // 注意：现网没有 m.snapshotServices()，这个快照循环目前内联在 tickServices 里
    // （manager.go:125-130）。要么先把它抽成 m.snapshotServices()，要么按现状内联展开。
    m.mu.RLock()
    svcs := make([]RankBizService, 0, len(m.services))
    for _, svc := range m.services {
        svcs = append(svcs, svc)
    }
    m.mu.RUnlock()
    if len(svcs) == 0 {
        return
    }

    var wg sync.WaitGroup
    for _, svc := range svcs {
        s := svc
        wg.Add(1)
        if err := taskpool.Global.SubmitWait(ctx, func() {
            defer wg.Done()
            if err := s.Tick(ctx, now); err != nil {
                zaplog.LoggerSugar.Errorf("rank tick err: %v", err)
            }
        }); err != nil {
            wg.Done() // 提交失败必须配平，否则 wg.Wait() 永久阻塞
            zaplog.LoggerSugar.Errorf("rank tick submit failed: %v", err)
        }
    }
    wg.Wait()

    m.tickPeriodicActivities(ctx, now) // 仍在全部 Tick 完成后执行
}
```

**为什么不自建临时池**

| 维度 | 自建临时 worker（每 tick 8 个 goroutine） | 全局池（第 00 条） |
|---|---|---|
| goroutine 数量 | 恒定 8，但有创建/销毁 churn | 恒定，常驻无 churn |
| panic 处理 | 需在调用点重复实现 | 池内统一兜底 |
| 容量管理 | 每处独立调参 | 全局统一 |
| 关闭语义 | 散落，易遗漏 | 随 `server.OnClose` 统一 drain |
| 与 05 的一致性 | 各写各的 | 与 warmUpAllServices 共用同一套机制 |

自建版本没有错，但既然要引入全局池（第 00 条），tick 直接接入即可，避免两套机制并存。

**两处实现细节需注意**

- **`m.snapshotServices()` 目前不存在**。快照循环内联在 `tickServices` 里（[manager.go:125-130](../../socialserver/internal/rank/manager.go)），上面 sketch 已按现状展开；若要抽成方法，`Item 05` 的 `warmUpAllServices` 是另一份**不同**的快照（遍历 `m.engineServices` 而非 `m.services`，元素类型是 `*engine.Service`），两者不能共用一个泛型快照函数。
- **`tickServices` 遍历的是 `m.services`（`RankBizService` 包装）**，而 `warmUpAllServices` 遍历 `m.engineServices`。池只负责执行，这个差异与池无关，但改造时容易看混。
- **并发度不等于 Service 数**：`Tick` 内部会走 `s.mu`、`LoadGroups`、`Range`。并发提交后同一 Service 的多个 tick 不会并发（批次内每个 Service 只提交一次），但不同 Service 会同时打 Redis —— **这才是接入池真正的收益**，也意味着 Redis 连接池大小要与 worker 数（32）匹配，否则收益会被连接等待吃掉。

| 优点 | 缺点 |
|---|---|
| tick 总耗时从 O(S) 降至 O(S/32)，结算时效性显著改善 | 依赖第 00 条先落地 |
| goroutine 数量全局有界，与 Service 数解耦 | 与 WarmUp 共享 worker，WarmUp 洪峰时 tick 会排队（`SubmitWait` 保证不被丢弃） |
| panic 兜底、关闭语义与其他任务统一 | 并发打 Redis，需确认连接池容量 ≥ worker 数 |
| 常驻循环的批次提交须用 `SubmitWaitTimeout`（见第 00 条），否则 ticker 静默丢 tick | 池内嵌套，需维持 worker 数 ≫ 并发批次数 |

---

### 05 · warmUpAllServices 改用全局协程池

**文件**：`manager.go`，`periodic/handler.go`

**问题**

`warmUpAllServices` 为每个 Service 启动一个 goroutine，100 个 Service 同时调用 `ensureLoaded`，每个触发 Redis HGetAll + MongoDB 查询，瞬时并发远超连接池容量。

同一问题还散落在另外两处：
- `manager.go:804` `go localNewSvc.WarmUp(context.Background())`
- `periodic/handler.go:257` `go svc.WarmUp` + 局部 `warmupSem`

三处各自为政，无全局约束。

**方案：与第 04 条同款做法，接入同一个全局池**

批量场景用 `SubmitWait` + 调用方 `sync.WaitGroup`（与 04 完全一致的模式）：

```go
// warmUpAllServices —— 与 tickServices 同构
var wg sync.WaitGroup
for _, svc := range svcs {
    s := svc
    wg.Add(1)
    if err := taskpool.Global.SubmitWait(ctx, func() {
        defer wg.Done()
        s.WarmUp(ctx)
    }); err != nil {
        wg.Done() // 提交失败必须配平，否则 wg.Wait() 永久阻塞
        zaplog.LoggerSugar.Warnf("rank warmup submit failed: %v", err)
    }
}
wg.Wait()
```

单点场景用非阻塞 `Submit`（WarmUp 可丢弃，丢掉的由后续懒加载兜底）：

```go
// manager.go:804 / periodic/handler.go:257 —— 替换裸 go
_ = taskpool.Global.Submit(func() { localNewSvc.WarmUp(context.Background()) })
```

`periodic/handler.go` 的局部 `warmupSem` 可以删除，由全局池统一约束。

**与 04 的唯一差异是提交方式**：tick 走 `SubmitWait`（不可丢弃），WarmUp 的单点提交走 `Submit`（池饱和时主动丢弃，保护 tick）。批量 WarmUp 因为要 `wg.Wait()` 汇总，仍用 `SubmitWait`。

| 优点 | 缺点 |
|---|---|
| 启动/轮次切换时 Redis/MongoDB 压力平滑 | 依赖第 00 条先落地 |
| 三处散落 goroutine 收敛为一个受控入口，局部 `warmupSem` 可删 | 与 tick 共享 worker，WarmUp 洪峰会占用较多 worker |
| 与 04 共用同一套池实现与关闭语义 | 单点 WarmUp 可能被拒绝（可接受：懒加载会兜底） |

---

## 🟡 中优先级

### 06 · Tick/Settle 增加结算短路（配置变更自动失效）

**文件**：`service.go`

**问题：先修正两处与原稿不符的事实**

原稿的两个论断经核对不成立，方案因此需要换掉。

**❌ 论断一：「`IsSettled()` 在每次 tick（每秒）都被调用」——不成立。**

`Tick()` 从不调用 `IsSettled()`（[service.go:340-378](../../socialserver/internal/rank/engine/service.go)）。真正的每秒开销来自 `Tick` 与 `Settle` **各自**的一次 `LoadGroups()`：

```go
// Tick：仅在 now >= settleAt 时执行（活动结束后）
if now < settleAt { return nil }        // 活动进行中：不读 groups
groups, err := s.store.LoadGroups()     // ← ①活动结束后每秒一次 HGETALL

// Settle：无条件读，即使所有分组已结算
groups, err := s.store.LoadGroups()     // ← ②每次都读，且全部已结算时完全浪费
```

即**活动结束后仍在 tick 的 Service，每秒 2 次 HGETALL**，而第 ② 次（`Settle`）在所有分组已结算时是纯浪费。这个状态会持续到 2 周保留期结束，是主要成本。

`IsSettled()` 的热调用点不是 tick，而是**每次查询**：`ResolveEngineService`（[manager_periodic.go:242](../../socialserver/internal/rank/manager_periodic.go)）以及 `ListServices`（[manager.go:373](../../socialserver/internal/rank/manager.go)）、GM 查询（[handler/rank.go:501](../../socialserver/internal/handler/rank.go)）。每次都要 `LoadGroups` + 两次加锁 + 遍历分组。

**❌ 论断二：「已结算的活动被重新开放——当前不存在这条路径」——不成立，这条路径存在。**

链路完整可走通：

| 步骤 | 位置 | 行为 |
|---|---|---|
| 1 | [handler/rank.go:510](../../socialserver/internal/handler/rank.go) `handleUpdateRankConfig` | GM 更新配置，把 `CloseTime` 推到未来 |
| 2 | [manager.go:266](../../socialserver/internal/rank/manager.go) `UpdateService` → `svc.UpdateConfig(cfg)` | **无任何已结算校验**，纯字段合并（[service.go:879-897](../../socialserver/internal/rank/engine/service.go)） |
| 3 | `canUpdateScore` | `now < CloseTime` → 重新返回 `InstanceStateOpen` |
| 4 | `UpsertScore` | 新玩家写入被接受（[service.go:382-389](../../socialserver/internal/rank/engine/service.go)） |
| 5 | `ensureGroupLocked` | 已结算分组不再复用（只挑 `GroupStateOpen`），**新建一个 Open 分组**（[group.go:26-51](../../socialserver/internal/rank/engine/group.go)） |
| 6 | `IsSettled()` | 内存里同时有已结算的旧分组和新 Open 分组 → 重新返回 **false** |

**因此 sticky `atomic.Bool` 会产生真实 bug**：标志一旦为 `true` 就永久短路，步骤 5 新建的分组从此不再被 tick/结算——机器人不增长，到点不结算，该分组永远停在 Open，其中玩家的奖励永久丢失。

**方案：记录「已按哪个 settleAt 完成结算」，而不是「是否已结算」**

把标志从 bool 换成 int64，存**观察到的 `settleAt`**：

```go
// settledAt 记录本节点已完成结算的 settleAt 值；0 表示未结算。
// 用 settleAt 而非 bool：GM 改配置把 settleAt 推后时自动失效，无需显式重置。
settledAt atomic.Int64

func (s *Service) effectiveSettleAt() int64 {
    if s.config.GameEndTime > 0 {
        return s.config.GameEndTime
    }
    return s.config.CloseTime
}

func (s *Service) Tick(ctx context.Context, now int64) error {
    s.mu.Lock(); s.ensureLoaded(); s.mu.Unlock()

    settleAt := s.effectiveSettleAt()
    if s.settledAt.Load() == settleAt {
        return nil   // 已按当前 settleAt 结算完毕，跳过以下全部 IO
    }

    if s.config.hasRobots() && now < settleAt {
        if s.store.TryLockRobotTick(now) { s.tickAllRobots(ctx, now) }
    }
    if now < settleAt { return nil }

    groups, err := s.store.LoadGroups()
    // ...原有 CloseInstance 循环...
    _, err = s.Settle(ctx)
    return err
}

// Settle 短路放在 LoadGroups 之前，消除已结算时的冗余 HGETALL
func (s *Service) Settle(ctx context.Context) (map[int32][]rank.RankMemberSnapshot, error) {
    settleAt := s.effectiveSettleAt()
    if s.settledAt.Load() == settleAt {
        return nil, nil
    }
    groups, err := s.store.LoadGroups()
    // ...原有逻辑：逐分组 TryLockSettle + CloseInstance + SettleInstance...
    //          抢锁失败的分组 continue，不作为失败退出

    // 置位必须同时满足两个条件，缺一不可（见下节「必改-1」与「必改-4」）：
    //   ① 活动已对写入关闭 —— 否则新分组还可能产生；
    //   ② 放在循环之后而非成功分支内 —— 否则抢锁失败的节点永远置不上位，
    //      短路只在 1/N 个节点生效。
    if s.canUpdateScore(time.Now().UnixMilli()) != rank.InstanceStateOpen {
        s.settledAt.Store(settleAt)
    }
    return results, nil
}

// IsSettled 命中短路时无需 LoadGroups，直接 true
func (s *Service) IsSettled() bool {
    // 短路的可信前提同样是「活动已对写入关闭」
    if s.canUpdateScore(time.Now().UnixMilli()) != rank.InstanceStateOpen &&
        s.settledAt.Load() == s.effectiveSettleAt() {
        return true
    }
    // ...原有 LoadGroups + 分组扫描逻辑...
}
```

**必改-1：原稿的「结算之后不可能再有新分组产生」不成立，短路必须自己加闸门**

原稿的论证是单向的——它只证明了「活动结束后写入被拒」，却把结论推广成了「结算之后不可能有新分组」。代码里有两个反例：

| 反例 | 代码 | 说明 |
|---|---|---|
| **① 时间比较方向不一致** | [`canUpdateScore`](../../socialserver/internal/rank/engine/service.go) 用 `now > s.config.GameEndTime`（**严格大于**，[service.go:333](../../socialserver/internal/rank/engine/service.go)）；`Tick` 用 `now >= settleAt`（[service.go:356](../../socialserver/internal/rank/engine/service.go)） | `GameEndTime` 是**独立于 `CloseTime` 的 GM 字段**（[rank.go:456](../../socialserver/internal/handler/rank.go)、`effectiveGameEndTime` 仅在 `gameEndTime == 0` 时才退化为 `CloseTime`）。当 `0 < GameEndTime < CloseTime` 时，`now == GameEndTime` 这一毫秒内 `Tick` 已结算，而 `UpsertScore` 仍被接受 → `ensureGroupLocked` 新建 Open 分组。窗口仅 1ms，但 `UpsertScore` 是高并发热路径，长期必然命中 |
| **② GM 提前手动结算** | [handler/rank.go:264](../../socialserver/internal/handler/rank.go) 直接调 `svc.Settle(ctx)`，**无任何时间校验**。该路径的**唯一调用方是 GM 后台**：`CallSettle` 在 gameserver 内只被 `internal/router/http/internal/gm/rank.go:243` 调用一次，即 GM 的「结算」按钮 | GM 在 `now < settleAt` 时结算，`settledAt` 置位；此后 `canUpdateScore` 仍是 `Open`，玩家继续进榜并**继续创建新分组** |

两者后果相同且不可逆：`Tick` 每次都在 `settledAt.Load() == settleAt` 处返回 → 新分组永远停在 `GroupStateOpen` → 永不 `CloseInstance`、永不 `SettleInstance` → **该分组玩家的成绩不进任何快照，奖励永久丢失**。更隐蔽的是 `IsSettled()` 同步短路返回 true，`ResolveEngineService`（[manager_periodic.go:242](../../socialserver/internal/rank/manager_periodic.go)）于是把查询路由到历史 MongoDB 路径——**玩家连自己参与过这个活动都查不到**。

**修复：把置位条件换成与写入门禁同一个谓词**，而不是假设不变量成立：

```go
if s.canUpdateScore(time.Now().UnixMilli()) != rank.InstanceStateOpen {
    s.settledAt.Store(settleAt)
}
```

这样短路区间恰好等于「活动对写入关闭之后」，与本文要优化的区间（活动结束后仍 tick 的那 2 周）完全重合，**收益不变**。`IsSettled()` 的短路必须用同一条件，否则仍会把活跃分组误判为历史。

> 顺带建议统一 `canUpdateScore` 的 `now > GameEndTime` 与 `Tick` 的 `now >= settleAt`（差一个等号）。但**统一后这道闸门仍要保留**——它能同时挡住 GM 提前结算这条路径，而那是纯时间比较挡不住的。

**必改-4：`settledAt.Store` 必须放在循环之后**

原稿注释写「成功结算后置位」，落点未定。若落进成功分支（即 `TryLockSettle` 为真的分支），则抢锁失败的节点永远置不上位，`Tick` 里那句 `if s.settledAt.Load() == settleAt { return nil }` 只在抢到锁的节点生效——**其余 N-1 个节点每秒 2 次 HGETALL 原样保留**（`Tick` 一次 + `Settle` 一次），收益被摊薄到 1/N。

正确落点是 `for` 循环之后：抢锁失败走 `continue`，循环结束后统一到达置位点。这样每个节点在观察到「已无可结算分组」后都能收敛短路。安全性由 `canUpdateScore` 闸门保证——**只有活动已关闭时才允许置位**，因此「节点 B 在节点 A 结算中途抢先置位」也不会漏掉任何分组。

**为什么 `settleAt` 比较是完备的——不需要任何显式重置**

有效性前提（**不是**原稿写的「结算后不可能有新分组」，见上节「必改-1」）：**置位只发生在 `canUpdateScore != Open` 之后**。在此前提下：

`UpsertScore` 在活动关闭后直接返回 `ErrInstanceClosed`（[service.go:382-389](../../socialserver/internal/rank/engine/service.go)），所以「短路置位后新玩家加入 → 建新分组」这条竞态不存在。

于是唯一能让活动"复活"的路径就是 **GM 改配置把 `settleAt` 推后**——而这恰好使 `settledAt != settleAt`，短路自动失效，无需在 `UpdateConfig` 里写任何重置代码。

| 场景 | `settledAt == settleAt`? | 行为 |
|---|---|---|
| 活动进行中 | 否（`settledAt=0`） | 与现状一致 |
| 已结算、配置未变 | 是 | 短路，跳过 2 次 HGETALL/s |
| 已结算 → GM 推后 `CloseTime` | 否（`settleAt` 变大） | 短路失效，新分组正常结算 ✅ |
| 已结算 → GM 提前 `CloseTime` | 否（`settleAt` 变小） | 短路失效，重走一次幂等结算（无害） |
| **GM 提前手动结算（`now < settleAt`）** | **否**（闸门拦住，不置位） | 与现状一致：`Tick` 在 `now >= settleAt` 时结算后续新建的分组 ✅ |
| **`now == GameEndTime` 边界那 1ms 建的新分组** | **否**（同上，闸门拦住） | 与现状一致 ✅ |
| 进程重启 | 否（`settledAt=0`） | 每个已结算 Service 多一次 `LoadGroups`（幂等，仅一次） |

**`Tick` 与 `Settle` 都必须短路**：`Settle` 有三个调用点——`Tick`（[service.go:376](../../socialserver/internal/rank/engine/service.go)）、GM 手动结算（[handler/rank.go:264](../../socialserver/internal/handler/rank.go)）、轮次推进（[periodic/handler.go:202](../../socialserver/internal/rank/periodic/handler.go)）——只在 `Tick` 里短路会漏掉后两者。

| 优点 | 缺点 |
|---|---|
| 活动结束后的每 Service 每秒 2 次 HGETALL 降为一次原子读，接近零开销 | 进程重启后每个已结算 Service 需多跑一次完整结算（幂等，各一次） |
| `IsSettled()` 在每次查询路径（`ResolveEngineService`）上免掉 `LoadGroups` + 两次加锁 | 对活动进行中的 Service 无收益（`settleAt` 尚未命中，仅多一次原子读） |
| 配置变更自动失效，无显式重置逻辑，无 sticky 标志的复活 bug | `effectiveSettleAt()` / `canUpdateScore()` 读 `s.config` 未持锁（与现有 `Tick` 写法一致） |
| **闸门用 `canUpdateScore` 而非时间比较，同时挡住 GM 提前结算与 1ms 边界两条路径** | 置位落点必须在循环之后，实现时容易放错（见「必改-4」） |
| 纯进程内字段，零迁移成本 | — |

---

### 07 · time.AfterFunc cleanup 纳入 Handler 生命周期管理

**文件**：`periodic/handler.go`

**问题**

`advanceRound` 用 `time.AfterFunc(cycleDelay, svc.CleanupLiveData)` 延迟清理历史轮次数据，timer 未被追踪：
- Server 关闭后 timer 到期，`CleanupLiveData` 调用已关闭的 Redis 连接
- Server 在 timer 到期前重启，清理永远不执行（`CleanupHistoricalRounds` 已有兜底）

**方案**

在 `Handler` 中维护 `timers []*time.Timer`（受 mu 保护），`Clear()` 时 `Stop()` 所有 timer。

| 优点 | 缺点 |
|---|---|
| 优雅关闭时不再对已关闭的 Redis 发 EXPIRE 命令 | 需加 timer 列表的并发管理，逻辑略复杂 |
| 启动时 `CleanupHistoricalRounds` 已做兜底，timer 意义仅为节约一个周期的 Redis 内存 | — |

---

### 08 · MemberIndex 键过期策略（已完成，含清理链路缺陷 (a)–(d3)）

**文件**：`member_index.go`，`manager.go`

**现状核验：TTL 已实现（上一轮改动）**

`MemberIndex` 现在持有 TTL，`Track` 时同步续期：

```go
memberIndexTTL = 7 * 24 * time.Hour   // manager.go:24
memberIndex: NewMemberIndex(rdb, memberIndexTTL)   // manager.go:51

func (idx *MemberIndex) Track(userID int64, entry MemberEntry) {
    if idx.rdb != nil {
        key := rediskeys.GetRankMemberIndexKey(userID)
        idx.rdb.SAdd(key, encodeMemberEntry(entry))
        if idx.ttl > 0 {
            idx.rdb.Expire(key, idx.ttl)
        }
        return
    }
    // ...内存实现（测试用）...
}
```

**⚠️ TTL 不是"滑动"的。** `Track` 只在**入榜**（`UpsertScore` 路径的 6 处 `Track` 调用）和懒重建时被调用，没有任何周期性刷新。因此 7 天是从**最后一次入榜**起算，而不是从查询或 tick 起算。

**原稿「不推荐 TTL」的两条理由经核对均不成立**

| 原稿理由 | 核对结果 |
|---|---|
| 「TTL 会导致活跃用户的历史记录被误清」 | ❌ 用户的**活跃**活动在每次入榜时都刷新 TTL；只有 7 天内无任何入榜行为的条目才过期，而这类条目对唯一的下游用途（GM 查询）价值本就低，且可重建 |
| 「Redis 重启后数据无法恢复」 | ❌ 已有懒重建：`GetMemberEntries` 在 miss 时调 `rebuildMemberIndex`，遍历各 engine service 的内存 `memberGroup` 重建（[manager.go:384-420](../../socialserver/internal/rank/manager.go)）。`memberGroup` 常驻内存（`WarmUp` 时加载一次），重建**不产生任何 Redis/MongoDB 访问**（但代价不止于此，见下文 (d3)） |

TTL 的实际影响面很小：**唯一的生产消费者是 GM 查询**（[handler/rank.go:654](../../socialserver/internal/handler/rank.go) → `GetMemberRankEntries`）。`LookupByBizType` 只被测试引用。所以过期不是数据丢失，只是"下次 GM 查询多一次纯内存扫描"。

**原稿的 ① 和 ② 大部分已经实现，不是新增工作**

| 原稿条目 | 实际情况 |
|---|---|
| ② WarmUp 重建 | **已实现**。`ensureLoaded` 在加载完 `memberGroup` 后，对全部成员回调 `onMemberJoin`（[service.go:56-82](../../socialserver/internal/rank/engine/service.go)），而 `onMemberJoin` 正是 `Track` 的挂载点（[manager.go:172](../../socialserver/internal/rank/manager.go)） |
| ① 活动删除时显式清理 | **已实现**。`RemoveUserEntries` 已存在并被 4 处调用（[manager.go:297](../../socialserver/internal/rank/manager.go)、`324`、`742`、`927`） |

**真正待修的缺陷（这才是本条剩下的工作）**

**(a) `RemoveByKey` 在生产环境是 no-op**

```go
func (idx *MemberIndex) RemoveByKey(key string) {
    if idx.rdb != nil {
        return      // ← 生产环境（rdb != nil）直接返回，什么都没做
    }
    // ...内存实现（测试用）...
}
```

`InitGlobalManager` 强制 `rdb != nil`，所以 `entries` map 分支在生产中是**死代码**。3 处调用点（[manager.go:289](../../socialserver/internal/rank/manager.go)、`733`、`920`）以为清理了索引，实际什么都没发生。

**(b) 清理存在隐式顺序依赖，且没有被封装**

`RemoveUserEntries` 需要先调 `GetAllMembers()` 从 `rank:members` 读出 userID 列表，因此**必须早于 `CleanupAll`**（后者会 `Del` 掉 `rank:members`）。当前正确顺序靠调用方手写维护：

```go
// manager.go:296-299 —— 顺序不能颠倒，否则索引清理静默失效
if members, err := svc.GetAllMembers(); err == nil && len(members) > 0 {
    m.memberIndex.RemoveUserEntries(bizType, actID, members)
}
svc.Cleanup()   // 内部 CleanupAll → Del rank:members
```

**(c) `GetAllMembers()` 失败时静默跳过**

`err == nil && len(members) > 0` 的守卫让 Redis 抖动（读失败）静默退化为"不清理"，且无任何日志。

**(d) `CleanupAll` 不清理 `rank:member_index`**

[store.go:435-446](../../socialserver/internal/rank/engine/store.go) 删除了 meta / groups / members / claims / mongo_checked / robots / robot_infos，独独漏掉 `rank:member_index`——(b) 的根因。它无法自己处理：`Store` 只持有 `bizId`（如 `balloon_1_r2`），而索引条目编码是 `{bizType}:{actID}:{groupID}`（如 `balloon:1:3`），周期轮次的后缀 `_r{N}` 让 bizId 不可逆。

**(d2) `RemoveUserEntries` 是逐条 `SRem`，没有分块也没有 pipeline**

[member_index.go:123-131](../../socialserver/internal/rank/member_index.go) 的循环体里每个成员发一条独立的 `SRem`：

```go
for userID, groupID := range members {
    entry := encodeMemberEntry(MemberEntry{BizType: bizType, ActID: actID, GroupID: groupID})
    idx.rdb.SRem(rediskeys.GetRankMemberIndexKey(userID), entry)   // ← 每人一次往返
}
```

`members` 是**整个活动**的成员表（`GetAllMembers()` 的返回值），不是单个分组，所以这里的规模是活动的全部参与人数。一个 10 万人活动删除时就是 10 万次同步往返，串行执行在清理路径上（`RemoveService` / `syncFromMongo` 的 30 秒循环里，持有 manager 侧的调用栈）。

修法与第 01 条同款：按成员分块（如 500/块）走一次 pipeline，或直接 `Pipelined` 提交全部。注意键是**按 userID 分散**的（`rank:member_index:{userID}`），在 Redis Cluster 下不同 userID 落到不同 slot，所以只能 pipeline、不能 MULTI，且每条命令仍需各自路由——pipeline 在这里省的是 RTT 而不是路由。

**(d3) 懒重建 `rebuildMemberIndex` 既无负缓存，又逐服务抢写锁**

第 08 条把懒重建描述成"纯内存扫描、无 Redis/MongoDB 访问"，这句话对**单次**重建成立，但漏了两项成本（[manager.go:396-420](../../socialserver/internal/rank/manager.go)）：

| 遗漏项 | 实际行为 | 后果 |
|---|---|---|
| 无负缓存 | 遍历全部 `m.engineServices`，对每个服务调 `GetMemberGroupID(userID)`；查不到就继续下一个。**miss 的结果不被记住** | GM 查询一个从未入榜的 userID 时，每次都要把 N 个服务全扫一遍，且下次同样再扫一遍 |
| `GetMemberGroupID` 取的是 `s.mu.Lock()`（**写锁**，不是 RLock） | 重建一个 userID 需要顺序获取 N 把写锁 | 与 `UpsertScore` 的写路径**直接争锁**。GM 批量查 U 个用户 × N 个服务 = U×N 次写锁获取，全部压在打榜热路径上 |

所以第 08 条"重建很便宜"的结论只在"单个用户、偶发 miss"下成立。GM 批量查询（[handler/rank.go:654](../../socialserver/internal/handler/rank.go)）恰恰是"U 个用户 × 一次"的形态，是最坏情况。

推论有两条，**都不属于第 08 条的交付范围，已登记为待办 G**：

- 重建结果需要一个**有界的负缓存**（例如 `map[userID]struct{}` + 短 TTL + 容量上限），否则重复 miss 会反复全扫
- 更根本的是让查询路径不必取写锁：若 `memberGroup` 的读用 `RLock` 即可，则重建与打榜可并行；否则应改成快照式读取（每服务缓存一份只读副本）

这两条都需要改 `engine.Service` 的锁粒度，属于独立改造，不应塞进第 08 条。

**TTL 使 (b)(c)(d) 从正确性问题降级为及时性问题**

这是本次核验最重要的结论：**索引键 7 天后必然过期**，所以任何清理失败最多留下 7 天的陈旧索引条目，之后自动消失，且 GM 查询在 miss 时还能懒重建。清理链路的正确性不再是硬依赖。因此：

- (a) ~ (d) **不阻塞上线**，属于清理质量改进
- **(d2)(d3) 不受 TTL 兜底影响**——它们是性能问题而非正确性问题，TTL 帮不上忙。但它们同样不阻塞上线：规模只在"大活动被删除"和"GM 批量查未入榜用户"两个场景下暴露，属于容量问题，登记为待办 G 与第 01 条一并处理
- 修复目标从"保证清理正确"下调为"避免无谓的 Redis 遍历 + 消除误导性代码"

**方案**

**① 统一清理入口，把顺序固定在一个地方**（对应 (b)(c)(d)，并为 (d2) 的分块改造留出落点）

`RemoveService` 与 `syncFromMongo` 各写一遍清理序列，其中 `syncFromMongo` 还**完全漏掉了 `svc.Cleanup()`**（[manager.go:925-930](../../socialserver/internal/rank/manager.go) 只删内存引用 + 清索引，不清理 Redis/Mongo），导致孤儿实例与文档残留。抽一个共享助手，顺序只写一次：

```go
// cleanupServiceData 按固定顺序清理一个服务的全部痕迹。
// 顺序不可调换：索引清理依赖 rank:members 仍存在。
func (m *Manager) cleanupServiceData(svc *engine.Service, bizType BizType, actID int32) {
    members, err := svc.GetAllMembers()
    switch {
    case err != nil:
        zaplog.LoggerSugar.Warnf("rank: get members for index cleanup bizType=%s actID=%d: %v (索引将由 7 天 TTL 兜底)", bizType, actID, err)
    case len(members) > 0:
        // 注意：RemoveUserEntries 目前是逐条 SRem（见上文 (d2)），
        // members 是整个活动的成员表，大活动下这里是 10 万次串行往返。
        // 修 (d2) 之前，这个助手把它们集中到了一处，但集中本身不解决规模问题。
        m.memberIndex.RemoveUserEntries(bizType, actID, members)
    }
    svc.Cleanup()   // 内部 CleanupAll 会删掉 rank:members，必须在其之前完成索引清理
}
```

**② 删除 `RemoveByKey` 及其 3 处调用点**（对应 (a)）

它既是生产 no-op，又是误导——留着会让后续维护者以为索引已被清理。索引清理已经完全由 `RemoveUserEntries` 承担。

| 优点 | 缺点 |
|---|---|
| 清理顺序固定在一处，不再依赖调用方手写 | `syncFromMongo` 补上 `Cleanup()` 后，孤儿 Redis/Mongo 数据被真正清除（行为变更，需回归验证） |
| `syncFromMongo` 的孤儿数据（Redis 实例 + Mongo 文档）得到清理 | — |
| 删除 3 处误导性的 no-op 调用与死代码 | — |
| TTL 已兜底，本项不阻塞上线 | (d2)(d3) 是性能问题，TTL 帮不上忙，已登记为待办 G |
| 已实现的 ①② 无需重复开发（原稿误判为新工作） | — |

---

### 09 · syncFromMongo 由「删除」改为「修复」

**文件**：`manager.go`

**问题：删除路径无法区分两种成因，且猜错会自我延续**

`syncFromMongo` 每 30 秒把「在 `m.engineServices` 但不在 MongoDB」的服务判定为已删除并清掉（[manager.go:895-930](../../socialserver/internal/rank/manager.go)）。但「不在 MongoDB」有两种完全不同的成因：

| 成因 | 应该做什么 |
|---|---|
| `dao.SaveRankConfig` 的异步写被丢弃（`maxRetries=1`，熔断打开 30s 后任务永久丢失） | **修复**——把内存中的 config 写回 MongoDB |
| GM 在本节点离线期间删除（`subscribeDeleteEvents` 漏了广播） | **删除** |

原稿的「保护窗口」方案（`registeredAt` + 60s）只覆盖了第一种成因的**首次**判定，而且只是把删除**延后 60 秒**——如果写队列持续积压超过 60s，误删照旧发生。它还引入了一个新字段和一个魔法常量。

**更严重的是：猜错的后果会自我延续。** 以 `tryRecoverPeriodicFromRedis` 为例：

```
崩溃于 Mongo 写入落盘前
  → syncFromRedis 从 rank:meta 恢复服务（该路径不写 Mongo，见 manager_periodic.go:31）
  → 30s 后 syncFromMongo 发现 Mongo 无此服务 → 删除
  → 下一次 syncFromRedis 再次恢复 → 再被删除 → 每 30 秒循环一次
```

`tryRecoverPeriodicFromRedis` 的降级恢复能力被完全抵消。`applyRankConfigDoc` 也有同样的形状：它从广播注册服务但**不写 MongoDB**（[manager.go:963-1038](../../socialserver/internal/rank/manager.go)），完全依赖广播源的写入落盘。

**方案：按「活动是否仍在进行」决定修复还是删除**

判别依据用 `cfg` 里已有的数据，不新增任何字段：

```go
for key := range existingKeys {
    if _, inMongo := mongoKeys[key]; inMongo {
        continue
    }
    svc, stillExists := m.engineServices[key]
    if !stillExists {
        continue
    }
    cfg := svc.GetConfig()
    now := time.Now().UnixMilli()

    // 活动仍在进行：MongoDB 缺失只可能是异步写被丢弃 → 修复，绝不删除
    if cfg.CloseTime == 0 || now < cfg.CloseTime {
        zaplog.LoggerSugar.Warnf("rank: config %s missing in mongo, repairing (activity still open)", key)
        if err := m.dao.SaveRankConfig(key, cfg); err != nil {
            zaplog.LoggerSugar.Errorf("rank: repair rank config %s failed: %v", key, err)
        }
        continue
    }

    // 活动已结束且 MongoDB 无记录：视为已删除，走统一清理
    m.cleanupServiceData(svc, BizType(cfg.BizType), cfg.ActID)   // 见第 08 条
    // ...从 services / engineServices 中摘除...
}
```

**为什么这个判据是完备的**

| 场景 | `CloseTime` 状态 | 行为 | 正确性 |
|---|---|---|---|
| 进行中的活动，写被丢弃 | `now < CloseTime` | 修复 | ✅ 活动不被误杀，且修复本身可重试 |
| 进行中的活动，`tryRecoverPeriodicFromRedis` 恢复 | `now < CloseTime` | 修复 | ✅ 终止振荡循环 |
| GM 删除（本节点漏广播）| 视活动状态 | 若已结束 → 删除；若仍在进行 → 先修复，GM 的删除由下一次广播/`syncFromMongo` 兜底 | ✅ |
| 活动超过 7 天（内存管理性剔除，[manager.go:786](../../socialserver/internal/rank/manager.go)）| `now >= CloseTime` | 删除 | ✅ 与原行为一致 |
| 无结束时间的常驻活动 | `CloseTime == 0` | 修复 | ✅ 常驻活动不应被剔除 |

**关键的取舍原则：删除是不可逆的，修复是幂等且自愈的。**

若修复写本身也被队列丢弃，下一轮 `syncFromMongo`（30 秒后）会发现它仍不在 Mongo，于是**再次修复**——天然重试，无需额外重试机制。反之，误删之后没有任何机制能把它恢复回来（除等待启动期的 `bootstrapRegistry` 扫描，而进程不重启就不会再跑，见第 01 条）。

在信息不足以区分两种成因时，选择**可恢复的那一侧**。

| 优点 | 缺点 |
|---|---|
| 根治误删，而非延后 60 秒；写队列长时间积压也不会误杀活动 | 若 GM 真的删除了一个**仍在进行**的活动，本节点要到下次广播或扫描才感知（原方案同样如此） |
| 终止 `tryRecoverPeriodicFromRedis` / `applyRankConfigDoc` 的 30 秒恢复-删除振荡 | 修复会在 Mongo 中重建 GM 已删除的活动文档（需确保 `DeleteService` 的 `rank:delete` 广播可靠；漏广播时靠 `syncFromRedis` 兜底无效，因为该活动已不在 Mongo） |
| 无需新增 `registeredAt` 字段，无魔法常量 | — |
| 复用第 08 条的 `cleanupServiceData`，顺带补上漏掉的 `svc.Cleanup()` | — |

**注意一处副作用**：若 GM 删除了一个**尚未结束**的活动，而某节点漏掉了 `rank:delete` 广播，本方案会把该活动**写回 MongoDB**（复活）。这是「优先可恢复」的必然代价。若要规避，可让 `DeleteService` 在 Redis 中留一个带 TTL 的删除墓碑供 `syncFromMongo` 查询——但那需要新增存储键，与「不改变现有存储结构」的约束冲突，因此本方案接受该副作用，并要求 `rank:delete` 广播的可靠性由重连订阅保障（`subscribeDeleteEvents` 已有重连逻辑）。

---

## 🟢 代码质量

### 10 · ListGroupRank / GetMemberRank 提取 resolveSettled 方法

**文件**：`service.go`

两个方法各含约 30 行相同的"检查内存 settledGroup → 检查 Redis group 状态 → loadAndCacheSettled"逻辑。

提取 `resolveSettled(groupID int32) []rank.RankMemberSnapshot` 私有方法，两处复用。

| 优点 | 缺点 |
|---|---|
| 消除 ~50 行冗余代码，单一修改点 | 抽取时需核对两处加锁位置的细微差异 |

---

### 11 · Settle() 减少 cloneSnapshots 调用次数（3 次 → 1 次）

**文件**：`service.go`

同一次结算中 `members` 被 `cloneSnapshots` 三次，产生三份相同副本。`settledGroup` 和 `SaveSettled` 可共享同一 clone（`SaveSettled` 是异步队列，`json.Marshal` 只读，安全）。

| 优点 | 缺点 |
|---|---|
| 结算时减少 2/3 的 snapshot 内存分配 | 需确认 `SaveSettled` 路径不修改 slice（当前实现安全） |

---

### 12 · findTier / robotAvatarInfo 改为 map 查找

**文件**：`service_robot.go`

`findTier(tierID)` 和 `robotAvatarInfo(robot)` 在每次机器人 tick（每秒×每机器人）时线性扫描配置数组。

在 `UpdateConfig` 时预建 `tierMap map[int32]*RobotTierCfg` 和 `infoMap map[int64]*RobotInfoEntry`，O(1) 替换 O(N) 扫描。

| 优点 | 缺点 |
|---|---|
| O(1) 查找，对大档次配置友好；代码更清晰 | Config 多两个 map，内存增量极小（档次数 < 10） |

---

### 13 · WarmUp 加原子快路径（原稿的 `sync.Once` 方案零收益，已否决）

**文件**：`service.go`

**原稿目标**：`ensureLoaded` 改用 `sync.Once`，首次 load 的 IO 不占 `s.mu`，竞争者不必等在主锁上。

**核对结论（先说结果）**：原稿的 `sync.Once` 方案**零收益，应否决**；两阶段改造的收益真实但需改动全部读路径的加锁边界，风险不匹配，**本轮不做**。本条改采**第三种做法**——`WarmUp` 加一条原子快路径，1 行、零风险，且真能去掉稳态下的无谓写锁获取。理由如下。

**为什么 `sync.Once` 是零收益——14 处调用点全部在持有 `s.mu` 时调用**

```go
// 每一处都是这个形状
s.mu.Lock()
s.ensureLoaded()
```

调用点：[service.go:241](../../socialserver/internal/rank/engine/service.go)（WarmUp）、`342`（Tick）、`392`（UpsertScore）、`479`、`531`、`647`、`713`、`735`、`750`、`757`、`772`、`788`、`816`、`825`、`851`、`860`。

把 `loaded bool` 换成 `sync.Once` 后，IO 依然运行在调用方已持有的 `s.mu` 内——**锁的持有时间不变，收益为 0**。原稿「其他 goroutine 在 `Do` 返回后立即继续，无需持有 `s.mu`」的前提是调用方不持锁，而当前所有调用方都持锁。

**第二个约束：状态安装必须留在锁内**

`ensureLoaded` 填充的 `s.groups`、`s.memberGroup`、`s.nextGroupID` 都受 `s.mu` 保护，并被 `ListGroupRank`、`GetMemberRank`、`IsSettled`、`ListGroups` 等读路径直接访问。把整个函数体塞进 `Once.Do` 会让这些字段在**无锁**下被写，与上述读路径构成数据竞争。因此必须拆成两阶段：

```go
func (s *Service) ensureLoaded() {
    if !s.store.available() {
        return   // 检查必须留在 Once 之外，见下
    }
    // 阶段一：纯 IO，只写 once 胜出者的结果，不碰 s.mu
    s.loadOnce.Do(func() { s.pendingState = s.fetchFromStore() })
    // 阶段二：安装到受 s.mu 保护的字段（调用方已持锁），幂等
    if s.pendingState != nil {
        s.installState(s.pendingState)
        s.pendingState = nil
    }
}
```

**`!s.store.available()` 检查不能省，也不能移入 `Once.Do`**

`available()` 是 `st != nil && st.rdb != nil`（[store.go:49](../../socialserver/internal/rank/engine/store.go)），构造后不可变——所以实际不会出现「先不可用、后可用」的时序，`loaded` 的早退分支在生产中也从不触发（`rdb` 恒非 nil）。但把检查放进 `Once.Do` 会让无 Redis 模式下的服务跑一次空 IO 并把 `Once` 永久置为 done，因此检查留在外面。

**顺带修正一处注释**

`ensureLoaded` 的注释（[service.go:54-55](../../socialserver/internal/rank/engine/service.go)）写着「并在 Redis 被清理后完整恢复」，这不成立：`loaded` 只在 `57`/`60` 两行出现，**没有任何重置点**，置 true 后永不重载。`ensureLoaded` 的「一次性」语义其实**已经等价于 `sync.Once`**——换成 `sync.Once` 不改变任何行为。

真正的 Redis 丢失恢复是按分组进行的 `recoverGroupData`：在检测到 `rank:mb` 中该分组实例缺失时从 MongoDB 重建（[service.go:433](../../socialserver/internal/rank/engine/service.go)、`518`、`592`、`623` 四个触发点）。原稿「`recoverGroupData` 路径不走 `ensureLoaded`，不受影响」这句是对的，但理由与本文不同。

**结论：本条的价值完全取决于是否愿意改 14 处调用点**

| 做法 | 收益 | 工作量 |
|---|---|---|
| 仅替换 `loaded bool` → `sync.Once` | **零**（语义已等价，IO 仍在锁内） | 小 |
| **`WarmUp` 加一条原子快路径**（见下） | 稳态下每秒去掉 N 次无谓的写锁获取 | **极小（1 行）** |
| 两阶段改造 + 14 处调用点改为锁外调用 `ensureLoaded` | 首次 IO 不占主锁 | 中，且触碰全部读路径的加锁区 |
| 不做 | — | — |

**推荐做中间那一行。** 原稿把本条框在「要么全改、要么不做」的二选一里，遗漏了第三种做法——它不需要动任何调用点，因为**有一个调用点是自己持锁的**：

```go
// manager.go:108-122 syncLoop —— 每 30 秒执行一次
m.syncFromMongo(ctx)
m.syncFromRedis(ctx)
m.warmUpAllServices(ctx)   // 遍历 m.engineServices，逐个 svc.WarmUp()
```

```go
// service.go:238-242 —— 改造前
func (s *Service) WarmUp(ctx context.Context) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.ensureLoaded()
}
```

`warmUpAllServices` 是 `WarmUp` 的唯一调用方，而 `WarmUp` 是**唯一自己获取 `s.mu` 的函数**（其余 13 处调用点的加锁由外层负责）。所以只要把早退提到加锁之前，就能在不触碰其他 13 处的前提下消掉稳态开销：

```go
func (s *Service) WarmUp(ctx context.Context) {
    if s.loadedFlag.Load() {   // 原子快路径：稳态下不再进 s.mu
        return
    }
    s.mu.Lock()
    defer s.mu.Unlock()
    s.ensureLoaded()
}
```

`loaded` 需要从 `bool` 升级为 `atomic.Bool`（`ensureLoaded` 内部改为 `s.loadedFlag.Store(true)`）。注意这是**全量替换**，不是与 `bool` 并存——`loaded` 只在 [service.go:57/60](../../socialserver/internal/rank/engine/service.go) 两处出现，改动面仅此一处加 `WarmUp`。

**收益量级要说清楚，不要夸大。** 现状是每服务每 30 秒一次 `s.mu` 写锁获取，抢到锁后只做一次 `if s.loaded { return }`。所以：

- **锁持有时间**：极短（纳秒级），不是瓶颈
- **消除的是"获取"而非"持有"**：省下的是每次 `Lock()` 的原子操作与排队，以及与 `UpsertScore` 的争抢机会
- 规模：N 个服务 × 每 30 秒 1 次 = N/30 次/秒的写锁获取，全部是空操作

对一个每秒被 `UpsertScore` 获取成百上千次的锁来说，这个降幅**不显著**。它的价值在于**成本几乎为零且方向正确**：原稿的 `sync.Once` 连这点收益都拿不到（IO 仍在锁内、语义已等价），而这一行是真的把稳态锁获取降到了 0。

**建议**：做 `WarmUp` 原子快路径（1 行，零风险）；`sync.Once` 与两阶段改造**都不做**——前者零收益，后者要求改动全部读路径的加锁边界，风险与收益不匹配。若要拿"首次 IO 不占主锁"的收益，应作为独立议题单独立项，不与本条混谈。

| 优点（仅 WarmUp 快路径） | 缺点 |
|---|---|
| 1 行改动，不触碰其余 13 处调用点与任何读路径 | 收益量级小（省的是锁获取，不是锁持有） |
| 稳态下 `syncLoop` 不再对每个服务做无谓的写锁获取 | 需要把 `loaded bool` 换成 `atomic.Bool`（全量替换，2 处引用） |
| 不改语义：`ensureLoaded` 的"一次性"语义本就等价于 `sync.Once` | — |
| 若将来做两阶段改造，这条快路径可以保留，不构成障碍 | — |

| 优点（两阶段改造后，暂不推荐） | 缺点 |
|---|---|
| 首次 load 的 IO 不占 `s.mu`，取消 `pendingState` 后语义等价 | 需改动 14 处调用点的加锁结构，回归面覆盖全部读路径 |
| 语义自描述：「只初始化一次」 | 仅替换 bool 则收益为零，属无效改动 |
| 顺带修正 `ensureLoaded` 的错误注释 | `pendingState` 引入一次额外内存持有（短暂） |

---

## 修订记录

### 2026-09-17（补充：两条基础原则的全量盘点）

按「① Redis 只是缓存，所有 key 都必须有 TTL 且必须有恢复路径；② 正常逻辑不得扫库」两条原则对全子系统做了一次全量盘点，新增文首「[基础原则与全量盘点](#基础原则与全量盘点2026-09-17-补充)」一节，并据此修正了第 02 条的台账、新增两个上线前必改。

**盘点结论**

| 项 | 结果 |
|---|---|
| 数据 key 总数 | 14 个（其中 `rank:max_score` 为死代码）→ **13 个在用** |
| 活跃期已有 TTL 的 | 仅 **2 个**：`rank:member_index`（7 天）、`rank:mongo_chk`（10 分钟） |
| 活跃期无 TTL 的 | **11 个**，未走 `Settle`→`CleanupLiveData` 或 `RemoveService` 的活动其 key 永久残留 |
| 恢复路径 | 11 个中 10 个有 Mongo 权威源可恢复；`rank:def` 可由内存 `s.config` 零 IO 重建；`rank:max_score` 无（死代码） |
| 扫库操作 | **全仓仅 1 处**：`manager.go:469` 的 `m.rdb.Keys(...)`，每 30 秒执行，且用的是阻塞式 `KEYS` 而非 `SCAN`。第 01 条已覆盖 |

**新增论断修正（第 02 条台账）**

| 条目 | 原说法 | 核对结果 |
|---|---|---|
| 02 | 台账只列 9 个 key | ❌ 漏 `rank:def` / `rank:inst` / `rank:mb` / `rank:seq`——这 4 个同样活跃期无 TTL，须一并纳入（现表已补全为 13 个） |
| 02 | `rank:settled` 的「设置者 = `SaveSettled`」 | ❌ `SaveSettled` 只写 MongoDB（[store.go:716-721](../../socialserver/internal/rank/engine/store.go)）。Redis 侧的写入者是 `SettleInstance`（[service_redis.go:308](../../common/rank/service_redis.go)，`Set` **无 TTL**），2 周 TTL 实际来自 `ExpireInstance`（[service_redis.go:635](../../common/rank/service_redis.go)） |
| 02 | `rank:settled` 有 2 周 TTL | ⚠️ 只是「半个」：`RestoreSettled`（[store.go:217](../../socialserver/internal/rank/engine/store.go)）用**无 TTL 的 `Set`**，而它恰恰在 key 已不存在时被调用 → 冷恢复把 TTL 抹掉 |
| 02（方案层） | 设好 TTL 即可 | ❌ **最大的漏洞**：`LoadGroups` / `GetMember` / `GetAllMembers` / `LoadRobots` / `RestoreSettled` 的回填全部用 `HSet`/`Set` 且**不带 TTL**（仅 `LoadGroupSettledCached` 用 `SetEX` 是对的）。一次「到期后读取」就会把 key 变永久，而这些正是历史查询的目标活动 → **TTL 在最该生效的场景失效**。此为**必改-5**（缺陷 3） |

**方案变更（本轮补充）**

| 条目 | 变更 |
|---|---|
| 02 | 台账补全为 13 个 key；新增「所有懒加载回填必须补 TTL」的硬要求（缺陷 3），并给出与 TTL 公式共用同一式子的 `backfill` 助手 |
| 02 / 新 | `rank:def` 的 TTL 必须与「`GetRank` miss → 从内存 `s.config` 零 IO 重建」成对落地，否则会周期性出现「活动在但玩家进不去」（`ErrDefinitionNotFound`）→ **必改-6**（缺陷 1） |
| 01 | 明确 bootstrap 的「一次性 + 失败可重试」结构（`m.registryBootstrapped`）是**原则二的落地保证**，不得改回 `syncLoop` 无条件调用；并记录当前用的是阻塞式 `KEYS` 而非 `SCAN`，第 01 条是双重修复 |

### 2026-09-17（首轮）

本轮逐条核对了代码，修正了原稿中若干与实现不符的论断，并按采纳结论重写了方案。

**论断修正（原稿说法不成立）**

| 条目 | 原稿说法 | 核对结果 |
|---|---|---|
| 06 | `IsSettled()` 在每次 tick（每秒）都被调用 | ❌ `Tick` 从不调用 `IsSettled`（[service.go:340-378](../../socialserver/internal/rank/engine/service.go)）。真实开销是 `Tick` 与 `Settle` **各自**一次 `LoadGroups()`；`IsSettled` 的热调用点是每次查询的 `ResolveEngineService` |
| 06 | 「已结算活动被重新开放——当前不存在这条路径」 | ❌ 路径存在且完整：GM 改配置 → `UpdateConfig`（无已结算校验）→ `canUpdateScore` 重新 Open → 新玩家建新分组 → `IsSettled` 重新为 false |
| 08 | 「不推荐 TTL：会误清活跃用户记录、Redis 重启不可恢复」 | ❌ 两条均不成立。索引的唯一生产消费者是 GM 查询，且 miss 时由 `rebuildMemberIndex` 从内存 `memberGroup` 重建（零 Redis/Mongo 访问） |
| 08 | ① 显式清理、② WarmUp 重建为待开发工作 | ❌ 均已实现：`RemoveUserEntries` 已在 4 处调用；`ensureLoaded` 已对全部成员回调 `onMemberJoin`（即 `Track`） |
| 13 | 「`sync.Once` 后其他 goroutine 无需持有 `s.mu`」 | ❌ **14 处调用点全部在持有 `s.mu` 时调用** `ensureLoaded`，仅替换 bool 收益为零 |
| 13 | `ensureLoaded` 注释称「在 Redis 被清理后完整恢复」 | ❌ `loaded` 无重置点，置 true 后永不重载；实际恢复靠按分组的 `recoverGroupData` |
| 01 | 未评估扫描频率 | 补充：`SCAN` 总功仍是 O(全库 key 数)，30s 频率下按 100 万 key / 10 节点约 670 次往返/秒；且 `KEYS`/`SCAN` 在集群下只命中一个随机 master |
| 01 | 「注册 SET 需在 8 处写路径维护」 | ❌ 核对后不成立：meta key 的创建只有 `Store.SaveActivityTimes` 一个入口、删除只有 `Store.CleanupAll` 一个出口，挂这两处即天然同步 |
| 01 | 修剪注册表需对每个成员发 `EXISTS rank:meta:{bizId}` | ❌ 这样命令数就是 O(活跃服务数)，1 万活跃服务下每 30 秒 2 万条，比它取代的扫描还贵。改为把 `deadline` 编进成员（`{bizId}:{deadlineMillis}`），修剪降为纯本地过滤 |

**二次复核新增的论断修正（2026-09-17 复核第 02、06 条方案时发现）**

| 条目 | 原（本文首轮）说法 | 核对结果 |
|---|---|---|
| 02 | 缓存 sketch 中 `groupsCacheExpiry` **只写不读** | ❌ 只判 `groupsCache != nil` 会让缓存**永不过期**。后果是其他节点新建的分组在某节点上**永远不被 tick**——而该节点可能因持锁而长期是唯一 tick 者。已改为 `now.Before(s.groupsCacheExpiry)` |
| 02 | 「Redis ops 从 O(S×M)/s 降至接近 0」 | ⚠️ 过度乐观。`tickGroupsCacheTTL=2s` 而 tick 周期 1s，每个键每 2 秒必被重读一次 → 稳态降幅约 **2×**，不是「接近 0」。已按真实量级改写并给出三行对照表 |
| 02 | 未识别 `Range(ctx, instanceID, 0, -1)` 是本条**最大**的 Redis 成本项 | ❌ 每分组每 tick 一次全量 ZSET 读，与分组大小同阶。分组大小上界是 `RankPeopleNum`（`ensureGroupLocked` 只在 `totalCount() < RankPeopleNum` 时复用），所以是**有界但非小**的常量。已补该节与双向修法 |
| 02 | 缓存 API 形状为按 `groupID` 取值 | ❌ 与真实调用点不符：`tickAllRobots` 需要的是**整份未结算分组列表**，按 groupID 取值落不了地。已改为 `tickGroups() ([]*Group, bool)` |
| 06 | 「结算之后不可能再有新分组产生」（整套短路的论证前提） | ❌ **不成立，且后果最严重**。① `canUpdateScore` 用 `now > GameEndTime`（严格），`Tick` 用 `now >= settleAt`，`0 < GameEndTime < CloseTime` 时存在 1ms 窗口；② `Settle` 唯一外部调用方是 **GM 后台**（[gm/rank.go:243](../../gameserver/internal/router/http/internal/gm/rank.go)），无任何时间闸门。短路后新分组永不结算 → **玩家奖励永久丢失**，且 `IsSettled()` 同步短路使该分组在查询侧**完全不可见**。已改为显式闸门 `canUpdateScore != InstanceStateOpen` |
| 06 | 未定义 `settledAt.Store` 的落点 | ⚠️ 若放在成功分支内，只有抢到 `TryLockSettle` 的 1/N 节点置位，其余节点每秒 2 次 HGETALL 原样保留。已明确「放在分组循环之后」 |
| 00 | `m.tickProcess(...)` | ❌ 该函数不存在，实际是 `m.tickServices(ctx, nowMs)`（[manager.go:124-137](../../socialserver/internal/rank/manager.go)），且它遍历的是 `m.services`（`RankBizService` 包装），不是 `m.engineServices` |
| 04 | `m.snapshotServices()` | ❌ 该方法不存在，快照循环目前**内联**在 `tickServices` 里（[manager.go:125-130](../../socialserver/internal/rank/manager.go)）。已给出「先抽取」与「按现状内联」两种写法 |
| 08 | 懒重建「不产生任何 Redis/MongoDB 访问」＝便宜 | ⚠️ 对单次成立，但漏两项成本：**无负缓存**（miss 不被记住，重复全扫 N 个服务）+ `GetMemberGroupID` 取的是 `s.mu.Lock()`（**写锁**）。GM 批量查 U 用户 × N 服务 = U×N 次写锁，直接压在 `UpsertScore` 热路径上。已补 (d3) |
| 08 | `RemoveUserEntries` 为已实现的清理手段（隐含认为成本可忽略） | ⚠️ 逐条 `SRem`，无分块无 pipeline；`members` 是**整个活动**的成员表，10 万人活动即 10 万次串行往返。已补 (d2) |
| 13 | 「要么全改 14 处调用点，要么不做」的二选一 | ❌ 遗漏第三种：`WarmUp` 是**唯一自己获取 `s.mu`** 的调用点（其余 13 处由外层加锁），只改它即可消掉稳态开销，**1 行、零风险**。已改为推荐该做法，`sync.Once` 与两阶段改造均不做 |

**方案变更**

| 条目 | 原方案 | 现方案 | 变更理由 |
|---|---|---|---|
| 01 | 维护服务注册 SET + SetNX 哨兵 + 两阶段发布 | **启动时 SCAN 建表 → 之后只读注册表**；成员编码 `{bizId}:{deadlineMillis}`，`SADD`/`SREM` 挂在 `Store.SaveActivityTimes`/`Store.CleanupAll`；meta 失效靠 deadline 本地过滤 + 批量 `SREM` 兜底；无哨兵、无迁移 | 采纳「首次 SCAN、后续查表」的两段式：稳态成本从 O(全库 key 数) 归零为 O(活跃服务数)。成员自带 deadline 使修剪无需任何 `EXISTS`，避免把 O(全库) 换成 O(活跃) 的同时又引入 O(活跃) 的逐键探测。去掉 SetNX 哨兵（节点中途崩溃会让哨兵永久残留，导致此后无人补种、`syncFromRedis` 静默失效）；每个节点独立 bootstrap，`SADD` 幂等无单点 |
| 02 | 按 `groupID` 取值的缓存 sketch，无有效期；活跃期 TTL =「活动周期 + 1 天」 | 缓存改为 `tickGroups() ([]*Group, bool)`（整份未结算分组列表）+ **2 秒有效期**；TTL 公式 =「距结束 + `SettledCacheTTL`」且改用 `effectiveSettleAt()`，`rank:meta` 同步设置，**上线前必须先灰度** | 缓存必须过期才能看到其他节点新建的分组；缓存对象必须是整份列表，按 ID 取值满足不了 `tickAllRobots`。原 TTL 公式会在 `T+1d` 提前销毁本应保留 2 周的历史数据；新公式的绝对过期时刻与设置时机无关，可只设一次、不侵入热路径；改用与第 06 条同一个 `effectiveSettleAt()` 可保证两者永不失配 |
| 06 | sticky `atomic.Bool` | `settledAt` 与当前 `settleAt` 比较 + **`canUpdateScore != Open` 闸门** + `Store` 放在分组循环之后 | sticky bool 在 GM 推后 `CloseTime` 后会让新建分组永不结算（奖励丢失）；`settleAt` 比较使配置变更自动失效，无需显式重置。**闸门是必须的**：原稿假设的「结算后不会再有新分组」与代码不符（见必改-1），否则短路会静默吞掉奖励 |
| 08 | 新增显式清理 + WarmUp 重建 | 统一 `cleanupServiceData` 助手 + 删除 `RemoveByKey` 死代码；另登记 (d2) `RemoveUserEntries` 分块/pipeline、(d3) `rebuildMemberIndex` 负缓存 + 写锁争抢为待办 G | 原稿 ①② 已实现；真实缺陷是 `RemoveByKey` 为生产 no-op、清理顺序靠调用方手写、`syncFromMongo` 漏调 `svc.Cleanup()`。(d2)(d3) 是性能问题而非正确性问题，需改 `engine.Service` 锁粒度，不宜塞进本条 |
| 09 | `registeredAt` + 60s 保护窗口（延迟删除） | 按「活动是否仍在进行」决定修复或删除 | 窗口只是延后删除而非根治；`tryRecoverPeriodicFromRedis` 存在 30 秒恢复-删除振荡循环。删除不可逆，修复幂等且自愈 |
| 13 | 替换为 `sync.Once` | **`WarmUp` 加 `atomic.Bool` 快路径；`sync.Once` 与两阶段改造均不做** | 仅替换 bool 无收益（14 处调用点全在锁内）；两阶段改造需改动全部读路径加锁边界，风险与收益不匹配。而 `WarmUp` 是唯一自己持锁的调用点，只改它即可消掉稳态锁获取，1 行、零风险 |

**新增待办（两轮累计发现，A–H 来自首轮，I–J 来自补充盘点；均未在 13 条内）**

| 项 | 问题 | 位置 |
|---|---|---|
| A | `applyRankConfigDoc` 与 `registerSubService` 注册服务时**不写 MongoDB**，完全依赖广播源写入落盘；写入被丢弃时与 `syncFromMongo` 形成振荡 | [manager.go:963](../../socialserver/internal/rank/manager.go)、[manager_periodic.go:34](../../socialserver/internal/rank/manager_periodic.go) |
| B | `syncFromMongo` 摘除服务时不调用 `svc.Cleanup()`，遗留孤儿 Redis 实例与 Mongo 文档（与 `RemoveService` 行为不一致） | [manager.go:925-930](../../socialserver/internal/rank/manager.go) |
| C | 持锁期间做 IO：`GetMemberRankEntries` 在 `RLock` 内做 N 次 Redis 往返；`ListServices` 在 `RLock` 内做 N 次 `IsSettled`（各含 `LoadGroups`）；`registerEngine` 在写锁内做 Redis+Mongo IO | [manager.go:423](../../socialserver/internal/rank/manager.go)、`356`、`145` |
| D | `MemberIndex.RemoveByKey` 为生产 no-op（`if rdb != nil { return }`），3 处调用点误以为已清理 | [member_index.go:102](../../socialserver/internal/rank/member_index.go) |
| E | `CleanupAll` 删除的 key 集合不含 `rank:member_index`，且因 `bizId` 与索引条目编码不可逆而无法自行处理 | [store.go:435-446](../../socialserver/internal/rank/engine/store.go) |
| F | 00 的补充：常驻 ticker 循环不能用无界 `SubmitWait`，否则 ticker channel（容量 1）静默丢 tick；需增加 `SubmitWaitTimeout` 提交入口 | 见第 00 条 |
| G | 两处随规模放大而未被覆盖的清理/重建开销：① `RemoveUserEntries` 逐条 `SRem`、无分块无 pipeline，而 `members` 是整个活动的成员表（10 万人 = 10 万次串行往返）；② `rebuildMemberIndex` **无负缓存**（miss 不被记住，重复全扫 N 个服务），且 `GetMemberGroupID` 取的是 `s.mu.Lock()` **写锁**，GM 批量查 U 用户 × N 服务 = U×N 次写锁，与 `UpsertScore` 直接争锁。两者都需改 `engine.Service` 的锁粒度，应独立立项 | [member_index.go:123-131](../../socialserver/internal/rank/member_index.go)、[manager.go:396-420](../../socialserver/internal/rank/manager.go) |
| H | 第 02 条活跃期 TTL 是全文**唯一不可逆**改动（`EXPIRE` 到期即删，不可回滚，且 `ttl <= 0` 会被 Redis 当作立即删除）。落地前须：① 按该条「⚠️ 唯一不可逆的改动」先跑只记录不设 TTL 的灰度；② 给「`ttl <= 0` 的分组数」加指标；③ 大活动（`RankPeopleNum` 打满）删档后验证 `rank:mb` / `rank:seq` 在保留期内仍可读 | 见第 02 条 |
| I | `rank:max_score` 是**死代码**：`RankMaxScoreKeyPrefix` / `GetRankMaxScoreKey` 全仓只有定义、无任何生产者与消费者。「真实玩家最高分」功能从未接入。建议删除定义或明确标注为预留 | [defines_rank.go:88-123](../../common/redis/defines_rank.go) |
| J | `setMongoChecked` 的 `SetEX(10min)` 会把同 bizId 其它 key 的 2 周 TTL **缩短**为 10 分钟（`SetEX` 覆盖 TTL）。当前**不可达**（它只在 Redis 与 Mongo 双空时调用，与 `CleanupLiveData` 的作用对象不重叠），但一旦引入滑动刷新或注册表修剪改变调用时机即变为可达 → `rank:groups` 被提前删除。**只要动 TTL 方案就必须一起改** | [store.go:42](../../socialserver/internal/rank/engine/store.go)、`407` |
