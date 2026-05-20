# 排行榜模块完整手册

## 一、模块概览

排行榜模块为气球小狗等活动提供分组排行榜服务，部署在 socialserver 上。

**核心特性**：
- **Redis 排行榜**：所有排名数据存储在 Redis，使用 Lua 脚本保证原子性
- **无状态多节点**：Redis 缓存 + MongoDB 持久化，任意节点可服务任意玩家
- **自动分组**：玩家按配置人数上限（如 30 人）自动分入独立排行榜
- **多期支持**：同一活动 (ActID) 可开多期 (Round)，每期独立分组和结算
- **机器人系统**：首位真实玩家进组后自动生成机器人，按配置增长积分
- **跨业务聚合**：GM 可按玩家 ID 查询其参与的所有排行榜

---

## 二、目录结构

```
socialserver/internal/rank/
├── manager.go           全局管理器（注册活动、启动恢复、聚合查询、Tick 驱动）
├── manager_test.go      管理器测试
├── types.go             公共类型：BizType, BizKey, MemberEntry, MemberRankEntry
├── biz_service.go       跨业务接口 RankBizService + balloonAdapter
├── member_index.go      成员索引（Redis SET 缓存，按 userID 查参与记录）
├── member_index_test.go
└── balloon/             气球小狗排行榜
    ├── types.go         Config, Group, GroupState, Option, RobotTierCfg, RobotInfoEntry
    ├── service.go       Service 核心（UpsertScore、查询、结算、奖励资格）
    ├── group.go         分组管理（创建分组、实例 ID 生成、bizId 计算）
    ├── store.go         Redis 缓存层（读写缓存，写穿透到 DAO）
    ├── dao.go           MongoDB 持久化层（活动注册、分组、成员、机器人）
    ├── robot.go         机器人纯函数（ID 生成、积分算法、配置类型）
    ├── service_robot.go Service 上的机器人方法（生成、Tick 驱动）
    ├── snapshot.go      快照工具函数
    └── service_test.go

common/rank/             基础排行榜引擎（不直接修改）
├── interface.go         Service 接口定义
├── types.go             Rank, RankInstance, RankMemberSnapshot, RankScoreItem
├── defines.go           枚举常量和错误定义
├── helper.go            NewInstanceID 等工具函数
├── service_redis.go     Redis 分布式实现（Lua 脚本、CAS 乐观锁）
├── manager_fixed.go     固定时间窗口管理器
└── manager_cycle.go     周期循环管理器

common/redis/
└── defines_rank.go      所有排行榜 Redis key 定义和生成函数
```

---

## 三、存储分层

```
┌─────────────────────────────────────────────┐
│              balloon.Service                │
│           （内存缓存 + 业务逻辑）             │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│              Store（缓存层）                 │
│     写：Redis + MongoDB 双写                 │
│     读：Redis 优先，miss → MongoDB 回填       │
└─────────┬─────────────────┬─────────────────┘
          │                 │
┌─────────▼─────┐   ┌──────▼───────┐
│  Redis 缓存    │   │  MongoDB     │
│  - 排名数据    │   │  - 活动注册表 │
│  - 分组/成员   │   │  - 分组数据   │
│  - 机器人状态  │   │  - 成员映射   │
│  - 原子计数器  │   │  - 机器人状态 │
└───────────────┘   └──────────────┘
```

---

## 四、服务器启动流程

```
server.OnInit()
    │
    ├── redis.InitMainRedis()           初始化 Redis 连接
    ├── mongodbmodule.Init()            初始化 MongoDB 连接
    ├── configmgr.LoadConfigs()         加载 JSON 配置文件
    └── rankservice.InitGlobalManager(redis.Main, dbName)
         │
         ├── NewRedisService(rdb)       创建 Redis 排行榜引擎
         ├── NewMemberIndex(rdb)        创建成员索引
         ├── NewDAO(dbName)             创建 MongoDB DAO
         ├── dao.EnsureIndexes()        确保 MongoDB 索引
         └── recoverFromMongo()         从 MongoDB 恢复活跃活动
              │
              ├── 读取 balloon_activity 集合
              ├── 过滤未过期活动
              └── 对每个活动调用 RegisterBalloon → 重建 Service
```

---

## 五、设置排行榜（注册活动）

### 调用方式

```go
manager := rankservice.GetGlobalManager()
service, err := manager.RegisterBalloon(ctx, balloon.Config{
    ActID:         1001,         // 活动 ID
    Round:         1,            // 期数（0=第一期）
    RankCode:      "balloon_score_1001",
    RankPeopleNum: 30,           // 每组最大人数（含机器人）
    OpenToken:     100,          // 进榜最低积分门槛
    OpenTime:      1716100000000, // 活动开放时间（Unix 毫秒）
    CloseTime:     1716200000000, // 活动关闭时间（Unix 毫秒）
    AutoSettle:    true,         // 到期自动结算
    RobotTiers:    robotTiers,   // 机器人档次配置（可选）
    RobotInfos:    robotInfos,   // 机器人信息池（可选）
})
```

### 内部流程

```
RegisterBalloon(cfg)
    │
    ├── RegisterRank → Redis            写排名定义（ScoreOrder=desc, TieBreak=first_enter）
    ├── NewService → balloon.Service    创建固定时间窗口管理器 + Store + DAO
    ├── dao.SaveActivity → MongoDB      持久化活动配置到注册表
    └── 注册到 Manager.services 和 balloonServices map
```

### 多期管理

```go
// 第 1 期
manager.RegisterBalloon(ctx, balloon.Config{ActID: 1001, Round: 1, ...})

// 第 2 期（第 1 期结束后）
manager.RegisterBalloon(ctx, balloon.Config{ActID: 1001, Round: 2, ...})

// 获取指定期
svc := manager.GetBalloonServiceByRound(1001, 2)

// 兼容旧接口（返回 Round=0）
svc := manager.GetBalloonService(1001)
```

---

## 六、内部逻辑流转

### 6.1 写入积分

```
UpsertScore(userID, totalScore, now, extra)
    │
    ├── 门槛检查：totalScore < OpenToken → 忽略
    ├── EnsureInstance → 推进时间窗口状态
    ├── 状态检查：State != Open → 返回 ErrInstanceNotOpen
    │
    ├── 查分组映射：
    │   ├── 内存缓存命中 → 直接使用
    │   ├── Redis 命中 → 回填内存
    │   └── MongoDB 命中 → 回填 Redis + 内存
    │
    ├── 新成员首次进入：
    │   ├── ensureGroupLocked → 找空位分组或创建新分组
    │   │   └── NextGroupID → Redis HINCRBY 原子递增
    │   ├── store.SetMember → Redis + MongoDB 双写
    │   ├── store.SaveGroup → Redis + MongoDB 双写
    │   └── onMemberJoin 回调 → MemberIndex.Track
    │
    ├── 首位真实玩家进组 → spawnRobotsForGroup
    │   ├── 按 RobotTierCfg 生成机器人（负数 ID）
    │   ├── BatchUpsertScore → Redis（写入机器人初始积分）
    │   └── store.SaveRobots → Redis + MongoDB
    │
    └── BatchUpsertScore → Redis（写入玩家积分）
```

### 6.2 Tick 驱动（定时调用）

```
Manager.Tick(now)
    │
    └── 遍历所有注册的 Service
         │
         └── Service.Tick(now)
              │
              ├── EnsureInstance → 推进状态（init→open→closed）
              ├── tickAllRobots → 机器人积分增长
              │   ├── 查当前第一名积分
              │   ├── 按档次配置计算增长目标
              │   ├── BatchUpsertScore → Redis
              │   └── store.SaveRobots → Redis + MongoDB
              │
              └── AutoSettle && now >= CloseTime → Settle
```

### 6.3 结算

```
Service.Settle(now)
    │
    └── 遍历所有未结算分组
         │
         ├── CloseInstance → Redis（原子 Lua 脚本关闭实例）
         ├── SettleInstance → Redis（生成排名快照并缓存）
         ├── 缓存快照到内存 settledGroup
         ├── 标记 Group.State = settled
         └── store.SaveGroup → Redis + MongoDB
```

---

## 七、玩家对排行榜的操作

### 7.1 查询分组排行榜

```go
// 查询指定分组的排行榜（0-based 闭区间）
list, err := service.ListGroupRank(ctx, groupID, 0, 29)
// 返回 []rank.RankMemberSnapshot，包含 MemberId, Score, Rank, Extra
```

**流程**：已结算 → 返回缓存快照；未结算 → 实时查询 Redis

### 7.2 查询个人排名

```go
snapshot, groupID, err := service.GetMemberRank(ctx, userID)
// snapshot: 名次快照（Score, Rank, Extra）
// groupID: 所在分组号
// snapshot==nil: 未上榜
```

### 7.3 查询奖励资格

```go
// 查询指定玩家是否有开启奖励资格
hasReward := service.HasOpenReward(userID)

// 获取所有有资格的玩家 ID
userIDs := service.GetOpenRewardUserIDs()
```

### 7.4 查询分组状态

```go
group := service.GetGroup(groupID)
// group.State: "open" / "full" / "settled"
// group.RealCount: 真实玩家数
// group.RobotCount: 机器人数
```

### 7.5 GM 查询（跨业务聚合）

```go
manager := rankservice.GetGlobalManager()

// 查询玩家参与的所有排行榜
entries := manager.GetMemberEntries(userID)
// 返回 []MemberEntry: [{BizType, ActID, Round, GroupID}, ...]

// 查询玩家所有排行榜的名次快照
rankEntries, err := manager.GetMemberRankEntries(ctx, userID)
// 返回 []MemberRankEntry: [{BizType, ActID, Round, GroupID, Snapshot}, ...]
```

---

## 八、Redis Key 完整清单

所有 key 定义在 `common/redis/defines_rank.go`，统一通过函数生成。

### 基础排名引擎（common/rank）

| Key | 类型 | 生成函数 | 说明 |
|-----|------|---------|------|
| `rank:def:{rankCode}` | STRING | `GetRankDefKey()` | 排名定义 JSON |
| `rank:inst:{instanceId}` | STRING | `GetRankInstKey()` | 实例元数据 JSON |
| `rank:mb:{instanceId}` | HASH | `GetRankMbKey()` | memberId → 成员数据 |
| `rank:seq:{instanceId}` | STRING | `GetRankSeqKey()` | 进榜序号原子计数器 |
| `rank:settled:{instanceId}` | STRING | `GetRankSettledKey()` | 结算快照 JSON |

### 业务层缓存（balloon）

| Key | 类型 | 生成函数 | 说明 |
|-----|------|---------|------|
| `balloon:meta:{bizId}` | HASH | `GetBalloonMetaKey()` | nextGroupID 等元数据 |
| `balloon:groups:{bizId}` | HASH | `GetBalloonGroupsKey()` | groupID → Group JSON |
| `balloon:members:{bizId}` | HASH | `GetBalloonMembersKey()` | userID → groupID |
| `balloon:max_score:{bizId}` | HASH | `GetBalloonMaxScoreKey()` | groupID → 最高积分 |
| `balloon:robots:{bizId}:{gid}` | HASH | `GetBalloonRobotsKey()` | robotID → state JSON |
| `balloon:robot_infos:{bizId}:{gid}` | SET | `GetBalloonRobotInfosKey()` | 已用 InfoID |

### 成员索引

| Key | 类型 | 生成函数 | 说明 |
|-----|------|---------|------|
| `rank:member_index:{userId}` | SET | `GetRankMemberIndexKey()` | 参与记录集合 |

其中 `bizId` = `act_{actID}` 或 `act_{actID}_r{round}`。

---

## 九、MongoDB Collection 清单

| Collection | 文档 _id | 索引 | 说明 |
|------------|---------|------|------|
| `balloon_activity` | `bizKey` | — | 活动注册表（启动恢复） |
| `balloon_group` | `{bizId}:{gid}` | `bizId` | 分组数据持久化 |
| `balloon_member` | `{bizId}:{uid}` | `bizId`, `userId` | 成员→分组映射 |
| `balloon_robot` | `{bizId}:{gid}:{mid}` | `(bizId, groupId)` | 机器人状态 |

---

## 十、多节点无状态架构

```
                    ┌─ social-node-1 ─┐
Player A ──hash──→  │  Service        │──→ Redis(缓存) + MongoDB(持久)
                    └─────────────────┘

节点增减 → hash 偏移：

                    ┌─ social-node-2 ─┐
Player A ──hash──→  │  Service        │──→ Redis → miss → MongoDB → 回填
                    │  ensureLoaded() │
                    └─────────────────┘
```

**写路径**：内存 → Redis → MongoDB（双写）
**读路径**：内存 → Redis → MongoDB（逐级回填）
**分组 ID**：Redis HINCRBY 原子递增，避免多节点冲突
**启动恢复**：从 MongoDB `balloon_activity` 恢复所有活跃 Service

---

## 十一、机器人子系统

| 项目 | 说明 |
|------|------|
| ID 规则 | 负数：`-(groupID * 10000 + index)`，与真实玩家正数 ID 不冲突 |
| 生成时机 | 首位真实玩家进入分组时，按 `RobotTierCfg` 批量生成 |
| 积分增长 | 每次 Tick 按档次配置推进（CD、目标比例、上限、锁定期） |
| 持久化 | Redis 缓存 + MongoDB 持久化 |
| 判断函数 | `IsRobotMemberID(memberID)` → `memberID < 0` |

### 增长算法

```
1. 剩余时间 ≤ LockTokenTime → 停止
2. 已达 MaxToken → 停止
3. 未到 GrowTokenCdMs → 等待
4. 当前分 - 玩家第一名分 > MaxDifferenceToken → 停止
5. 目标分 = 第一名积分 × rand(MinPermille, MaxPermille) / 1000
6. 目标分 > 当前分 → 增长；否则不变
```

---

## 十二、扩展指南

### 添加新业务排行榜

1. 新建 `internal/rank/charm/` 目录
2. 实现 `Service`，包含 `GetMemberRank`、`Tick`、`IsSettled`
3. 在 `types.go` 添加 `BizTypeCharm`
4. 在 `biz_service.go` 添加 `charmAdapter`
5. 在 `manager.go` 添加 `RegisterCharm`、`charmServices` map
6. 在 `common/redis/defines_rank.go` 添加对应 key 函数

### 添加新 Redis Key

1. 在 `common/redis/defines_rank.go` 添加常量和生成函数
2. 在 `balloon/store.go` 中使用 `rediskeys.GetXxxKey()` 调用
3. 不要在业务代码中硬编码 key 字符串
