# SocialServer 框架架构文档

> 版本：1.0
> 日期：2026-08-06
> 维护者：sunbin

---

## 目录

1. [整体架构](#1-整体架构)
2. [模块详解](#2-模块详解)
3. [存储模型](#3-存储模型)
4. [核心数据流](#4-核心数据流)
5. [机器人系统](#5-机器人系统)
6. [多节点一致性](#6-多节点一致性)
7. [配置体系](#7-配置体系)
8. [依赖关系](#8-依赖关系)

---

## 1. 整体架构

### 1.1 服务定位

SocialServer 是一个**无状态**的 gRPC 微服务，专门负责游戏内周期排行榜的全生命周期管理。它不处理任何游戏核心逻辑，仅接受来自 GameServer 的积分上报，并对外提供实时排名查询、结算、奖励发放等能力。

### 1.2 分层结构

```text
┌─────────────────────────────────────────────────┐
│          GameServer / GM (gRPC 调用方)           │
└───────────────────┬─────────────────────────────┘
                    │  S2S gRPC
┌───────────────────▼─────────────────────────────┐
│         Router 层  (internal/router/)            │
│  rpc/social/rank_handler.go  ← 17 个 RPC 实现   │
│  http/router.go              ← /health、pprof    │
└───────────────────┬─────────────────────────────┘
                    │
┌───────────────────▼─────────────────────────────┐
│         Manager 层  (internal/rank/manager.go)  │
│  服务注册表：map[bizKey] → RankBizService         │
│  后台循环：tick(1s) / sync(30s) / subscribe      │
└───────────────────┬─────────────────────────────┘
                    │
┌───────────────────▼─────────────────────────────┐
│     Engine 层  (internal/rank/engine/)           │
│  engine.Service：分组管理、积分写入、结算快照       │
│  Store：Redis + MongoDB 双层存储协调              │
│  DAO：MongoDB 异步持久化队列                      │
└──────────┬────────────────────┬─────────────────┘
           │                    │
   ┌───────▼──────┐    ┌────────▼──────┐
   │  Redis       │    │  MongoDB      │
   │  (热数据)    │    │  (持久化)     │
   └──────────────┘    └───────────────┘
```

### 1.3 关键包路径

```text
socialserver/
  internal/
    server.go                    # 服务生命周期
    rank/
      manager.go                 # 全局管理器
      manager_periodic.go        # 周期排行榜 Manager 方法（委托 periodic.Handler）；实现 ServiceRegistrar
      biz_service.go             # 业务服务接口
      types.go                   # 公共类型；PeriodicState/RoundInfo 类型别名
      member_index.go            # 成员索引
      config_loader.go           # JSON 配置加载
      once/biz.go                # 一次性排行榜业务服务适配器（原 timebounded/）
      periodic/
        state.go                 # PeriodicState、RoundInfo：周期状态与轮次推进
        handler.go               # Handler 编排；ServiceRegistrar 接口（解循环依赖）
      engine/
        service.go               # 核心排行榜引擎
        store.go                 # 双层存储
        dao.go                   # MongoDB DAO
        group.go                 # 分组管理
        robot.go                 # 机器人算法
        service_robot.go         # 机器人生成/驱动
        snapshot.go              # 快照工具
        types.go                 # 数据结构
    router/
      rpc/social/rank_handler.go # RPC 入口
      http/router.go             # HTTP 路由
```

---

## 2. 模块详解

### 2.1 server.go — 服务生命周期

实现 `golib/module` 接口，负责整个服务的启动与关闭编排。

**`LoadConfig()`**：读取 `.devops.yaml`，填充全局 `config.Default`（Redis、MongoDB、HTTP、RPC、日志、etcd、pprof 等所有参数）。

**`OnInit()`** 初始化顺序：

1. etcd 服务发现注册
2. Redis 连接
3. MongoDB 连接
4. JSON 业务配置加载（`configmgr.LoadConfigs()`）
5. MongoDB 任务队列初始化
6. 排行榜全局管理器初始化（`rankservice.InitGlobalManager()`）
7. gRPC 服务器启动
8. HTTP 服务器启动
9. etcd 节点刷新

**`OnClose()`** 按逆序优雅关闭：排行榜管理器 → RPC → etcd → 任务队列 → Redis → MongoDB。

---

### 2.2 rank.Manager — 全局管理器

**文件**：[internal/rank/manager.go](../internal/rank/manager.go)

Manager 是排行榜系统的单例门面，持有两张内存注册表，以及周期排行榜处理器：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `services` | `map[string]RankBizService` | `bizKey → 业务服务` |
| `engineServices` | `map[string]*engine.Service` | `bizKey → 引擎实例` |
| `periodicHandler` | `*periodic.Handler` | 周期排行榜编排（状态管理、轮次推进、历史查询） |

**bizKey** 格式：`{bizType}:{actID}`，例如 `balloon:1001`。

#### 初始化流程

```text
InitGlobalManager()
  ├── engine.NewDAO()              → 创建 MongoDB DAO，EnsureIndexes
  ├── commonrank.NewRedisService() → 基于 common/rank 的 Redis 排行榜服务
  ├── NewMemberIndex()             → 成员索引
  ├── syncFromMongo()              → 从 MongoDB 恢复未过期活动
  ├── syncFromRedis()              → 从 Redis 补充内存状态
  ├── warmUpAllServices()          → 预热（触发 ensureLoaded）
  └── startBackground()
        ├── tickLoop()             → 每秒
        ├── syncLoop()             → 每 30 秒
        └── subscribeDeleteEvents() → Redis pub/sub 监听
```

#### 后台循环

| 循环 | 周期 | 职责 |
| --- | --- | --- |
| `tickLoop` | 1 秒 | 遍历所有 Service，触发 `Tick()`（机器人增长 + 自动结算检查） |
| `syncLoop` | 30 秒 | 从 MongoDB 全量同步，重建丢失的 Service；清理已过期活动 |
| `subscribeDeleteEvents` | 事件驱动 | 监听 Redis 删除广播，移除本节点内存中对应的 Service 实例 |

#### Register / 注销

```go
// 注册新活动（GM 接口调用）
// rankType=RankTypeOnce → 调用 registerSubService 直接创建 engine.Service
// rankType=RankTypePeriodic → 委托 periodicHandler.Register，由 Handler 管理轮次生命周期
func (m *Manager) Register(ctx context.Context, bizType string, cfg engine.Config) error

// 删除活动（同时广播 Redis 事件通知其他节点）
func (m *Manager) Delete(ctx context.Context, bizKey string) error
```

Manager 同时实现 `periodic.ServiceRegistrar` 接口，供 `periodic.Handler` 回调注册新轮次：

```go
func (m *Manager) RegisterRoundService(ctx context.Context, bizType, logicalKey string, cfg engine.Config) (*engine.Service, error)
func (m *Manager) GetEngineServiceByKey(logicalKey string) *engine.Service
```

---

### 2.3 engine.Service — 核心排行榜引擎

**文件**：[internal/rank/engine/service.go](../internal/rank/engine/service.go)

每个 `(bizType, actID)` 对对应一个 Service 实例。Service 内部维护：

- `groups []Group`：当前所有分组（从 Redis 加载，写入时同步回 Redis）
- `memberGroup map[int64]int32`：`userID → groupID` 映射
- `settledGroup map[int32][]RankMemberSnapshot`：已结算分组的快照

#### 分组机制

新玩家上报积分时，Service 寻找一个 `open` 且人数未满的分组。若所有分组已满，则自动创建新分组，并为其生成新的 `instanceID`。

```text
UpsertScore(userID, score)
  ├── 若玩家已在某组 → 直接更新积分
  ├── 若玩家未入组
  │   ├── 找到未满的 open 分组
  │   ├── 若无 → 创建新分组（ensureGroupLocked）
  │   └── 分配玩家到组
  ├── 首个真实玩家入组 → 触发机器人生成（spawnRobotsForGroup）
  └── 写入 store（Redis + MongoDB 双写）
```

#### 关键方法

| 方法 | 说明 |
| --- | --- |
| `UpsertScore(ctx, userID, score, avatarInfo)` | 更新积分，返回排名快照 |
| `GetMemberRank(ctx, userID)` | 查询单个玩家排名 |
| `ListGroupRank(ctx, userID, start, end)` | 查询玩家所在组的榜单 |
| `Settle(ctx)` | 封榜，生成所有分组的结算快照 |
| `Tick(ctx, now)` | 驱动机器人增长；检查活动是否到期需自动结算 |
| `GetRewardUsers(ctx)` | 返回所有组的结算快照（供发奖） |
| `ClaimReward(ctx, userID)` | 标记用户已领奖 |
| `GetClaimStatus(ctx, userID)` | 查询领奖状态 |

#### 无状态恢复

Service 在首次业务调用时通过 `ensureLoaded()` 从 Redis 恢复完整运行时状态（分组列表、成员映射、机器人状态），无需依赖本地内存。节点重启或 hash 偏移后自动透明恢复。

---

### 2.4 once.BizService — 一次性排行榜适配器

**文件**：[internal/rank/once/biz.go](../internal/rank/once/biz.go)

`BizService` 实现 `RankBizService` 接口，是 `engine.Service` 的薄包装。它将所有一次性业务类型（`balloon`、`egg`、`camper_competition` 及未来新增类型）统一适配，内部不含任何业务特性。

```go
type BizService struct {
    bizType string
    svc     *engine.Service
}

func (b *BizService) BizType() string     { return b.bizType }
func (b *BizService) GetMemberRank(...)   { return b.svc.GetMemberRank(...) }
func (b *BizService) Tick(...)            { return b.svc.Tick(...) }
func (b *BizService) IsSettled() bool     { return b.svc.IsSettled() }
```

新增排行榜类型不需要新建 BizService 实现，只需在 `RankBase.json` 增加配置条目。

---

### 2.5 periodic.Handler — 周期排行榜编排

**文件**：[internal/rank/periodic/handler.go](../internal/rank/periodic/handler.go)

`Handler` 管理所有周期排行榜活动的状态和轮次生命周期，从 `Manager` struct 中解耦出来，避免周期逻辑与核心注册逻辑混合。

```go
// ServiceRegistrar 由 Manager 实现，注入给 Handler，打破循环依赖
type ServiceRegistrar interface {
    RegisterRoundService(ctx context.Context, bizType, logicalKey string, cfg engine.Config) (*engine.Service, error)
    GetEngineServiceByKey(logicalKey string) *engine.Service
}

type Handler struct {
    mu       sync.RWMutex
    states   map[string]*PeriodicState  // logicalKey → 状态
    rdb      *goredis.Redis
    dao      *engine.DAO
    registry ServiceRegistrar           // 回调 Manager 注册新轮次 Service
}
```

**锁顺序保证（防死锁）**：`m.mu`（Manager）→ `h.mu`（Handler）。`advanceRound` 在持有 `h.mu` 期间不直接调用 Manager；需要回调时先释放 `h.mu`，再通过 `registry.RegisterRoundService` 获取 `m.mu`。

**主要方法：**

| 方法 | 说明 |
| --- | --- |
| `Register(ctx, bizType, logicalKey, cfg, cycleDays)` | 注册或恢复周期活动，创建当前轮 Service（按当前时间定位） |
| `TickAll(ctx, now)` | 遍历所有状态，检查轮次是否到期并推进 |
| `GetState(logicalKey)` | 读取当前 PeriodicState |
| `GetRoundInfos(bizType, actID)` | 返回所有历史轮次摘要 |
| `GetHistoricalRoundList / GetHistoricalRewardUsers / ClaimHistoricalReward` | 历史轮次数据查询与领奖 |

---

### 2.6 MemberIndex — 成员索引

**文件**：[internal/rank/member_index.go](../internal/rank/member_index.go)

MemberIndex 使用 Redis SET 维护每个玩家参与的所有排行榜记录。

```text
Key:   rank:member:{userID}
Value: SET of strings，每条格式为 "{bizType}:{actID}:{groupID}"
```

**用途**：GM 接口 `S2SGMGetUserRankList` 需要跨业务查询某个玩家参与的所有排行榜，通过 MemberIndex 一次性获取所有归属关系，再批量查询各引擎实例。

---

## 3. 存储模型

### 3.1 Redis Key 设计

所有 Key 通过 `common/redis` 包统一管理前缀，避免命名冲突。

#### 排行榜主体（来自 `common/rank`）

| Key 模式 | 类型 | 说明 |
| --- | --- | --- |
| `rank:{instanceID}:mb` | ZSet | 排名主体，score 为积分，member 为 userID |
| `rank:{instanceID}:info` | Hash | 榜单元数据（state、memberCount、version） |
| `rank:{instanceID}:members` | Hash | 成员扩展信息（enterTime、sequence 等） |

#### 引擎运行时状态（`engine/store.go`）

| Key 模式 | 类型 | 说明 |
| --- | --- | --- |
| `rank:svc:{bizID}:groups` | Hash | 分组列表，field=groupID，value=Group 序列化 |
| `rank:svc:{bizID}:members` | Hash | userID → groupID 映射 |
| `rank:svc:{bizID}:nextgid` | String | 下一个可用 groupID（自增计数器） |
| `rank:svc:{bizID}:times` | Hash | openTime / closeTime / gameEndTime |
| `rank:svc:{bizID}:settled:{groupID}` | String | 分组结算快照（JSON） |
| `rank:svc:{bizID}:claims` | Set | 已领奖的 userID 集合 |
| `rank:robot:{bizID}:{groupID}` | Hash | 机器人状态（score、pendingScore、nextTickAt） |

#### 成员索引

| Key 模式 | 类型 | 说明 |
| --- | --- | --- |
| `rank:member:{userID}` | Set | 玩家参与的所有 `bizType:actID:groupID` |

#### 删除广播

| Key 模式 | 类型 | 说明 |
| --- | --- | --- |
| `rank:del:event` | Pub/Sub Channel | 活动删除事件广播，value 为 bizKey |

### 3.2 MongoDB 集合设计

| 集合 | 说明 | 关键字段 |
| --- | --- | --- |
| `rank_activity` | 活动配置与状态 | `bizType, actID, openTime, closeTime, state` |
| `rank_group` | 分组数据 | `bizID, groupID, state, realCount, robotCount` |
| `rank_member` | 成员记录 | `bizID, groupID, userID, score, enterTime` |
| `rank_robot` | 机器人数据 | `bizID, groupID, robots[]` |
| `rank_settled` | 结算快照 | `bizID, groupID, members[], settleTime` |
| `rank_claim` | 领奖记录 | `bizID, userID, claimedAt` |

#### 异步写入队列

MongoDB 写入通过 `engine.DAO` 的内部 `mongoTask` 队列异步处理，业务调用路径不会阻塞在 MongoDB IO 上。队列为有界通道，满载时丢弃非关键写入并记录日志。

---

## 4. 核心数据流

### 4.1 积分更新流程

```text
GameServer
  └─ S2SUpsertScore(bizType, actID, userID, totalScore, avatarInfo, round)
       │
       ▼
  rank_handler.S2SUpsertScore()
  ├── lookupEngineService(bizType, actID)  ← 从 Manager 取 engine.Service
  │
  ├── [周期排行榜] 轮次校验
  │   ├── 获取 PeriodicState.CurrentRound
  │   └── 若 req.Round > 0 且 req.Round != CurrentRound
  │       └── 返回 CODE_RANK_ROUND_CHANGED { CurrentRound }
  │
  ├── engine.Service.UpsertScore()
  │   ├── ensureLoaded()                   ← 首次调用时从 Redis 恢复状态
  │   ├── 查找 memberGroup[userID]
  │   │   ├── 已在组 → 直接更新
  │   │   └── 未在组 → 分配组（必要时新建）
  │   ├── store.UpsertMemberGroup()        ← Redis Hash 写 userID→groupID
  │   ├── rankService.UpsertScore()        ← Redis ZSet ZADD（Lua 原子）
  │   ├── store.SaveMember()               ← Redis Hash + MongoDB async
  │   └── 若首个真实玩家 → spawnRobotsForGroup()
  │
  └── 返回 PBMemberRankInfo（排名、积分、组信息）+ currentRound
```

### 4.2 活动结算流程

```text
GM 调用 S2SSettle  或  tickLoop 检测到 closeTime 已过
  │
  ▼
engine.Service.Settle()
  ├── 遍历所有 groups
  ├── 对每个 group 调用 rankService.Snapshot()  ← Redis ZRange 全量导出
  ├── 过滤机器人（memberID < 0），计算 DisplayRank
  ├── store.SaveSettled(groupID, snapshot)        ← Redis + MongoDB 持久化
  └── 标记 service 为 settled 状态

GameServer 后续调用 S2SGetRewardUsers → 读取 settled 快照 → 发奖
GameServer 调用 S2SClaimReward       → 记录领奖状态
```

### 4.3 活动创建/恢复流程

```text
GM 调用 S2SCreateRankConfig(bizType, actID, openTime, closeTime)
  │
  ▼
Manager.Register()
  ├── config_loader.LoadEngineConfig(bizType)  ← 读取 RankBase.json + RobotRank.json
  ├── engine.NewService(config, rdb, dao)
  ├── once.NewBizService(bizType, engineSvc)     ← 一次性类型
  ├── manager.services[bizKey] = bizService
  └── manager.engineServices[bizKey] = engineSvc

── 周期排行榜注册 ──
Manager.Register() with rankType=Periodic
  └── periodicHandler.Register(ctx, bizType, logicalKey, cfg, cycleDays)
        ├── 创建初始 PeriodicState（按当前时间定位 CurrentRound，计算对应轮窗口）
        ├── registry.RegisterRoundService(bizType, roundBizId, cfg) ← 回调 Manager
        └── 保存 PeriodicState 到 MongoDB

── 节点重启后 ──
Manager.syncFromMongo()
  ├── 从 rank_activity 集合查出所有未过期活动
  └── 对每条记录重走上面的 Register 流程（复用相同 Redis 数据）
```

---

## 5. 机器人系统

### 5.1 设计目标

当真实玩家数量较少时，机器人用于填充排行榜，制造竞争感。机器人对客户端表现为普通玩家，但在发奖时被过滤。

### 5.2 机器人 ID 规范

```text
robotID = -(groupID * 10000 + index)
```

- 负数 memberID 全部视为机器人
- `index` 从 1 开始，每组最多 `RobotTierCfg.Num` 个机器人

### 5.3 分档配置（RobotRank.json）

每个 bizType 配置 4 档（A/B/C/D），对应不同的积分增长速度：

| 字段 | 说明 |
| --- | --- |
| `Num` | 该档机器人数量 |
| `DefaultTokenRange` | 初始积分范围 `min,max` |
| `GrowTokenCd` | 增长 CD（秒） |
| `GrowTokenRange` | 每次增长量（基点，万分之一） |
| `MaxToken` | 积分上限 |
| `MaxDifferenceToken` | 与第一名的最大分差（超出则停止增长） |
| `LockTokenTime` | 活动结束前多少秒停止增长 |
| `OvertakeTime` | 追赶阶段开始时间（活动结束前秒数） |
| `OvertakeInterval` | 追赶阶段增长间隔 |

### 5.4 Tick 驱动流程

```text
tickLoop()  ── 每秒 ──
  └── 遍历 engineServices
        └── service.Tick(ctx, now)
              └── tickAllRobots(ctx, now)
                    ├── 遍历所有 open 分组
                    ├── 读取 robot 状态（Redis Hash）
                    ├── 计算本次增量（CD 检查 + 追赶逻辑 + 分差限制）
                    ├── 若有 PendingScore → 触发超越事件
                    └── store.SaveRobots() ← Redis + MongoDB 异步持久化
```

### 5.5 机器人生成时机

首个真实玩家加入某分组时，调用 `spawnRobotsForGroup()`：

1. 根据 `RobotTierCfg` 计算各档数量
2. 生成负数 memberID
3. 随机初始积分（`DefaultTokenRange`）
4. 写入 Redis + MongoDB
5. 调用 `rankService.UpsertScore()` 加入榜单

---

## 6. 多节点一致性

SocialServer 设计为无状态服务，支持多节点部署。一致性通过以下机制保障：

### 6.1 内存状态来源

所有运行时状态（分组、成员、机器人）存储在 Redis，内存仅作为读缓存。节点启动时通过 `syncFromMongo + syncFromRedis` 恢复完整状态。

### 6.2 写入原子性

积分写入使用 Redis Lua 脚本，保证 ZADD + 成员 Hash 更新在同一原子操作内完成，避免并发竞态。

### 6.3 删除广播

删除活动时，执行节点向 Redis Channel `rank:del:event` 发布 bizKey。所有节点的 `subscribeDeleteEvents` goroutine 收到事件后，从本地 `services` 和 `engineServices` 注册表中移除对应实例，防止"僵尸服务"继续提供查询。

### 6.4 定期全量同步

`syncLoop`（30 秒）从 MongoDB 全量拉取活动列表，处理以下场景：

- 其他节点创建了新活动（本节点错过了 Register 调用）
- Redis 数据因过期被清理，需要重建 Service 实例
- 已过期活动的内存清理

---

## 7. 配置体系

### 7.1 环境配置层（YAML）

通过 `build.sh` 选择环境，对应 `conf/.devops*.yaml` 被复制为 `bin/.devops.yaml`。运行时通过 `yamlcfg.YamlCfg` 读取，填充 `config.Default` 结构体。

```text
conf/
  .devops.yaml             # 开发环境（172.20.4.224）
  .devops_test.yaml        # 测试环境
  .devops_inter.yaml       # 预发布环境
  .devops_production.yaml  # 生产环境
```

### 7.2 业务配置层（JSON）

运行时由 `configmgr.LoadConfigs()` 加载，路径由 YAML 中 `configDir` 指定。

```text
config/
  RankBase.json      # 排行榜类型定义（bizType、分组上限、入榜门槛）
  RobotRank.json     # 机器人档位配置（每个 bizType 四档）
  RobotName.json     # 机器人名称/头像池（100 条）
```

#### RankBase.json 字段说明

| 字段 | 说明 |
| --- | --- |
| `id` | 内部数字 ID |
| `bizType` | 业务类型标识（用于 RPC 参数） |
| `rankPeopleNum` | 每组人数上限 |
| `balloonRankOpenToken` | 入榜门槛积分 |
| `topNameColor` | 榜首名称颜色（客户端展示） |
| `name` | 国际化 Key |

---

## 8. 依赖关系

### 8.1 内部模块依赖（local replace）

```text
socialserver
  ├── golib       → 基础组件（Redis、MongoDB、zaplog、HTTP、module 框架）
  ├── common      → 共享类型（rank.Service 接口、Redis Key 常量、错误定义）
  └── pbcommon    → Protobuf 生成代码（gRPC 服务定义、消息结构）
```

### 8.2 主要外部依赖

| 依赖 | 版本 | 用途 |
| --- | --- | --- |
| `gin-gonic/gin` | v1.11.0 | HTTP 框架 |
| `google.golang.org/grpc` | v1.78.0 | gRPC 框架 |
| `go-redis/v9` | v9.14.0 | Redis 客户端 |
| `mongo-driver` | v1.17.6 | MongoDB 驱动 |
| `go.uber.org/zap` | v1.27.0 | 结构化日志 |
| `spf13/viper` | v1.21.0 | 配置读取 |
| `bytedance/sonic` | v1.14.1 | 高性能 JSON 序列化 |
| `etcd/client/v3` | v3.6.5 | 服务注册与发现 |

### 8.3 proto 依赖（pbcommon/gen/ss/msg）

SocialServer 注册两个 gRPC 服务：

- `pb.GameServerServer`：接收来自 GameServer 的 S2S 调用
- `pb.SocailServerServer`：提供 SocialServer 特有接口

所有 proto 定义位于兄弟模块 `pbcommon`，本仓库不含 `.proto` 文件。

---

**最后更新**：2026-08-21
