# SocialServer - 社交服务器

SocialServer 是一个基于 Go 的后端微服务，提供游戏内各类竞技活动的**周期排行榜**能力。支持气球小狗、蛋蛋装饰、房车杯等多种业务类型，通过统一的引擎层实现高性能分布式排行榜。

**语言**：Go 1.25.1  
**通信协议**：gRPC + HTTP  
**核心存储**：Redis（热数据）+ MongoDB（持久化）  
**架构模式**：无状态多节点，支持水平扩展

---

## 核心特性

- **分布式排行榜**：基于 Redis Sorted Set + Lua 脚本，保证原子性操作
- **自动分组**：玩家按配置人数上限自动分入独立排行榜组
- **机器人系统**：首位真实玩家进组后自动生成机器人，按 CD 增长积分
- **成员索引**：Redis SET 缓存玩家参与的所有排行榜，支持跨业务查询
- **配置驱动扩展**：新增排行榜类型只需修改 JSON 配置文件，无需改代码
- **无状态设计**：所有运行时状态持久化到 Redis/MongoDB，节点重启自动恢复
- **双层存储**：写入时 Redis + MongoDB 双写；读取时 Redis 优先，miss 回源 MongoDB

---

## 项目结构

```text
socialserver/
├── cmd/
│   └── main.go                      # 服务启动入口，注入版本信息
│
├── internal/
│   ├── server.go                    # 服务生命周期（OnInit / OnClose）、配置加载
│   │
│   ├── rank/                        # 排行榜领域
│   │   ├── types.go                 # 公共类型：BizType、BizKey、RankTypeOnce/Periodic、PeriodicState/RoundInfo 类型别名
│   │   ├── biz_service.go           # RankBizService 接口定义
│   │   ├── manager.go               # 全局 Manager：服务注册、后台 tick/sync/订阅
│   │   ├── manager_periodic.go      # 周期排行榜 Manager 方法（委托 periodic.Handler）；实现 ServiceRegistrar
│   │   ├── member_index.go          # 成员索引：Redis SET，查询玩家参与的所有排行榜
│   │   ├── config_loader.go         # 加载 RankBase.json / RobotRank.json 到 engine.Config
│   │   │
│   │   ├── once/
│   │   │   └── biz.go               # 一次性排行榜业务服务适配器（原 timebounded/）
│   │   │
│   │   ├── periodic/
│   │   │   ├── state.go             # PeriodicState、RoundInfo：周期状态与轮次推进
│   │   │   └── handler.go           # Handler 编排；ServiceRegistrar 接口（解循环依赖）
│   │   │
│   │   └── engine/                  # 通用排行榜引擎（无业务特性）
│   │       ├── types.go             # GroupState、Config、Group、Option
│   │       ├── service.go           # 核心服务：UpsertScore、GetMemberRank、Settle 等
│   │       ├── group.go             # 分组管理：ensureGroup、mergeGroups
│   │       ├── robot.go             # 机器人数据结构与 tick/spawn 算法
│   │       ├── service_robot.go     # 机器人生成与增长驱动
│   │       ├── store.go             # Redis + MongoDB 双层存储协调
│   │       ├── dao.go               # MongoDB DAO（异步 mongoTask 队列）
│   │       └── snapshot.go          # cloneSnapshots / sliceSnapshots 工具
│   │
│   └── router/
│       ├── rpc/
│       │   ├── rpc.go               # gRPC 服务器初始化、keepalive 配置
│       │   └── social/
│       │       ├── server.go        # ServerHandler，S2SCompleted stub
│       │       └── rank_handler.go  # 全部 17 个 RPC Handler 实现
│       └── http/
│           └── router.go            # Gin 路由：/health、/debug/pprof/
│
├── conf/                            # 环境配置（YAML）
│   ├── .devops.yaml                 # 开发环境
│   ├── .devops_test.yaml            # 测试环境
│   ├── .devops_inter.yaml           # 预发布环境
│   └── .devops_production.yaml      # 生产环境
│
├── config/                          # 游戏数据配置（JSON，运行时加载）
│   ├── RankBase.json                # 排行榜类型定义（bizType、分组上限、门槛等）
│   ├── RobotRank.json               # 机器人分档配置（每个 bizType 四档 A/B/C/D）
│   └── RobotName.json               # 机器人名称 / 头像池（100 条）
│
├── doc/                             # 设计文档
│   ├── architecture.md              # 框架架构详细文档（本文）
│   └── balloon_rank_design.md       # 排行榜三层分层设计方案
│
├── scripts/
│   ├── build.sh                     # 编译脚本（交互选环境，注入 git hash）
│   ├── start.sh                     # 后台启动（nohup，写 PID 文件）
│   ├── stop.sh                      # 按 PID 停止
│   ├── restart.sh                   # 重启
│   ├── status.sh                    # 检查进程状态
│   └── package.sh                   # 打包 release
│
├── go.mod / go.sum                  # 依赖（本地 replace：golib、common、pbcommon）
└── README.md
```

---

## 架构概览

```text
GameServer / GM
      │  gRPC (S2S)
      ▼
 rank_handler.go          ← RPC 入口，参数校验
      │
      ▼
 rank.Manager             ← 服务注册表，路由到对应 engine.Service
      │
      ▼
 engine.Service           ← 核心排行榜引擎（每个 bizType+actID 一个实例）
  ├── UpsertScore         ←  分组分配、首次入组触发机器人、Redis ZSet 写入
  ├── GetMemberRank       ←  读取玩家排名快照
  ├── ListGroupRank       ←  读取榜单前 N 名
  ├── Settle              ←  封榜，生成结算快照
  └── Tick / TickRobots   ←  机器人积分驱动（每秒调用）
      │
      ▼
 engine.Store             ← Redis + MongoDB 双层存储
  ├── Redis               ←  Sorted Set（排名主体）、Hash（成员/分组元数据）
  └── MongoDB             ←  异步持久化（mongoTask 队列，不阻塞业务）
```

---

## 启动流程

```text
1. main.go
   └── server.LoadConfig()           # 读取 .devops.yaml → config.Default

2. server.OnInit()
   ├── redis.InitMainRedis()         # 连接 Redis
   ├── mongodbmodule.Init()          # 连接 MongoDB
   ├── configmgr.LoadConfigs()       # 加载 RankBase / RobotRank / RobotName JSON
   ├── rankservice.InitGlobalManager()
   │   ├── engine.NewDAO()           # 创建 MongoDB DAO，EnsureIndexes
   │   ├── commonrank.NewRedisService()
   │   ├── NewMemberIndex()
   │   ├── syncFromMongo()           # 恢复 MongoDB 中未过期的活动
   │   ├── syncFromRedis()           # 从 Redis 补充内存状态
   │   ├── warmUpAllServices()       # 预热全部服务
   │   └── startBackground()
   │       ├── tickLoop()            # 每秒：驱动机器人增长、自动结算
   │       ├── syncLoop()            # 每 30 秒：与 MongoDB 全量同步
   │       └── subscribeDeleteEvents() # 订阅 Redis 删除广播，多节点一致性
   ├── initRPCServer()               # 注册 gRPC 服务
   └── initHTTPServer()              # 启动 Gin HTTP 服务
```

---

## 构建与运行

### 编译

```bash
cd socialserver
./scripts/build.sh
# 交互选环境（1=local 2=test 3=production 4=inter），20 秒超时默认 local
# 输出 bin/socialserver，同时复制对应 .devops*.yaml 到 bin/.devops.yaml
```

### 启动 / 停止

```bash
./scripts/start.sh    # nohup 后台启动，PID 写入 bin/socialserver.pid
./scripts/stop.sh     # 按 PID 停止
./scripts/restart.sh
./scripts/status.sh
```

### 监听端口（开发环境）

| 用途 | 地址 |
| --- | --- |
| gRPC | `0.0.0.0:10101` |
| HTTP | `0.0.0.0:10001` |
| pprof | `0.0.0.0:10901` |

---

## RPC 接口速览

所有接口实现在 [internal/router/rpc/social/rank_handler.go](internal/router/rpc/social/rank_handler.go)。

### 业务接口（GameServer → SocialServer）

| 方法 | 说明 |
| --- | --- |
| `S2SUpsertScore` | 更新玩家积分，返回最新排名快照；周期排行榜校验 `round` 字段，轮次不符返回 `CODE_RANK_ROUND_CHANGED` |
| `S2SGetRankList` | 获取玩家所在组的排行榜前 N 名（支持历史轮次） |
| `S2SGetMemberRank` | 获取单个玩家的排名信息 |
| `S2SSettle` | 触发活动下所有分组结算 |
| `S2SGetRewardUsers` | 获取有奖励资格的玩家列表 |
| `S2SClaimReward` | 原子幂等写入领奖记录；首次返回 `claimed=false`，重复返回 `claimed=true` |
| `S2SGetClaimStatus` | 查询玩家领奖状态 |
| `S2SGetRankCurRound` | 查询周期排行榜所有轮次摘要（currentRound + 各轮 settled 状态） |

### GM 接口（运营后台）

| 方法 | 说明 |
| --- | --- |
| `S2SListRankBizTypes` | 列出所有已配置的 bizType（来自 RankBase.json） |
| `S2SCreateRankConfig` | 注册新排行榜活动 |
| `S2SGetRankConfig` | 查询活动配置与当前统计 |
| `S2SUpdateRankConfig` | 修改活动的开/关/截止时间 |
| `S2SDeleteRankConfig` | 删除活动及全部数据 |
| `S2SListRankConfigs` | 列出所有已注册活动 |
| `S2SGMGetUserRankList` | 查询某玩家参与的所有排行榜 |
| `S2SGMGetGroupRankList` | 查询某分组的完整榜单 |
| `S2SGMGetRankInstanceList` | 列出某活动的所有分组实例 |
| `S2SGMGetInstanceRankList` | 查询某分组实例的完整榜单 |

---

## 配置说明

### 环境配置（conf/.devops.yaml）

```yaml
game: merge
cluster: local
module: socialserver
configDir: "../config"        # 指向 config/ 目录

redis:
  - host: 127.0.0.1
    port: 6379
    password: ""

mongodb:
  uri: mongodb://127.0.0.1:27017
  database: mergeHope
  maxPoolSize: 100
  minPoolSize: 10

http:
  http_listen_addr: 0.0.0.0:10001

rpc:
  server:
    address: 0.0.0.0:10101
    enable_reflection: true

logCfg:
  logLevel: debug
  developMode: true

pprof:
  enabled: true
  addr: 0.0.0.0:10901
```

### 添加新排行榜类型（无需改代码）

**1. 在 `config/RankBase.json` 新增条目**

```json
{
  "id": "4",
  "bizType": "my_activity",
  "rankPeopleNum": "30",
  "balloonRankOpenToken": "9000",
  "topNameColor": "#266519",
  "name": "activity_my_activity_rank_title"
}
```

**2.（可选）在 `config/RobotRank.json` 新增机器人档位配置**

### 3. 通过 GM 接口注册活动

```bash
# gRPC: S2SCreateRankConfig
bizType: "my_activity"
actId: 1001
openTime: <unix_ms>
closeTime: <unix_ms>
```

完成，新类型自动继承所有通用能力（分组、机器人、结算、奖励）。

---

## 关键设计决策

| 决策 | 说明 |
| --- | --- |
| **Redis ZSet + Lua** | 排名写入通过 Lua 脚本保证原子性，避免并发竞态 |
| **双层异步持久化** | 写入先到 Redis，MongoDB 通过 mongoTask 队列异步消费，不阻塞业务 |
| **负数 memberID 标识机器人** | 机器人 ID = `-(groupID * 10000 + index)`，查询时按符号区分 |
| **负缓存哨兵** | Redis miss 后用 `\x00` 占位，防止雪崩打穿 MongoDB |
| **多节点一致性** | 删除活动时广播 Redis pub/sub 事件，所有节点同步移除内存中的 Service 实例 |
| **配置驱动扩展** | `RankBase.json` 是唯一扩展点，`once.BizService` 和 `periodic.Handler` 完全通用 |
| **periodic 子包隔离** | 周期排行榜逻辑封装在 `periodic/` 子包（`Handler` + `PeriodicState`），通过 `ServiceRegistrar` 接口回调 Manager，避免循环依赖 |
| **轮次竞态保护** | `S2SUpsertScore` 校验请求 `round` 与 `PeriodicState.CurrentRound`，轮次不符直接拒绝并返回最新轮次，防止旧轮数据写入新轮 Service |

---

## 文档

- [架构详细文档](docs/architecture.md) - 模块详解、存储模型、数据流、机器人系统
- [排行榜完整流程](docs/rank_flow.html) - 一次性/周期排行榜流程、接口清单、多节点安全

---

**最后更新**：2026-08-21  
**维护者**：sunbin
