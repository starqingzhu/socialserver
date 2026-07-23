# SocialServer - 社交服务器

SocialServer 是一个基于 Go 的后端微服务，主要提供排行榜系统支持。它为游戏内的各类竞技活动（如气球小狗、蛋蛋排行等）提供分布式、高性能的排行榜引擎。

**项目语言**：Go 1.25.1  
**部署方式**：gRPC + HTTP  
**核心存储**：Redis（缓存）+ MongoDB（持久化）  
**架构模式**：无状态多节点，支持水平扩展

---

## 核心特性

- **分布式排行榜**：基于 Redis Sorted Set + Lua 脚本，保证原子性操作
- **自动分组机制**：玩家按配置人数上限自动分入独立排行榜组，避免人数过多
- **多期并行**：同一活动支持多期（Round），每期独立结算
- **机器人系统**：当真实玩家进组后自动生成机器人，按配置增长积分，提升竞争感
- **成员索引**：Redis SET 缓存玩家参与的所有排行榜，支持跨业务查询
- **无状态扩展**：任意节点可服务任意玩家，内存数据定期同步到 MongoDB

---

## 项目结构

```
socialserver/
├── cmd/
│   └── main.go                  # 服务启动入口
│
├── internal/
│   ├── server.go                # 服务器生命周期、配置加载
│   ├── rank/                    # 排行榜核心模块
│   │   ├── manager.go           # 全局管理器：活动注册、启动恢复、后台驱动
│   │   ├── types.go             # 公共类型：BizType、BizKey、MemberEntry
│   │   ├── biz_service.go       # 业务接口和适配器
│   │   ├── member_index.go      # 成员索引：查询玩家参与的所有排行榜
│   │   ├── config_loader.go     # 配置加载器
│   │   │
│   │   ├── balloon/             # 气球排行榜业务实现
│   │   │   ├── biz.go           # 业务主类
│   │   │   ├── types.go         # 配置、分组状态等类型
│   │   │   ├── service.go       # Service：核心业务逻辑（积分、排名、结算）
│   │   │   ├── group.go         # 分组管理
│   │   │   ├── store.go         # Redis 缓存层
│   │   │   ├── dao.go           # MongoDB 持久化
│   │   │   ├── robot.go         # 机器人算法
│   │   │   └── service_robot.go # Service 上的机器人操作
│   │   │
│   │   ├── egg/                 # 蛋蛋排行榜业务实现
│   │   │   └── biz.go           # 蛋蛋排行榜业务
│   │   │
│   │   └── engine/              # 通用排行榜引擎
│   │       ├── types.go         # 通用排行榜数据结构
│   │       ├── service.go       # 通用排行榜服务接口
│   │       ├── store.go         # 缓存抽象
│   │       ├── dao.go           # 数据访问抽象
│   │       ├── snapshot.go      # 快照工具函数
│   │       ├── group.go         # 分组管理基类
│   │       ├── robot.go         # 机器人基类
│   │       └── service_robot.go # 机器人处理
│   │
│   └── router/
│       ├── rpc/
│       │   ├── rpc.go           # gRPC 服务启动
│       │   └── social/
│       │       ├── server.go    # RPC 服务处理器
│       │       └── rank_handler.go  # 排行榜 RPC 接口实现
│       │
│       └── http/
│           └── router.go        # HTTP 路由配置
│
├── conf/                        # 环境配置
│   ├── config.yaml              # 本地配置模板
│   ├── .devops.yaml             # 开发环境配置
│   ├── .devops_inter.yaml       # 测试环境配置
│   ├── .devops_production.yaml  # 生产环境配置
│   └── .devops_test.yaml        # CI 测试配置
│
├── scripts/
│   ├── build.sh                 # 编译脚本
│   └── start.sh                 # 启动脚本
│
├── doc/
│   ├── rank_module.md           # 排行榜模块完整手册
│   ├── balloon_rank_design.md   # 气球排行榜设计文档
│   └── rank_real_scenario_test_report.md  # 实战测试报告
│
└── go.mod / go.sum              # 依赖管理
```

---

## 排行榜系统架构

### 数据分层

```
┌────────────────────────────────────────────┐
│       RPC Handler / HTTP Handler           │  请求入口
└────────────────────┬─────────────────────┘
                     │
┌────────────────────▼─────────────────────┐
│         Business Service Layer            │
│  (balloon.Service / egg.Service / ...)    │  业务逻辑
└────────────────────┬─────────────────────┘
                     │
┌────────────────────▼─────────────────────┐
│         Store Layer (Redis Cache)         │  
│    读写缓存，miss 时回源 MongoDB           │
└────────────────────┬─────────────────────┘
                     │
        ┌────────────┴────────────┐
        │                         │
┌───────▼────────┐      ┌────────▼─────┐
│   Redis        │      │   MongoDB    │
│ - 排名数据     │      │ - 活动配置   │
│ - 分组/成员    │      │ - 分组数据   │
│ - 机器人状态   │      │ - 成员记录   │
│ - 原子计数器   │      │ - 机器人数据 │
└────────────────┘      └──────────────┘
```

### 核心模块说明

#### 1. Manager（全局管理器）
- **职责**：活动生命周期管理、服务注册、后台驱动
- **位置**：[manager.go](internal/rank/manager.go)
- **关键方法**：
  - `InitGlobalManager()` - 初始化，从 MongoDB 恢复已有活动
  - `RegisterBalloon()` - 注册新气球排行榜
  - `RegisterEgg()` - 注册新蛋蛋排行榜
  - `tickServices()` - 每秒驱动，处理定时任务
  - `syncLoop()` - 每 30 秒与 MongoDB 同步

#### 2. MemberIndex（成员索引）
- **职责**：维护玩家参与的所有排行榜记录
- **位置**：[member_index.go](internal/rank/member_index.go)
- **存储**：Redis SET，key 格式 `rank:member:{userID}`
- **使用场景**：查询玩家参与的所有活动

#### 3. balloon.Service（气球排行榜）
- **职责**：气球排行榜核心业务逻辑
- **位置**：[balloon/service.go](internal/rank/balloon/service.go)
- **关键方法**：
  - `UpsertScore()` - 更新玩家积分
  - `GetMemberRank()` - 获取玩家排名
  - `GetRankList()` - 获取排行榜前 N 名
  - `Close()` - 结算并关闭活动
  - `TickRobots()` - 机器人积分增长

#### 4. engine.Service（通用排行榜引擎）
- **职责**：提供通用的排行榜操作接口
- **位置**：[engine/service.go](internal/rank/engine/service.go)
- **特点**：基于 Redis Sorted Set + Lua 脚本，支持原子性操作

#### 5. Store 层（缓存+持久化）
- **职责**：Redis 缓存与 MongoDB 持久化协调
- **策略**：
  - **写**：Redis + MongoDB 双写，保证数据一致性
  - **读**：Redis 优先，miss 时回源 MongoDB 并重新填充缓存

---

## 启动流程

```
1. main.go 启动
   └── server.LoadConfig()           # 加载 YAML 配置

2. server.OnInit() 初始化
   ├── redis.InitMainRedis()         # 连接 Redis
   ├── mongodbmodule.Init()          # 连接 MongoDB
   ├── configmgr.LoadConfigs()       # 加载业务配置
   │
   ├── rankservice.InitGlobalManager()
   │   ├── NewRedisService()         # 创建 Redis 排行榜引擎
   │   ├── NewMemberIndex()          # 创建成员索引
   │   ├── NewDAO()                  # 创建 MongoDB DAO
   │   ├── dao.EnsureIndexes()       # 创建数据库索引
   │   └── recoverFromMongo()        # 恢复未过期活动
   │       ├── 读取 balloon_activity 集合
   │       └── 对每个活动调用 RegisterBalloon()
   │
   └── 启动后台任务
       ├── tickLoop()                # 每秒 Tick，驱动机器人增长
       ├── syncLoop()                # 每 30 秒同步数据
       └── subscribeDeleteEvents()   # 监听删除事件
```

---

## RPC 接口

所有 RPC 接口在 [rank_handler.go](internal/router/rpc/social/rank_handler.go) 中实现：

### 1. S2SUpsertScore - 更新排行榜积分
**请求**：
```protobuf
message PBS2SUpsertScoreRequest {
  string bizType              # 业务类型: "balloon" / "egg"
  int32 actId                 # 活动 ID
  int32 userId                # 玩家 ID
  int64 totalScore            # 总积分
  int64 timestamp             # 时间戳
  AvatarInfo avatarInfo       # 玩家信息
}
```

**响应**：返回最新排名信息，包括排名、总分、成员数等

### 2. S2SGetRankList - 获取排行榜列表
**请求**：指定业务、活动、玩家和排名范围
**响应**：返回排行榜前 N 名玩家的完整信息

### 3. GM 接口
**路径**：[internal/router/http](internal/router/http)

---

## 配置管理

### 环境变量支持

主要通过 YAML 配置文件：
- `conf/config.yaml` - 本地开发配置
- `conf/.devops.yaml` - 各环境配置

### 关键配置项

**Redis**：
```yaml
redis:
  - host: localhost
    port: 6379
    password: ""
    index: 0
```

**MongoDB**：
```yaml
mongo:
  database: socialserver
  url: mongodb://localhost:27017
```

**HTTP 服务**：
```yaml
http:
  listenAddr: 0.0.0.0:8080
```

**RPC 服务**：
```yaml
rpc:
  server:
    address: 0.0.0.0:50051
```

---

## 构建与运行

### 编译

```bash
cd socialserver
./scripts/build.sh
```

**构建参数**：
- 使用 git commit hash 作为版本号
- 记录编译时间戳
- 输出到 `./bin/socialserver`

### 启动

```bash
./scripts/start.sh
```

**启动参数**：
- 自动加载 `conf/config.yaml` 配置
- 日志输出到 `../logs/socialserver.log`
- 支持 `-binVersion` 参数查看版本信息

### 监听端口

- **gRPC**：默认 `0.0.0.0:50051`
- **HTTP**：默认 `0.0.0.0:8080`
- **Pprof**：支持性能分析（配置文件中启用）

---

## 核心业务流程

### 玩家积分更新流程

```
GameServer S2S RPC
    ↓
rank_handler.S2SUpsertScore()
    ↓
lookupEngineService() 查找对应排行榜
    ↓
Service.UpsertScore()
    ├─→ 检查活动状态（是否已关闭）
    ├─→ 获取或创建玩家所在分组
    ├─→ 更新积分（Redis Sorted Set）
    ├─→ 触发自动分组（如果人数达到上限）
    └─→ 触发机器人生成（如果首个真实玩家进组）
    ↓
Store.Set() 双写
    ├─→ 写入 Redis（缓存）
    └─→ 写入 MongoDB（持久化）
    ↓
返回最新排名信息给 GameServer
```

### 活动启动流程

```
GM 注册新活动
    ↓
Manager.RegisterBalloon(config)
    ├─→ 创建 Service 实例
    ├─→ 从 MongoDB 恢复已有分组
    ├─→ 从 Redis 恢复缓存数据
    └─→ 注册到全局管理器
    ↓
后台 Tick 循环
    ├─→ tickServices() - 每秒驱动
    │   └─→ TickRobots() - 机器人积分增长
    └─→ syncLoop() - 每 30 秒同步 MongoDB
```

### 活动结算流程

```
活动时间到期 或 GM 手动结算
    ↓
Service.Close()
    ├─→ 停止接收新积分
    ├─→ 生成最终排名
    ├─→ 计算奖励资格
    ├─→ 保存结算数据到 MongoDB
    └─→ 从管理器中移除
    ↓
GameServer 拉取结算数据
    └─→ 发放奖励
```

---

## 关键设计决策

### 1. Redis + MongoDB 双层存储
- **优势**：高效查询 + 数据持久化
- **权衡**：需要保证一致性，采用异步回源机制

### 2. 自动分组算法
- **目标**：避免单个排行榜过大，提升竞争激烈度
- **实现**：玩家达到配置上限时自动切分至新组
- **优势**：支持任意规模玩家

### 3. Lua 脚本保证原子性
- **使用场景**：积分更新、排名查询、分组转移
- **优势**：避免竞态条件，保证数据一致性

### 4. 无状态服务设计
- **优势**：支持水平扩展，任意节点可替换
- **权衡**：状态数据必须持久化

---

## 监控与调试

### 日志位置
```
../logs/socialserver.log          # 应用日志
../logs/socialserver.INFO.log     # 信息级日志
```

### 关键日志点

- **启动**：`social server start...`
- **初始化**：`rank global manager initialized`
- **积分更新**：`S2SUpsertScore resp ok rank=...`
- **后台任务**：`syncLoop working...`
- **异常**：各接口的错误日志

### 性能分析

支持 Pprof 性能分析（若配置启用）：
```
http://localhost:6060/debug/pprof/
```

---

## 依赖项

### 主要库
- `go.mongodb.org/mongo-driver` - MongoDB 驱动
- `google.golang.org/grpc` - gRPC 框架
- `github.com/gin-gonic/gin` - HTTP 框架
- `github.com/redis/go-redis/v9` - Redis 驱动
- `go.uber.org/zap` - 日志库

### 内部库
- `common` - 共享配置、错误定义
- `golib` - 基础组件库（日志、Redis、MongoDB、HTTP）
- `pbcommon` - 共享 Protobuf 定义

---

## 文档

- [排行榜模块完整手册](doc/rank_module.md) - 详细架构和 API
- [气球排行榜设计文档](doc/balloon_rank_design.md) - 业务设计细节
- [实战测试报告](doc/rank_real_scenario_test_report.md) - 性能测试结果

---

## 常见问题

### Q: 支持多少人的排行榜？
**A**: 通过自动分组机制支持任意规模。单个分组通常控制在 30-100 人，自动分裂成多个组。

### Q: 排名是否实时更新？
**A**: 是的。积分更新后立即反映在排名中，基于 Redis Sorted Set 的高性能操作。

### Q: 服务可以扩展吗？
**A**: 可以。所有状态存储在 Redis/MongoDB，任意节点可处理任意玩家，支持水平扩展。

### Q: 数据一致性如何保证？
**A**: 
- Redis 使用 Lua 脚本保证原子性
- 双写 Redis + MongoDB 保证持久化
- 异步同步机制处理不一致情况

---

## 贡献指南

### 代码风格
- 遵循 Go 官方风格指南
- 使用 `go fmt` 格式化代码
- 添加适当的注释说明逻辑

### 提交规范
- 使用清晰的 commit message
- 关键修改需要更新对应的测试和文档

### 测试
```bash
go test ./...
go test -race ./...  # 检查竞态条件
```

---

## 许可证

待补充

---

**最后更新**：2026-07-22  
**维护者**：sunbin
