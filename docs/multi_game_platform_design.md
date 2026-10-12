# SocialServer 多游戏平台化改造方案

## 1. 设计原则

1. **bizId 内嵌 gameId**：SocialServer 的核心命名空间是 `bizId = {bizType}_{actID}`，多游戏场景下改为 `bizId = g{gameId}_{bizType}_{actID}`，天然隔离所有 Redis/MongoDB 数据，不侵入 `common/rank` 底层引擎。
2. **rankCode 内嵌 gameId**：`rankCode` 是 `commonrank.Service` 的 Redis Key 组成部分，内嵌 gameId 后自动隔离 `rank:def`、`rank:inst`、`rank:mb` 等全局 Key。
3. **不改 proto**：gameId 通过 gRPC metadata `x-game-id` 透传，与 GameServer/PayServer 方案一致。
4. **成员索引隔离**：`rank:member_index:{userId}` 增加 gameId 前缀，避免同一用户跨游戏排行记录互相覆盖。

---

## 2. 服务定位与数据边界

SocialServer 承载排行榜全生命周期：

```
创建配置 ──→ 注册引擎 ──→ 玩家上榜 ──→ 查询排名 ──→ 结算 ──→ 领奖
```

多游戏场景下：
- 游戏 A 的 `balloon_1001` 排行榜与游戏 B 的 `balloon_1001` 是完全独立的两个活动
- 同一 userId 在游戏 A 排行第 1，在游戏 B 可能未上榜，数据必须隔离
- 配置、分组、成员映射、积分、结算快照、领奖记录全部按 gameId 隔离

---

## 3. gameId 接收与透传

### 3.1 gRPC metadata 提取

```go
// socialserver/internal/router/rpc/social/server.go
import (
    "strconv"
    "google.golang.org/grpc/metadata"
)

const GameIdMetadataKey = "x-game-id"

func extractGameId(ctx context.Context) int32 {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok { return 0 }
    vals := md.Get(GameIdMetadataKey)
    if len(vals) == 0 { return 0 }
    id, _ := strconv.ParseInt(vals[0], 10, 32)
    return int32(id)
}

// grpcDispatch 中注入 GameId
func grpcDispatch[Req, Resp proto.Message](ctx context.Context, cmd string, req Req) (Resp, error) {
    var zero Resp
    payload, err := proto.Marshal(req)
    if err != nil { return zero, grpcstatus.Error(codes.Internal, err.Error()) }
    meta := libdispatch.Meta{
        Protocol: libdispatch.ProtocolGRPC,
        GameId:   extractGameId(ctx),
    }
    respBytes, dispatchErr := dispatch.Main.Dispatch(ctx, meta, cmd, payload)
    // ...
}
```

### 3.2 Meta → Handler 透传

```go
// socialserver/internal/handler/rank.go（或新增 handler）
func handleUpsertScore(_ context.Context, meta libdispatch.Meta, req *pb.PBS2SUpsertScoreRequest, resp *pb.PBS2SUpsertScoreResponse) error {
    gameId := meta.GameId
    svc := rankservice.GetGlobalManager().GetEngineService(gameId, BizType(req.BizType), req.ActId)
    // ...
}
```

---

## 4. 核心隔离策略：bizId / rankCode 内嵌 gameId

### 4.1 改造前

```go
rankCode := fmt.Sprintf("%s_score_%d", bizType, actID)
bizId := fmt.Sprintf("%s_%d", bizType, actID)   // 如 "balloon_1001"
bizKey := NewBizKey(bizType, actID).String()      // 如 "balloon:1001"
```

### 4.2 改造后

```go
// socialserver/internal/rank/manager.go

func buildRankCode(gameId int32, bizType BizType, actID int32) string {
    return fmt.Sprintf("g%d_%s_score_%d", gameId, bizType, actID)
}

func buildBizId(gameId int32, bizType BizType, actID int32) string {
    return fmt.Sprintf("g%d_%s_%d", gameId, bizType, actID)
}

func buildBizKey(gameId int32, bizType BizType, actID int32) string {
    return fmt.Sprintf("g%d_%s:%d", gameId, bizType, actID)
}
```

**效果**：
- `rankCode = "g1_balloon_score_1001"` → Redis `rank:def:g1_balloon_score_1001` 自动隔离
- `bizId = "g1_balloon_1001"` → 所有 engine.Store 的 Redis Key（`rank:meta:{bizId}`、`rank:mb:{instanceId}` 等）自动隔离
- MongoDB 查询 `filter := bson.M{"bizId": bizId}` 自动隔离

### 4.3 Manager 层方法签名调整

```go
// socialserver/internal/rank/manager.go

func (m *Manager) Register(ctx context.Context, gameId int32, bizType BizType, cfg engine.Config) error {
    cfg.RankCode = buildRankCode(gameId, bizType, cfg.ActID)
    cfg.BizType = string(bizType)
    // 后续逻辑不变，bizId / bizKey / rankCode 已含 gameId
    key := buildBizKey(gameId, bizType, cfg.ActID)
    // ...
}

func (m *Manager) GetService(gameId int32, bizType BizType, actID int32) RankBizService {
    key := buildBizKey(gameId, bizType, actID)
    // ...
}

func (m *Manager) GetEngineService(gameId int32, bizType BizType, actID int32) *engine.Service {
    key := buildBizKey(gameId, bizType, actID)
    // ...
}

func (m *Manager) RemoveService(gameId int32, bizType BizType, actID int32) error {
    key := buildBizKey(gameId, bizType, actID)
    // ...
}

func (m *Manager) UpdateService(gameId int32, bizType BizType, actID int32, cfg engine.Config) error {
    key := buildBizKey(gameId, bizType, actID)
    // ...
}
```

### 4.4 成员索引隔离

```go
// socialserver/internal/rank/member_index.go

func (mi *MemberIndex) trackKey(userID int64) string {
    // 改造前: return fmt.Sprintf("rank:member_index:%d", userID)
    // 需要 gameId，但 MemberIndex 是全局单例
}
```

**方案**：`MemberIndex` 改为按 gameId 分区，或 Key 增加 gameId 前缀。

推荐 **Key 增加 gameId**：

```go
func (mi *MemberIndex) trackKey(gameId int32, userID int64) string {
    return fmt.Sprintf("g%d:rank:member_index:%d", gameId, userID)
}
```

对应 `Track`、`Lookup`、`RemoveUserEntries` 等方法增加 `gameId int32` 参数。

---

## 5. Redis 数据隔离

由于 bizId / rankCode 已内嵌 gameId，以下 Key 自动隔离，**无需额外前缀**：

| Key 类型 | 示例（改造后） | 说明 |
|---------|--------------|------|
| `rank:def:{rankCode}` | `rank:def:g1_balloon_score_1001` | 排行榜定义 |
| `rank:meta:{bizId}` | `rank:meta:g1_balloon_1001` | 活动元数据 |
| `rank:mb:{instanceId}` | `rank:mb:g1_balloon_score_1001#...` | 成员榜单 |
| `rank:members:{bizId}` | `rank:members:g1_balloon_1001` | 成员分组映射 |
| `rank:groups:{bizId}` | `rank:groups:g1_balloon_1001` | 分组信息 |
| `rank:inst:{instanceId}` | `rank:inst:g1_balloon_score_1001#...` | 榜单实例 |

**需要手动加前缀的 Key**：

| Key | 改造后 | 说明 |
|-----|--------|------|
| `rank:active_services` | `g{gameId}:rank:active_services` | 活跃服务注册表 |
| `rank:member_index:{userId}` | `g{gameId}:rank:member_index:{userId}` | 成员索引 |
| `rank:create_channel` | 保持全局（广播用） | 创建广播频道 |
| `rank:delete_channel` | 保持全局（广播用） | 删除广播频道 |

> `rank:active_services` 中的成员是 `{bizId}:{deadline}`，bizId 已含 gameId，所以注册表内容天然隔离。但注册表 Key 本身若不加前缀，SMEMBERS 会返回所有游戏的混合列表，需按前缀过滤。更简单的做法是为每个 gameId 维护独立的注册表 Key：`g{gameId}:rank:active_services`。

---

## 6. MongoDB 数据隔离

### 6.1 Database-per-game

```go
// socialserver/internal/rank/engine/dao.go

func (d *DAO) databaseFor(gameId int32) string {
    if gameId == 0 {
        return d.dbName // 迁移期兜底
    }
    def := registry.MustGet(gameId)
    return def.MongoDatabase
}
```

所有 `d.session().Find(d.dbName, ...)` 改为 `d.session().Find(d.databaseFor(gameId), ...)`。

由于 DAO 方法内不直接持有 gameId，需从调用链传入。建议在 `engine.Service` 初始化时绑定 `gameId`，DAO 通过 Service 获取。

### 6.2 集合索引

索引字段 `bizId` 已含 gameId，查询自动命中索引，无需改动。

---

## 7. 配置层隔离

### 7.1 排行榜配置文件

当前 `config/` 下的排行榜参数（`RankPeopleNum`、`OpenToken`、`RobotTiers` 等）按 `bizType` 从文件加载：

```go
func FillConfigFromFiles(bizType BizType, cfg *engine.Config) error { ... }
```

多游戏场景下，不同游戏的同 `bizType` 可能需要不同参数（如游戏 A 的 balloon 排行榜取前 100 名，游戏 B 取前 50 名）。

**方案**：配置文件按 gameId 分目录或分文件：

```
config/
├── rank/
│   ├── g1/
│   │   ├── balloon.json
│   │   └── egg.json
│   └── g2/
│       ├── balloon.json
│       └── egg.json
```

`FillConfigFromFiles` 增加 `gameId` 参数：

```go
func FillConfigFromFiles(gameId int32, bizType BizType, cfg *engine.Config) error {
    path := fmt.Sprintf("config/rank/g%d/%s.json", gameId, bizType)
    // ...
}
```

若多游戏初期参数相同，可降级为共享配置，优先读取 `config/rank/g{gameId}/{bizType}.json`，不存在时 fallback 到 `config/rank/default/{bizType}.json`。

---

## 8. 周期排行榜特殊处理

周期排行榜的 `periodic.Handler` 管理 `logicalKey`（即 `bizKey`）。改造后 `logicalKey` 已含 gameId：

```go
// 改造前: logicalKey = "balloon:1001"
// 改造后: logicalKey = "g1_balloon:1001"
```

`periodic.State` 的 Redis 存储 Key（如 `rank:periodic:{logicalKey}`）自动隔离，无需额外处理。

---

## 9. GM 接口

SocialServer 当前无独立 HTTP GM 接口，所有 GM 操作通过 GameServer → SocialServer 的 gRPC 调用完成。GameServer 的 GM handler 已在主文档中规划增加 `gameId` 透传，SocialServer 侧只需在 RPC handler 中从 Meta 提取即可。

若未来需要直接暴露 HTTP GM 接口，需：
1. 请求体/Header 中携带 `gameId`
2. 调用 Manager 方法时传入 gameId

---

## 10. 实施步骤

| 步骤 | 内容 | 文件 |
|------|------|------|
| 1 | RPC Server 提取 metadata gameId 注入 Meta | `socialserver/internal/router/rpc/social/server.go` |
| 2 | 增加 buildRankCode / buildBizId / buildBizKey 工具函数 | `socialserver/internal/rank/manager.go` |
| 3 | Manager 所有公开方法增加 gameId 参数，替换 bizKey 构造 | `socialserver/internal/rank/manager.go` |
| 4 | MemberIndex Key 增加 gameId 前缀 | `socialserver/internal/rank/member_index.go` |
| 5 | 活跃服务注册表按 gameId 分 Key | `socialserver/internal/rank/manager.go` |
| 6 | DAO 层 databaseFor 适配 | `socialserver/internal/rank/engine/dao.go` |
| 7 | 配置文件加载增加 gameId 参数 | `socialserver/internal/rank/config_loader.go` |
| 8 | 回归测试：同 userId 跨游戏参与同名活动互不干扰 |

---

## 11. 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| bizId 格式变更导致 Redis 存量数据失效 | 升级后旧排行榜数据丢失 | 灰度迁移：新活动用新格式，旧活动保持旧格式运行至结束；或脚本批量 rename Redis Key |
| rankCode 变更导致 commonrank.Service 的 Redis Key 全部变化 | 同左 | 同左，旧 rankCode 数据自然过期（7 天 TTL） |
| 配置文件按 gameId 分目录后维护成本增加 | 多游戏配置管理复杂 | 提供配置继承机制：g{gameId} 不存在时 fallback 到 default |
| MongoDB databaseFor 需从 Service 获取 gameId | DAO 与 Service 耦合加深 | 在 engine.Service 初始化时将 gameId 写入 DAO 结构体 |
