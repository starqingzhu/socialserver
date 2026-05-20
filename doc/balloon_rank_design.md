# 排行榜分层设计文档

> 版本：v2.0  
> 日期：2026-05-14  
> 作者：bsun  
> 状态：重构方案

---

## 一、重构目标

当前设计把排行榜底层能力、时间规则、气球活动业务规则混在了一起，导致几个问题：

1. 排行榜底层无法复用，任何一个新活动都要复制一套分组、结算、存储逻辑。
2. 开启时间、结束时间、周期生成等时间语义没有沉淀成统一模型。
3. 机器人、奖励、入榜门槛、分组容量这些明显属于业务层的能力，侵入了排行榜核心层。

本次重构后的目标是明确三层职责：

1. 基础排行榜：只提供纯排行榜能力，不携带任何业务属性。
2. 时间型排行榜：在基础排行榜之上，补充时间窗口和周期能力。
3. 业务排行榜：业务侧只做规则装配，复用底层排行榜能力。

---

## 二、总体架构

```
Client / GameServer / 定时任务
            │
            ▼
      Business Rank Layer
  （气球榜、工会榜、赛季榜等）
            │
            ▼
       Timed Rank Layer
  ┌─────────────────────────┐
  │ 1. 固定起止时间排行榜    │
  │ 2. 周期性排行榜          │
  └─────────────────────────┘
            │
            ▼
        Base Rank Layer
    （成员、分数、名次、快照）
            │
            ▼
      Redis + MongoDB
```

### 2.1 分层职责

| 层级 | 负责内容 | 不负责内容 |
|---|---|---|
| Base Rank | 排名写入、排名查询、并列规则、榜单实例读写、快照输出 | 活动门槛、奖励、机器人、周期生成 |
| Timed Rank | 榜单生命周期、开启关闭、结算触发、周期实例生成 | 业务准入、业务展示字段 |
| Business Rank | 入榜规则、分组规则、机器人、奖励、业务接口组装 | 排名底层存储细节 |

结论：以后任何业务排行榜都不直接操作 Redis ZSet，而是通过统一的排行榜底层接口进行读写。

---

## 三、基础排行榜设计

基础排行榜是一个纯能力层，只回答四个问题：

1. 一个榜单实例里有哪些成员。
2. 每个成员当前分数是多少。
3. 当前名次是什么。
4. 当前或结算时的榜单快照是什么。

### 3.1 核心抽象

#### RankDefinition

定义榜单规则模板，但不带具体业务信息。

```go
type RankDefinition struct {
    RankCode       string `bson:"rankCode"`       // 榜单定义编码，唯一标识一类排行榜，例如 balloon_score
    RankName       string `bson:"rankName"`       // 榜单名称，便于配置和管理时识别
    ScoreOrder     string `bson:"scoreOrder"`     // 分值排序方向，desc 表示高分在前，asc 表示低分在前
    TieBreakPolicy string `bson:"tieBreakPolicy"` // 同分排序策略，当前建议固定为 first_enter
    MaxQuerySize   int32  `bson:"maxQuerySize"`   // 单次最大拉榜数量，避免超大区间读取
    CreateTime     int64  `bson:"createTime"`     // 建榜时间，榜单定义首次创建时间
    UpdateTime     int64  `bson:"updateTime"`     // 最后更新时间，榜单定义最近一次修改时间
}
```

字段说明：

1. `RankCode`：榜单模板编码，业务层通过它选择底层排行模板。
2. `RankName`：榜单模板中文名称，方便策划、运营、研发对齐。
3. `ScoreOrder`：定义分数是升序还是降序排序。
4. `TieBreakPolicy`：定义同分时谁排前面，当前文档统一为先进榜优先。
5. `MaxQuerySize`：限制单次查询返回条数，防止底层被大范围扫描打爆。
6. `CreateTime`：榜单定义创建时间。
7. `UpdateTime`：榜单定义最后更新时间。

#### RankInstance

一个可独立读写的榜单实例，是底层真正操作的单位。

```go
type RankInstance struct {
    InstanceId      string `bson:"instanceId"`      // 榜单实例 ID，底层唯一主键
    RankCode        string `bson:"rankCode"`        // 关联的榜单定义编码
    BizId           string `bson:"bizId"`           // 业务侧实例归属标识，例如 actId、seasonId
    State           string `bson:"state"`           // 榜单状态：init / open / closed / settled
    Version         int64  `bson:"version"`         // 版本号，用于并发控制和状态推进
    OpenTime        int64  `bson:"openTime"`        // 开榜时间
    CloseTime       int64  `bson:"closeTime"`       // 封榜时间，到点后禁止写入
    SettleTime      int64  `bson:"settleTime"`      // 结算完成时间
    MemberCount     int64  `bson:"memberCount"`     // 当前榜单成员数
    CreateTime      int64  `bson:"createTime"`      // 实例创建时间
    UpdateTime      int64  `bson:"updateTime"`      // 实例最后更新时间
    LastScoreUpdate int64  `bson:"lastScoreUpdate"` // 最近一次成员分数变更时间
}
```

字段说明：

1. `InstanceId`：单个榜单实例的唯一编号，所有读写都围绕它展开。
2. `RankCode`：说明这个实例使用哪套榜单定义。
3. `BizId`：记录该实例对应的业务归属，便于反查。
4. `State`：当前生命周期状态。
5. `Version`：用于状态机推进和乐观锁控制。
6. `OpenTime`：允许写入和实时查榜的开始时间。
7. `CloseTime`：停止写入的时间。
8. `SettleTime`：最终快照生成并完成结算的时间。
9. `MemberCount`：当前成员总数。
10. `CreateTime`：实例创建时间。
11. `UpdateTime`：实例元数据最后更新时间。
12. `LastScoreUpdate`：最近一次分数变更时间，用于监控和清理。

#### RankMemberSnapshot

底层输出的统一成员快照，不关心成员是玩家、机器人还是别的实体。

```go
type RankMemberSnapshot struct {
    MemberId     int64             `bson:"memberId"`     // 成员 ID，统一使用 int64
    Score        int64             `bson:"score"`        // 当前分数
    Rank         int64             `bson:"rank"`         // 当前名次
    EnterTime    int64             `bson:"enterTime"`    // 首次入榜时间，同分时先进榜排前面
    UpdateTime   int64             `bson:"updateTime"`   // 最近一次分数更新时间
    Sequence     int64             `bson:"sequence"`     // 入榜自增序号，作为兜底稳定排序字段
    Extra        map[string]int64  `bson:"extra"`        // 业务附加整型信息，例如 groupId、robotFlag、zoneId
}
```

字段说明：

1. `MemberId`：成员唯一 ID，统一改为 `int64`，便于和游戏内用户 ID 体系对齐。
2. `Score`：当前参与排序的分数。
3. `Rank`：当前计算出的名次。
4. `EnterTime`：首次进入该榜单实例的时间，用于同分时先进榜优先。
5. `UpdateTime`：最近一次更新分数的时间。
6. `Sequence`：成员首次入榜时分配的递增序号，当时间精度不足时作为兜底排序依据。
7. `Extra`：给业务层预留的附加字段，底层只存不解释，便于快照里携带业务必要信息。

### 3.2 Base Rank 提供的标准能力

#### 写能力

1. AddMember：成员首次入榜。
2. UpsertScore：更新成员分数。
3. BatchUpsertScore：批量更新分数。
4. RemoveMember：删除成员。
5. SealInstance：封榜，不再允许写入。

#### 查能力

1. GetRank：查询某成员当前名次。
2. GetScore：查询某成员当前分数。
3. Range：按名次区间读取榜单。
4. TopN：读取前 N 名。
5. Snapshot：导出完整榜单快照，用于结算和归档。

### 3.3 名次规则

底层统一支持标准竞赛排名：

```text
分数高者在前。
同分同名次。
同分数时，先进榜的成员展示顺序更靠前。
后续名次按前面人数跳号。
示例：100, 100, 90, 80 -> 1, 1, 3, 4
```

同分时的稳定顺序由 `TieBreakPolicy` 决定，仅用于展示顺序，不影响并列名次：

1. `first_enter`：先进入榜单的成员在前。
2. 若 `EnterTime` 相同，则按 `Sequence` 小的在前。

建议：当前排行榜底层统一固定使用 `first_enter`，不要给每个业务开放太多同分策略，否则会增加实现和排障复杂度。

### 3.4 存储设计

#### Redis

| Key 模式 | 类型 | 说明 |
|---|---|---|
| `rank:core:{instanceId}:zset` | ZSet | 排行主体，score 为主分值 |
| `rank:core:{instanceId}:meta` | Hash | 榜单元信息，如 state、memberCount、version |
| `rank:core:{instanceId}:members` | Hash | 榜内所有成员扩展信息，field 为 memberId，value 为序列化后的成员元数据 |

说明：

1. Base Rank 只保存排序所需的最小元信息。
2. 昵称、头像、机器人标记、奖励结果等全部不放在底层排行榜里。
3. 同一榜单的所有成员扩展信息收敛到一个 `members` key 内，避免每个成员一个 key 带来的大量读写和管理成本。
4. 业务展示字段从业务域或缓存域补齐。

推荐的 `members` value 结构：

```json
{
    "memberId": 10001,
    "enterTime": 1747123200000,
    "updateTime": 1747123260000,
    "sequence": 12,
    "extra": {
        "groupId": 5,
        "robotFlag": 0
    }
}
```

读取排行榜时，流程调整为：

1. 先通过 `ZREVRANGE` 或 `ZRANGE` 读取目标区间成员列表。
2. 再对这些 `memberId` 一次性执行 `HMGET rank:core:{instanceId}:members`。
3. 在内存中合并分数、入榜时间、附加字段并生成 `RankMemberSnapshot`。

这样可以把成员元数据读取控制在单 key 多 field 模式，避免海量 key 扫描。

#### MongoDB

##### RankInstanceDoc

```go
type RankInstanceDoc struct {
    InstanceId      string `bson:"instanceId"`
    RankCode        string `bson:"rankCode"`
    BizId           string `bson:"bizId"`
    State           string `bson:"state"`
    OpenTime        int64  `bson:"openTime"`
    CloseTime       int64  `bson:"closeTime"`
    SettleTime      int64  `bson:"settleTime"`
    MemberCount     int64  `bson:"memberCount"`
    CreateTime      int64  `bson:"createTime"`
    UpdateTime      int64  `bson:"updateTime"`
    LastScoreUpdate int64  `bson:"lastScoreUpdate"`
}
```

##### RankSettleSnapshotDoc

```go
type RankSettleSnapshotDoc struct {
    InstanceId string                `bson:"instanceId"`
    SettleTime int64                 `bson:"settleTime"`
    Members    []RankMemberSnapshot  `bson:"members"`
}
```

### 3.5 接口约束

基础排行榜接口入参与出参必须保持业务无关：

```go
type RankService interface {
    OpenInstance(ctx context.Context, instance RankInstance) error
    SealInstance(ctx context.Context, instanceId string) error
    BatchUpsertScore(ctx context.Context, instanceId string, items []RankScoreItem) error
    GetRank(ctx context.Context, instanceId string, memberId int64) (*RankMemberSnapshot, error)
    Range(ctx context.Context, instanceId string, start int64, end int64) ([]RankMemberSnapshot, error)
    Snapshot(ctx context.Context, instanceId string) ([]RankMemberSnapshot, error)
}
```

这里不允许出现 `actId`、`reward`、`robot`、`groupRule` 这类业务字段。

---

## 四、时间型排行榜设计

时间型排行榜建立在 Base Rank 之上，统一处理榜单生命周期，但仍然不处理具体业务规则。

### 4.1 两类时间模型

#### 1. 固定起止时间排行榜

适用于有明确开启和结束时间的活动榜。

```go
type FixedWindowRankSpec struct {
    RankCode   string `bson:"rankCode"`
    OpenTime   int64  `bson:"openTime"`
    CloseTime  int64  `bson:"closeTime"`
    AutoSettle bool   `bson:"autoSettle"`
}
```

特点：

1. 一个配置对应一个或少量明确实例。
2. 到 `OpenTime` 自动开榜，到 `CloseTime` 自动封榜。
3. 适合限时活动榜、赛事榜、赛季榜。

#### 2. 周期性排行榜

适用于按日、周、月或自定义周期不断生成新榜单的场景。

```go
type CycleRankSpec struct {
    RankCode      string `bson:"rankCode"`
    CycleType     string `bson:"cycleType"`     // day / week / month / custom
    TimeZone      string `bson:"timeZone"`
    StartOffset   int32  `bson:"startOffset"`
    DurationSec   int64  `bson:"durationSec"`
    RetainCycles  int32  `bson:"retainCycles"`
}
```

特点：

1. 运行时持续生成新的 `RankInstance`。
2. 每个周期实例有自己的 `instanceId` 和结算快照。
3. 适合周榜、月榜、每日活跃榜。

### 4.2 生命周期状态机

```text
init -> open -> closed -> settled
```

状态说明：

1. `init`：实例已创建，未到开榜时间。
2. `open`：可写入分数、可查询实时榜单。
3. `closed`：禁止写入，允许读取，用于结算前冻结。
4. `settled`：结算快照已生成，业务可基于快照发奖和归档。

### 4.3 Timed Rank 层职责

1. 负责实例创建与实例状态推进。
2. 负责定时开榜、封榜、结算触发。
3. 负责根据周期配置生成新实例。
4. 负责在结算时调用 Base Rank 的 `Snapshot` 生成最终快照。

Timed Rank 不负责：

1. 判定某个玩家是否有资格入榜。
2. 给玩家发奖。
3. 决定机器人如何生成。
4. 组装客户端展示字段。

### 4.4 实例编号建议

统一使用实例级 ID，避免业务代码直接拼 Redis Key。

```text
固定时间榜：{rankCode}:{bizId}:{yyyyMMddHHmmss}
周期榜：{rankCode}:{cycleType}:{periodKey}
业务分组榜：{rankCode}:{bizId}:{groupId}:{periodKey}
```

---

## 五、业务排行榜设计

业务排行榜是对底层能力的装配，不再重写一套排行榜系统。

### 5.1 业务层应该做什么

1. 定义业务榜单配置。
2. 决定哪些用户进入哪个榜单实例。
3. 在合适时机调用底层 `BatchUpsertScore`。
4. 基于结算快照计算奖励。
5. 组装业务接口返回字段。

### 5.2 业务层典型组件

| 组件 | 职责 |
|---|---|
| EligibilityPolicy | 判断用户是否满足入榜条件 |
| BoardRouter | 判断用户进入哪个榜单实例或分组实例 |
| ScoreAdapter | 把业务分值转换成排行榜分值 |
| RewardCalculator | 根据结算快照计算奖励 |
| ViewAssembler | 把排行榜快照组装成客户端展示对象 |

### 5.3 业务层与底层的调用关系

```text
业务事件产生分数
    -> 业务层判定是否入榜
    -> 业务层决定 instanceId
    -> 调用 Base Rank 写分
    -> Timed Rank 在到期时封榜并产出快照
    -> 业务层读取快照做奖励与展示
```

结论：排行榜核心只负责“排”，业务层负责“为什么排、谁能排、排完做什么”。

---

## 六、气球排行榜落地方案

气球排行榜不再被视为“排行榜系统本身”，而是一个业务排行榜实现。

### 6.1 气球榜分层映射

| 能力 | 所属层级 | 说明 |
|---|---|---|
| 排名写入、查榜、导出快照 | Base Rank | 纯排行榜底层能力 |
| 活动开始时间、结束时间、结算时机 | Timed Rank | 固定起止时间排行榜 |
| 入榜门槛、分组、机器人、奖励 | Balloon Business Rank | 气球活动业务规则 |

### 6.2 气球榜的业务对象

#### BalloonRankConfig

```go
type BalloonRankConfig struct {
    ActId                 int32 `bson:"actId"`
    RankPeopleNum         int32 `bson:"rankPeopleNum"`
    BalloonRankOpenToken  int64 `bson:"balloonRankOpenToken"`
    OpenTime              int64 `bson:"openTime"`
    CloseTime             int64 `bson:"closeTime"`
}
```

#### BalloonRankGroup

```go
type BalloonRankGroup struct {
    ActId         int32  `bson:"actId"`
    GroupId       int32  `bson:"groupId"`
    InstanceId    string `bson:"instanceId"`
    State         string `bson:"state"`       // open / sealed / settled
    RealCount     int32  `bson:"realCount"`
    RobotCount    int32  `bson:"robotCount"`
    OpenTime      int64  `bson:"openTime"`
    CloseTime     int64  `bson:"closeTime"`
    SettleTime    int64  `bson:"settleTime"`
}
```

说明：

1. `GroupId` 是业务分组概念，不是底层排行榜概念。
2. 每个分组映射一个底层 `RankInstance`。
3. 业务层维护 `userId -> groupId -> instanceId` 的关系。

### 6.3 气球榜运行流程

#### 入榜

```text
玩家积分变化
    -> Balloon 业务层判断累计积分是否达到 balloonRankOpenToken
    -> 若未入组，则通过 BoardRouter 分配 groupId
    -> 找到该 groupId 对应的 instanceId
    -> 调用 Base Rank.AddMember / BatchUpsertScore
```

#### 分组

分组逻辑保留在业务层：

1. 优先找当前 `open` 且未满的业务分组。
2. 若已满，则创建新的业务分组。
3. 新分组创建时，同时创建一个新的底层 `RankInstance`。
4. 分组满员后，业务分组置为 `sealed`，但底层榜单仍可继续更新组内成员积分，直到活动关闭。

#### 机器人

机器人完全属于业务层，底层只把机器人看作普通成员：

1. 业务层决定什么时候投放机器人。
2. 业务层决定机器人分数增长规则。
3. 底层只接收 `memberId` 和 `score`，不区分真人与机器人。
4. 需要区分时，可由业务层在 `RankMemberSnapshot.Extra` 或业务元数据中标记。

#### 结算

```text
到达活动结束时间
    -> Timed Rank 将各 instance 封榜并导出快照
    -> Balloon 业务层读取快照
    -> 过滤机器人，计算真实玩家奖励
    -> 写入 BalloonRankResult
    -> 通知 GameServer 发奖
```

### 6.4 气球榜保留的业务表

#### BalloonRankResult

```go
type BalloonRankResult struct {
    ActId       int32        `bson:"actId"`
    GroupId     int32        `bson:"groupId"`
    UserId      string       `bson:"userId"`
    IsRobot     bool         `bson:"isRobot"`
    FinalScore  int64        `bson:"finalScore"`
    DisplayRank int32        `bson:"displayRank"`
    RealRank    int32        `bson:"realRank"`
    Rewards     []RewardItem `bson:"rewards"`
    SettleTime  int64        `bson:"settleTime"`
}
```

该表属于业务结算结果，不属于基础排行榜层。

---

## 七、接口设计建议

### 7.1 底层 RPC

建议抽象为通用排行榜 RPC，而不是直接定义成气球榜 RPC。

```protobuf
service RankService {
  rpc BatchUpsertScore (PBRankBatchUpsertScoreRequest)
      returns (PBRankBatchUpsertScoreResponse);

  rpc GetRankList (PBRankListRequest)
      returns (PBRankListResponse);

  rpc GetMemberRank (PBRankMemberRequest)
      returns (PBRankMemberResponse);

  rpc SettleInstance (PBRankSettleRequest)
      returns (PBRankSettleResponse);
}
```

核心入参应该围绕 `instanceId`，而不是直接围绕 `actId`。

如果需要按成员查询名次，成员 ID 类型也应统一使用 `int64`，与底层结构保持一致。

### 7.2 气球业务 RPC

气球业务层可以保留自己的业务接口，但内部翻译到底层接口：

1. `S2SBalloonScoreUpdate`：接收气球业务分数，内部定位 `instanceId` 后调用底层。
2. `S2SBalloonSettle`：接收活动结算触发，内部批量结算该活动下所有 `instanceId`。
3. `/social/balloon/rank/list`：查询业务榜单并补齐昵称、头像、奖励信息。

---

## 八、实现建议

### 8.1 推荐目录结构

```text
internal/
  rank/
    core/        // Base Rank
    timed/       // Fixed window / cycle rank
    biz/
      balloon/   // 气球排行榜业务实现
```

### 8.2 推荐实现顺序

1. 先实现 `core`，提供统一的实例读写、拉榜、快照能力。
2. 再实现 `timed`，把固定时间榜和周期榜生命周期跑通。
3. 最后把现有气球榜逻辑迁入 `biz/balloon`，只保留业务规则。

### 8.3 迁移原则

1. 原有 `balloon:rank:*` Redis Key 不再作为长期架构基准。
2. 新实现全部以 `instanceId` 为主键。
3. 与活动强相关的配置和结果表继续保留在气球业务域。
4. 机器人逻辑不进入 `core` 和 `timed`。

---

## 九、收益

重构后会有几个直接收益：

1. 新活动只需要写业务层，不需要再造排行榜底座。
2. 固定时间榜和周期榜的时间语义统一，后续周榜、月榜、赛季榜都能复用。
3. 机器人、奖励、分组这些高变化规则被隔离在业务层，底层稳定性更高。
4. 存储模型统一为 `RankDefinition + RankInstance + Snapshot`，运维和排障更简单。

---

## 十、待确认项

| # | 问题 | 建议 |
|---|---|---|
| 1 | 同分时展示顺序按先达成、后达成还是固定序列？ | 统一固化到 `TieBreakPolicy` |
| 2 | GameServer 上报的是全量分数还是增量分数？ | 建议统一上报全量，底层取 max |
| 3 | 气球榜分组满员后是否还允许新用户进入新组？ | 建议允许，直到活动关闭 |
| 4 | 机器人是否参与客户端展示名次但不参与发奖？ | 建议参与展示名次，不参与真实奖励 |
| 5 | 结算由 SocialServer 主动回调还是 GameServer 主动拉取？ | 二选一，但不影响底层设计 |

以上确认后，可以进入代码实现阶段。
