# socialserver/rank 集成测试报告

**版本**: v3.0  
**生成日期**: 2026-05-14  
**环境**: Go 1.21, Windows AMD64 (12 CPU)  
**时区约定**: 所有时间戳均为 **UTC+0（零时区）毫秒级 Unix 时间戳**

---

## 一、架构层次回顾

```
socialserver/internal/rank/manager.go     ← 全局单例 RankManager（服务注册、Tick 调度）
socialserver/internal/rank/balloon/       ← 气球活动业务排行榜
  service.go                              ← 分组、得分门槛、结算逻辑
common/rank/                              ← 基础排行榜层（内存实现 MemoryService）
  timed.go                                ← 固定窗口 / 周期榜生命周期管理（UTC+0）
  service.go                              ← Service 接口 + MemoryService 实现
  types.go                                ← 类型定义、枚举、错误常量
```

### v3.0 业务层新增
| 类型 | 说明 |
|------|------|
| `balloon.GroupState` | 独立业务分组状态枚举：`open` / `full` / `settled` |
| `rank.Service.UpdateInstance` | 接口级实例元数据更新，消除 timed.go 中的类型断言 |

---

## 二、功能测试结果

```
go test -v -count=1 ./internal/rank/...
```

### 2.1 RankManager 集成测试（manager_test.go）

| 测试用例 | 验证场景 | 结果 |
|----------|----------|------|
| `TestManagerBalloonScenario` | 5用户写入、2组分组正确、用户名次查询、Tick 触发结算、关闭后拒绝写入 | ✅ PASS |

**关键断言**：
- 用户 10001(120分) / 10002(220分) → 组1，ranking: 10002 #1, 10001 #2  
- 用户 10003(180分) / 10004(160分) → 组2，ranking: 10003 #1, 10004 #2  
- `GetMemberRank(10003)` → groupID=2, Rank=1 ✅  
- `manager.Tick(5500)` 触发结算，组1结算后 Rank #1=220分 #2=120分 ✅  
- `UpsertScore(10005, 5600)` 返回 `ErrInstanceNotOpen` ✅

### 2.2 Balloon 业务单元测试（balloon/service_test.go）

| 测试用例 | 验证场景 | 结果 |
|----------|----------|------|
| `TestBalloonServiceAssignsGroupAndRanks` | 写入2用户、分组正确、排名正确、Extra["groupId"]正确 | ✅ PASS |
| `TestBalloonServiceCreatesNewGroupWhenFull` | 3用户×上限2 → 自动创建第2组，第1组状态变为 `GroupStateFull` | ✅ PASS |
| `TestBalloonServiceRejectsClosedActivityAndSettles` | Tick超时触发结算，之后写入返回 `ErrInstanceNotOpen` | ✅ PASS |

**功能测试：4/4 全部通过**

---

## 三、性能测试结果

```
go test -bench=. -benchmem -count=1 ./internal/rank/...
```

| Benchmark | 说明 | 次数 | ns/op | B/op | allocs/op | 对比 v2.0 |
|-----------|------|------|-------|------|-----------|-----------|
| `BenchmarkManagerBalloonScenario` | 1000用户写入50人/组分20组，全量结算 | 412 | **2,927,642** | **2,487,234** | **23,821** | ↓9% B/op；↓8% allocs |

**v2.0 基准（参考）**：2,976,154 ns/op, 2,743,566 B/op, 25,824 allocs/op

---

## 四、关键设计决策

### 时区
所有周期窗口、时间戳均以 **UTC+0** 计算。`CycleRankSpec.TimeZone` 字段已删除，`timed.go` 内硬编码 `time.UTC`，不再依赖本地时区。

### GroupState 语义
`balloon.GroupState` 独立于底层 `rank.InstanceStateType`：
- `GroupStateOpen`：分组接受新成员（RealCount < RankPeopleNum）
- `GroupStateFull`：人数已满，不再分配新成员；底层榜单实例仍处于 `InstanceStateOpen`，可继续更新已有成员分数
- `GroupStateSettled`：结算完成，排名固化

### 接口扩展（UpdateInstance）
`rank.Service.UpdateInstance` 允许原子更新实例元数据（状态、时间戳），Version 自动递增。
`FixedWindowManager` 和 `CycleManager` 的 init→open 状态迁移均通过此接口完成，消除了对 `*MemoryService` 的类型断言。

### 得分更新语义
- 默认仅允许**更优分数**覆盖（desc 榜：新分 > 旧分；asc 榜：新分 < 旧分）
- `RankScoreItem.AllowLower=true` 可强制覆盖（适用于绑定总分场景）

---

## 五、已知局限与后续规划

| 项目 | 状态 |
|------|------|
| Redis/MongoDB 持久化实现 | 未实现（当前纯内存） |
| RPC/HTTP 接口接入排行榜服务 | 未实现 |
| 多实例水平扩展 | 需 Redis 实现后支持 |
| `GetRank` 已结算路径线性查找 O(n) | 可优化为 map 索引，当前量级无需 |

