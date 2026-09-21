package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonrank "common/rank"
	"golib/gpool"
	goredis "golib/redis"

	"github.com/alicebob/miniredis/v2"
)

// 本文件用真实 engine.Service.Tick 代码路径（真实 Redis 命令打到 miniredis）压测
// gpool 的 workers/queueLen 取值，而不是用 time.Sleep 占位。
//
// 生产参数来源：config/RankBase.json 的 rankPeopleNum=30（每分组 30 人），
// 所以一个 10 万人活动 ≈ 3333 个分组，而 Tick 的 tickAllRobots 是**在单个任务内串行遍历全部分组**的。
// 这决定了瓶颈形状：并发度 = service 数，单任务耗时 = 分组数 × 每组 Redis 往返数。

const (
	benchRankPeopleNum  = 30
	benchRobotsPerGroup = benchRankPeopleNum - 1
	benchRealScore      = 1000
	benchRobotScore     = 100
)

// countingRankService 统计 Service 层实际发出的 Redis 往返次数，
// 用来量化"一次 Tick 到底打了几次 Redis"，而不只是看墙钟时间。
type countingRankService struct {
	commonrank.Service
	rangeCalls  atomic.Int64
	upsertCalls atomic.Int64
}

func (c *countingRankService) Range(ctx context.Context, instanceID string, start, end int64) ([]commonrank.RankMemberSnapshot, error) {
	c.rangeCalls.Add(1)
	return c.Service.Range(ctx, instanceID, start, end)
}

func (c *countingRankService) BatchUpsertScore(ctx context.Context, instanceID string, items []commonrank.RankScoreItem) error {
	c.upsertCalls.Add(1)
	return c.Service.BatchUpsertScore(ctx, instanceID, items)
}

func benchRedisConfig(addr string) *goredis.RedisConfig {
	// 与生产同一组连接池参数（golib/redis.NewRedisConfig 的默认值），
	// 否则压出来的最优 workers 会被测试里更小的池尺寸扭曲。
	return &goredis.RedisConfig{
		RedisAddrs:     []string{addr},
		RedisDBIndex:   1,
		RedisMaxIdle:   20,
		RedisMaxActive: 200,
		RedisPoolSize:  100,
	}
}

// benchShape 描述一个压测 Service 的业务形状。真实形状与「放大形状」的差别很大：
// 线上 RobotRank.json 每个 bizType 每分组只有 5 个机器人、growTokenCd ≥ 600s，
// 因此稳态 Tick 基本只读不写；本文件的默认形状（29 机器人 + cd=0）是刻意放大的上界。
type benchShape struct {
	groups   int   // 分组数
	robots   int   // 每分组机器人数
	growCdMs int64 // 机器人增长冷却（0 = 每个 tick 都写回，即最坏情况）
}

// newBenchServiceOn 是 benchShape 的默认（放大）形状包装：groups 个分组、每分组
// 29 个机器人、cd=0。
func newBenchServiceOn(tb testing.TB, rdb *goredis.Redis, rs commonrank.Service, bizType, rankCode string, actID int32, groups int) *Service {
	return newBenchServiceShaped(tb, rdb, rs, bizType, rankCode, actID, benchShape{
		groups: groups, robots: benchRobotsPerGroup, growCdMs: 0,
	})
}

// newBenchServiceShaped 在给定的 Redis 客户端上建一个 Service，并预先造好 sh.groups 个
// 「已满」分组（每分组 1 个真实玩家 + sh.robots 个机器人），使一次 Tick 必然走
// tickAllRobots 且遍历全部分组。
//
// actID 必须每个 Service 各不相同：bizId = "{bizType}_{actID}" 是实例 ID 与全部
// rank key 的组成部分，actID 相同会让多个 Service 撞在同一批 key 上（实例重复创建）。
//
// 直接构造持久化状态而不用 UpsertScore 灌数据：ensureGroupLocked 每建一个新成员都会
// LoadGroups 一次，灌 G 个分组是 O(G²) 的 Redis 往返，会把压测时间全花在建数据上。
func newBenchServiceShaped(tb testing.TB, rdb *goredis.Redis, rs commonrank.Service, bizType, rankCode string, actID int32, sh benchShape) *Service {
	tb.Helper()
	ctx := context.Background()
	now := time.Now().UnixMilli()
	groups := sh.groups

	cfg := Config{
		BizType:       bizType,
		ActID:         actID,
		RankCode:      rankCode,
		RankPeopleNum: benchRankPeopleNum,
		OpenTime:      now - time.Hour.Milliseconds(),
		CloseTime:     now + 24*time.Hour.Milliseconds(),
		GameEndTime:   now + 24*time.Hour.Milliseconds(),
		RobotTiers: []RobotTierCfg{{
			TierID:             1,
			Num:                int32(sh.robots),
			DefaultTokenMin:    benchRobotScore,
			DefaultTokenMax:    benchRobotScore,
			GrowTokenCdMs:      sh.growCdMs,
			GrowTokenMinBps:    9000,
			GrowTokenMaxBps:    11000,
			MaxToken:           1_000_000,
			MaxDifferenceToken: 1_000_000,
			LockTokenTimeMs:    60_000,
		}},
		RobotInfos: []RobotInfoEntry{{InfoID: 1, Name: "robot"}},
	}
	def := commonrank.Rank{
		RankCode:       rankCode,
		RankName:       bizType + "_rank",
		ScoreOrder:     commonrank.ScoreOrderDesc,
		TieBreakPolicy: commonrank.TieBreakPolicyFirstEnter,
		CreateTime:     cfg.OpenTime,
		UpdateTime:     cfg.OpenTime,
	}
	if err := rs.RegisterRank(ctx, def); err != nil {
		tb.Fatalf("RegisterRank: %v", err)
	}
	svc, err := NewService(rs, cfg, rdb, nil, WithRankDef(def))
	if err != nil {
		tb.Fatalf("NewService: %v", err)
	}
	svc.WarmUp(ctx)

	for gid := 1; gid <= groups; gid++ {
		groupID := int32(gid)
		instID := svc.groupInstanceID(groupID)

		items := make([]commonrank.RankScoreItem, 0, benchRankPeopleNum)
		items = append(items, commonrank.RankScoreItem{
			MemberId: 100_000 + int64(gid),
			Score:    benchRealScore,
			AtTime:   now,
		})
		for i := 0; i < sh.robots; i++ {
			items = append(items, commonrank.RankScoreItem{
				MemberId: robotMemberID(groupID, int32(i)),
				Score:    benchRobotScore,
				AtTime:   now,
			})
		}
		if err := rs.OpenInstance(ctx, commonrank.RankInstance{
			InstanceId:  instID,
			RankCode:    rankCode,
			BizId:       svc.bizId(),
			State:       commonrank.InstanceStateOpen,
			OpenTime:    cfg.OpenTime,
			CloseTime:   cfg.CloseTime,
			GameEndTime: cfg.GameEndTime,
			CreateTime:  now,
			UpdateTime:  now,
		}); err != nil {
			tb.Fatalf("OpenInstance group %d: %v", gid, err)
		}
		if err := rs.BatchUpsertScore(ctx, instID, items); err != nil {
			tb.Fatalf("BatchUpsertScore group %d: %v", gid, err)
		}

		robots := make([]*robotState, sh.robots)
		for i := range robots {
			robots[i] = &robotState{
				MemberID: robotMemberID(groupID, int32(i)),
				TierID:   1,
				InfoID:   1,
				Score:    benchRobotScore,
			}
		}

		svc.mu.Lock()
		g := &Group{
			GroupID:    groupID,
			InstanceID: instID,
			State:      GroupStateFull,
			RealCount:  1,
			RobotCount: int32(sh.robots),
		}
		svc.groups = append(svc.groups, g)
		_ = svc.store.SaveGroup(g)
		_ = svc.store.SaveRobots(groupID, robots)
		svc.mu.Unlock()
	}
	return svc
}

// clearTickCaches 清空第 02 条的 2s 软缓存，强制下一次 Tick 走「冷路径」（即真正打 Redis 的那条）。
// 缓存用 time.Now() 判定，压测跑得比 2s 快，不清就会一直命中缓存、压出几乎零 IO 的假数据。
func clearTickCaches(svc *Service) {
	svc.cacheMu.Lock()
	svc.groupsCache = nil
	svc.groupsCacheExpiry = time.Time{}
	svc.robotsCache = nil
	svc.scoreCache = nil
	svc.cacheMu.Unlock()
}

// benchNow 是跨所有子基准共享的单调递增逻辑时钟（毫秒），每次取用前进 1 秒。
//
// 必须全局单调而不是每个子基准各自 time.Now() 起算：TryLockRobotTick 的锁键里带秒数
// （rank:robot_tick:{bizId}:{sec}，TTL 3s），相邻子基准的秒区间一旦重叠，后者就会命中
// 前者残留的锁，整个 tickAllRobots 被跳过 —— 压出「worker 越多越快」的假数据。
var benchNow atomic.Int64

func nextBenchNow() int64 {
	if benchNow.Load() == 0 {
		benchNow.CompareAndSwap(0, time.Now().UnixMilli())
	}
	return benchNow.Add(1000)
}

// BenchmarkRedisRTT 用一次 GetInstance（恰好 1 个 Redis 往返）标定本机 loopback RTT，
// 供把下面的结果外推到生产网络 RTT 使用。
func BenchmarkRedisRTT(b *testing.B) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatalf("miniredis: %v", err)
	}
	b.Cleanup(mr.Close)
	rdb := goredis.NewRedis(benchRedisConfig(mr.Addr()))
	rs := commonrank.NewRedisService(rdb)
	svc := newBenchServiceOn(b, rdb, rs, "rtt", "rtt_score_1", 1, 1)
	instID := svc.groupInstanceID(1)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rs.GetInstance(ctx, instID); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTickColdByGroupCount 测「一次冷 Tick」的耗时与 Redis 往返数随分组数的增长。
// 这是整条链路的主导项：单任务耗时 = 分组数 × 每组往返数，与池大小无关。
func BenchmarkTickColdByGroupCount(b *testing.B) {
	for _, groups := range []int{1, 10, 50, 200} {
		b.Run(fmt.Sprintf("groups=%d", groups), func(b *testing.B) {
			mr, err := miniredis.Run()
			if err != nil {
				b.Fatalf("miniredis: %v", err)
			}
			b.Cleanup(mr.Close)
			rdb := goredis.NewRedis(benchRedisConfig(mr.Addr()))
			counting := &countingRankService{Service: commonrank.NewRedisService(rdb)}
			svc := newBenchServiceOn(b, rdb, counting, "cold", fmt.Sprintf("cold_score_%d", groups), 1, groups)

			now := nextBenchNow()
			rangeBefore := counting.rangeCalls.Load()
			upsertBefore := counting.upsertCalls.Load()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				now = nextBenchNow() // 推进一秒：TryLockRobotTick 是 per-second 的 SETNX，不推进则后续 Tick 直接短路
				clearTickCaches(svc)
				if err := svc.Tick(context.Background(), now); err != nil {
					b.Fatalf("Tick: %v", err)
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(counting.rangeCalls.Load()-rangeBefore)/float64(b.N), "range-calls/tick")
			b.ReportMetric(float64(counting.upsertCalls.Load()-upsertBefore)/float64(b.N), "upsert-calls/tick")
		})
	}
}

// BenchmarkTickCostDecomposition 把「一次冷 Tick」拆成两个最重的子项，确认谁在主导：
//   - Range(0,-1)：GetInstance + 全量成员 HGETALL + GetRank（3 个往返 + JSON）
//   - BatchUpsertScore：名字叫 Batch，实现却是**逐个 item 一次 Lua Eval**（见
//     common/rank/service_redis.go:421-454），机器人越多往返越多。
// 结论决定优化方向：若 BatchUpsertScore 占大头，那么池调多大都救不了单任务耗时，
// 该优化的是「一次 Lua 写完整批」而不是 gpool 的 workers。
func BenchmarkTickCostDecomposition(b *testing.B) {
	const groups = 10
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatalf("miniredis: %v", err)
	}
	b.Cleanup(mr.Close)
	rdb := goredis.NewRedis(benchRedisConfig(mr.Addr()))
	rs := commonrank.NewRedisService(rdb)
	svc := newBenchServiceOn(b, rdb, rs, "decomp", "decomp_score_1", 1, groups)
	ctx := context.Background()

	now := time.Now().UnixMilli()
	items := make([]commonrank.RankScoreItem, 0, benchRobotsPerGroup)
	for i := 0; i < benchRobotsPerGroup; i++ {
		items = append(items, commonrank.RankScoreItem{
			MemberId: robotMemberID(1, int32(i)),
			Score:    benchRobotScore + int64(i),
			AtTime:   now,
		})
	}
	instID := svc.groupInstanceID(1)

	b.Run("Range", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := rs.Range(ctx, instID, 0, -1); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("BatchUpsertScore-29-items", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := rs.BatchUpsertScore(ctx, instID, items); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("BatchUpsertScore-1-item", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := rs.BatchUpsertScore(ctx, instID, items[:1]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPoolOverheadVsWorkers 隔离「池自身开销随 workers 增长」这件事，完全不碰 Redis。
//
// 起因：真机扫描里 N=32、w=500 比 w=32 慢 46%，但 N=32 时只有 32 个任务，
// 多出的 468 个 worker 本应完全闲置、不该有影响。若开销真来自池/调度而非 Redis，
// 那么把任务体换成等长的 time.Sleep（3.4ms = 实测真实单次 Tick 耗时）后，
// 退化应当同样复现 —— 本基准就是判据：
//
//	w≥32 时批次 ≈ 3.4ms（32 个任务真并发）；若 w=500 明显更慢，则退化与 Redis 无关。
func BenchmarkPoolOverheadVsWorkers(b *testing.B) {
	const (
		tasks       = 32
		taskLatency = 3400 * time.Microsecond
	)
	for _, workers := range []int{8, 16, 32, 48, 64, 128, 256, 500} {
		b.Run(fmt.Sprintf("tasks=%d/w=%d", tasks, workers), func(b *testing.B) {
			pool := gpool.New("ovhbench", workers, 5120)
			defer pool.Close(context.Background())
			ctx := context.Background()

			var skipped atomic.Int64

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				for j := 0; j < tasks; j++ {
					wg.Add(1)
					if err := pool.SubmitWaitTimeout(ctx, func() {
						defer wg.Done()
						time.Sleep(taskLatency)
					}, 200*time.Millisecond); err != nil {
						wg.Done()
						skipped.Add(1)
					}
				}
				wg.Wait()
			}
			b.StopTimer()

			if sk := skipped.Load(); sk > 0 {
				b.Fatalf("池饱和导致 %d 次提交被跳过", sk)
			}
		})
	}
}

// BenchmarkTickBatchPool 是本文件的主结论来源：镜像 manager.tickServices 的批次形状
// （单生产者按序 SubmitWaitTimeout 提交 N 个 Service 的 Tick，然后 wg.Wait），
// 扫描 N（并发服务数）× workers × queueLen，看批次墙钟时间在哪里停止改善。
//
// 两个 redis 模式，用来把「池的维度」与「共享后端串行化」分开：
//   - shared：N 个 Service 共用一台 miniredis（≈生产单节点/单分片行为）。
//     真实 Redis 单线程处理命令，所以这里池开多大都不会更快——瓶颈在后端吞吐。
//   - per-service：每个 Service 一台独立 miniredis，后端可并行服务。
//     这个模式才真正回答「workers 是否够」：只要 workers ≥ N，批次就该跑满上限，
//     再往上加 worker 不再有收益。生产若是 Cluster 且各 service 落不同 slot，行为接近这个模式。
func BenchmarkTickBatchPool(b *testing.B) {
	const groupsPerService = 30
	for _, n := range []int{4, 16} {
		for _, perServiceRedis := range []bool{false, true} {
			mode := "shared"
			if perServiceRedis {
				mode = "per-service"
			}
			b.Run(fmt.Sprintf("services=%d/redis=%s", n, mode), func(b *testing.B) {
				var svcs []*Service
				if perServiceRedis {
					for i := 0; i < n; i++ {
						mr, err := miniredis.Run()
						if err != nil {
							b.Fatalf("miniredis: %v", err)
						}
						b.Cleanup(mr.Close)
						rdb := goredis.NewRedis(benchRedisConfig(mr.Addr()))
						svcs = append(svcs, newBenchServiceOn(b, rdb, commonrank.NewRedisService(rdb),
							"batch", fmt.Sprintf("bsolo_%d_%d", n, i), int32(i+1), groupsPerService))
					}
				} else {
					mr, err := miniredis.Run()
					if err != nil {
						b.Fatalf("miniredis: %v", err)
					}
					b.Cleanup(mr.Close)
					rdb := goredis.NewRedis(benchRedisConfig(mr.Addr()))
					rs := commonrank.NewRedisService(rdb)
					for i := 0; i < n; i++ {
						svcs = append(svcs, newBenchServiceOn(b, rdb, rs,
							"batch", fmt.Sprintf("bshare_%d_%d", n, i), int32(i+1), groupsPerService))
					}
				}

				for _, workers := range []int{4, 8, 16, 32, 64} {
					for _, queueLen := range []int{1, 4096} {
						b.Run(fmt.Sprintf("w=%d/q=%d", workers, queueLen), func(b *testing.B) {
							pool := gpool.New("bench", workers, queueLen)
							defer pool.Close(context.Background())
							ctx := context.Background()

							var skipped atomic.Int64

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								now := nextBenchNow()
								for _, s := range svcs {
									clearTickCaches(s)
								}

								var wg sync.WaitGroup
								for _, s := range svcs {
									wg.Add(1)
									// 与 manager.tickServices 逐字同形：超时即跳过该 service，配平 wg.Done
									if err := pool.SubmitWaitTimeout(ctx, func() {
										defer wg.Done()
										_ = s.Tick(ctx, now)
									}, 200*time.Millisecond); err != nil {
										wg.Done()
										skipped.Add(1)
									}
								}
								wg.Wait()
							}
							b.StopTimer()

							if sk := skipped.Load(); sk > 0 {
								b.Fatalf("池饱和导致 %d 次提交被跳过（workers=%d 小于服务数 %d 时才会发生）", sk, workers, n)
							}
						})
					}
				}
			})
		}
	}
}
