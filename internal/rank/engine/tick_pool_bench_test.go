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

// newBenchServiceOn 在给定的 Redis 客户端上建一个 Service，并预先造好 groups 个
// 「已满」分组（每分组 1 个真实玩家 + 29 个机器人），使一次 Tick 必然走
// tickAllRobots 且遍历全部分组。
//
// 直接构造持久化状态而不用 UpsertScore 灌数据：ensureGroupLocked 每建一个新成员都会
// LoadGroups 一次，灌 G 个分组是 O(G²) 的 Redis 往返，会把压测时间全花在建数据上。
func newBenchServiceOn(tb testing.TB, rdb *goredis.Redis, rs commonrank.Service, bizType, rankCode string, groups int) *Service {
	tb.Helper()
	ctx := context.Background()
	now := time.Now().UnixMilli()

	cfg := Config{
		BizType:       bizType,
		ActID:         1,
		RankCode:      rankCode,
		RankPeopleNum: benchRankPeopleNum,
		OpenTime:      now - time.Hour.Milliseconds(),
		CloseTime:     now + 24*time.Hour.Milliseconds(),
		GameEndTime:   now + 24*time.Hour.Milliseconds(),
		RobotTiers: []RobotTierCfg{{
			TierID:             1,
			Num:                benchRobotsPerGroup,
			DefaultTokenMin:    benchRobotScore,
			DefaultTokenMax:    benchRobotScore,
			GrowTokenCdMs:      0, // 每个 tick 都允许增长 ⇒ 每次 Tick 都产生真实写回
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
		for i := 0; i < benchRobotsPerGroup; i++ {
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

		robots := make([]*robotState, benchRobotsPerGroup)
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
			RobotCount: benchRobotsPerGroup,
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
	svc := newBenchServiceOn(b, rdb, rs, "rtt", "rtt_score_1", 1)
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
			svc := newBenchServiceOn(b, rdb, counting, "cold", fmt.Sprintf("cold_score_%d", groups), groups)

			now := time.Now().UnixMilli()
			rangeBefore := counting.rangeCalls.Load()
			upsertBefore := counting.upsertCalls.Load()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				now += 1000 // 推进一秒：TryLockRobotTick 是 per-second 的 SETNX，不推进则后续 Tick 直接短路
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

// BenchmarkTickBatchPool 是本文件的主结论来源：镜像 manager.tickServices 的批次形状
// （单生产者按序 SubmitWaitTimeout 提交 N 个 Service 的 Tick，然后 wg.Wait），
// 扫描 N（并发服务数）× workers × queueLen，看批次墙钟时间在哪里停止改善。
//
// N 个 Service 共用同一个 Redis 客户端与同一台 miniredis：真实 Redis 同样是单线程处理命令，
// 共用才能暴露「瓶颈在 Redis 命令吞吐，而不在池的 worker 数」这一事实。
func BenchmarkTickBatchPool(b *testing.B) {
	const groupsPerService = 30
	for _, n := range []int{4, 16, 64} {
		b.Run(fmt.Sprintf("services=%d", n), func(b *testing.B) {
			mr, err := miniredis.Run()
			if err != nil {
				b.Fatalf("miniredis: %v", err)
			}
			b.Cleanup(mr.Close)
			rdb := goredis.NewRedis(benchRedisConfig(mr.Addr()))
			rs := commonrank.NewRedisService(rdb)

			svcs := make([]*Service, n)
			for i := range svcs {
				svcs[i] = newBenchServiceOn(b, rdb, rs, "batch", fmt.Sprintf("batch_score_%d_%d", n, i), groupsPerService)
			}

			for _, workers := range []int{4, 16, 32, 64, 128} {
				for _, queueLen := range []int{1, 4096} {
					b.Run(fmt.Sprintf("w=%d/q=%d", workers, queueLen), func(b *testing.B) {
						pool := gpool.New("bench", workers, queueLen)
						defer pool.Close(context.Background())
						ctx := context.Background()

						var skipped atomic.Int64
						now := time.Now().UnixMilli()

						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							now += 1000
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

						if n := skipped.Load(); n > 0 {
							b.Fatalf("池饱和导致 %d 次提交被跳过（本应只在批次远超 worker 数时发生）", n)
						}
						b.ReportMetric(float64(n), "services")
					})
				}
			}
		})
	}
}
