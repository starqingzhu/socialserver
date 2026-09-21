package engine

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonrank "common/rank"
	"golib/gpool"
	goredis "golib/redis"

	"github.com/redis/go-redis/v9"
)

// 本文件把 tick_pool_bench_test.go 的压测台搬到真实 Redis，并按**实测的真实业务形状**建模。
//
// 形状来源（对 172.20.4.224:6379 db0 的只读聚合，见 docs/gpool_tuning.md 附录）：
//
//	rank:meta 去重 bizId = 567，其中 566 个是 camper_competition 的 _r{N} 历史轮次残留；
//	有真实状态（inst/seq/groups/mb/members 齐全）的服务只有 5 个；
//	**每个服务只有 1 个分组**（单服务分组数 min=p50=p90=max=1，n=5）；抽样分组 HLen=1。
//
// RobotRank.json：每个 bizType 每分组只有 5 个机器人（num 1+1+1+2），
// 且 growTokenCd 为 600s~3600s，比 tickInterval(1s) 大 3 个数量级
// ⇒ **稳态下绝大多数 Tick 不产生任何写**，只做读。这与「29 机器人 + cd=0 每 tick 全量写回」
// 的放大形状差别极大，因此两种形状都测：
//
//	steady —— 真实 cd(600s) + 5 机器人：只读路径，即线上稳态。
//	burst  —— cd=0 + 5 机器人：每 tick 全量写回，是「最坏情况」上界。
//
// 落点固定 db9（开压前已确认 DBSIZE=0），压测结束整库清空，不触碰 db0 的业务数据。
const (
	realRedisAddr   = "172.20.4.224:6379"
	realRedisPasswd = "123456"
	realRedisDB     = 9

	realGroupsPerService = 1
	realRobotsPerGroup   = 5
	realGrowCdMs         = 600_000 // camper_competition A 档真实值：growTokenCd=600s
)

// realBenchShapes 是本次压测的两种形状。steady 对应线上稳态，burst 是写路径上界。
var realBenchShapes = []struct {
	name string
	sh   benchShape
}{
	{"steady", benchShape{groups: realGroupsPerService, robots: realRobotsPerGroup, growCdMs: realGrowCdMs}},
	{"burst", benchShape{groups: realGroupsPerService, robots: realRobotsPerGroup, growCdMs: 0}},
}

// realBenchSeq 保证每次建数据产生的 bizId 全局唯一。
//
// Go 基准框架会先用 N=1 校准再正式跑，同一段建数据代码因此会被执行两次以上；
// miniredis 每次都是全新实例所以掩盖了这点，而真实 Redis 的状态跨调用保留，
// 第二次执行就会在 OpenInstance 上撞「rank instance already exists」。
// 隔离必须靠唯一 bizId，不能靠「新实例」。
var realBenchSeq atomic.Int64

func realUniq(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, realBenchSeq.Add(1))
}

// realRedisBenchConfig 与生产同一组连接池参数（golib/redis 默认值），
// 否则压出来的最优 workers 会被测试里更小的池尺寸扭曲。
func realRedisBenchConfig() *goredis.RedisConfig {
	return &goredis.RedisConfig{
		RedisAddrs:     []string{realRedisAddr},
		RedisPasswd:    realRedisPasswd,
		RedisDBIndex:   realRedisDB,
		RedisMaxIdle:   20,
		RedisMaxActive: 200,
		RedisPoolSize:  100,
	}
}

// requireRealRedis 让这些基准默认不跑。
//
// 它们需要一个可达的专用真机 Redis，并且会写入、最后清空 db9；放进常规
// `go test ./...` 会让其它人的测试去连一台私有机器、甚至清掉不属于自己的数据。
// 显式跑：
//
//	GPBENCH_REAL_REDIS=1 go test ./internal/rank/engine/ -run '^$' \
//	    -bench 'BenchmarkReal' -benchtime=20x
func requireRealRedis(b *testing.B) {
	if os.Getenv("GPBENCH_REAL_REDIS") != "1" {
		b.Skip("需要专用真机 Redis（会写入并清空 db9）：设 GPBENCH_REAL_REDIS=1 后显式跑")
	}
}

// realBenchRD / realBenchAdmin 是整个真机压测共享的两个客户端（懒初始化，进程内复用）。
//
// 必须共享而不是每个子基准各建一个：每个客户端的 PoolSize=100，子基准有几十个，
// 各建一个会累积上千条连接，在 Windows 上直接打爆临时端口
// （实测表现：收尾的 flush 报 "Only one usage of each socket address ... permitted"）。
// 共享单客户端也更贴近生产（生产就是 redis.Main 一个客户端）。
var (
	realBenchOnce  sync.Once
	realBenchRD    *goredis.Redis
	realBenchAdmin *redis.Client
)

func realBenchClients() (*goredis.Redis, *redis.Client) {
	realBenchOnce.Do(func() {
		realBenchRD = goredis.NewRedis(realRedisBenchConfig())
		realBenchAdmin = redis.NewClient(&redis.Options{
			Addr: realRedisAddr, Password: realRedisPasswd, DB: realRedisDB,
		})
	})
	return realBenchRD, realBenchAdmin
}

// flushRealBenchDB 清空压测专用 db9。开压前用于保证干净起点，压测后用于回收全部 key。
// 该 db 专供本次压测，db0 的业务数据不在此范围内。
func flushRealBenchDB(tb testing.TB) {
	tb.Helper()
	_, admin := realBenchClients()
	if err := admin.FlushDB(context.Background()).Err(); err != nil {
		tb.Fatalf("flush db%d: %v", realRedisDB, err)
	}
}

// realBenchKeyCount 报告 db9 当前 key 数，用于确认压测后已回收干净。
func realBenchKeyCount() int64 {
	_, admin := realBenchClients()
	n, err := admin.DBSize(context.Background()).Result()
	if err != nil {
		return -1
	}
	return n
}

// BenchmarkRealRedisRTT 用一次 GetInstance（恰好 1 个 Redis 往返）标定到这台 Redis 的
// 网络 RTT。这是把 miniredis 的 loopback 结论外推到网络后端的换算基准。
func BenchmarkRealRedisRTT(b *testing.B) {
	requireRealRedis(b)
	flushRealBenchDB(b)
	b.Cleanup(func() {
		flushRealBenchDB(b)
		b.Logf("压测后 db%d key 数 = %d", realRedisDB, realBenchKeyCount())
	})

	rdb, _ := realBenchClients()
	rs := commonrank.NewRedisService(rdb)
	tag := realUniq("rtt_real")
	svc := newBenchServiceShaped(b, rdb, rs, tag, tag+"_score_1", 1,
		benchShape{groups: 1, robots: realRobotsPerGroup, growCdMs: realGrowCdMs})
	instID := svc.groupInstanceID(1)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rs.GetInstance(ctx, instID); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRealTickSingle 测**单个服务一次冷 Tick**的真实耗时与命令数，两种形状对比。
// 这是批次时间的下限来源：批次墙钟 ≥ 该值 × ceil(N/workers)。
func BenchmarkRealTickSingle(b *testing.B) {
	requireRealRedis(b)
	flushRealBenchDB(b)
	b.Cleanup(func() {
		flushRealBenchDB(b)
		b.Logf("压测后 db%d key 数 = %d", realRedisDB, realBenchKeyCount())
	})

	for _, shape := range realBenchShapes {
		b.Run(shape.name, func(b *testing.B) {
			rdb, _ := realBenchClients()
			counting := &countingRankService{Service: commonrank.NewRedisService(rdb)}
			tag := realUniq("tick_real_" + shape.name)
			svc := newBenchServiceShaped(b, rdb, counting, tag, tag+"_score_1", 1, shape.sh)

			rangeBefore := counting.rangeCalls.Load()
			upsertBefore := counting.upsertCalls.Load()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 推进逻辑时钟：TryLockRobotTick 是 per-second SETNX，不推进则后续 Tick 直接短路
				clearTickCaches(svc)
				if err := svc.Tick(context.Background(), nextBenchNow()); err != nil {
					b.Fatalf("Tick: %v", err)
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(counting.rangeCalls.Load()-rangeBefore)/float64(b.N), "range/tick")
			b.ReportMetric(float64(counting.upsertCalls.Load()-upsertBefore)/float64(b.N), "upsert/tick")
		})
	}
}

// BenchmarkRealTickByGroupCount 测真实形状下「单次 Tick 随分组数增长」的斜率。
//
// 这是文档里最有行动价值的一条：tickAllRobots 在**单个任务内串行**遍历该服务的全部分组
// （engine/service_robot.go:245-251），而 tickServices 又对整批 wg.Wait()，
// 所以单个大活动就能吃掉整个 1s tick 预算，且**加 worker 完全无效**（分组循环在一个任务里）。
// 本基准给出「每多一个分组要多少毫秒」，从而算出预算耗尽的分组数阈值。
//
// 只测 steady（真实 cd），因为那是线上常态。
func BenchmarkRealTickByGroupCount(b *testing.B) {
	requireRealRedis(b)
	flushRealBenchDB(b)
	b.Cleanup(func() {
		flushRealBenchDB(b)
		b.Logf("压测后 db%d key 数 = %d", realRedisDB, realBenchKeyCount())
	})

	shape := realBenchShapes[0] // steady
	for _, groups := range []int{1, 10, 50, 200} {
		b.Run(fmt.Sprintf("groups=%d", groups), func(b *testing.B) {
			rdb, _ := realBenchClients()
			counting := &countingRankService{Service: commonrank.NewRedisService(rdb)}
			tag := realUniq("grp")
			sh := shape.sh
			sh.groups = groups
			svc := newBenchServiceShaped(b, rdb, counting, tag, tag+"_score_1", 1, sh)

			rangeBefore := counting.rangeCalls.Load()
			upsertBefore := counting.upsertCalls.Load()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				clearTickCaches(svc)
				if err := svc.Tick(context.Background(), nextBenchNow()); err != nil {
					b.Fatalf("Tick: %v", err)
				}
			}
			elapsed := b.Elapsed().Seconds()
			b.StopTimer()

			// Range 调用数应正比于分组数：它是「每个分组一次组内全量读」的直接证据
			rangePerTick := float64(counting.rangeCalls.Load()-rangeBefore) / float64(b.N)
			b.ReportMetric(rangePerTick, "range/tick")
			b.ReportMetric(rangePerTick/float64(groups), "range/group")
			b.ReportMetric(float64(counting.upsertCalls.Load()-upsertBefore)/float64(b.N), "upsert/tick")
			if elapsed > 0 {
				b.ReportMetric(elapsed*1000/float64(b.N)/float64(groups), "ms/group")
			}
		})
	}
}

// BenchmarkRealTickWorkersKnee 在真实形状上细扫 workers，定位「再加 worker 不再变快」的膝盖。
//
// 动机：上面的粗扫（w=4/32/500）显示 w=500 反而比 w=32 慢，说明最优值在 32 附近而不是越大越好；
// 但 32 与 500 之间没有采样点，定不出该写哪个数。这里在 8~500 之间取 9 个点。
//
// 只测 steady（真实 cd，稳态只读路径），因为那才是线上常态；burst 的形状与膝盖位置一致
// （见上面粗扫），不必重复。
func BenchmarkRealTickWorkersKnee(b *testing.B) {
	requireRealRedis(b)
	flushRealBenchDB(b)
	b.Cleanup(func() {
		flushRealBenchDB(b)
		b.Logf("压测后 db%d key 数 = %d", realRedisDB, realBenchKeyCount())
	})

	shape := realBenchShapes[0] // steady
	for _, n := range []int{32, 200, 500} {
		b.Run(fmt.Sprintf("services=%d", n), func(b *testing.B) {
			rdb, _ := realBenchClients()
			rs := commonrank.NewRedisService(rdb)

			tag := realUniq("knee")
			svcs := make([]*Service, 0, n)
			for i := 0; i < n; i++ {
				svcs = append(svcs, newBenchServiceShaped(b, rdb, rs,
					tag, fmt.Sprintf("%s_s%d", tag, i+1), int32(i+1), shape.sh))
			}

			// 顺序混淆检验：子基准是按顺序串行跑的，若把 workers 固定升序，
			// w=500 永远最后一个执行，于是任何随时间漂移（Redis 侧负载、共享连接池状态、
			// 网络抖动）都会被误读成「worker 越多越慢」。
			// GPBENCH_KNEE_ORDER=desc 时倒序跑同一组 workers：若退化跟着「位置」走（倒序后 w=8 最慢），
			// 就是漂移；若仍跟着「worker 数」走（w=500 仍最慢），才是真的与 workers 相关。
			workerGrid := []int{8, 16, 32, 48, 64, 100, 128, 256, 500}
			if os.Getenv("GPBENCH_KNEE_ORDER") == "desc" {
				for l, r := 0, len(workerGrid)-1; l < r; l, r = l+1, r-1 {
					workerGrid[l], workerGrid[r] = workerGrid[r], workerGrid[l]
				}
			}

			for _, workers := range workerGrid {
				b.Run(fmt.Sprintf("w=%d", workers), func(b *testing.B) {
					pool := gpool.New("kneebench", workers, 5120)
					defer pool.Close(context.Background())
					ctx := context.Background()

					var tickErrs atomic.Int64

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						now := nextBenchNow()
						for _, s := range svcs {
							clearTickCaches(s)
						}
						var wg sync.WaitGroup
						for _, s := range svcs {
							wg.Add(1)
							if err := pool.SubmitWaitTimeout(ctx, func() {
								defer wg.Done()
								if err := s.Tick(ctx, now); err != nil {
									tickErrs.Add(1)
								}
							}, 200*time.Millisecond); err != nil {
								wg.Done()
							}
						}
						wg.Wait()
					}
					elapsed := b.Elapsed().Seconds()
					b.StopTimer()

					b.ReportMetric(float64(tickErrs.Load())/float64(b.N*n)*100, "tick-err%")
					if elapsed > 0 {
						b.ReportMetric(float64(b.N*n)/elapsed, "ticks/s")
					}
				})
			}
		})
	}
}

// BenchmarkRealTickBatchPool 是主实验：镜像 manager.tickServices 的批次形状
// （单生产者按序 SubmitWaitTimeout 提交 N 个服务的 Tick，然后 wg.Wait），
// 在真实 Redis 上扫描 N（并发服务数）× workers × queueLen，看：
//
//  1. workers < N 时是否出现 SubmitWaitTimeout 丢 tick（= 线上「排行榜静默停更」）；
//  2. workers ≥ N 之后再加 worker 是否还有收益（预期：没有，因为后端是单节点串行）；
//  3. queueLen 对吞吐有无影响（预期：无，只吸收提交抖动）。
//
// 关键对照是 workers=500 与 workers=32 的耗时：实测 500 反而更慢，见 docs/gpool_tuning.md。
func BenchmarkRealTickBatchPool(b *testing.B) {
	requireRealRedis(b)
	flushRealBenchDB(b)
	b.Cleanup(func() {
		flushRealBenchDB(b)
		b.Logf("压测后 db%d key 数 = %d", realRedisDB, realBenchKeyCount())
	})

	for _, shape := range realBenchShapes {
		for _, n := range []int{5, 32, 200, 500} {
			b.Run(fmt.Sprintf("shape=%s/services=%d", shape.name, n), func(b *testing.B) {
				rdb, _ := realBenchClients()
				rs := commonrank.NewRedisService(rdb)

				// 建 N 个服务并复用给所有 workers/queueLen 组合，避免把时间花在重复建数据上。
				// tag 按「本次父级调用」唯一：框架重跑父级时会用新 tag，不撞已存在的实例。
				tag := realUniq("bpool_" + shape.name)
				svcs := make([]*Service, 0, n)
				for i := 0; i < n; i++ {
					svcs = append(svcs, newBenchServiceShaped(b, rdb, rs,
						tag,
						fmt.Sprintf("%s_s%d", tag, i+1),
						int32(i+1), shape.sh))
				}

				for _, workers := range []int{4, 32, 500} {
					for _, queueLen := range []int{1, 5120} {
						b.Run(fmt.Sprintf("w=%d/q=%d", workers, queueLen), func(b *testing.B) {
							pool := gpool.New("realbench", workers, queueLen)
							defer pool.Close(context.Background())
							ctx := context.Background()

							var submitted, skipped, tickErrs atomic.Int64

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								now := nextBenchNow()
								for _, s := range svcs {
									clearTickCaches(s)
								}

								var wg sync.WaitGroup
								for _, s := range svcs {
									wg.Add(1)
									submitted.Add(1)
									// 与 manager.tickServices 逐字同形：超时即跳过该 service，配平 wg.Done
									if err := pool.SubmitWaitTimeout(ctx, func() {
										defer wg.Done()
										// Tick 的错误必须计数：workers 远超连接池(100) 时，
										// 多出的 worker 会阻塞在 go-redis 取连接上（PoolTimeout 4s），
										// 失败会被忽略，压出来的耗时就是假的。
										if err := s.Tick(ctx, now); err != nil {
											tickErrs.Add(1)
										}
									}, 200*time.Millisecond); err != nil {
										wg.Done()
										skipped.Add(1)
									}
								}
								wg.Wait()
							}
							elapsed := b.Elapsed().Seconds()
							b.StopTimer()

							sk := skipped.Load()
							// 报告丢 tick 比例而不是直接 Fatal：本实验要的正是「workers<N 时会丢」这个结论
							b.ReportMetric(float64(sk)/float64(submitted.Load())*100, "tick-drop%")
							b.ReportMetric(float64(tickErrs.Load())/float64(submitted.Load())*100, "tick-err%")
							if elapsed > 0 {
								b.ReportMetric(float64(b.N*n)/elapsed, "ticks/s")
							}
						})
					}
				}
			})
		}
	}
}
