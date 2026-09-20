package rankservice

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	commonrank "common/rank"
	"golib/gpool"
	"socialserver/internal/rank/engine"
	"socialserver/internal/rank/periodic"
)

// fakeBizService is a minimal RankBizService for exercising tickServices'
// pool-based fan-out (docs/rank_optimization.md 第 04 条) without a real engine.Service.
type fakeBizService struct {
	bizType   string
	tickCalls int32
}

func (f *fakeBizService) BizType() string { return f.bizType }
func (f *fakeBizService) GetMemberRank(ctx context.Context, userID int64) (*commonrank.RankMemberSnapshot, int32, error) {
	return nil, 0, nil
}
func (f *fakeBizService) Tick(ctx context.Context, now int64) error {
	atomic.AddInt32(&f.tickCalls, 1)
	return nil
}
func (f *fakeBizService) IsSettled() bool { return false }

// fakeRankService is a stub commonrank.Service that satisfies engine.NewService's
// non-nil check. Its methods are never actually invoked in these tests: a degraded
// Store (rdb=nil) makes ensureLoaded() bail out before touching rankService at all
// (see engine/service.go ensureLoaded: "if s.loaded || !s.store.available() { return }").
type fakeRankService struct{ commonrank.Service }

// withTestPool installs a small gpool.Global for the duration of the test
// (fewer workers than tasks), restoring whatever was there afterward.
func withTestPool(t *testing.T, workers, queueLen int) {
	t.Helper()
	prev := gpool.Global
	pool := gpool.New("test", workers, queueLen)
	gpool.Global = pool
	t.Cleanup(func() {
		_ = pool.Close(context.Background())
		gpool.Global = prev
	})
}

func TestTickServicesRunsAllViaPoolEvenWhenPoolSmallerThanBatch(t *testing.T) {
	// Pool 只有 2 个 worker、队列容量 1，但要 tick 5 个 service：只要每个任务能在
	// tickSubmitTimeout 内被 worker 取走（这里的 fakeBizService.Tick 近乎零耗时），
	// SubmitWaitTimeout 就应该像旧版无界 SubmitWait 一样把全部 5 个都提交完并等待执行完成，
	// 而不是像 Submit 那样在队列满时立即丢弃——这是第 04 条把 tick 与可丢弃的 WarmUp 区分开的原因。
	withTestPool(t, 2, 1)

	m := &Manager{
		services:        make(map[string]RankBizService),
		periodicHandler: periodic.NewHandler(nil, nil, nil),
	}
	fakes := make([]*fakeBizService, 5)
	for i := range fakes {
		f := &fakeBizService{bizType: "fake"}
		fakes[i] = f
		m.services[string(rune('a'+i))] = f
	}

	done := make(chan struct{})
	go func() {
		m.tickServices(context.Background(), time.Now().UnixMilli())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("tickServices did not return in time — SubmitWait likely deadlocked")
	}

	for i, f := range fakes {
		if atomic.LoadInt32(&f.tickCalls) != 1 {
			t.Fatalf("service %d: expected exactly 1 Tick call, got %d", i, f.tickCalls)
		}
	}
}

// TestTickServicesSkipsWhenPoolSaturatedPastTimeout 验证待办 F 的核心行为：当协程池
// 长时间（超过 tickSubmitTimeout）被占满时，tickServices 的每次提交必须在 tickSubmitTimeout
// 内放弃并跳过该 service（等下一轮 tick 重试），而不是像旧版无界 SubmitWait 那样一直阻塞、
// 拖住 tickLoop 的消费 goroutine 导致后续 ticker 事件被静默丢弃。
func TestTickServicesSkipsWhenPoolSaturatedPastTimeout(t *testing.T) {
	withTestPool(t, 1, 0) // 唯一 worker + 无缓冲队列：一旦 worker 被占满，后续提交必须排队等待。

	// 用一个阻塞任务占满唯一的 worker，直到测试结束才释放。
	blockCh := make(chan struct{})
	defer close(blockCh)
	if err := gpool.Global.SubmitWait(context.Background(), func() { <-blockCh }); err != nil {
		t.Fatalf("occupy sole worker: %v", err)
	}

	m := &Manager{
		services:        make(map[string]RankBizService),
		periodicHandler: periodic.NewHandler(nil, nil, nil),
	}
	fakes := make([]*fakeBizService, 2)
	for i := range fakes {
		f := &fakeBizService{bizType: "fake"}
		fakes[i] = f
		m.services[string(rune('a'+i))] = f
	}

	start := time.Now()
	done := make(chan struct{})
	go func() {
		m.tickServices(context.Background(), time.Now().UnixMilli())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("tickServices did not return — SubmitWaitTimeout must give up after tickSubmitTimeout, not block forever")
	}
	elapsed := time.Since(start)

	// 每个 service 的提交最多等待 tickSubmitTimeout 后放弃；2 个 service 串行提交，
	// 总耗时应远小于池满时无界阻塞的量级（给足余量以避免测试机负载导致的抖动误报）。
	if elapsed > 2*time.Second {
		t.Fatalf("tickServices took %v to return while pool was saturated — SubmitWaitTimeout does not appear bounded", elapsed)
	}

	for i, f := range fakes {
		if calls := atomic.LoadInt32(&f.tickCalls); calls != 0 {
			t.Fatalf("service %d: expected Tick to be skipped (pool saturated), but got %d calls", i, calls)
		}
	}
}

func TestTickServicesNoServicesStillRunsPeriodic(t *testing.T) {
	withTestPool(t, 2, 1)
	m := &Manager{
		services:        make(map[string]RankBizService),
		periodicHandler: periodic.NewHandler(nil, nil, nil),
	}
	// 不应 panic 或阻塞：无 service 时应直接跳过池提交，仍走 tickPeriodicActivities。
	m.tickServices(context.Background(), time.Now().UnixMilli())
}

func newDegradedEngineService(t *testing.T, bizType string, actID int32) *engine.Service {
	t.Helper()
	now := time.Now().UnixMilli()
	cfg := engine.Config{
		BizType:       bizType,
		ActID:         actID,
		RankCode:      "test_score_" + bizType,
		RankPeopleNum: 10,
		OpenTime:      now,
		CloseTime:     now + int64(time.Hour/time.Millisecond),
	}
	// rdb=nil、dao=nil ⇒ Store 处于「不可用」降级态，SaveActivityTimes/WarmUp 等均为 no-op，
	// 与 store.go 文档注释里「nil rdb/dao = no-op」的既有降级模式一致，无需真实 Redis/Mongo。
	svc, err := engine.NewService(fakeRankService{}, cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

func TestWarmUpAllServicesRunsAllViaPoolEvenWhenPoolSmallerThanBatch(t *testing.T) {
	// 与上面 tickServices 的用例对称（docs/rank_optimization.md 第 05 条）：
	// 批量 WarmUp 同样走 SubmitWait + WaitGroup，池小于批次大小时必须阻塞排队而不是丢弃。
	withTestPool(t, 2, 1)

	m := &Manager{engineServices: make(map[string]*engine.Service)}
	for i := 0; i < 5; i++ {
		m.engineServices[string(rune('a'+i))] = newDegradedEngineService(t, "fake", int32(i))
	}

	done := make(chan struct{})
	go func() {
		m.warmUpAllServices(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("warmUpAllServices did not return in time — SubmitWait likely deadlocked")
	}
}
