package rankservice

import (
	"context"
	"sync"
	"testing"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
	goredis "golib/redis"
	"socialserver/internal/rank/engine"

	"github.com/alicebob/miniredis/v2"
)

// 本文件覆盖待办 C：注册表的锁内不能再有 IO。
// 断言方式统一为「IO 卡住时 m.mu 仍可被独占获取」——只有真的把 IO 挪到锁外，
// 这条断言才成立；若哪天有人把 IO 挪回锁内，测试会超时失败而不是安静地慢下去。

// blockingRankService 是一个会把 GetMemberRank 卡在 channel 上的 RankBizService，
// 用来把「IO 进行中」这个瞬间拉长到可以被外部观察。
type blockingRankService struct {
	entered chan struct{} // 进入 GetMemberRank 时关闭
	release chan struct{} // 关闭后 GetMemberRank 才返回
}

func newBlockingRankService() *blockingRankService {
	return &blockingRankService{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *blockingRankService) BizType() string { return "blocking" }

func (b *blockingRankService) GetMemberRank(ctx context.Context, userID int64) (*commonrank.RankMemberSnapshot, int32, error) {
	close(b.entered)
	<-b.release
	return &commonrank.RankMemberSnapshot{MemberId: userID, Rank: 1}, 1, nil
}

func (b *blockingRankService) Tick(ctx context.Context, now int64) error { return nil }
func (b *blockingRankService) IsSettled() bool                           { return false }

// mustAcquireManagerLock 断言在当前协程之外，m.mu 可以被独占获取。
// 之所以用写锁而不是读锁：读锁与其他读者共存，证明不了「锁已释放」。
func mustAcquireManagerLock(t *testing.T, m *Manager, what string) {
	t.Helper()
	acquired := make(chan struct{})
	go func() {
		m.mu.Lock()
		close(acquired)
		m.mu.Unlock()
	}()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatalf("%s 期间 m.mu 无法被独占获取 ⇒ IO 仍在锁内（待办 C）", what)
	}
}

// TestGetMemberRankEntriesDoesNotHoldLockDuringIO 是待办 C 的主断言：
// GetMemberRankEntries 在查某个榜的名次时，必须已经放开了 m.mu。
// 改之前它是 defer RUnlock 包住整个循环的，一个用户的 GM 查询会把 N 次 Redis 往返
// 全压在注册表读锁上，而注册/删除服务要拿写锁。
func TestGetMemberRankEntriesDoesNotHoldLockDuringIO(t *testing.T) {
	blocking := newBlockingRankService()
	idx := NewMemberIndex(nil, 0) // 内存模式：Lookup 不需要 Redis
	const userID = int64(60001)
	idx.Track(userID, MemberEntry{BizType: "blocking", ActID: 1, GroupID: 1})

	m := &Manager{
		memberIndex:    idx,
		services:       map[string]RankBizService{"blocking:1": blocking},
		engineServices: map[string]*engine.Service{},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := m.GetMemberRankEntries(context.Background(), userID); err != nil {
			t.Errorf("GetMemberRankEntries: %v", err)
		}
	}()

	select {
	case <-blocking.entered:
	case <-time.After(time.Second):
		t.Fatal("GetMemberRank 未被调用，用例前提不成立")
	}

	// 此刻 GetMemberRank 正卡在 channel 上 = IO 进行中。
	mustAcquireManagerLock(t, m, "GetMemberRank 进行中")

	close(blocking.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("GetMemberRankEntries 未返回")
	}
}

// TestConcurrentRegisterEngineReturnsSingleInstance 锁死 registerEngine 的并发语义（待办 C 的
// 副作用）：IO 挪到锁外之后，多个协程会各造一份 engine.Service，但锁内二次检查必须保证
// 只有一个被插入，且**所有调用方都拿到同一个指针**。
//
// 返回自己那份副本是最容易犯的错：调用方各自持有一份 Service，内存里的分组/成员缓存
// 从那一刻起分叉，玩家会随机落到两份互不可见的状态上。
func TestConcurrentRegisterEngineReturnsSingleInstance(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewRedis(&goredis.RedisConfig{RedisAddrs: []string{mr.Addr()}})
	if rdb == nil {
		t.Fatalf("miniredis client: NewRedis returned nil for addr %s", mr.Addr())
	}
	m := &Manager{
		rdb:            rdb,
		rankService:    commonrank.NewRedisService(rdb),
		memberIndex:    NewMemberIndex(rdb, memberIndexTTL),
		services:       make(map[string]RankBizService),
		engineServices: make(map[string]*engine.Service),
	}

	now := time.Now().UnixMilli()
	cfg := engine.Config{
		BizType:       "concreg",
		ActID:         1,
		RankCode:      "concreg_score_1",
		RankPeopleNum: 10,
		OpenTime:      now - int64(time.Hour/time.Millisecond),
		CloseTime:     now + int64(time.Hour/time.Millisecond),
	}

	const goroutines = 8
	results := make([]*engine.Service, goroutines)
	errs := make([]error, goroutines)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = m.registerEngine(context.Background(), "concreg", cfg)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("第 %d 个 registerEngine: %v", i, err)
		}
	}
	first := results[0]
	if first == nil {
		t.Fatal("registerEngine 返回 nil service")
	}
	for i, svc := range results {
		if svc != first {
			t.Fatalf("第 %d 个调用方拿到了另一份 Service（%p ≠ %p）⇒ 内存状态会分叉", i, svc, first)
		}
	}

	m.mu.RLock()
	registered := m.engineServices["concreg:1"]
	m.mu.RUnlock()
	if registered != first {
		t.Fatalf("注册表里存的不是返回给调用方的那一个（%p ≠ %p）", registered, first)
	}
}

// TestRegisterEngineSecondCallIsNoop 覆盖已存在时的快速路径：
// 不重复注册 rank:def、不重复写 Mongo，且返回同一个实例。
func TestRegisterEngineSecondCallIsNoop(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewRedis(&goredis.RedisConfig{RedisAddrs: []string{mr.Addr()}})
	m := &Manager{
		rdb:            rdb,
		rankService:    commonrank.NewRedisService(rdb),
		memberIndex:    NewMemberIndex(rdb, memberIndexTTL),
		services:       make(map[string]RankBizService),
		engineServices: make(map[string]*engine.Service),
	}
	now := time.Now().UnixMilli()
	cfg := engine.Config{
		BizType: "concreg2", ActID: 2, RankCode: "concreg2_score_2", RankPeopleNum: 10,
		OpenTime: now, CloseTime: now + int64(time.Hour/time.Millisecond),
	}

	first, err := m.registerEngine(context.Background(), "concreg2", cfg)
	if err != nil {
		t.Fatalf("首次 registerEngine: %v", err)
	}
	defKey := rediskeys.GetRankDefKey(cfg.RankCode)
	mr.Del(defKey) // 若第二次调用真的重跑了注册，这个 key 会被重新写出来

	second, err := m.registerEngine(context.Background(), "concreg2", cfg)
	if err != nil {
		t.Fatalf("第二次 registerEngine: %v", err)
	}
	if second != first {
		t.Fatalf("第二次 registerEngine 返回了另一份 Service（%p ≠ %p）", second, first)
	}
	if mr.Exists(defKey) {
		t.Fatalf("第二次 registerEngine 重跑了注册路径（重新写入了 %s）⇒ 快速路径失效", defKey)
	}
}
