package engine

import (
	"context"
	"testing"
	"time"

	"common/rank"
)

// newLookupTestService builds a degraded Service (rdb=nil, dao=nil) with robot config,
// used to exercise the map-lookup helpers introduced by 第 12 条 without touching Redis/MongoDB.
func newLookupTestService(t *testing.T) *Service {
	t.Helper()
	cfg := Config{
		RankCode:      "test_score_lookup",
		RankPeopleNum: 10,
		RobotTiers: []RobotTierCfg{
			{TierID: 1, MaxToken: 100},
			{TierID: 2, MaxToken: 200},
		},
		RobotInfos: []RobotInfoEntry{
			{InfoID: 11, Name: "robot-a", Avatar: 1, Frame: 1},
			{InfoID: 22, Name: "robot-b", Avatar: 2, Frame: 2},
		},
	}
	svc, err := NewService(&fakeSettleRankService{}, cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

// TestFindTierUsesMapLookup 验证第 12 条：findTier 通过预建的 tierMap 命中已存在及未知档次。
func TestFindTierUsesMapLookup(t *testing.T) {
	svc := newLookupTestService(t)

	tier := svc.findTier(2)
	if tier == nil || tier.MaxToken != 200 {
		t.Fatalf("expected tier 2 with MaxToken=200, got %+v", tier)
	}
	if got := svc.findTier(999); got != nil {
		t.Fatalf("expected nil for unknown tier, got %+v", got)
	}
}

// TestRobotAvatarInfoUsesMapLookup 验证第 12 条：robotAvatarInfo 通过预建的 infoMap
// 命中已存在的 InfoID，未知 InfoID 时退化为仅含 UserId 的 AvatarInfo。
func TestRobotAvatarInfoUsesMapLookup(t *testing.T) {
	svc := newLookupTestService(t)

	robot := &robotState{MemberID: -1, InfoID: 22}
	info := svc.robotAvatarInfo(robot)
	if info == nil || info.Name != "robot-b" || info.UserId != -1 {
		t.Fatalf("expected robot-b avatar info with UserId=-1, got %+v", info)
	}

	unknown := &robotState{MemberID: -2, InfoID: 9999}
	fallback := svc.robotAvatarInfo(unknown)
	if fallback == nil || fallback.Name != "" || fallback.UserId != -2 {
		t.Fatalf("expected bare fallback AvatarInfo for unknown InfoID, got %+v", fallback)
	}
}

// TestResolveSettledCacheHitReturnsWithoutLookup 验证第 10 条：resolveSettled 命中调用方
// 已持锁读到的内存缓存时直接返回，不再走 Store/内存分组扫描分支。
func TestResolveSettledCacheHitReturnsWithoutLookup(t *testing.T) {
	svc := newLookupTestService(t)
	cached := []rank.RankMemberSnapshot{{MemberId: 1, Score: 100}}

	got := svc.resolveSettled(1, cached)
	if len(got) != 1 || got[0].MemberId != 1 {
		t.Fatalf("expected cached snapshot to be returned unchanged, got %+v", got)
	}
}

// TestResolveSettledMissWithNoKnownGroupReturnsNil 验证第 10 条：缓存未命中且分组信息
// （Store 与内存 s.groups）都找不到时安全返回 nil，不 panic。
func TestResolveSettledMissWithNoKnownGroupReturnsNil(t *testing.T) {
	svc := newLookupTestService(t)

	got := svc.resolveSettled(42, nil)
	if got != nil {
		t.Fatalf("expected nil for unknown group, got %+v", got)
	}
}

// TestResolveSettledMissFallsBackToInMemoryGroupCache 验证第 10 条的内存分组分支：
// Store 查不到分组（降级态）时退化扫描 s.groups，命中已结算分组的内存缓存。
func TestResolveSettledMissFallsBackToInMemoryGroupCache(t *testing.T) {
	svc := newLookupTestService(t)
	groupID := int32(7)

	svc.mu.Lock()
	svc.groups = append(svc.groups, &Group{GroupID: groupID, State: GroupStateSettled})
	svc.settledGroup[groupID] = []rank.RankMemberSnapshot{{MemberId: 5, Score: 50}}
	svc.mu.Unlock()

	got := svc.resolveSettled(groupID, nil)
	if len(got) != 1 || got[0].MemberId != 5 {
		t.Fatalf("expected settled snapshot recovered from in-memory group cache, got %+v", got)
	}
}

// fakeSettleWithSnapshotsRankService 是 fakeSettleRankService 的变体：SettleInstance 返回
// 真实快照数据，用于验证第 11 条「同一份 clone 在 results 与 settledGroup 间共享」。
type fakeSettleWithSnapshotsRankService struct {
	rank.Service
}

func (f *fakeSettleWithSnapshotsRankService) CloseInstance(ctx context.Context, instanceId string, closeTime int64) error {
	return nil
}

func (f *fakeSettleWithSnapshotsRankService) SettleInstance(ctx context.Context, instanceId string, settleTime int64) ([]rank.RankMemberSnapshot, error) {
	return []rank.RankMemberSnapshot{{MemberId: 1, Score: 100, Rank: 1}}, nil
}

func (f *fakeSettleWithSnapshotsRankService) GetInstance(ctx context.Context, instanceId string) (*rank.RankInstance, error) {
	return nil, rank.ErrInstanceNotFound
}

func (f *fakeSettleWithSnapshotsRankService) ExpireInstance(ctx context.Context, instanceId string, d time.Duration) error {
	return nil
}

// TestSettleSharesSingleClonedSnapshotAcrossResultsAndCache 验证第 11 条：一次结算中
// members 只 cloneSnapshots 一次，results 与 settledGroup 共享同一份底层数组，
// 而不是各自持有独立副本（3 次 clone → 1 次）。
func TestSettleSharesSingleClonedSnapshotAcrossResultsAndCache(t *testing.T) {
	now := time.Now().UnixMilli()
	cfg := Config{
		RankCode:      "test_score_settle_share",
		RankPeopleNum: 10,
		OpenTime:      now - int64(time.Hour/time.Millisecond),
		CloseTime:     now - int64(time.Minute/time.Millisecond), // 已关闭
	}
	fake := &fakeSettleWithSnapshotsRankService{}
	svc, err := NewService(fake, cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	group := &Group{GroupID: 1, InstanceID: svc.groupInstanceID(1), State: GroupStateOpen}
	svc.mu.Lock()
	svc.groups = append(svc.groups, group)
	svc.mu.Unlock()

	results, err := svc.Settle(context.Background())
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	resultSnap := results[1]
	if len(resultSnap) != 1 {
		t.Fatalf("expected 1 snapshot in results, got %d", len(resultSnap))
	}

	svc.mu.Lock()
	cachedSnap := svc.settledGroup[1]
	svc.mu.Unlock()
	if len(cachedSnap) != 1 {
		t.Fatalf("expected 1 snapshot in settledGroup cache, got %d", len(cachedSnap))
	}

	if &resultSnap[0] != &cachedSnap[0] {
		t.Fatalf("第 11 条 violated: results and settledGroup must share the same cloned snapshot backing array, got distinct copies")
	}
}

// TestGetMemberGroupIDDoesNotRequireExclusiveLock 锁死待办 G-d3：
// GetMemberGroupID 只读 s.memberGroup，必须用读锁。这里先由测试自身持住读锁，
// 再从另一个协程调用它——用读锁时两者可以共存；若退回用写锁则会一直等在那里，
// 表现为超时失败。GM 的批量查询正因此不必与 UpsertScore（持写锁、内含 Redis 往返）串行。
func TestGetMemberGroupIDDoesNotRequireExclusiveLock(t *testing.T) {
	svc := newLookupTestService(t)
	svc.mu.Lock()
	svc.memberGroup[1001] = 7
	svc.mu.Unlock()

	svc.mu.RLock() // 模拟另一个并发读者
	defer svc.mu.RUnlock()

	done := make(chan int32, 1)
	go func() {
		gid, _ := svc.GetMemberGroupID(1001)
		done <- gid
	}()

	select {
	case gid := <-done:
		if gid != 7 {
			t.Fatalf("GetMemberGroupID(1001)=%d, want 7", gid)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("GetMemberGroupID 阻塞在 s.mu 上 ⇒ 用了写锁，GM 批量查询会被得分写入堵住（待办 G-d3）")
	}
}

// TestWarmUpFastPathSkipsMutexWhenAlreadyLoaded 验证第 13 条：一旦 loaded 已置位，
// WarmUp 必须走原子快路径直接返回，不再尝试获取 s.mu —— 否则会在 s.mu 被其他协程
// 长时间持有时（例如 UpsertScore 处理中）被无谓阻塞。
func TestWarmUpFastPathSkipsMutexWhenAlreadyLoaded(t *testing.T) {
	svc := newLookupTestService(t)
	svc.loaded.Store(true)

	svc.mu.Lock()
	defer svc.mu.Unlock()

	done := make(chan struct{})
	go func() {
		svc.WarmUp(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WarmUp blocked on s.mu despite loaded==true; atomic fast path (第 13 条) is not taking effect")
	}
}
