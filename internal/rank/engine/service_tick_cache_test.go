package engine

import (
	"context"
	"testing"
	"time"

	"common/rank"
)

// fakeTickCacheRankService is a minimal rank.Service stub used to exercise the
// tickAllRobots/ensureGroupInstance soft-cache helpers introduced by
// docs/rank_optimization.md 第 02/03 条, without a real Redis/Mongo backend.
type fakeTickCacheRankService struct {
	rank.Service

	getInstanceCalls int
	getInstanceErr   error
	getInstance      *rank.RankInstance

	openInstanceCalls int

	rangeCalls int
	rangeResp  []rank.RankMemberSnapshot
	rangeErr   error
}

func (f *fakeTickCacheRankService) GetInstance(ctx context.Context, instanceId string) (*rank.RankInstance, error) {
	f.getInstanceCalls++
	if f.getInstanceErr != nil {
		return nil, f.getInstanceErr
	}
	return f.getInstance, nil
}

func (f *fakeTickCacheRankService) OpenInstance(ctx context.Context, instance rank.RankInstance) error {
	f.openInstanceCalls++
	return nil
}

func (f *fakeTickCacheRankService) BatchUpsertScore(ctx context.Context, instanceId string, items []rank.RankScoreItem) error {
	return nil
}

func (f *fakeTickCacheRankService) Range(ctx context.Context, instanceId string, start int64, end int64) ([]rank.RankMemberSnapshot, error) {
	f.rangeCalls++
	if f.rangeErr != nil {
		return nil, f.rangeErr
	}
	return f.rangeResp, nil
}

func (f *fakeTickCacheRankService) ExpireInstance(ctx context.Context, instanceId string, d time.Duration) error {
	return nil
}

func newTickCacheTestService(t *testing.T, fake *fakeTickCacheRankService) *Service {
	t.Helper()
	cfg := Config{
		RankCode:      "test_score_tick_cache",
		RankPeopleNum: 10,
	}
	svc, err := NewService(fake, cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

// --- 第 02 条：tickGroups 2s Cache-Aside ---

// TestTickGroupsCacheHitReturnsCachedSliceWithoutStoreCall 验证第 02 条必改-2：
// 有效期内命中缓存时直接返回缓存内容，不回源 Store（degraded store 回源永远得到 nil，
// 命中缓存则能看到预先写入的哨兵分组）。
func TestTickGroupsCacheHitReturnsCachedSliceWithoutStoreCall(t *testing.T) {
	svc := newTickCacheTestService(t, &fakeTickCacheRankService{})
	sentinel := []*Group{{GroupID: 999, State: GroupStateOpen}}

	svc.cacheMu.Lock()
	svc.groupsCache = sentinel
	svc.groupsCacheExpiry = time.Now().Add(tickGroupsCacheTTL)
	svc.cacheMu.Unlock()

	got, ok := svc.tickGroups()
	if !ok || len(got) != 1 || got[0].GroupID != 999 {
		t.Fatalf("expected cache hit returning sentinel group, got %+v ok=%v", got, ok)
	}
}

// TestTickGroupsCacheExpiredFallsBackToStore 验证第 02 条必改-2：缓存过期后必须回源 Store，
// 不能继续返回陈旧数据——否则本节点会永远看不到其他节点新建的分组。
func TestTickGroupsCacheExpiredFallsBackToStore(t *testing.T) {
	svc := newTickCacheTestService(t, &fakeTickCacheRankService{})
	stale := []*Group{{GroupID: 999, State: GroupStateOpen}}

	svc.cacheMu.Lock()
	svc.groupsCache = stale
	svc.groupsCacheExpiry = time.Now().Add(-time.Second) // 已过期
	svc.cacheMu.Unlock()

	got, ok := svc.tickGroups()
	if !ok {
		t.Fatalf("expected ok=true (degraded store LoadGroups returns nil,nil), got ok=false")
	}
	if len(got) != 0 {
		t.Fatalf("expected expired cache to be bypassed in favor of a fresh (empty) store read, got %+v", got)
	}
}

// --- 第 02 条：tickRobots 2s Cache-Aside ---

// TestTickRobotsCacheHitReturnsCachedSlice 验证机器人列表缓存命中时直接返回，不回源。
func TestTickRobotsCacheHitReturnsCachedSlice(t *testing.T) {
	svc := newTickCacheTestService(t, &fakeTickCacheRankService{})
	sentinel := []*robotState{{MemberID: -1, TierID: 1}}

	svc.cacheMu.Lock()
	svc.robotsCache = map[int32]robotsCacheEntry{
		5: {robots: sentinel, expiry: time.Now().Add(tickGroupsCacheTTL)},
	}
	svc.cacheMu.Unlock()

	got, ok := svc.tickRobots(5)
	if !ok || len(got) != 1 || got[0].MemberID != -1 {
		t.Fatalf("expected cache hit returning sentinel robot, got %+v ok=%v", got, ok)
	}
}

// TestTickRobotsCacheExpiredFallsBackToStore 验证机器人缓存过期后回源 Store，
// 不再返回陈旧的机器人状态。
func TestTickRobotsCacheExpiredFallsBackToStore(t *testing.T) {
	svc := newTickCacheTestService(t, &fakeTickCacheRankService{})
	stale := []*robotState{{MemberID: -1, TierID: 1}}

	svc.cacheMu.Lock()
	svc.robotsCache = map[int32]robotsCacheEntry{
		5: {robots: stale, expiry: time.Now().Add(-time.Second)},
	}
	svc.cacheMu.Unlock()

	got, ok := svc.tickRobots(5)
	if !ok {
		t.Fatalf("expected ok=true (degraded store LoadRobots returns nil,nil), got ok=false")
	}
	if len(got) != 0 {
		t.Fatalf("expected expired robots cache to be bypassed, got %+v", got)
	}
}

// TestInvalidateRobotsCacheForcesReload 验证 invalidateRobotsCache 清空指定分组缓存后，
// 下一次 tickRobots 不会再命中旧数据（用于 spawnRobotsForGroup 新增机器人后立即生效）。
func TestInvalidateRobotsCacheForcesReload(t *testing.T) {
	svc := newTickCacheTestService(t, &fakeTickCacheRankService{})
	stale := []*robotState{{MemberID: -1, TierID: 1}}

	svc.cacheMu.Lock()
	svc.robotsCache = map[int32]robotsCacheEntry{
		5: {robots: stale, expiry: time.Now().Add(tickGroupsCacheTTL)},
	}
	svc.cacheMu.Unlock()

	svc.invalidateRobotsCache(5)

	got, ok := svc.tickRobots(5)
	if !ok || len(got) != 0 {
		t.Fatalf("expected invalidated cache to force a fresh (empty) store read, got %+v ok=%v", got, ok)
	}
}

// TestSpawnRobotsForGroupInvalidatesRobotsCache 验证第 02 条的写路径钩子：
// spawnRobotsForGroup 生成新机器人后必须清空该分组的 robotsCache，
// 否则新机器人要等 tickGroupsCacheTTL（2s）才会被下一次 tick 感知。
func TestSpawnRobotsForGroupInvalidatesRobotsCache(t *testing.T) {
	fake := &fakeTickCacheRankService{getInstanceErr: rank.ErrInstanceNotFound}
	cfg := Config{
		RankCode:      "test_score_spawn_invalidate",
		RankPeopleNum: 10,
		RobotTiers: []RobotTierCfg{
			{TierID: 1, Num: 1, MaxToken: 100, DefaultTokenMin: 1, DefaultTokenMax: 1},
		},
		RobotInfos: []RobotInfoEntry{
			{InfoID: 1, Name: "robot-a"},
		},
	}
	svc, err := NewService(fake, cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	groupID := int32(5)
	svc.cacheMu.Lock()
	svc.robotsCache = map[int32]robotsCacheEntry{
		groupID: {robots: []*robotState{{MemberID: -1}}, expiry: time.Now().Add(tickGroupsCacheTTL)},
	}
	svc.cacheMu.Unlock()

	now := time.Now().UnixMilli()
	if err := svc.spawnRobotsForGroup(context.Background(), groupID, 5, now); err != nil {
		t.Fatalf("spawnRobotsForGroup: %v", err)
	}

	svc.cacheMu.RLock()
	_, stillCached := svc.robotsCache[groupID]
	svc.cacheMu.RUnlock()
	if stillCached {
		t.Fatalf("expected robotsCache entry for group %d to be invalidated after spawning new robots", groupID)
	}
}

// --- 第 02 条「遗漏的最大一项」：tickGroupScores 缓存 Range(0,-1) ---

// TestTickGroupScoresCachesRangeResultWithinTTL 验证 tickGroupScores 在有效期内复用缓存，
// 不重复调用 Range；过期后重新回源。
func TestTickGroupScoresCachesRangeResultWithinTTL(t *testing.T) {
	fake := &fakeTickCacheRankService{
		rangeResp: []rank.RankMemberSnapshot{
			{MemberId: -1, Score: 500},
			{MemberId: 42, Score: 300},
		},
	}
	svc := newTickCacheTestService(t, fake)

	first, ok := svc.tickGroupScores(context.Background(), 1, "inst-1")
	if !ok {
		t.Fatalf("expected first tickGroupScores call to succeed")
	}
	if first.firstScore != 500 || first.realFirstScore != 300 {
		t.Fatalf("unexpected scores: %+v", first)
	}
	if fake.rangeCalls != 1 {
		t.Fatalf("expected exactly 1 Range call after first tickGroupScores, got %d", fake.rangeCalls)
	}

	second, ok := svc.tickGroupScores(context.Background(), 1, "inst-1")
	if !ok || second != first {
		t.Fatalf("expected cached scores to be reused unchanged, got %+v ok=%v", second, ok)
	}
	if fake.rangeCalls != 1 {
		t.Fatalf("expected Range NOT to be called again within TTL, got %d calls", fake.rangeCalls)
	}

	// 手动使缓存过期，验证会重新回源。
	svc.cacheMu.Lock()
	entry := svc.scoreCache[1]
	entry.expiry = time.Now().Add(-time.Second)
	svc.scoreCache[1] = entry
	svc.cacheMu.Unlock()

	if _, ok := svc.tickGroupScores(context.Background(), 1, "inst-1"); !ok {
		t.Fatalf("expected tickGroupScores to succeed after cache expiry")
	}
	if fake.rangeCalls != 2 {
		t.Fatalf("expected Range to be called again after cache expiry, got %d calls", fake.rangeCalls)
	}
}

// --- 第 03 条：ensureGroupInstance 正向缓存 ---

// TestEnsureGroupInstanceSkipsGetInstanceWithinVerifyInterval 验证第 03 条：
// 有效期内命中正向缓存时跳过 GetInstance，避免每次 UpsertScore 都产生一次 Redis RTT。
func TestEnsureGroupInstanceSkipsGetInstanceWithinVerifyInterval(t *testing.T) {
	fake := &fakeTickCacheRankService{}
	svc := newTickCacheTestService(t, fake)
	instanceID := "inst-verify-1"
	now := time.Now().UnixMilli()

	svc.markInstanceVerified(instanceID, now)

	if err := svc.ensureGroupInstance(context.Background(), instanceID, 1, now+1000); err != nil {
		t.Fatalf("ensureGroupInstance: %v", err)
	}
	if fake.getInstanceCalls != 0 {
		t.Fatalf("expected GetInstance to be skipped within verify interval, got %d calls", fake.getInstanceCalls)
	}
}

// TestEnsureGroupInstanceReVerifiesAfterIntervalExpires 验证正向缓存到期后重新回源确认。
func TestEnsureGroupInstanceReVerifiesAfterIntervalExpires(t *testing.T) {
	fake := &fakeTickCacheRankService{getInstance: &rank.RankInstance{InstanceId: "inst-verify-2"}}
	svc := newTickCacheTestService(t, fake)
	instanceID := "inst-verify-2"
	now := time.Now().UnixMilli()

	svc.markInstanceVerified(instanceID, now-instanceVerifyInterval-1)

	if err := svc.ensureGroupInstance(context.Background(), instanceID, 1, now); err != nil {
		t.Fatalf("ensureGroupInstance: %v", err)
	}
	if fake.getInstanceCalls != 1 {
		t.Fatalf("expected GetInstance to be called once after verify interval expired, got %d calls", fake.getInstanceCalls)
	}
}

// TestEnsureGroupInstanceCreatesAndMarksVerifiedWhenMissing 验证实例不存在时会创建，
// 并把新实例标记为已确认（供后续调用命中正向缓存）。
func TestEnsureGroupInstanceCreatesAndMarksVerifiedWhenMissing(t *testing.T) {
	fake := &fakeTickCacheRankService{getInstanceErr: rank.ErrInstanceNotFound}
	svc := newTickCacheTestService(t, fake)
	instanceID := "inst-verify-3"
	now := time.Now().UnixMilli()

	if err := svc.ensureGroupInstance(context.Background(), instanceID, 1, now); err != nil {
		t.Fatalf("ensureGroupInstance: %v", err)
	}
	if fake.openInstanceCalls != 1 {
		t.Fatalf("expected OpenInstance to be called once, got %d", fake.openInstanceCalls)
	}

	svc.cacheMu.RLock()
	st, ok := svc.instanceStates[instanceID]
	svc.cacheMu.RUnlock()
	if !ok || st.verifiedAt != now {
		t.Fatalf("expected instanceStates to record verifiedAt=%d, got %+v ok=%v", now, st, ok)
	}

	// 第二次调用应命中正向缓存，不再重新调用 GetInstance/OpenInstance
	// （调用计数应停留在第一次调用时的值，不再增长）。
	if err := svc.ensureGroupInstance(context.Background(), instanceID, 1, now+1); err != nil {
		t.Fatalf("ensureGroupInstance (second call): %v", err)
	}
	if fake.getInstanceCalls != 1 || fake.openInstanceCalls != 1 {
		t.Fatalf("expected second call to hit cache (still 1 GetInstance from the first call, still 1 OpenInstance), got getInstance=%d openInstance=%d",
			fake.getInstanceCalls, fake.openInstanceCalls)
	}
}

// TestCleanupLiveDataInvalidatesInstanceState 验证第 03 条的显式清理钩子：
// CleanupLiveData 给实例设置 TTL 后必须立即清空 instanceStates 里对应的正向缓存标记，
// 否则 Service 仍驻留内存时 ensureGroupInstance 可能在 instanceVerifyInterval 内误判实例仍然存在。
func TestCleanupLiveDataInvalidatesInstanceState(t *testing.T) {
	fake := &fakeTickCacheRankService{}
	svc := newTickCacheTestService(t, fake)

	groupID := int32(7)
	instanceID := svc.groupInstanceID(groupID)
	svc.mu.Lock()
	svc.groups = append(svc.groups, &Group{GroupID: groupID, InstanceID: instanceID, State: GroupStateOpen})
	svc.mu.Unlock()

	svc.markInstanceVerified(instanceID, time.Now().UnixMilli())

	svc.CleanupLiveData()

	svc.cacheMu.RLock()
	_, stillVerified := svc.instanceStates[instanceID]
	svc.cacheMu.RUnlock()
	if stillVerified {
		t.Fatalf("expected instanceStates entry for %s to be invalidated by CleanupLiveData", instanceID)
	}
}
