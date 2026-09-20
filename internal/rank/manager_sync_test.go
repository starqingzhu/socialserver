package rankservice

import (
	"context"
	"fmt"
	"testing"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
	goredis "golib/redis"
	"socialserver/internal/rank/engine"

	"github.com/alicebob/miniredis/v2"
)

// TestActivityStillOpen 覆盖 docs/rank_optimization.md 第 09 条的完备性论证表：
// syncFromMongo 用它区分「MongoDB 缺失只是异步写被丢弃」（应修复）还是
// 「活动已结束、大概率是 GM 删除」（应删除）。
func TestActivityStillOpen(t *testing.T) {
	now := time.Now().UnixMilli()
	cases := []struct {
		name      string
		closeTime int64
		want      bool
	}{
		{"permanent activity (CloseTime==0)", 0, true},
		{"mid-activity", now + int64(time.Hour/time.Millisecond), true},
		{"just closed (now == CloseTime)", now, false},
		{"long ended", now - int64(time.Hour/time.Millisecond), false},
	}
	for _, c := range cases {
		got := activityStillOpen(engine.Config{CloseTime: c.closeTime}, now)
		if got != c.want {
			t.Errorf("%s: activityStillOpen(CloseTime=%d, now=%d) = %v, want %v", c.name, c.closeTime, now, got, c.want)
		}
	}
}

// fakeCleanupRankService tracks DeleteRankDef calls so tests can confirm
// cleanupServiceData actually invoked svc.Cleanup() rather than silently no-op'ing.
type fakeCleanupRankService struct {
	commonrank.Service
	deleteRankDefCalls int
}

func (f *fakeCleanupRankService) DeleteRankDef(ctx context.Context, rankCode string) error {
	f.deleteRankDefCalls++
	return nil
}

// newCleanupOrderFixture 造出待办 E 所依赖的最小现场：`rank:members` 里有成员，
// `rank:member_index` 里有对应的索引条目。两侧都用真实 miniredis，
// 因为这条不变量讲的正是"两个 key 之间的顺序"，用内存实现或伪造的 Redis 都证明不了。
func newCleanupOrderFixture(t *testing.T, bizType BizType, actID int32) (*Manager, *engine.Service, *goredis.Redis, *miniredis.Miniredis, int64) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewRedis(&goredis.RedisConfig{RedisAddrs: []string{mr.Addr()}})
	if rdb == nil {
		t.Fatalf("miniredis client: NewRedis returned nil for addr %s", mr.Addr())
	}
	cfg := engine.Config{
		BizType:       string(bizType),
		ActID:         actID,
		RankCode:      fmt.Sprintf("%s_score_%d", bizType, actID),
		RankPeopleNum: 10,
	}
	svc, err := engine.NewService(&fakeCleanupRankService{}, cfg, rdb, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	m := &Manager{
		rdb:         rdb,
		rankService: &fakeCleanupRankService{},
		memberIndex: NewMemberIndex(rdb, memberIndexTTL),
	}
	m.rankService = &fakeCleanupRankService{}

	const userID, groupID = int64(70001), int32(2)
	// Manager 侧统一用 "{bizType}_{actID}" 作为 bizId（forceCleanupOrphan 同款写法）。
	bizID := fmt.Sprintf("%s_%d", bizType, actID)
	if _, err := rdb.HSet(rediskeys.GetRankMembersKey(bizID), fmt.Sprint(userID), fmt.Sprint(groupID)); err != nil {
		t.Fatalf("HSet rank:members: %v", err)
	}
	m.memberIndex.Track(userID, MemberEntry{BizType: bizType, ActID: actID, GroupID: groupID})
	return m, svc, rdb, mr, userID
}

// TestCleanupServiceDataRemovesIndexBeforeCleanup 是待办 E 的正向断言：
// cleanupServiceData 必须先把索引条目按 `rank:members` 清干净，再让 Cleanup 删掉那张表。
func TestCleanupServiceDataRemovesIndexBeforeCleanup(t *testing.T) {
	m, svc, rdb, mr, userID := newCleanupOrderFixture(t, "eorder", 1)
	entry := encodeMemberEntry(MemberEntry{BizType: "eorder", ActID: 1, GroupID: 2})
	if !indexMembers(t, mr, userID)[entry] {
		t.Fatalf("前置失败：索引条目 %s 未写入", entry)
	}

	m.cleanupServiceData(svc, "eorder", 1)

	if indexMembers(t, mr, userID)[entry] {
		t.Fatalf("cleanupServiceData 之后索引条目 %s 仍存在 ⇒ 索引清理没有跑在 rank:members 被删之前", entry)
	}
	if exists, _ := rdb.Exists(rediskeys.GetRankMembersKey("eorder_1")); exists {
		t.Fatalf("cleanupServiceData 之后 rank:members 仍存在 ⇒ Cleanup 没有生效")
	}
}

// TestCleanupBeforeIndexRemovalLosesTheIndex 是同一不变量的反向对照：
// 先 Cleanup（rank:members 没了），再想清索引就已经读不到成员表了。
// 这个用例锁死"顺序是承重的"——若哪天有人把两行调换，上面的用例会红，
// 而这里证明了红的理由不是风格问题，是索引条目会真的留下来。
func TestCleanupBeforeIndexRemovalLosesTheIndex(t *testing.T) {
	_, svc, _, mr, userID := newCleanupOrderFixture(t, "ewrong", 1)
	entry := encodeMemberEntry(MemberEntry{BizType: "ewrong", ActID: 1, GroupID: 2})

	svc.Cleanup() // 故意先做错的那一步

	members, err := svc.GetAllMembers()
	if err != nil {
		t.Fatalf("GetAllMembers: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("前置失败：Cleanup 之后 rank:members 仍有 %d 条，本例证明不了顺序的承重性", len(members))
	}
	if !indexMembers(t, mr, userID)[entry] {
		t.Fatalf("前置失败：顺序反了却仍然清掉了索引，说明这条不变量并不承重")
	}
	// 反向顺序下索引条目只能靠 7 天 TTL 兜底——这正是把 Cleanup 封进 cleanupServiceData 的理由。
}

// TestForceCleanupOrphanRemovesIndexBeforeCleanup 覆盖 forceCleanupOrphan 这条并列路径：
// 它不经过 engine.Service，但必须遵守同一条顺序不变量（先读 rank:members 清索引，再 CleanupAll）。
func TestForceCleanupOrphanRemovesIndexBeforeCleanup(t *testing.T) {
	m, _, rdb, mr, userID := newCleanupOrderFixture(t, "eorphan", 1)
	entry := encodeMemberEntry(MemberEntry{BizType: "eorphan", ActID: 1, GroupID: 2})

	m.forceCleanupOrphan(context.Background(), "eorphan", 1)

	if indexMembers(t, mr, userID)[entry] {
		t.Fatalf("forceCleanupOrphan 之后索引条目 %s 仍存在 ⇒ 它与 cleanupServiceData 的顺序不一致", entry)
	}
	if exists, _ := rdb.Exists(rediskeys.GetRankMembersKey("eorphan_1")); exists {
		t.Fatalf("forceCleanupOrphan 之后 rank:members 仍存在")
	}
}

// TestCleanupServiceDataAlwaysRunsCleanupEvenWithoutMembers 验证第 08 条统一清理入口
// cleanupServiceData：即便 GetAllMembers 返回空（本用例用 store 降级态模拟 Redis 抖动/无数据），
// svc.Cleanup() 仍必须无条件执行——否则会重现「删除服务只清了内存/索引，残留 Redis+Mongo 孤儿数据」
// 的缺陷（syncFromMongo 此前就漏了这一步）。
func TestCleanupServiceDataAlwaysRunsCleanupEvenWithoutMembers(t *testing.T) {
	fake := &fakeCleanupRankService{}
	cfg := engine.Config{
		BizType:       "fake",
		ActID:         1,
		RankCode:      "test_score_fake",
		RankPeopleNum: 10,
	}
	svc, err := engine.NewService(fake, cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	m := &Manager{memberIndex: NewMemberIndex(nil, 0)}
	m.cleanupServiceData(svc, "fake", 1)

	if fake.deleteRankDefCalls != 1 {
		t.Fatalf("expected Cleanup() to run exactly once (DeleteRankDef called), got %d calls", fake.deleteRankDefCalls)
	}
}
