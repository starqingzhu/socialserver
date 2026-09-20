package engine

import (
	"context"
	"testing"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
	goredis "golib/redis"

	"github.com/alicebob/miniredis/v2"
)

// 本文件覆盖缺陷 6：结算（Settle）与周期轮次清理（CleanupLiveData）都必须给活跃期 key 设 TTL，
// 且两者「实现统一、TTL 值不统一」——周期路径必须继续用固定的 2 周，不能改成按活动算的绝对值。

// newMiniRedisSettleService 构造一个 rdb 真实（miniredis）、dao 缺失的 Service，
// 配 fakeSettleRankService，使 Settle 的整条路径（含设 TTL）都能在单测里跑起来。
func newMiniRedisSettleService(t *testing.T, cfg Config) (*Service, *fakeSettleRankService, *miniredis.Miniredis) {
	t.Helper()
	if cfg.BizType == "" {
		cfg.BizType = "ttltest"
	}
	if cfg.ActID == 0 {
		cfg.ActID = 1
	}
	if cfg.RankCode == "" {
		cfg.RankCode = "test_score_settle_ttl"
	}
	if cfg.RankPeopleNum == 0 {
		cfg.RankPeopleNum = 10
	}
	mr := miniredis.RunT(t)
	rdb := goredis.NewRedis(&goredis.RedisConfig{RedisAddrs: []string{mr.Addr()}})
	if rdb == nil {
		t.Fatalf("miniredis client: NewRedis returned nil for addr %s", mr.Addr())
	}
	fake := &fakeSettleRankService{}
	svc, err := NewService(fake, cfg, rdb, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, fake, mr
}

// liveKeys 是活跃期数据的 key 集合，与 Store.ExpireLiveData 覆盖的集合一一对应。
// Settle 与 CleanupLiveData 共用同一份定义，因此两边都能用它做全量断言。
func liveKeys(bizID string, groupIDs ...int32) []string {
	keys := []string{
		rediskeys.GetRankMetaKey(bizID),
		rediskeys.GetRankGroupsKey(bizID),
		rediskeys.GetRankMembersKey(bizID),
		rediskeys.GetRankClaimsKey(bizID),
		rediskeys.GetRankMongoCheckedKey(bizID),
	}
	for _, gid := range groupIDs {
		keys = append(keys,
			rediskeys.GetRankRobotsKey(bizID, gid),
			rediskeys.GetRankRobotInfosKey(bizID, gid))
	}
	return keys
}

// seedLiveData 让活跃期 key 集合全部存在，供 TTL 断言有对象可查。
func seedLiveData(t *testing.T, svc *Service, groupID int32) {
	t.Helper()
	group := &Group{
		GroupID:    groupID,
		InstanceID: svc.groupInstanceID(groupID),
		State:      GroupStateOpen,
	}
	if err := svc.store.SaveGroup(group); err != nil {
		t.Fatalf("SaveGroup: %v", err)
	}
	if err := svc.store.SetMember(1001, groupID); err != nil {
		t.Fatalf("SetMember: %v", err)
	}
	if err := svc.store.SetClaim(1001, 1700000000000); err != nil {
		t.Fatalf("SetClaim: %v", err)
	}
	if err := svc.store.SaveRobots(groupID, []*robotState{{MemberID: 9001}}); err != nil {
		t.Fatalf("SaveRobots: %v", err)
	}
	if err := svc.store.SaveUsedInfoIDs(groupID, map[int64]struct{}{9001: {}}); err != nil {
		t.Fatalf("SaveUsedInfoIDs: %v", err)
	}
	svc.store.setMongoChecked()
}

// TestSettleSetsTTLOnAllLiveKeys 是缺陷 6 的主断言：Settle 之后活跃期 key 集合里
// 每一个 key 都必须带 TTL，且不短于 SettledCacheTTL。
// 前置先 PERSIST 掉所有 TTL，所以这些 TTL 只能是 Settle 设的。
func TestSettleSetsTTLOnAllLiveKeys(t *testing.T) {
	now := time.Now().UnixMilli()
	svc, fake, mr := newMiniRedisSettleService(t, Config{
		OpenTime:  now - int64(time.Hour/time.Millisecond),
		CloseTime: now - int64(time.Minute/time.Millisecond), // 已关闭
	})
	bizID := svc.bizId()
	seedLiveData(t, svc, 1)

	keys := liveKeys(bizID, 1)
	for _, k := range keys {
		if _, err := svc.store.rdb.PERSIST(k); err != nil {
			t.Fatalf("PERSIST %s: %v", k, err)
		}
		if mr.TTL(k) != 0 {
			t.Fatalf("前置失败：PERSIST 后 %s TTL=%v，应为 0", k, mr.TTL(k))
		}
	}

	if _, err := svc.Settle(context.Background()); err != nil {
		t.Fatalf("Settle: %v", err)
	}

	for _, k := range keys {
		ttl := mustTTL(t, mr, k)
		if ttl < commonrank.SettledCacheTTL {
			t.Errorf("Settle 后 %s TTL=%v，want >= %v（缺陷 6：不设 TTL 就是永久 key）",
				k, ttl, commonrank.SettledCacheTTL)
		}
	}

	// 已结算分组的 rank:inst/mb/seq/settled 走 ExpireInstance，也要被设上同一个 TTL。
	if len(fake.expireTTLs) == 0 {
		t.Fatalf("Settle 未对任何实例调用 ExpireInstance ⇒ rank:inst/mb/seq 仍是永久 key")
	}
	for i, d := range fake.expireTTLs {
		if d < commonrank.SettledCacheTTL {
			t.Errorf("第 %d 次 ExpireInstance TTL=%v，want >= %v", i+1, d, commonrank.SettledCacheTTL)
		}
	}
}

// TestSettleTTLCoversAlreadySettledGroups 覆盖崩溃窗口：分组在之前的进程里已被置为 settled，
// 但没来得及设 TTL。Settle 的循环对这类分组直接 continue，若不显式把它们收进 settledGroups，
// 它们的 key 永远补不上 TTL——这是缺陷 6 里最隐蔽的一条漏网路径。
func TestSettleTTLCoversAlreadySettledGroups(t *testing.T) {
	now := time.Now().UnixMilli()
	svc, _, mr := newMiniRedisSettleService(t, Config{
		OpenTime:  now - int64(time.Hour/time.Millisecond),
		CloseTime: now - int64(time.Minute/time.Millisecond),
	})
	bizID := svc.bizId()
	seedLiveData(t, svc, 1)

	// 关键前置：把分组改成「已结算」——Settle 会跳过它，但 TTL 仍必须补上。
	group := &Group{GroupID: 1, InstanceID: svc.groupInstanceID(1), State: GroupStateSettled}
	if err := svc.store.SaveGroup(group); err != nil {
		t.Fatalf("SaveGroup: %v", err)
	}
	keys := liveKeys(bizID, 1)
	for _, k := range keys {
		if _, err := svc.store.rdb.PERSIST(k); err != nil {
			t.Fatalf("PERSIST %s: %v", k, err)
		}
	}

	if _, err := svc.Settle(context.Background()); err != nil {
		t.Fatalf("Settle: %v", err)
	}

	for _, k := range keys {
		ttl := mustTTL(t, mr, k)
		if ttl < commonrank.SettledCacheTTL {
			t.Errorf("崩溃窗口分组：Settle 后 %s TTL=%v，want >= %v（已被 settled 的分组被漏掉了）",
				k, ttl, commonrank.SettledCacheTTL)
		}
	}
}

// TestSettleTTLUsesFloorNotClampForLongPastActivity 证明结算路径用的是带下限的 backfillTTL()，
// 而不是裸的 ttlFor。活动已结束 30 天时 ttlFor 会算出负值并被钳制成 1 分钟——
// 那会把刚结算的数据在 1 分钟内删掉。下限保证至少 14 天。
func TestSettleTTLUsesFloorNotClampForLongPastActivity(t *testing.T) {
	const thirtyDays = int64(30 * 24 * time.Hour / time.Millisecond)
	now := time.Now().UnixMilli()
	svc, fake, mr := newMiniRedisSettleService(t, Config{
		OpenTime:  now - 2*thirtyDays,
		CloseTime: now - thirtyDays, // 已结束整整 30 天：ttlFor 会钳制成 1 分钟
	})
	bizID := svc.bizId()
	seedLiveData(t, svc, 1)

	// 先确认前置条件成立：裸 ttlFor 在这个活动上确实会钳制。
	if got := ttlFor(svc.effectiveSettleAt()); got != time.Minute {
		t.Fatalf("前置失败：ttlFor(已结束 30 天)=%v，本用例要求它为 1 分钟的钳制值", got)
	}

	if _, err := svc.Settle(context.Background()); err != nil {
		t.Fatalf("Settle: %v", err)
	}

	for _, k := range liveKeys(bizID, 1) {
		ttl := mustTTL(t, mr, k)
		if ttl <= time.Minute {
			t.Errorf("Settle 后 %s TTL=%v ⇒ 用了 ttlFor 的钳制值，结算数据会在 1 分钟内被删（缺陷 6）", k, ttl)
		}
		if ttl < commonrank.SettledCacheTTL {
			t.Errorf("Settle 后 %s TTL=%v，want >= %v", k, ttl, commonrank.SettledCacheTTL)
		}
	}
	for i, d := range fake.expireTTLs {
		if d <= time.Minute {
			t.Errorf("第 %d 次 ExpireInstance TTL=%v ⇒ 钳制值，会把已结算实例立刻删掉", i+1, d)
		}
	}
}

// TestSettleDoesNotTouchGroupsHeldByOtherNodes 锁死「只对已结算分组设 TTL」：
// 抢不到结算锁（另一节点正在结算）的分组本轮不结算，它的 per-group key 就不能被设上 TTL，
// 否则等于把一个仍在使用的活跃分组提前判了死刑。
func TestSettleDoesNotTouchGroupsHeldByOtherNodes(t *testing.T) {
	now := time.Now().UnixMilli()
	svc, _, mr := newMiniRedisSettleService(t, Config{
		OpenTime:  now - int64(time.Hour/time.Millisecond),
		CloseTime: now - int64(time.Minute/time.Millisecond),
	})
	bizID := svc.bizId()
	seedLiveData(t, svc, 1)

	// 抢先占住分组 1 的结算锁，模拟另一节点正在结算它。
	lockKey := "rank:settle:{" + bizID + "}:1"
	if ok, err := svc.store.rdb.SetNX(lockKey, "1", 10*time.Minute); err != nil || !ok {
		t.Fatalf("预占结算锁失败: ok=%v err=%v", ok, err)
	}
	robotsKey := rediskeys.GetRankRobotsKey(bizID, 1)
	if _, err := svc.store.rdb.PERSIST(robotsKey); err != nil {
		t.Fatalf("PERSIST %s: %v", robotsKey, err)
	}

	if _, err := svc.Settle(context.Background()); err != nil {
		t.Fatalf("Settle: %v", err)
	}

	if ttl := mr.TTL(robotsKey); ttl != 0 {
		t.Fatalf("抢锁失败的分组其 %s 被设了 TTL=%v ⇒ 未结算分组的活跃数据会被提前删除", robotsKey, ttl)
	}
}

// TestCleanupLiveDataKeepsConstantRetention 锁死「实现统一、TTL 值不统一」：
// 周期轮次清理必须继续用固定的 2 周，而不是按活动结算时刻算出的（更长的）绝对值。
// 活动还剩 60 天时写路径的 TTL 远大于 2 周，若 CleanupLiveData 误用同一个函数，
// 这里会看到远大于 SettledCacheTTL 的值。
func TestCleanupLiveDataKeepsConstantRetention(t *testing.T) {
	const sixtyDays = int64(60 * 24 * time.Hour / time.Millisecond)
	now := time.Now().UnixMilli()
	svc, _, mr := newMiniRedisSettleService(t, Config{
		OpenTime:    now - sixtyDays,
		CloseTime:   now + sixtyDays, // 活动还剩 60 天
		GameEndTime: now + sixtyDays,
	})
	bizID := svc.bizId()
	seedLiveData(t, svc, 1)

	// 前置：确认写路径的 TTL 在这个活动上确实远大于 2 周，否则本用例证明不了「值没被统一」。
	if w := svc.store.writeTTL(); w <= commonrank.SettledCacheTTL {
		t.Fatalf("前置失败：writeTTL=%v，本用例要求它 > SettledCacheTTL(%v)", w, commonrank.SettledCacheTTL)
	}

	svc.CleanupLiveData()

	for _, k := range liveKeys(bizID, 1) {
		ttl := mustTTL(t, mr, k)
		if ttl != commonrank.SettledCacheTTL {
			t.Errorf("CleanupLiveData 后 %s TTL=%v，want 恰好 %v（周期路径必须是固定 2 周）",
				k, ttl, commonrank.SettledCacheTTL)
		}
	}
}

// TestSettleTTLNeverShortensOnRepeat 覆盖 GM 把活动时间改早之后再结算一遍的场景：
// 此时 groups 全部已是 settled（走崩溃窗口分支），结算时刻却落在很久以前，
// 裸 ttlFor 会算出 1 分钟的钳制值。结算路径必须用带下限的 backfillTTL()，
// 否则一次配置变更就能把已结算的数据在 1 分钟内清空。
func TestSettleTTLNeverShortensOnRepeat(t *testing.T) {
	const fortyDays = int64(40 * 24 * time.Hour / time.Millisecond)
	now := time.Now().UnixMilli()
	svc, _, mr := newMiniRedisSettleService(t, Config{
		OpenTime:  now - int64(time.Hour/time.Millisecond),
		CloseTime: now - int64(time.Minute/time.Millisecond),
	})
	bizID := svc.bizId()
	seedLiveData(t, svc, 1)

	if _, err := svc.Settle(context.Background()); err != nil {
		t.Fatalf("第一次 Settle: %v", err)
	}
	metaKey := rediskeys.GetRankMetaKey(bizID)
	before := mustTTL(t, mr, metaKey)

	// GM 把活动结束时间改到 40 天前：settleAt 变化 ⇒ settledAt 闸门不再短路，
	// 结算会重跑一遍，且此时 ttlFor 必然退化成 1 分钟钳制值。
	svc.mu.Lock()
	svc.config.CloseTime = now - fortyDays
	svc.config.GameEndTime = 0
	svc.refreshActivityEndLocked()
	svc.mu.Unlock()
	if got := ttlFor(svc.effectiveSettleAt()); got != time.Minute {
		t.Fatalf("前置失败：改早后 ttlFor=%v，本用例要求它为 1 分钟的钳制值", got)
	}

	if _, err := svc.Settle(context.Background()); err != nil {
		t.Fatalf("第二次 Settle: %v", err)
	}

	after := mustTTL(t, mr, metaKey)
	if after < commonrank.SettledCacheTTL {
		t.Fatalf("重复 Settle 后 %s TTL=%v，want >= %v ⇒ 已结算数据被 1 分钟钳制值提前删除",
			metaKey, after, commonrank.SettledCacheTTL)
	}
	if after < before-time.Minute {
		t.Fatalf("重复 Settle 把 %s 的 TTL 从 %v 削到 %v ⇒ 重复结算会缩短保留期", metaKey, before, after)
	}
}

// TestSetMongoCheckedDoesNotShortenExistingTTL 覆盖待办 J：
// rank:mongo_chk 同时属于 ExpireLiveData 的 key 集合（被设成 2 周保留期）。
// setMongoChecked 若用 SetEX，会把那 2 周削成 mongoCheckedTTL(10 分钟)。
func TestSetMongoCheckedDoesNotShortenExistingTTL(t *testing.T) {
	svc, _, mr := newMiniRedisSettleService(t, Config{})
	key := rediskeys.GetRankMongoCheckedKey(svc.bizId())

	// 1) 造出 key（10 分钟哨兵）。
	svc.store.setMongoChecked()
	mustTTL(t, mr, key)

	// 2) 提升到 2 周保留期，等价于 ExpireLiveData 对该 key 做的事。
	if _, err := svc.store.rdb.Expire(key, commonrank.SettledCacheTTL); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	before := mustTTL(t, mr, key)
	if before < commonrank.SettledCacheTTL {
		t.Fatalf("前置失败：%s TTL=%v，want >= %v", key, before, commonrank.SettledCacheTTL)
	}

	// 3) 再判定一次「Mongo 为空」：这一步绝不能把 TTL 削回 10 分钟。
	svc.store.setMongoChecked()
	after := mustTTL(t, mr, key)

	if after < before {
		t.Fatalf("setMongoChecked 把 %s 的 TTL 从 %v 削到 %v（待办 J：SetEX 会清掉已有 TTL）", key, before, after)
	}
	if after < commonrank.SettledCacheTTL {
		t.Fatalf("setMongoChecked 后 %s TTL=%v 短于 2 周保留期，说明既有 TTL 被哨兵期覆盖", key, after)
	}
}
