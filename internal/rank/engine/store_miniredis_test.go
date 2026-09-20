package engine

import (
	"testing"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
)

// TestBackfillAfterExpiryRecreatesBoundedKey 是本方案最重要的一条断言（缺陷 3）。
// 它同时证明三件事：回填的 key 带 TTL；TTL 到期后 key 真的消失；再次回填产生的新 key
// 仍然带 TTL —— 而不是变成永久 key，也没有退化成 ttlFor 的 1 分钟夹紧值。
func TestBackfillAfterExpiryRecreatesBoundedKey(t *testing.T) {
	dao := newFakeMongo()
	dao.members[1001] = 3
	// activityEnd 为 nil ⇒ 非活动上下文，backfillTTL 恒为 SettledCacheTTL
	store, mr := newMiniRedisStore(t, "biz_a_1", nil, dao)
	membersKey := rediskeys.GetRankMembersKey("biz_a_1")

	// 1) 首次读：Redis miss ⇒ 命中 Mongo 并回填
	gid, found, err := store.GetMember(1001)
	if err != nil || !found || gid != 3 {
		t.Fatalf("first GetMember = (%d,%v,%v), want (3,true,nil)", gid, found, err)
	}
	if ttl := mustTTL(t, mr, membersKey); ttl <= 0 {
		t.Fatalf("缺陷 3：回填出的 %s 没有 TTL（TTL=%v）⇒ 永久 key", membersKey, ttl)
	} else if ttl < commonrank.SettledCacheTTL {
		t.Fatalf("回填 TTL=%v 短于 SettledCacheTTL(%v) ⇒ 会让后续查询反复打 Mongo", ttl, commonrank.SettledCacheTTL)
	}

	// 2) 推进虚拟时钟越过保留期：key 必须真的过期消失
	mr.FastForward(commonrank.SettledCacheTTL + time.Hour)
	if mr.Exists(membersKey) {
		t.Fatalf("期望 %s 在保留期后过期，但它仍然存在", membersKey)
	}

	// 3) 再次读：再次 miss、再次回填。新 key 的 TTL 必须重新带上且恰为下限——
	//    这一步才是"不会重建成永久 key"的真正证明（第 1 步只覆盖了首次写入）。
	gid, found, err = store.GetMember(1001)
	if err != nil || !found || gid != 3 {
		t.Fatalf("second GetMember = (%d,%v,%v), want (3,true,nil)", gid, found, err)
	}
	if ttl := mustTTL(t, mr, membersKey); ttl != commonrank.SettledCacheTTL {
		t.Fatalf("重新回填后 TTL=%v，want 恰好 %v（既不能是 0，也不能是 1 分钟的振荡值）",
			ttl, commonrank.SettledCacheTTL)
	}
}

// TestLoadGroupsBackfillCarriesTTL 覆盖 LoadGroups 的懒加载回填路径。
func TestLoadGroupsBackfillCarriesTTL(t *testing.T) {
	dao := newFakeMongo()
	dao.groups[7] = &Group{GroupID: 7}
	store, mr := newMiniRedisStore(t, "biz_b_1", nil, dao)
	key := rediskeys.GetRankGroupsKey("biz_b_1")

	groups, err := store.LoadGroups()
	if err != nil || len(groups) != 1 {
		t.Fatalf("LoadGroups = (%d groups, %v), want (1, nil)", len(groups), err)
	}
	if ttl := mustTTL(t, mr, key); ttl < commonrank.SettledCacheTTL {
		t.Fatalf("LoadGroups 回填的 %s TTL=%v，want >= %v", key, ttl, commonrank.SettledCacheTTL)
	}
}

// TestGetAllMembersBackfillCarriesTTL 覆盖 GetAllMembers 的批量懒加载回填路径。
func TestGetAllMembersBackfillCarriesTTL(t *testing.T) {
	dao := newFakeMongo()
	dao.members[11] = 1
	dao.members[22] = 2
	store, mr := newMiniRedisStore(t, "biz_c_1", nil, dao)
	key := rediskeys.GetRankMembersKey("biz_c_1")

	members, err := store.GetAllMembers()
	if err != nil || len(members) != 2 {
		t.Fatalf("GetAllMembers = (%d members, %v), want (2, nil)", len(members), err)
	}
	if ttl := mustTTL(t, mr, key); ttl < commonrank.SettledCacheTTL {
		t.Fatalf("GetAllMembers 回填的 %s TTL=%v，want >= %v", key, ttl, commonrank.SettledCacheTTL)
	}
}

// TestLoadRobotsBackfillCarriesTTL 覆盖 LoadRobots 的分组级回填路径。
func TestLoadRobotsBackfillCarriesTTL(t *testing.T) {
	dao := newFakeMongo()
	dao.robots[5] = []*robotState{{MemberID: 9001}}
	store, mr := newMiniRedisStore(t, "biz_d_1", nil, dao)
	key := rediskeys.GetRankRobotsKey("biz_d_1", 5)

	robots, err := store.LoadRobots(5)
	if err != nil || len(robots) != 1 {
		t.Fatalf("LoadRobots = (%d robots, %v), want (1, nil)", len(robots), err)
	}
	if ttl := mustTTL(t, mr, key); ttl < commonrank.SettledCacheTTL {
		t.Fatalf("LoadRobots 回填的 %s TTL=%v，want >= %v", key, ttl, commonrank.SettledCacheTTL)
	}
}

// TestGetClaimBackfillAndNegativeCacheBothCarryTTL 覆盖 GetClaim 的两条写入分支：
// Mongo 命中回填一个真实值、Mongo 未命中写负缓存哨兵。两者都必须带 TTL——
// 负缓存尤其容易被漏掉，而它一旦永久驻留就会一直压着"Mongo 没有该 claim"这个已过期的结论。
func TestGetClaimBackfillAndNegativeCacheBothCarryTTL(t *testing.T) {
	dao := newFakeMongo()
	dao.claims[100] = 1700000000000
	store, mr := newMiniRedisStore(t, "biz_e_1", nil, dao)
	key := rediskeys.GetRankClaimsKey("biz_e_1")

	// 分支一：Mongo 命中
	ct, found, err := store.GetClaim(100)
	if err != nil || !found || ct != 1700000000000 {
		t.Fatalf("GetClaim(100) = (%d,%v,%v), want (1700000000000,true,nil)", ct, found, err)
	}
	if ttl := mustTTL(t, mr, key); ttl < commonrank.SettledCacheTTL {
		t.Fatalf("GetClaim 命中回填的 %s TTL=%v，want >= %v", key, ttl, commonrank.SettledCacheTTL)
	}

	// 分支二：Mongo 未命中 ⇒ 负缓存哨兵，同样必须带 TTL
	_, found, err = store.GetClaim(200)
	if err != nil || found {
		t.Fatalf("GetClaim(200) = (_,%v,%v), want (_,false,nil)", found, err)
	}
	if ttl := mustTTL(t, mr, key); ttl < commonrank.SettledCacheTTL {
		t.Fatalf("负缓存写入后 %s TTL=%v，want >= %v（哨兵不能永久驻留）", key, ttl, commonrank.SettledCacheTTL)
	}
}

// TestAtomicClaimSetsTTL 覆盖 AtomicClaim：Lua 脚本只 HSET 不设过期，TTL 必须由 Go 侧补上，
// 否则 rank:claims 是永久 key。这里也顺带验证脚本后续分支的 HSET 不会清掉已设的 TTL
// （HSET 只改值；会清 TTL 的是不带 KEEPTTL 的 SET）——因此一次 Expire 足以覆盖三个写入分支。
func TestAtomicClaimSetsTTL(t *testing.T) {
	dao := newFakeMongo()
	store, mr := newMiniRedisStore(t, "biz_f_1", nil, dao)
	key := rediskeys.GetRankClaimsKey("biz_f_1")

	claimed, _, err := store.AtomicClaim(300, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("AtomicClaim: %v", err)
	}
	if claimed {
		t.Fatalf("首次领取不应报告为已领取")
	}
	if ttl := mustTTL(t, mr, key); ttl < commonrank.SettledCacheTTL {
		t.Fatalf("AtomicClaim 后 %s TTL=%v，want >= %v（Lua 不设过期，必须由 Go 侧补）",
			key, ttl, commonrank.SettledCacheTTL)
	}

	// 已领取分支（claimedInt==1，Lua 未写入）不得把既有 TTL 弄丢或改短。
	claimed, _, err = store.AtomicClaim(300, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("second AtomicClaim: %v", err)
	}
	if !claimed {
		t.Fatalf("第二次领取应报告为已领取")
	}
	if ttl := mustTTL(t, mr, key); ttl < commonrank.SettledCacheTTL {
		t.Fatalf("重复领取后 %s TTL=%v，want >= %v", key, ttl, commonrank.SettledCacheTTL)
	}
}

// TestBackfillWritesBeforeExpire 锁死 backfill 的顺序：必须先写、后 EXPIRE。
// 反过来（EXPIRE 作用在不存在的 key 上）会被 Redis 静默丢弃，结果就是 TTL 为 0 的永久 key。
func TestBackfillWritesBeforeExpire(t *testing.T) {
	store, mr := newMiniRedisStore(t, "biz_g_1", nil, nil)
	key := "rank:test:order"

	store.backfill(key, func() {
		store.rdb.HSet(key, "f", "v")
	})
	if ttl := mustTTL(t, mr, key); ttl != commonrank.SettledCacheTTL {
		t.Fatalf("backfill 后 TTL=%v，want %v", ttl, commonrank.SettledCacheTTL)
	}

	// 对照：对不存在的 key 先 EXPIRE 再写，TTL 必然丢失。这就是顺序不能反的证明。
	other := "rank:test:order-bad"
	_, _ = store.rdb.Expire(other, time.Hour)
	store.rdb.HSet(other, "f", "v")
	if ttl := mustTTL(t, mr, other); ttl != 0 {
		t.Fatalf("对照组 TTL=%v，期望 0（EXPIRE 作用在不存在的 key 上被丢弃）——若此断言失败说明前提有误", ttl)
	}
}
