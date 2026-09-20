package engine

import (
	"testing"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
)

// 本文件锁死第 02 条：写路径落下的每个活跃期 key 都必须带 TTL。
// 只加读路径回填 TTL 而不加写路径 TTL，活跃期 key 仍然是永久的——同一个不变量缺一不可，
// 所以这里逐个写点断言，而不是抽查一个代表。

// TestAllActiveWritePathsLeaveBoundedTTL 覆盖全部 9 个活跃期写点。
// Store 的 activityEnd 传 nil（等价于另外 9 处非活动上下文的 Store），此时 writeTTL() 走
// ttlFor(0) == ColdDataTTL 兜底；重点在于「有 TTL 且为正」，不在具体数值。
func TestAllActiveWritePathsLeaveBoundedTTL(t *testing.T) {
	const bizID = "biz_write_1"
	store, mr := newMiniRedisStore(t, bizID, nil, nil)

	// 每个用例：先执行写操作，再断言它负责的 key 存在且 TTL 为正。
	// 一个写点可能同时写多个 key（IncrRealCount 写 meta + groups），全部列出。
	cases := []struct {
		name  string
		keys  []string
		write func()
	}{
		{
			name: "SaveGroup",
			keys: []string{rediskeys.GetRankGroupsKey(bizID)},
			write: func() {
				_ = store.SaveGroup(&Group{GroupID: 1})
			},
		},
		{
			name: "IncrRealCount",
			keys: []string{rediskeys.GetRankGroupsKey(bizID), rediskeys.GetRankMetaKey(bizID)},
			write: func() {
				if _, err := store.IncrRealCount(&Group{GroupID: 1}); err != nil {
					t.Fatalf("IncrRealCount: %v", err)
				}
			},
		},
		{
			name: "NextGroupID",
			keys: []string{rediskeys.GetRankMetaKey(bizID)},
			write: func() {
				if _, err := store.NextGroupID(); err != nil {
					t.Fatalf("NextGroupID: %v", err)
				}
			},
		},
		{
			name: "RestoreNextGroupID",
			keys: []string{rediskeys.GetRankMetaKey(bizID)},
			write: func() {
				store.RestoreNextGroupID(50)
			},
		},
		{
			name: "SetMember",
			keys: []string{rediskeys.GetRankMembersKey(bizID)},
			write: func() {
				_ = store.SetMember(1001, 1)
			},
		},
		{
			name: "SaveRobots",
			keys: []string{rediskeys.GetRankRobotsKey(bizID, 1)},
			write: func() {
				_ = store.SaveRobots(1, []*robotState{{MemberID: 9001}})
			},
		},
		{
			name: "SaveUsedInfoIDs",
			keys: []string{rediskeys.GetRankRobotInfosKey(bizID, 1)},
			write: func() {
				_ = store.SaveUsedInfoIDs(1, map[int64]struct{}{9001: {}})
			},
		},
		{
			name: "SetClaim",
			keys: []string{rediskeys.GetRankClaimsKey(bizID)},
			write: func() {
				_ = store.SetClaim(1001, 1700000000000)
			},
		},
	}

	for _, tc := range cases {
		tc.write()
		for _, key := range tc.keys {
			if ttl := mustTTL(t, mr, key); ttl <= 0 {
				t.Errorf("%s 写完后 %s 的 TTL=%v ⇒ 永久 key", tc.name, key, ttl)
			}
		}
	}

	// SaveActivityTimes 单独一条：它用本函数收到的活动时间算 TTL，不读 activityEnd 闭包，
	// 所以即使 Store 的 activityEnd 为 nil 也必须设出按活动时间算出的绝对值。
	closeTime := time.Now().Add(30 * 24 * time.Hour).UnixMilli()
	store.SaveActivityTimes(time.Now().UnixMilli(), closeTime, 0)
	metaTTL := mustTTL(t, mr, rediskeys.GetRankMetaKey(bizID))
	want := time.Until(time.UnixMilli(closeTime)) + commonrank.SettledCacheTTL
	if metaTTL < want-time.Minute || metaTTL > want+time.Minute {
		t.Errorf("SaveActivityTimes 后 meta TTL=%v，want ≈%v（activityEnd+SettledCacheTTL，且与 activityEnd 闭包无关）", metaTTL, want)
	}
}

// TestActiveWriteTTLTracksActivityEnd 证明写路径设的是「活动结束 + 保留期」的绝对时刻，
// 而不是一个固定时长——这是 ttlFor 相对写死 SettledCacheTTL 的全部价值所在：
// 活动还剩 90 天时 key 必须活到活动结束后 14 天，而不是从现在起 14 天（那会在活动进行中就被删）。
func TestActiveWriteTTLTracksActivityEnd(t *testing.T) {
	const bizID = "biz_write_2"
	endMs := time.Now().Add(90 * 24 * time.Hour).UnixMilli()
	store, mr := newMiniRedisStore(t, bizID, func() int64 { return endMs }, nil)

	_ = store.SetMember(1001, 1)

	key := rediskeys.GetRankMembersKey(bizID)
	ttl := mustTTL(t, mr, key)
	if ttl <= commonrank.SettledCacheTTL {
		t.Fatalf("活动还剩 90 天时 %s 的 TTL=%v，必须 > SettledCacheTTL(%v)——固定时长会让活跃期的 key 提前过期",
			key, ttl, commonrank.SettledCacheTTL)
	}
	want := time.Until(time.UnixMilli(endMs)) + commonrank.SettledCacheTTL
	if ttl < want-time.Minute || ttl > want+time.Minute {
		t.Fatalf("%s TTL=%v，want ≈%v", key, ttl, want)
	}
}

// TestRepeatedActiveWriteIsIdempotent 锁死「tick 热路径不刷新 TTL」这条设计选择：
// 同一个活动的第 N 次写入必须算到同一个绝对到期时刻，既不给 key 续命（TTL 不增长），
// 也不把既有保留期削短。续命会因为高频写入把 key 永久留在 Redis 里——
// 那只是把「永久 key」换了个形式。
func TestRepeatedActiveWriteIsIdempotent(t *testing.T) {
	const bizID = "biz_write_3"
	endMs := time.Now().Add(60 * 24 * time.Hour).UnixMilli()
	store, mr := newMiniRedisStore(t, bizID, func() int64 { return endMs }, nil)
	key := rediskeys.GetRankMembersKey(bizID)

	_ = store.SetMember(1001, 1)
	first := mustTTL(t, mr, key)

	_ = store.SetMember(1002, 1)
	second := mustTTL(t, mr, key)

	if second > first {
		t.Fatalf("重复写入把 TTL 从 %v 续到了 %v ⇒ 高频写入会让 key 永不回收", first, second)
	}
	if first-second > time.Minute {
		t.Fatalf("重复写入把 TTL 从 %v 削到 %v ⇒ 提前删除活跃数据", first, second)
	}
}

// TestPastActivityWriteStillSetsPositiveTTL 覆盖 ttlFor 的钳制分支（缺陷 7）：
// 活动早已结束时不能因为「算出来是负数」就跳过 EXPIRE——跳过就是永久 key。
// 期望得到 1 分钟的短 TTL，而不是 0。
func TestPastActivityWriteStillSetsPositiveTTL(t *testing.T) {
	const bizID = "biz_write_4"
	endMs := time.Now().Add(-20 * 24 * time.Hour).UnixMilli()
	store, mr := newMiniRedisStore(t, bizID, func() int64 { return endMs }, nil)

	_ = store.SetMember(1001, 1)

	ttl := mustTTL(t, mr, rediskeys.GetRankMembersKey(bizID))
	if ttl <= 0 {
		t.Fatalf("活动已结束时 TTL=%v ⇒ 永久 key（缺陷 7 回归）", ttl)
	}
	if ttl > time.Minute {
		t.Fatalf("活动已结束 20 天时 TTL=%v，want 1 分钟钳制值——算出的绝对时刻早已过去", ttl)
	}
}

// TestBackfillTTLNeverShorterThanActiveWriteTTL 锁死两个 TTL 函数的相对关系：
// 同一活动下 backfillTTL() >= writeTTL()。读路径若比写路径短，一次懒加载回填就会把
// 写路径设好的长 TTL 削掉（活动剩 3 个月时从 ~104d 削到 14d），这是 P3 与 P4 必须同批的原因。
func TestBackfillTTLNeverShorterThanActiveWriteTTL(t *testing.T) {
	for _, offset := range []time.Duration{
		time.Hour,
		24 * time.Hour,
		7 * 24 * time.Hour,
		90 * 24 * time.Hour,
		time.Minute,
		-time.Hour,
		-30 * 24 * time.Hour,
	} {
		endMs := time.Now().Add(offset).UnixMilli()
		store, _ := newMiniRedisStore(t, "biz_ttl_rel", func() int64 { return endMs }, nil)
		if w, b := store.writeTTL(), store.backfillTTL(); b < w {
			t.Errorf("activityEnd 偏移 %v：backfillTTL=%v < writeTTL=%v ⇒ 回填会削短写路径的 TTL", offset, b, w)
		}
		if store.backfillTTL() < commonrank.SettledCacheTTL {
			t.Errorf("activityEnd 偏移 %v：backfillTTL=%v < SettledCacheTTL ⇒ 永久 key 或提前删除", offset, store.backfillTTL())
		}
	}
}
