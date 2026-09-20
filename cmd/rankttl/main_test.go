package main

import (
	"testing"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
)

// TestClassifyCoversEveryRealKey 用 common/redis 的真实构造函数生成 key，而不是手写字面量。
// 这样一旦某个 key 的格式变了，本用例会失败，而不是让工具静默漏掉或错误归类一整类 key。
func TestClassifyCoversEveryRealKey(t *testing.T) {
	cases := []struct {
		key  string
		want string
		ttl  time.Duration
	}{
		{rediskeys.GetRankDefKey("balloon_score_1001"), "rank:def", commonrank.ColdDataTTL},
		{rediskeys.GetRankMemberIndexKey(20001), "rank:member_index", commonrank.ColdDataTTL},
		{rediskeys.RankActiveServicesKey, "rank:{active_services}", commonrank.ColdDataTTL},

		{rediskeys.GetRankInstKey("inst1"), "rank:inst", commonrank.SettledCacheTTL},
		{rediskeys.GetRankMbKey("inst1"), "rank:mb", commonrank.SettledCacheTTL},
		{rediskeys.GetRankSeqKey("inst1"), "rank:seq", commonrank.SettledCacheTTL},
		{rediskeys.GetRankSettledKey("inst1"), "rank:settled", commonrank.SettledCacheTTL},
		{rediskeys.GetRankMetaKey("balloon_1001"), "rank:meta", commonrank.SettledCacheTTL},
		{rediskeys.GetRankGroupsKey("balloon_1001"), "rank:groups", commonrank.SettledCacheTTL},
		{rediskeys.GetRankMembersKey("balloon_1001"), "rank:members", commonrank.SettledCacheTTL},
		{rediskeys.GetRankRobotsKey("balloon_1001", 3), "rank:robots", commonrank.SettledCacheTTL},
		{rediskeys.GetRankRobotInfosKey("balloon_1001", 3), "rank:robot_infos", commonrank.SettledCacheTTL},
		{rediskeys.GetRankClaimsKey("balloon_1001"), "rank:claims", commonrank.SettledCacheTTL},
		{rediskeys.GetRankMongoCheckedKey("balloon_1001"), "rank:mongo_chk", commonrank.SettledCacheTTL},
	}

	for _, tc := range cases {
		c, ok := classify(tc.key)
		if !ok {
			t.Errorf("%s 未被识别（应为 %s）", tc.key, tc.want)
			continue
		}
		if c.name != tc.want {
			t.Errorf("%s 归到了 %s，应为 %s", tc.key, c.name, tc.want)
		}
		if c.ttl != tc.ttl {
			t.Errorf("%s 的 TTL 是 %v，应为 %v", tc.key, c.ttl, tc.ttl)
		}
		if c.skip {
			t.Errorf("%s 被标成了锁 key，数据 key 不应被跳过", tc.key)
		}
	}
}

// TestClassifyLocksAreSkipped 锁 key 必须被识别为 skip。
//
// 这不是洁癖：锁 key 一旦被当成数据 key，工具会给它加上 14 天 TTL，反而把一把本该 30 秒就
// 消失的锁变成长期存活的锁——比不修更糟。两个锁 key 的构造格式直接来自
// engine/store.go 的 TryLockSettle / TryLockRobotTick（写死在同一行的 fmt.Sprintf 里，
// 没有导出常量可引用，所以这里只能照抄格式）。
func TestClassifyLocksAreSkipped(t *testing.T) {
	for _, key := range []string{
		"rank:settle:{balloon_1001}:3",      // store.go TryLockSettle
		"rank:robot_tick:{balloon_1001}:17", // store.go TryLockRobotTick
	} {
		c, ok := classify(key)
		if !ok {
			t.Errorf("%s 未被识别；锁 key 必须在 classes 表里显式登记为 skip", key)
			continue
		}
		if !c.skip {
			t.Errorf("%s 被当成了数据 key（归类到 %s），会给锁加上长 TTL", key, c.name)
		}
	}
}

// TestClassifyNearMissPrefixes 锁死两组只差一两个字符的前缀不会互相串。
// 这是本工具最容易写错的地方，也是唯一一处「写错就会伤害生产」的地方。
func TestClassifyNearMissPrefixes(t *testing.T) {
	settled, ok := classify(rediskeys.GetRankSettledKey("inst1"))
	if !ok || settled.skip {
		t.Fatalf("rank:settled: 被当成了锁（skip=%v, ok=%v）", settled.skip, ok)
	}
	lock, ok := classify("rank:settle:{balloon_1001}:3")
	if !ok || !lock.skip {
		t.Fatalf("rank:settle: 没有被当成锁（skip=%v, ok=%v）", lock.skip, ok)
	}

	infos, ok := classify(rediskeys.GetRankRobotInfosKey("balloon_1001", 3))
	if !ok || infos.name != "rank:robot_infos" {
		t.Fatalf("rank:robot_infos: 归到了 %q（ok=%v）", infos.name, ok)
	}
	robots, ok := classify(rediskeys.GetRankRobotsKey("balloon_1001", 3))
	if !ok || robots.name != "rank:robots" {
		t.Fatalf("rank:robots: 归到了 %q（ok=%v）", robots.name, ok)
	}
	tick, ok := classify("rank:robot_tick:{balloon_1001}:17")
	if !ok || !tick.skip {
		t.Fatalf("rank:robot_tick: 没有被当成锁（skip=%v, ok=%v）", tick.skip, ok)
	}
}

// TestClassifyUnknown 未登记的 key 必须落进「未识别」而不是被硬塞进某一类。
// 工具对新增 key 类型的态度是报告而不是猜——猜错会把一个不该动的 key 改掉。
func TestClassifyUnknown(t *testing.T) {
	for _, key := range []string{
		"rank:brand_new_thing:{x}", // 将来新增的数据 key
		"rank:meta",                // 裸前缀，真实 key 一定带 ":{...}" 后缀
		"rank:",                    // 退化输入
		"rankfoo:bar",              // 不是 rank 命名空间
		"other:rank:meta:{x}",      // 前缀位置不对
	} {
		if c, ok := classify(key); ok {
			t.Errorf("%s 本应未被识别，却被归到了 %s", key, c.name)
		}
	}
}

// TestUnknownPrefixBuckets 未识别 key 的桶名要可读且有界，否则报告会刷屏。
func TestUnknownPrefixBuckets(t *testing.T) {
	cases := map[string]string{
		"rank:brand_new:{x}":       "rank:brand_new:",
		"rank:{weird_tag}:1":       "rank:{weird_tag}",
		"rank:no_colon_at_all":     "rank:no_colon_at_all",
		"rank:robot_tickish:{a}:1": "rank:robot_tickish:",
	}
	for key, want := range cases {
		if got := unknownPrefix(key); got != want {
			t.Errorf("unknownPrefix(%q) = %q，应为 %q", key, got, want)
		}
	}
}
