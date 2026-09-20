package rankservice

import (
	"testing"

	rediskeys "common/redis"
	goredis "golib/redis"

	"github.com/alicebob/miniredis/v2"
)

// newMiniRedisMemberIndex 起一个 miniredis 支撑的 MemberIndex。
// 索引 key 是每用户一个，断言必须落在真实 Redis 语义上（SADD/SREM 的集合行为），
// 内存实现（rdb == nil）走的是另一条分支，证明不了生产路径。
func newMiniRedisMemberIndex(t *testing.T) (*MemberIndex, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewRedis(&goredis.RedisConfig{RedisAddrs: []string{mr.Addr()}})
	if rdb == nil {
		t.Fatalf("miniredis client: NewRedis returned nil for addr %s", mr.Addr())
	}
	return NewMemberIndex(rdb, memberIndexTTL), mr
}

// indexMembers 读出某个用户当前的索引条目集合。
// miniredis 的 SMEMBERS 对不存在的 key 返回错误（真实 Redis 返回空数组），
// 集合删空后 key 会消失，因此必须先 Exists 再读。
func indexMembers(t *testing.T, mr *miniredis.Miniredis, userID int64) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	key := rediskeys.GetRankMemberIndexKey(userID)
	if !mr.Exists(key) {
		return got
	}
	raw, err := mr.SMembers(key)
	if err != nil {
		t.Fatalf("SMembers(%s): %v", key, err)
	}
	for _, m := range raw {
		got[m] = true
	}
	return got
}

// TestRemoveUserEntriesRemovesAllAcrossChunks 覆盖待办 G-d2 的分块边界：
// 成员数取 removeUserEntriesChunk 的 2 倍多（1200），跨 3 个 pipeline 分块，
// 用以证明分块只影响往返次数、不影响"一个都不漏"。
func TestRemoveUserEntriesRemovesAllAcrossChunks(t *testing.T) {
	idx, mr := newMiniRedisMemberIndex(t)
	const bizType, actID = "g2", int32(1)

	members := make(map[int64]int32, 1200)
	for i := 0; i < 1200; i++ {
		userID := int64(10000 + i)
		groupID := int32(i%10 + 1)
		members[userID] = groupID
		idx.Track(userID, MemberEntry{BizType: bizType, ActID: actID, GroupID: groupID})
	}
	if len(members) <= 2*removeUserEntriesChunk {
		t.Fatalf("前置失败：成员数 %d 未跨过 2 个分块（chunk=%d）", len(members), removeUserEntriesChunk)
	}

	idx.RemoveUserEntries(bizType, actID, members)

	for userID, groupID := range members {
		want := encodeMemberEntry(MemberEntry{BizType: bizType, ActID: actID, GroupID: groupID})
		if indexMembers(t, mr, userID)[want] {
			t.Fatalf("成员 %d 的索引条目 %s 未被删除（分块漏掉了这一条）", userID, want)
		}
	}
}

// TestRemoveUserEntriesKeepsOtherActivities 锁死删除的精确性：
// 同一用户可能同时参加多个活动，RemoveUserEntries 只能删掉目标活动的条目。
// 删多了会让用户在其它活动里"凭空消失"，是数据正确性问题而非清理质量问题。
func TestRemoveUserEntriesKeepsOtherActivities(t *testing.T) {
	idx, mr := newMiniRedisMemberIndex(t)
	const userID = int64(20001)

	idx.Track(userID, MemberEntry{BizType: "target", ActID: 1, GroupID: 3})
	idx.Track(userID, MemberEntry{BizType: "target", ActID: 2, GroupID: 4})  // 同 bizType 不同 actID
	idx.Track(userID, MemberEntry{BizType: "keeper", ActID: 1, GroupID: 5}) // 同 actID 不同 bizType

	idx.RemoveUserEntries("target", 1, map[int64]int32{userID: 3})

	got := indexMembers(t, mr, userID)
	if got[encodeMemberEntry(MemberEntry{BizType: "target", ActID: 1, GroupID: 3})] {
		t.Fatalf("目标条目未被删除：%v", got)
	}
	for _, keep := range []MemberEntry{
		{BizType: "target", ActID: 2, GroupID: 4},
		{BizType: "keeper", ActID: 1, GroupID: 5},
	} {
		if !got[encodeMemberEntry(keep)] {
			t.Fatalf("误删了其它活动的索引条目 %+v，剩余 %v", keep, got)
		}
	}
}

// TestRemoveUserEntriesEmptyIsNoop 覆盖空输入：不发起任何 Redis 命令，也不 panic。
// 调用方（cleanupServiceData / forceCleanupOrphan / syncFromMongo）经常拿到空成员表。
func TestRemoveUserEntriesEmptyIsNoop(t *testing.T) {
	idx, _ := newMiniRedisMemberIndex(t)
	idx.RemoveUserEntries("g2", 1, nil)
	idx.RemoveUserEntries("g2", 1, map[int64]int32{})
}

// TestRemoveUserEntriesMissingKeyIsHarmless 覆盖"索引 key 已被 TTL 回收"：
// 分块 pipeline 里的 SRem 作用在不存在的 key 上会返回 0 而不是错误，
// 因此整批删除必须成功返回，不能因为个别成员没有索引就中断。
func TestRemoveUserEntriesMissingKeyIsHarmless(t *testing.T) {
	idx, mr := newMiniRedisMemberIndex(t)
	const present = int64(30001)

	idx.Track(present, MemberEntry{BizType: "g2", ActID: 1, GroupID: 1})

	members := map[int64]int32{present: 1}
	for i := 0; i < 10; i++ {
		members[int64(40000+i)] = int32(i + 1) // 这些用户从未建过索引
	}

	idx.RemoveUserEntries("g2", 1, members)

	if indexMembers(t, mr, present)[encodeMemberEntry(MemberEntry{BizType: "g2", ActID: 1, GroupID: 1})] {
		t.Fatalf("有索引的那一个也没被删掉")
	}
	if mr.Exists(rediskeys.GetRankMemberIndexKey(40000)) {
		t.Fatalf("为不存在的用户凭空创建了 key %s", rediskeys.GetRankMemberIndexKey(40000))
	}
}

// TestTrackRefreshesTTLEveryCall 钉住索引 key 的 TTL 由 Track 负责续期（缺陷 8 的落点）：
// RemoveUserEntries 只删条目、不设 TTL，索引键不出现永久 key 这件事全靠 Track。
func TestTrackRefreshesTTLEveryCall(t *testing.T) {
	idx, mr := newMiniRedisMemberIndex(t)

	idx.Track(50001, MemberEntry{BizType: "g2", ActID: 1, GroupID: 1})
	key := rediskeys.GetRankMemberIndexKey(50001)
	if ttl := mr.TTL(key); ttl <= 0 {
		t.Fatalf("%s 的 TTL=%v ⇒ 永久 key（缺陷 8）", key, ttl)
	}
	if ttl := mr.TTL(key); ttl != memberIndexTTL {
		t.Fatalf("%s 的 TTL=%v, want %v", key, ttl, memberIndexTTL)
	}

	// 仍属于同一活动、但换了分组时也必须续期，否则活跃用户的索引会莫名过期。
	mr.FastForward(memberIndexTTL / 2)
	idx.Track(50001, MemberEntry{BizType: "g2", ActID: 1, GroupID: 2})
	if ttl := mr.TTL(key); ttl != memberIndexTTL {
		t.Fatalf("再次 Track 后 TTL=%v, want %v（未续期）", ttl, memberIndexTTL)
	}
	if got := indexMembers(t, mr, 50001); len(got) != 2 {
		t.Fatalf("索引条目数=%d, want 2（不同分组应各自保留一条），内容 %v", len(got), got)
	}
}
