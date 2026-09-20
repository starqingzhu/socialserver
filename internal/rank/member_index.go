package rankservice

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	rediskeys "common/redis"
	goredis "golib/redis"
	"golib/zaplog"
)

type MemberIndex struct {
	rdb     *goredis.Redis
	ttl     time.Duration // 0 = no expiry
	entries map[int64][]MemberEntry
}

func NewMemberIndex(rdb *goredis.Redis, ttl time.Duration) *MemberIndex {
	return &MemberIndex{
		rdb:     rdb,
		ttl:     ttl,
		entries: make(map[int64][]MemberEntry),
	}
}

func encodeMemberEntry(e MemberEntry) string {
	return fmt.Sprintf("%s:%d:%d", e.BizType, e.ActID, e.GroupID)
}

func decodeMemberEntry(s string) (MemberEntry, bool) {
	parts := strings.SplitN(s, ":", 3)
	if len(parts) != 3 {
		return MemberEntry{}, false
	}
	actID, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return MemberEntry{}, false
	}
	groupID, err := strconv.ParseInt(parts[2], 10, 32)
	if err != nil {
		return MemberEntry{}, false
	}
	return MemberEntry{
		BizType: BizType(parts[0]),
		ActID:   int32(actID),
		GroupID: int32(groupID),
	}, true
}

func (idx *MemberIndex) Track(userID int64, entry MemberEntry) {
	if idx.rdb != nil {
		key := rediskeys.GetRankMemberIndexKey(userID)
		idx.rdb.SAdd(key, encodeMemberEntry(entry))
		if idx.ttl > 0 {
			idx.rdb.Expire(key, idx.ttl)
		}
		return
	}
	for _, existing := range idx.entries[userID] {
		if existing == entry {
			return
		}
	}
	idx.entries[userID] = append(idx.entries[userID], entry)
}

func (idx *MemberIndex) Lookup(userID int64) []MemberEntry {
	if idx.rdb != nil {
		raw, err := idx.rdb.SMembers(rediskeys.GetRankMemberIndexKey(userID))
		if err != nil || len(raw) == 0 {
			return nil
		}
		result := make([]MemberEntry, 0, len(raw))
		for _, s := range raw {
			if e, ok := decodeMemberEntry(s); ok {
				result = append(result, e)
			}
		}
		return result
	}
	src := idx.entries[userID]
	if len(src) == 0 {
		return nil
	}
	dst := make([]MemberEntry, len(src))
	copy(dst, src)
	return dst
}

func (idx *MemberIndex) LookupByBizType(userID int64, bizType BizType) []MemberEntry {
	all := idx.Lookup(userID)
	var result []MemberEntry
	for _, e := range all {
		if e.BizType == bizType {
			result = append(result, e)
		}
	}
	return result
}

// removeUserEntriesChunk 是 RemoveUserEntries 的 pipeline 分块大小。
// 取 512：足够把往返次数压掉两个数量级，又不会让单次 pipeline 的命令数大到
// 内存/网络包体积失控（每用户 key 不同，无法合并成一条 SRem，只能靠 pipeline 降往返）。
const removeUserEntriesChunk = 512

// RemoveUserEntries 从 Redis 中批量移除指定活动下所有成员的索引条目。
// members 为 userID → groupID 映射，与 balloon.Service.GetAllMembers() 返回值对应。
//
// 每用户一个独立 key，因此无法合并成一条 SRem；改为按 removeUserEntriesChunk 分块走
// pipeline，10 万成员从 10 万次往返降到约 200 次（待办 G-d2）。
// 用 context.Background() 与 golib 包装方法内部一致：清理路径本就是 best-effort，
// 不因调用方 ctx 取消而半途停下——分块之间停下来只会留下更难解释的中间态。
func (idx *MemberIndex) RemoveUserEntries(bizType BizType, actID int32, members map[int64]int32) {
	if idx.rdb == nil || len(members) == 0 {
		return
	}
	ctx := context.Background()
	pipe := idx.rdb.Pipeline()
	pending := 0
	flush := func() {
		if pending == 0 {
			return
		}
		if _, err := pipe.Exec(ctx); err != nil {
			zaplog.LoggerSugar.Warnf("rank member index: remove %d entries bizType=%s actID=%d: %v",
				pending, bizType, actID, err)
		}
		pipe = idx.rdb.Pipeline()
		pending = 0
	}
	for userID, groupID := range members {
		entry := encodeMemberEntry(MemberEntry{BizType: bizType, ActID: actID, GroupID: groupID})
		pipe.SRem(ctx, rediskeys.GetRankMemberIndexKey(userID), entry)
		pending++
		if pending >= removeUserEntriesChunk {
			flush()
		}
	}
	flush()
}
