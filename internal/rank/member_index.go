package rankservice

import (
	"fmt"
	"strconv"
	"strings"

	rediskeys "common/redis"
	goredis "golib/redis"
)

// MemberIndex 全局成员索引，维护 userID → 排行榜参与记录的映射。
// 当 rdb 不为 nil 时数据缓存到 Redis（多节点共享），否则退化为纯内存模式（测试用）。
type MemberIndex struct {
	rdb     *goredis.Redis
	entries map[int64][]MemberEntry
}

func NewMemberIndex(rdb *goredis.Redis) *MemberIndex {
	return &MemberIndex{
		rdb:     rdb,
		entries: make(map[int64][]MemberEntry),
	}
}

func encodeMemberEntry(e MemberEntry) string {
	return fmt.Sprintf("%s:%d:%d:%d", e.BizType, e.ActID, e.Round, e.GroupID)
}

func decodeMemberEntry(s string) (MemberEntry, bool) {
	parts := strings.SplitN(s, ":", 4)
	if len(parts) != 4 {
		return MemberEntry{}, false
	}
	actID, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return MemberEntry{}, false
	}
	round, err := strconv.ParseInt(parts[2], 10, 32)
	if err != nil {
		return MemberEntry{}, false
	}
	groupID, err := strconv.ParseInt(parts[3], 10, 32)
	if err != nil {
		return MemberEntry{}, false
	}
	return MemberEntry{
		BizType: BizType(parts[0]),
		ActID:   int32(actID),
		Round:   int32(round),
		GroupID: int32(groupID),
	}, true
}

// Track 记录用户参与了某个排行榜分组。幂等。
func (idx *MemberIndex) Track(userID int64, entry MemberEntry) {
	if idx.rdb != nil {
		idx.rdb.SAdd(rediskeys.GetRankMemberIndexKey(userID), encodeMemberEntry(entry))
		return
	}
	for _, existing := range idx.entries[userID] {
		if existing == entry {
			return
		}
	}
	idx.entries[userID] = append(idx.entries[userID], entry)
}

// Lookup 返回用户参与的所有排行榜记录。
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

// LookupByBizType 返回用户在指定业务类型下的所有排行榜记录。
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

// RemoveByKey 删除所有匹配指定 BizKey 的记录（仅内存模式）。
func (idx *MemberIndex) RemoveByKey(key BizKey) {
	if idx.rdb != nil {
		return
	}
	for userID, list := range idx.entries {
		filtered := list[:0]
		for _, e := range list {
			if !(e.BizType == key.BizType && e.ActID == key.ActID && e.Round == key.Round) {
				filtered = append(filtered, e)
			}
		}
		if len(filtered) == 0 {
			delete(idx.entries, userID)
		} else {
			idx.entries[userID] = filtered
		}
	}
}
