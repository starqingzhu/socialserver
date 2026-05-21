package rankservice

import (
	"fmt"
	"strconv"
	"strings"

	rediskeys "common/redis"
	goredis "golib/redis"
)

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

func (idx *MemberIndex) RemoveByKey(key string) {
	if idx.rdb != nil {
		return
	}
	for userID, list := range idx.entries {
		filtered := list[:0]
		for _, e := range list {
			if !(NewBizKey(e.BizType, e.ActID).String() == key) {
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

// RemoveUserEntries 从 Redis 中批量移除指定活动下所有成员的索引条目。
// members 为 userID → groupID 映射，与 balloon.Service.GetAllMembers() 返回值对应。
func (idx *MemberIndex) RemoveUserEntries(bizType BizType, actID int32, members map[int64]int32) {
	if idx.rdb == nil || len(members) == 0 {
		return
	}
	for userID, groupID := range members {
		entry := encodeMemberEntry(MemberEntry{BizType: bizType, ActID: actID, GroupID: groupID})
		idx.rdb.SRem(rediskeys.GetRankMemberIndexKey(userID), entry)
	}
}
