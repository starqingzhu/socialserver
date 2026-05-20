package rankservice

import commonrank "common/rank"

// BizType 业务类型标识。
type BizType string

const (
	BizTypeBalloon BizType = "balloon"
)

// BizKey 唯一标识一个业务排行榜服务实例（业务类型 + 活动 + 期数）。
type BizKey struct {
	BizType BizType
	ActID   int32
	Round   int32
}

// MemberEntry 记录一个用户在某个排行榜分组中的参与信息。
type MemberEntry struct {
	BizType BizType
	ActID   int32
	Round   int32
	GroupID int32
}

// MemberRankEntry 在 MemberEntry 基础上附带用户当前的名次快照。
type MemberRankEntry struct {
	MemberEntry
	Snapshot *commonrank.RankMemberSnapshot
}
