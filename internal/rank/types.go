package rankservice

import (
	"fmt"

	commonrank "common/rank"
)

// BizType 业务类型标识（周期排行榜类型）。
// 支持的业务类型由 RankBase.json 配置文件动态定义，无需在代码中硬编码。
type BizType string

// BizKey 唯一标识一个业务排行榜服务实例（业务类型 + 活动ID）。
type BizKey struct {
	BizType BizType
	ActID   int32
}

func NewBizKey(bizType BizType, actID int32) BizKey {
	return BizKey{BizType: bizType, ActID: actID}
}

func (k BizKey) String() string {
	return fmt.Sprintf("%s:%d", k.BizType, k.ActID)
}

// MemberEntry 记录一个用户在某个排行榜分组中的参与信息。
type MemberEntry struct {
	BizType BizType
	ActID   int32
	GroupID int32
}

// MemberRankEntry 在 MemberEntry 基础上附带用户当前的名次快照。
type MemberRankEntry struct {
	MemberEntry
	Snapshot *commonrank.RankMemberSnapshot
}
