package rankservice

import (
	"context"

	commonrank "common/rank"
)

// RankBizService 跨业务聚合接口。
// 所有业务排行榜服务（balloon、egg 等）通过适配器实现此接口，
// 使 Manager 能够统一管理生命周期和聚合查询。
type RankBizService interface {
	BizType() string
	GetMemberRank(ctx context.Context, userID int64) (*commonrank.RankMemberSnapshot, int32, error)
	Tick(ctx context.Context, now int64) error
	IsSettled() bool
}
