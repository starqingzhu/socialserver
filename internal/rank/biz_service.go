package rankservice

import (
	"context"

	commonrank "common/rank"
	"socialserver/internal/rank/balloon"
)

// RankBizService 跨业务聚合接口。
// 所有业务排行榜服务（balloon、charm 等）通过适配器实现此接口，
// 使 Manager 能够统一管理生命周期和聚合查询。
type RankBizService interface {
	BizType() BizType
	GetMemberRank(ctx context.Context, userID int64) (*commonrank.RankMemberSnapshot, int32, error)
	Tick(ctx context.Context, now int64) error
	IsSettled() bool
}

// balloonAdapter 将 balloon.Service 适配为 RankBizService 接口。
type balloonAdapter struct {
	svc     *balloon.Service
	bizType BizType
}

func (a *balloonAdapter) BizType() BizType { return a.bizType }

func (a *balloonAdapter) GetMemberRank(ctx context.Context, userID int64) (*commonrank.RankMemberSnapshot, int32, error) {
	return a.svc.GetMemberRank(ctx, userID)
}

func (a *balloonAdapter) Tick(ctx context.Context, now int64) error {
	return a.svc.Tick(ctx, now)
}

func (a *balloonAdapter) IsSettled() bool {
	return a.svc.IsSettled()
}
