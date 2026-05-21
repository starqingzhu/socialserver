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
type balloonAdapter = BalloonBizService

// BalloonBizService 将 balloon.Service 适配为 RankBizService 接口（导出，供 handler 层类型断言）。
type BalloonBizService struct {
	Svc     *balloon.Service
	bizType BizType
}

func (a *BalloonBizService) BizType() BizType { return a.bizType }

func (a *BalloonBizService) GetMemberRank(ctx context.Context, userID int64) (*commonrank.RankMemberSnapshot, int32, error) {
	return a.Svc.GetMemberRank(ctx, userID)
}

func (a *BalloonBizService) Tick(ctx context.Context, now int64) error {
	return a.Svc.Tick(ctx, now)
}

func (a *BalloonBizService) IsSettled() bool {
	return a.Svc.IsSettled()
}
