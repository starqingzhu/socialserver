package timebounded

import (
	"context"

	commonrank "common/rank"
	"socialserver/internal/rank/engine"
)

// BizService 周期排行榜业务服务适配器，实现 rank.RankBizService 接口。
// 支持 balloon、egg、camper_competition 等所有周期性排行榜。
type BizService struct {
	Svc     *engine.Service
	bizType string
}

func NewBizService(bizType string, svc *engine.Service) *BizService {
	return &BizService{
		Svc:     svc,
		bizType: bizType,
	}
}

func (b *BizService) BizType() string { return b.bizType }

func (b *BizService) GetMemberRank(ctx context.Context, userID int64) (*commonrank.RankMemberSnapshot, int32, error) {
	return b.Svc.GetMemberRank(ctx, userID)
}

func (b *BizService) Tick(ctx context.Context, now int64) error {
	return b.Svc.Tick(ctx, now)
}

func (b *BizService) IsSettled() bool {
	return b.Svc.IsSettled()
}
