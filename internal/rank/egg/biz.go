package egg

import (
	"context"

	commonrank "common/rank"
	"socialserver/internal/rank/engine"
)

const BizTypeName = "egg"

// BizService 彩蛋活动排行榜业务服务适配器，实现 rank.RankBizService 接口。
type BizService struct {
	Svc *engine.Service
}

func (b *BizService) BizType() string { return BizTypeName }

func (b *BizService) GetMemberRank(ctx context.Context, userID int64) (*commonrank.RankMemberSnapshot, int32, error) {
	return b.Svc.GetMemberRank(ctx, userID)
}

func (b *BizService) Tick(ctx context.Context, now int64) error {
	return b.Svc.Tick(ctx, now)
}

func (b *BizService) IsSettled() bool {
	return b.Svc.IsSettled()
}
