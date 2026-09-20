package handler

import (
	"context"
	"testing"

	libdispatch "golib/dispatch"
	pb "pbcommon/gen/ss/msg"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestHandleCreateRankConfigRejectsMissingCloseAndGameEndTime 验证
// docs/rank_optimization.md 待办 K：CloseTime 与 GameEndTime 不能同时缺省，
// 否则 engine.Service.effectiveSettleAt() 退化为 0，Tick 的 settledAt==settleAt
// 短路在第一次调用就命中，活动从此既不再 tick 机器人也永不结算。
func TestHandleCreateRankConfigRejectsMissingCloseAndGameEndTime(t *testing.T) {
	req := &pb.PBS2SCreateRankConfigRequest{
		BizType:  "balloon",
		ActId:    1,
		OpenTime: 1000,
		// CloseTime 和 GameEndTime 均缺省。
	}
	resp := &pb.PBS2SCreateRankConfigResponse{}

	err := handleCreateRankConfig(context.Background(), libdispatch.Meta{}, req, resp)
	if err == nil {
		t.Fatalf("expected error when both CloseTime and GameEndTime are unset, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected codes.InvalidArgument, got %v (err=%v)", status.Code(err), err)
	}
}

// TestHandleCreateRankConfigAllowsGameEndTimeOnly 验证只要 GameEndTime 或 CloseTime
// 任一非零即可通过校验（不要求两者都设置），校验失败必须发生在触碰全局 manager 之前，
// 否则本测试会因 rank manager 未初始化而 panic/误报为 Internal 错误。
func TestHandleCreateRankConfigAllowsGameEndTimeOnly(t *testing.T) {
	req := &pb.PBS2SCreateRankConfigRequest{
		BizType:     "balloon",
		ActId:       1,
		OpenTime:    1000,
		GameEndTime: 2000,
	}
	resp := &pb.PBS2SCreateRankConfigResponse{}

	err := handleCreateRankConfig(context.Background(), libdispatch.Meta{}, req, resp)
	if status.Code(err) == codes.InvalidArgument {
		t.Fatalf("did not expect InvalidArgument when GameEndTime is set, got %v", err)
	}
}
