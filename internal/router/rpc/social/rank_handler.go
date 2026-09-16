package social

import (
	"context"

	pb "pbcommon/gen/ss/msg"
	"socialserver/internal/dispatch"
)

// --- 排行榜玩家接口 ---

func (h *ServerHandler) S2SUpsertScore(ctx context.Context, req *pb.PBS2SUpsertScoreRequest) (*pb.PBS2SUpsertScoreResponse, error) {
	return grpcDispatch[*pb.PBS2SUpsertScoreRequest, *pb.PBS2SUpsertScoreResponse](ctx, dispatch.CmdS2SUpsertScore, req)
}

func (h *ServerHandler) S2SGetRankList(ctx context.Context, req *pb.PBS2SGetRankListRequest) (*pb.PBS2SGetRankListResponse, error) {
	return grpcDispatch[*pb.PBS2SGetRankListRequest, *pb.PBS2SGetRankListResponse](ctx, dispatch.CmdS2SGetRankList, req)
}

func (h *ServerHandler) S2SGetMemberRank(ctx context.Context, req *pb.PBS2SGetMemberRankRequest) (*pb.PBS2SGetMemberRankResponse, error) {
	return grpcDispatch[*pb.PBS2SGetMemberRankRequest, *pb.PBS2SGetMemberRankResponse](ctx, dispatch.CmdS2SGetMemberRank, req)
}

func (h *ServerHandler) S2SSettle(ctx context.Context, req *pb.PBS2SSettleRequest) (*pb.PBS2SSettleResponse, error) {
	return grpcDispatch[*pb.PBS2SSettleRequest, *pb.PBS2SSettleResponse](ctx, dispatch.CmdS2SSettle, req)
}

func (h *ServerHandler) S2SGetRewardUsers(ctx context.Context, req *pb.PBS2SGetRewardUsersRequest) (*pb.PBS2SGetRewardUsersResponse, error) {
	return grpcDispatch[*pb.PBS2SGetRewardUsersRequest, *pb.PBS2SGetRewardUsersResponse](ctx, dispatch.CmdS2SGetRewardUsers, req)
}

func (h *ServerHandler) S2SClaimReward(ctx context.Context, req *pb.PBS2SClaimRewardRequest) (*pb.PBS2SClaimRewardResponse, error) {
	return grpcDispatch[*pb.PBS2SClaimRewardRequest, *pb.PBS2SClaimRewardResponse](ctx, dispatch.CmdS2SClaimReward, req)
}

func (h *ServerHandler) S2SGetClaimStatus(ctx context.Context, req *pb.PBS2SGetClaimStatusRequest) (*pb.PBS2SGetClaimStatusResponse, error) {
	return grpcDispatch[*pb.PBS2SGetClaimStatusRequest, *pb.PBS2SGetClaimStatusResponse](ctx, dispatch.CmdS2SGetClaimStatus, req)
}

// --- 排行榜配置管理接口 ---

func (h *ServerHandler) S2SListRankBizTypes(ctx context.Context, req *pb.PBS2SListRankBizTypesRequest) (*pb.PBS2SListRankBizTypesResponse, error) {
	return grpcDispatch[*pb.PBS2SListRankBizTypesRequest, *pb.PBS2SListRankBizTypesResponse](ctx, dispatch.CmdS2SListRankBizTypes, req)
}

func (h *ServerHandler) S2SCreateRankConfig(ctx context.Context, req *pb.PBS2SCreateRankConfigRequest) (*pb.PBS2SCreateRankConfigResponse, error) {
	return grpcDispatch[*pb.PBS2SCreateRankConfigRequest, *pb.PBS2SCreateRankConfigResponse](ctx, dispatch.CmdS2SCreateRankConfig, req)
}

func (h *ServerHandler) S2SGetRankConfig(ctx context.Context, req *pb.PBS2SGetRankConfigRequest) (*pb.PBS2SGetRankConfigResponse, error) {
	return grpcDispatch[*pb.PBS2SGetRankConfigRequest, *pb.PBS2SGetRankConfigResponse](ctx, dispatch.CmdS2SGetRankConfig, req)
}

func (h *ServerHandler) S2SUpdateRankConfig(ctx context.Context, req *pb.PBS2SUpdateRankConfigRequest) (*pb.PBS2SUpdateRankConfigResponse, error) {
	return grpcDispatch[*pb.PBS2SUpdateRankConfigRequest, *pb.PBS2SUpdateRankConfigResponse](ctx, dispatch.CmdS2SUpdateRankConfig, req)
}

func (h *ServerHandler) S2SDeleteRankConfig(ctx context.Context, req *pb.PBS2SDeleteRankConfigRequest) (*pb.PBS2SDeleteRankConfigResponse, error) {
	return grpcDispatch[*pb.PBS2SDeleteRankConfigRequest, *pb.PBS2SDeleteRankConfigResponse](ctx, dispatch.CmdS2SDeleteRankConfig, req)
}

func (h *ServerHandler) S2SListRankConfigs(ctx context.Context, req *pb.PBS2SListRankConfigsRequest) (*pb.PBS2SListRankConfigsResponse, error) {
	return grpcDispatch[*pb.PBS2SListRankConfigsRequest, *pb.PBS2SListRankConfigsResponse](ctx, dispatch.CmdS2SListRankConfigs, req)
}

func (h *ServerHandler) S2SGetRankCurRound(ctx context.Context, req *pb.PBS2SGetRankCurRoundRequest) (*pb.PBS2SGetRankCurRoundResponse, error) {
	return grpcDispatch[*pb.PBS2SGetRankCurRoundRequest, *pb.PBS2SGetRankCurRoundResponse](ctx, dispatch.CmdS2SGetRankCurRound, req)
}

// --- GM 查询接口 ---

func (h *ServerHandler) S2SGMGetUserRankList(ctx context.Context, req *pb.PBS2SGMGetUserRankListRequest) (*pb.PBS2SGMGetUserRankListResponse, error) {
	return grpcDispatch[*pb.PBS2SGMGetUserRankListRequest, *pb.PBS2SGMGetUserRankListResponse](ctx, dispatch.CmdS2SGMGetUserRankList, req)
}

func (h *ServerHandler) S2SGMGetGroupRankList(ctx context.Context, req *pb.PBS2SGMGetGroupRankListRequest) (*pb.PBS2SGMGetGroupRankListResponse, error) {
	return grpcDispatch[*pb.PBS2SGMGetGroupRankListRequest, *pb.PBS2SGMGetGroupRankListResponse](ctx, dispatch.CmdS2SGMGetGroupRankList, req)
}

func (h *ServerHandler) S2SGMGetRankInstanceList(ctx context.Context, req *pb.PBS2SGMGetRankInstanceListRequest) (*pb.PBS2SGMGetRankInstanceListResponse, error) {
	return grpcDispatch[*pb.PBS2SGMGetRankInstanceListRequest, *pb.PBS2SGMGetRankInstanceListResponse](ctx, dispatch.CmdS2SGMGetRankInstanceList, req)
}

func (h *ServerHandler) S2SGMGetInstanceRankList(ctx context.Context, req *pb.PBS2SGMGetInstanceRankListRequest) (*pb.PBS2SGMGetInstanceRankListResponse, error) {
	return grpcDispatch[*pb.PBS2SGMGetInstanceRankListRequest, *pb.PBS2SGMGetInstanceRankListResponse](ctx, dispatch.CmdS2SGMGetInstanceRankList, req)
}
