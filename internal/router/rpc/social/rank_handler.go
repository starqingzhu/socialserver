package social

import (
	"context"
	"errors"
	"time"

	"common/configmgr"
	cfgtypes "common/configmgr/types"
	"common/rank"
	"golib/zaplog"
	commonMsg "pbcommon/gen/common/msg"
	pb "pbcommon/gen/ss/msg"
	rankservice "socialserver/internal/rank"
	"socialserver/internal/rank/engine"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --- 排行榜业务接口 ---

// S2SUpsertScore 服务器间接口：更新用户积分并返回最新排名信息。
func (h *ServerHandler) S2SUpsertScore(ctx context.Context, req *pb.PBS2SUpsertScoreRequest) (resp *pb.PBS2SUpsertScoreResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SUpsertScore req bizType=%s actId=%d userId=%d totalScore=%d ts=%d round=%d",
		req.BizType, req.ActId, req.UserId, req.TotalScore, req.Timestamp, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SUpsertScore resp userId=%d err=%v", req.UserId, retErr)
		} else {
			var myRank int64
			if resp.MyRank != nil {
				myRank = resp.MyRank.Rank
			}
			zaplog.LoggerSugar.Infof("[rank] S2SUpsertScore resp userId=%d rank=%d round=%d msgCode=%v",
				req.UserId, myRank, resp.CurrentRound, resp.MsgCode)
		}
	}()

	// 周期排行榜轮次校验：若 round>0 且与当前活跃轮次不符，拒绝写入并返回最新轮次
	bizType := rankservice.BizType(req.BizType)
	currentRound := int32(1)
	manager := rankservice.GetGlobalManager()
	if manager != nil {
		if ps := manager.GetPeriodicState(bizType, req.ActId); ps != nil {
			currentRound = ps.GetCurrentRound()
			if req.Round > 0 && req.Round != ps.GetCurrentRound() {
				return &pb.PBS2SUpsertScoreResponse{
					MsgCode:      commonMsg.MsgCode_CODE_RANK_ROUND_CHANGED,
					CurrentRound: ps.GetCurrentRound(),
				}, nil
			}
		}
	}

	svc, err := lookupEngineService(bizType, req.ActId)
	if err != nil {
		return nil, err
	}
	if err := svc.UpsertScore(ctx, req.UserId, req.TotalScore, req.Timestamp, protoAvatarInfoToRank(req.AvatarInfo)); err != nil {
		if errors.Is(err, rank.ErrInstanceClosed) {
			zaplog.LoggerSugar.Infof("[rank] S2SUpsertScore activity closed userId=%d bizType=%s", req.UserId, req.BizType)
			return &pb.PBS2SUpsertScoreResponse{MsgCode: commonMsg.MsgCode_CODE_RANK_ACTIVITY_CLOSED}, nil
		}
		return nil, rankErrorToStatus(err)
	}
	snapshot, _, err := svc.GetMemberRank(ctx, req.UserId)
	if err != nil {
		zaplog.LoggerSugar.Warnf("[rank] S2SUpsertScore GetMemberRank userId=%d err=%v", req.UserId, err)
		return &pb.PBS2SUpsertScoreResponse{MsgCode: commonMsg.MsgCode_CODE_OK, CurrentRound: currentRound}, nil
	}

	myRank := snapshotToProto(snapshot)
	ret := &pb.PBS2SUpsertScoreResponse{
		MsgCode:      commonMsg.MsgCode_CODE_OK,
		MyRank:       myRank,
		CurrentRound: currentRound,
	}
	zaplog.LoggerSugar.Infof("[rank] S2SUpsertScore updated score for userId=%d resp:%s", req.UserId, ret.String())
	return ret, nil
}

// 获取排行榜列表
func (h *ServerHandler) S2SGetRankList(ctx context.Context, req *pb.PBS2SGetRankListRequest) (resp *pb.PBS2SGetRankListResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SGetRankList req bizType=%s actId=%d userId=%d start=%d end=%d round=%d",
		req.BizType, req.ActId, req.UserId, req.Start, req.End, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SGetRankList resp userId=%d err=%v", req.UserId, retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SGetRankList resp userId=%d memberCount=%d currentRound=%d",
				req.UserId, len(resp.Members), resp.CurrentRound)
		}
	}()
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	bizType := rankservice.BizType(req.BizType)

	svc, isHistorical := manager.ResolveEngineService(bizType, req.ActId, req.Round)

	// 确定当前轮次（一次性排行榜固定为 1）
	currentRound := int32(1)
	if ps := manager.GetPeriodicState(bizType, req.ActId); ps != nil {
		currentRound = ps.GetCurrentRound()
	}

	if isHistorical {
		// 历史轮次：从 MongoDB 读取已结算数据
		round := req.Round
		snapshots, mySnap, err := manager.GetHistoricalRoundList(ctx, bizType, req.ActId, round, req.UserId, req.Start, req.End)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return &pb.PBS2SGetRankListResponse{
			MsgCode:      commonMsg.MsgCode_CODE_OK,
			Members:      snapshotsToProto(snapshots),
			MyRank:       snapshotToProto(mySnap),
			CurrentRound: currentRound,
		}, nil
	}

	if svc == nil {
		return nil, status.Errorf(codes.NotFound, "service not found: bizType=%s actId=%d", req.BizType, req.ActId)
	}
	snapshot, groupID, err := svc.GetMemberRank(ctx, req.UserId)
	if err != nil {
		return nil, rankErrorToStatus(err)
	}
	if groupID == 0 {
		groups := svc.ListGroups()
		if len(groups) == 0 {
			return nil, status.Errorf(codes.NotFound, "user %d not found in any group for bizType=%s actId=%d", req.UserId, req.BizType, req.ActId)
		}
		minGroup := groups[0]
		for _, g := range groups[1:] {
			if g.GroupID < minGroup.GroupID {
				minGroup = g
			}
		}
		groupID = minGroup.GroupID
	}
	snapshots, err := svc.ListGroupRank(ctx, groupID, req.Start, req.End)
	if err != nil {
		return nil, rankErrorToStatus(err)
	}

	myRank := snapshotToProto(snapshot)
	members := snapshotsToProto(snapshots)
	ret := &pb.PBS2SGetRankListResponse{
		MsgCode:      commonMsg.MsgCode_CODE_OK,
		Members:      members,
		MyRank:       myRank,
		CreateTime:   svc.GetGroupCreateTime(ctx, groupID),
		CurrentRound: currentRound,
	}

	zaplog.LoggerSugar.Infof("[rank] S2SGetRankList updated score for userId=%d myRank=%s membersCount=%d", req.UserId, myRank.String(), len(members))
	return ret, nil
}

func (h *ServerHandler) S2SGetMemberRank(ctx context.Context, req *pb.PBS2SGetMemberRankRequest) (resp *pb.PBS2SGetMemberRankResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SGetMemberRank req bizType=%s actId=%d userId=%d round=%d",
		req.BizType, req.ActId, req.UserId, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SGetMemberRank resp userId=%d err=%v", req.UserId, retErr)
		} else {
			var memberRank, score int64
			if resp.Snapshot != nil {
				memberRank = resp.Snapshot.Rank
				score = resp.Snapshot.Score
			}
			zaplog.LoggerSugar.Infof("[rank] S2SGetMemberRank resp userId=%d rank=%d score=%d settleStage=%d",
				req.UserId, memberRank, score, resp.SettleStage)
		}
	}()
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	bizType := rankservice.BizType(req.BizType)
	svc, isHistorical := manager.ResolveEngineService(bizType, req.ActId, req.Round)
	if isHistorical {
		round := req.Round
		snapshots, mySnap, err := manager.GetHistoricalRoundList(ctx, bizType, req.ActId, round, req.UserId, 0, -1)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		_ = snapshots
		return &pb.PBS2SGetMemberRankResponse{Snapshot: snapshotToProto(mySnap), SettleStage: 1}, nil
	}
	if svc == nil {
		return nil, status.Errorf(codes.NotFound, "service not found: bizType=%s actId=%d", req.BizType, req.ActId)
	}
	snapshot, groupID, err := svc.GetMemberRank(ctx, req.UserId)
	if err != nil {
		return nil, rankErrorToStatus(err)
	}
	settleStage := int32(0)
	if groupID > 0 {
		g := svc.GetGroup(groupID)
		if g != nil && g.State == engine.GroupStateSettled {
			settleStage = 1
		}
	}
	return &pb.PBS2SGetMemberRankResponse{Snapshot: snapshotToProto(snapshot), SettleStage: settleStage}, nil
}

func (h *ServerHandler) S2SSettle(ctx context.Context, req *pb.PBS2SSettleRequest) (resp *pb.PBS2SSettleResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SSettle req bizType=%s actId=%d timestamp=%d",
		req.BizType, req.ActId, req.Timestamp)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SSettle resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SSettle resp groupCount=%d", len(resp.Groups))
		}
	}()
	svc, err := lookupEngineService(rankservice.BizType(req.BizType), req.ActId)
	if err != nil {
		return nil, err
	}
	results, err := svc.Settle(ctx)
	if err != nil {
		return nil, rankErrorToStatus(err)
	}
	groups := make([]*pb.PBS2SSettleGroupResult, 0, len(results))
	for gid, snapshots := range results {
		groups = append(groups, &pb.PBS2SSettleGroupResult{GroupId: gid, Members: snapshotsToProto(snapshots)})
	}
	return &pb.PBS2SSettleResponse{Groups: groups}, nil
}

func (h *ServerHandler) S2SGetRewardUsers(ctx context.Context, req *pb.PBS2SGetRewardUsersRequest) (resp *pb.PBS2SGetRewardUsersResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SGetRewardUsers req bizType=%s actId=%d round=%d", req.BizType, req.ActId, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SGetRewardUsers resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SGetRewardUsers resp userCount=%d", len(resp.UserIds))
		}
	}()
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	bizType := rankservice.BizType(req.BizType)
	svc, isHistorical := manager.ResolveEngineService(bizType, req.ActId, req.Round)
	if isHistorical {
		userIDs, err := manager.GetHistoricalRewardUsers(ctx, bizType, req.ActId, req.Round)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return &pb.PBS2SGetRewardUsersResponse{UserIds: userIDs}, nil
	}
	if svc == nil {
		return nil, status.Errorf(codes.NotFound, "service not found: bizType=%s actId=%d", req.BizType, req.ActId)
	}
	return &pb.PBS2SGetRewardUsersResponse{UserIds: svc.GetOpenRewardUserIDs()}, nil
}

// --- 奖励领取接口 ---

func (h *ServerHandler) S2SClaimReward(ctx context.Context, req *pb.PBS2SClaimRewardRequest) (resp *pb.PBS2SClaimRewardResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SClaimReward req bizType=%s actId=%d userId=%d round=%d",
		req.BizType, req.ActId, req.UserId, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SClaimReward resp userId=%d err=%v", req.UserId, retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SClaimReward resp userId=%d claimed=%v claimTime=%d",
				req.UserId, resp.Claimed, resp.ClaimTime)
		}
	}()
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	bizType := rankservice.BizType(req.BizType)
	svc, isHistorical := manager.ResolveEngineService(bizType, req.ActId, req.Round)
	if isHistorical {
		claimed, claimTime, err := manager.ClaimHistoricalReward(bizType, req.ActId, req.Round, req.UserId)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return &pb.PBS2SClaimRewardResponse{Claimed: claimed, ClaimTime: claimTime}, nil
	}
	if svc == nil {
		return nil, status.Errorf(codes.NotFound, "service not found: bizType=%s actId=%d", req.BizType, req.ActId)
	}
	claimed, claimTime, err := svc.ClaimReward(req.UserId, time.Now().UnixMilli())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.PBS2SClaimRewardResponse{Claimed: claimed, ClaimTime: claimTime}, nil
}

func (h *ServerHandler) S2SGetClaimStatus(ctx context.Context, req *pb.PBS2SGetClaimStatusRequest) (resp *pb.PBS2SGetClaimStatusResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SGetClaimStatus req bizType=%s actId=%d userId=%d round=%d",
		req.BizType, req.ActId, req.UserId, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SGetClaimStatus resp userId=%d err=%v", req.UserId, retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SGetClaimStatus resp userId=%d claimed=%v claimTime=%d",
				req.UserId, resp.Claimed, resp.ClaimTime)
		}
	}()
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	bizType := rankservice.BizType(req.BizType)
	svc, isHistorical := manager.ResolveEngineService(bizType, req.ActId, req.Round)
	if isHistorical {
		claimed, claimTime, err := manager.GetHistoricalClaimStatus(bizType, req.ActId, req.Round, req.UserId)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return &pb.PBS2SGetClaimStatusResponse{Claimed: claimed, ClaimTime: claimTime}, nil
	}
	if svc == nil {
		return nil, status.Errorf(codes.NotFound, "service not found: bizType=%s actId=%d", req.BizType, req.ActId)
	}
	claimed, claimTime, err := svc.GetClaimStatus(req.UserId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.PBS2SGetClaimStatusResponse{Claimed: claimed, ClaimTime: claimTime}, nil
}

// --- GM 管理接口 ---

// S2SListRankBizTypes GM：查询支持的业务排行榜类型列表
func (h *ServerHandler) S2SListRankBizTypes(ctx context.Context, req *pb.PBS2SListRankBizTypesRequest) (resp *pb.PBS2SListRankBizTypesResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SListRankBizTypes")
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SListRankBizTypes resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SListRankBizTypes resp count=%d", len(resp.BizTypes))
		}
	}()

	cf, err := configmgr.GetConfigByFileName("RankBase")
	if err != nil {
		return &pb.PBS2SListRankBizTypesResponse{MsgCode: commonMsg.MsgCode_CODE_SERVER_INNER}, nil
	}

	bizTypeMap := make(map[string]bool)
	for _, item := range cf.GetData() {
		cfg, ok := item.(cfgtypes.RankBaseConfig)
		if !ok {
			continue
		}
		bizTypeMap[cfg.BizType] = true
	}

	bizTypes := make([]string, 0, len(bizTypeMap))
	for bizType := range bizTypeMap {
		bizTypes = append(bizTypes, bizType)
	}

	return &pb.PBS2SListRankBizTypesResponse{
		MsgCode:  commonMsg.MsgCode_CODE_OK,
		BizTypes: bizTypes,
	}, nil
}

func (h *ServerHandler) S2SCreateRankConfig(ctx context.Context, req *pb.PBS2SCreateRankConfigRequest) (resp *pb.PBS2SCreateRankConfigResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SCreateRankConfig req bizType=%s actId=%d openTime=%d closeTime=%d gameEndTime=%d",
		req.BizType, req.ActId, req.OpenTime, req.CloseTime, req.GameEndTime)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SCreateRankConfig resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SCreateRankConfig resp ok")
		}
	}()
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	cfg := engine.Config{
		ActID:       req.ActId,
		OpenTime:    req.OpenTime,
		CloseTime:   req.CloseTime,
		GameEndTime: req.GameEndTime,
	}
	if err := manager.Register(ctx, rankservice.BizType(req.BizType), cfg); err != nil {
		return nil, rankErrorToStatus(err)
	}
	return &pb.PBS2SCreateRankConfigResponse{}, nil
}

func (h *ServerHandler) S2SGetRankConfig(ctx context.Context, req *pb.PBS2SGetRankConfigRequest) (resp *pb.PBS2SGetRankConfigResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SGetRankConfig req bizType=%s actId=%d", req.BizType, req.ActId)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SGetRankConfig resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SGetRankConfig resp rankCode=%s groupCount=%d memberCount=%d",
				resp.RankCode, resp.GroupCount, resp.MemberCount)
		}
	}()
	svc, err := lookupEngineService(rankservice.BizType(req.BizType), req.ActId)
	if err != nil {
		return nil, err
	}
	cfg := svc.GetConfig()

	manager := rankservice.GetGlobalManager()
	var rankType pb.RankType = pb.RankType_RANK_TYPE_ONCE
	var cycleMinutes int32
	var currentRound int32
	if manager != nil {
		if state := manager.GetPeriodicState(rankservice.BizType(req.BizType), req.ActId); state != nil {
			rankType = pb.RankType_RANK_TYPE_PERIODIC
			cycleMinutes = state.CycleMinutes
			currentRound = state.GetCurrentRound()
		}
	}

	return &pb.PBS2SGetRankConfigResponse{
		BizType: cfg.BizType, ActId: cfg.ActID, RankCode: cfg.RankCode,
		RankPeopleNum: cfg.RankPeopleNum, OpenToken: cfg.OpenToken,
		OpenTime: cfg.OpenTime, CloseTime: cfg.CloseTime, GameEndTime: effectiveGameEndTime(cfg.GameEndTime, cfg.CloseTime),
		RobotTiers: cfgRobotTiersToProto(cfg.RobotTiers), RobotInfos: cfgRobotInfosToProto(cfg.RobotInfos),
		Settled: svc.IsSettled(), GroupCount: svc.GroupCount(), MemberCount: svc.MemberCount(),
		RankType: rankType, CycleMinutes: cycleMinutes, CurrentRound: currentRound,
	}, nil
}

func (h *ServerHandler) S2SUpdateRankConfig(ctx context.Context, req *pb.PBS2SUpdateRankConfigRequest) (resp *pb.PBS2SUpdateRankConfigResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SUpdateRankConfig req bizType=%s actId=%d openTime=%d closeTime=%d gameEndTime=%d",
		req.BizType, req.ActId, req.OpenTime, req.CloseTime, req.GameEndTime)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SUpdateRankConfig resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SUpdateRankConfig resp ok")
		}
	}()
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	cfg := engine.Config{
		OpenTime:    req.OpenTime,
		CloseTime:   req.CloseTime,
		GameEndTime: req.GameEndTime,
	}
	if err := manager.UpdateService(rankservice.BizType(req.BizType), req.ActId, cfg); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &pb.PBS2SUpdateRankConfigResponse{}, nil
}

func (h *ServerHandler) S2SDeleteRankConfig(ctx context.Context, req *pb.PBS2SDeleteRankConfigRequest) (resp *pb.PBS2SDeleteRankConfigResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SDeleteRankConfig req bizType=%s actId=%d", req.BizType, req.ActId)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SDeleteRankConfig resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SDeleteRankConfig resp ok")
		}
	}()
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	if err := manager.RemoveService(rankservice.BizType(req.BizType), req.ActId); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &pb.PBS2SDeleteRankConfigResponse{}, nil
}

func (h *ServerHandler) S2SListRankConfigs(ctx context.Context, req *pb.PBS2SListRankConfigsRequest) (resp *pb.PBS2SListRankConfigsResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SListRankConfigs req bizType=%s", req.BizType)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SListRankConfigs resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SListRankConfigs resp count=%d", len(resp.Ranks))
		}
	}()
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	infos := manager.ListServices(rankservice.BizType(req.BizType))
	ranks := make([]*pb.PBRankConfigSummary, len(infos))
	for i, info := range infos {
		var rankType pb.RankType = pb.RankType_RANK_TYPE_ONCE
		var cycleMinutes, currentRound int32
		// OBS9: 周期排行榜的 OpenTime/CloseTime 取整体活动窗口（TotalOpenTime/TotalCloseTime），
		// 而非当前子轮注册到 engine.Config 的轮次窗口（避免调用方误读为活动总时间）。
		openTime := info.Config.OpenTime
		closeTime := info.Config.CloseTime
		gameEndTime := effectiveGameEndTime(info.Config.GameEndTime, info.Config.CloseTime)
		if state := manager.GetPeriodicState(info.BizType, info.ActID); state != nil {
			rankType = pb.RankType_RANK_TYPE_PERIODIC
			cycleMinutes = state.CycleMinutes
			currentRound = state.GetCurrentRound()
			openTime = state.TotalOpenTime
			closeTime = state.TotalCloseTime
			gameEndTime = state.TotalCloseTime
		}
		ranks[i] = &pb.PBRankConfigSummary{
			BizType:      string(info.BizType),
			ActId:        info.ActID,
			RankCode:     info.Config.RankCode,
			OpenTime:     openTime,
			CloseTime:    closeTime,
			GameEndTime:  gameEndTime,
			Settled:      info.Settled,
			GroupCount:   info.GroupCount,
			MemberCount:  info.MemberCount,
			CreateTime:   info.CreateTime,
			RankType:     rankType,
			CycleMinutes: cycleMinutes,
			CurrentRound: currentRound,
		}
	}
	return &pb.PBS2SListRankConfigsResponse{Ranks: ranks}, nil
}

// S2SGetRankCurRound 查询排行榜活动当前轮次信息（周期/一次性排行榜通用）。
func (h *ServerHandler) S2SGetRankCurRound(ctx context.Context, req *pb.PBS2SGetRankCurRoundRequest) (resp *pb.PBS2SGetRankCurRoundResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SGetRankCurRound req bizType=%s actId=%d", req.BizType, req.ActId)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SGetRankCurRound resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SGetRankCurRound resp currentRound=%v", resp.CurrentRound)
		}
	}()
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	info, err := manager.GetCurrentRoundInfo(rankservice.BizType(req.BizType), req.ActId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &pb.PBS2SGetRankCurRoundResponse{
		MsgCode: commonMsg.MsgCode_CODE_OK,
		CurrentRound: &pb.PBRoundInfo{
			Round:     info.Round,
			OpenTime:  info.OpenTime,
			CloseTime: info.CloseTime,
			Settled:   info.Settled,
			Current:   info.Current,
		},
	}, nil
}

// --- 辅助函数 ---

func lookupEngineService(bizType rankservice.BizType, actID int32) (*engine.Service, error) {
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	svc := manager.GetEngineService(bizType, actID)
	if svc == nil {
		return nil, status.Errorf(codes.NotFound, "service not found: bizType=%s actId=%d", bizType, actID)
	}
	return svc, nil
}

func cfgRobotTiersToProto(tiers []engine.RobotTierCfg) []*pb.PBRobotTierCfg {
	if len(tiers) == 0 {
		return nil
	}
	result := make([]*pb.PBRobotTierCfg, len(tiers))
	for i, t := range tiers {
		result[i] = &pb.PBRobotTierCfg{
			TierId: t.TierID, Num: t.Num,
			DefaultTokenMin: t.DefaultTokenMin, DefaultTokenMax: t.DefaultTokenMax,
			GrowTokenCdMs: t.GrowTokenCdMs, GrowTokenMinPermille: t.GrowTokenMinBps,
			GrowTokenMaxPermille: t.GrowTokenMaxBps, MaxToken: t.MaxToken,
			MaxDifferenceToken: t.MaxDifferenceToken, LockTokenTimeMs: t.LockTokenTimeMs,
		}
	}
	return result
}

func cfgRobotInfosToProto(infos []engine.RobotInfoEntry) []*commonMsg.PBAvatarInfo {
	if len(infos) == 0 {
		return nil
	}
	result := make([]*commonMsg.PBAvatarInfo, len(infos))
	for i, info := range infos {
		result[i] = &commonMsg.PBAvatarInfo{UserId: info.InfoID, Name: info.Name, Avatar: info.Avatar, Frame: info.Frame}
	}
	return result
}

func snapshotToProto(s *rank.RankMemberSnapshot) *pb.PBRankMemberSnapshot {
	if s == nil {
		return nil
	}
	snap := &pb.PBRankMemberSnapshot{
		MemberId: s.MemberId, Score: s.Score, Rank: s.Rank,
		EnterTime: s.EnterTime, UpdateTime: s.UpdateTime, Sequence: s.Sequence,
	}
	if s.AvatarInfo != nil {
		snap.AvatarInfo = &commonMsg.PBAvatarInfo{
			UserId: s.AvatarInfo.UserId,
			Name:   s.AvatarInfo.Name,
			Avatar: s.AvatarInfo.Avatar,
			Frame:  s.AvatarInfo.Frame,
		}
	}
	return snap
}

func protoAvatarInfoToRank(p *commonMsg.PBAvatarInfo) *rank.AvatarInfo {
	if p == nil {
		return nil
	}
	return &rank.AvatarInfo{
		UserId: p.UserId, Name: p.Name, Avatar: p.Avatar, Frame: p.Frame,
	}
}

func snapshotsToProto(ss []rank.RankMemberSnapshot) []*pb.PBRankMemberSnapshot {
	if len(ss) == 0 {
		return nil
	}
	result := make([]*pb.PBRankMemberSnapshot, len(ss))
	for i := range ss {
		result[i] = snapshotToProto(&ss[i])
	}
	return result
}

func rankErrorToStatus(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, rank.ErrInstanceNotOpen):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, rank.ErrInstanceNotFound), errors.Is(err, rank.ErrDefinitionNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, rank.ErrInvalidRankSpec):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, rank.ErrVersionConflict):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// effectiveGameEndTime 返回实际生效的玩法结束时间。
func effectiveGameEndTime(gameEndTime, closeTime int64) int64 {
	if gameEndTime == 0 {
		return closeTime
	}
	return gameEndTime
}

// --- GM 查询接口（独立，不与玩家接口共用） ---

// S2SGMGetUserRankList GM：通过userId查询该用户所在的所有排行榜列表（含当前排名快照）。
func (h *ServerHandler) S2SGMGetUserRankList(ctx context.Context, req *pb.PBS2SGMGetUserRankListRequest) (resp *pb.PBS2SGMGetUserRankListResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank][gm] S2SGMGetUserRankList req userId=%d", req.UserId)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank][gm] S2SGMGetUserRankList resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank][gm] S2SGMGetUserRankList resp entryCount=%d", len(resp.Entries))
		}
	}()
	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return nil, status.Error(codes.Internal, "rank manager not initialized")
	}
	rankEntries, err := manager.GetMemberRankEntries(ctx, req.UserId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	entries := make([]*pb.PBGMUserRankEntry, 0, len(rankEntries))
	for _, e := range rankEntries {
		entry := &pb.PBGMUserRankEntry{
			BizType:  string(e.BizType),
			ActId:    e.ActID,
			GroupId:  e.GroupID,
			Snapshot: snapshotToProto(e.Snapshot),
		}
		entries = append(entries, entry)
	}
	return &pb.PBS2SGMGetUserRankListResponse{Entries: entries}, nil
}

// S2SGMGetGroupRankList GM：通过排行榜列表某一项（bizType+actId+groupId）查该榜所有排名。
func (h *ServerHandler) S2SGMGetGroupRankList(ctx context.Context, req *pb.PBS2SGMGetGroupRankListRequest) (resp *pb.PBS2SGMGetGroupRankListResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank][gm] S2SGMGetGroupRankList req bizType=%s actId=%d groupId=%d",
		req.BizType, req.ActId, req.GroupId)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank][gm] S2SGMGetGroupRankList resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank][gm] S2SGMGetGroupRankList resp memberCount=%d", len(resp.Members))
		}
	}()
	svc, err := lookupEngineService(rankservice.BizType(req.BizType), req.ActId)
	if err != nil {
		return nil, err
	}
	snapshots, err := svc.ListGroupRank(ctx, req.GroupId, 0, -1)
	if err != nil {
		return nil, rankErrorToStatus(err)
	}
	members := snapshotsToProto(snapshots)
	ret := &pb.PBS2SGMGetGroupRankListResponse{
		Members: members,
		MsgCode: commonMsg.MsgCode_CODE_OK,
	}
	zaplog.LoggerSugar.Infof("[rank][gm] S2SGMGetGroupRankList bizType=%s actId=%d groupId=%d memberCount=%d",
		req.BizType, req.ActId, req.GroupId, len(members))
	return ret, nil
}

// S2SGMGetRankInstanceList GM：通过排行榜配置（bizType+actId）查该配置的所有组实例列表。
func (h *ServerHandler) S2SGMGetRankInstanceList(ctx context.Context, req *pb.PBS2SGMGetRankInstanceListRequest) (resp *pb.PBS2SGMGetRankInstanceListResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank][gm] S2SGMGetRankInstanceList req bizType=%s actId=%d", req.BizType, req.ActId)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank][gm] S2SGMGetRankInstanceList resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank][gm] S2SGMGetRankInstanceList resp groupCount=%d", len(resp.Groups))
		}
	}()
	svc, err := lookupEngineService(rankservice.BizType(req.BizType), req.ActId)
	if err != nil {
		return nil, err
	}
	groups := svc.ListGroups()
	pbGroups := make([]*pb.PBGMRankGroupInstance, 0, len(groups))
	for _, g := range groups {
		pbGroups = append(pbGroups, &pb.PBGMRankGroupInstance{
			GroupId:    g.GroupID,
			InstanceId: g.InstanceID,
			RealCount:  g.RealCount,
			RobotCount: g.RobotCount,
			State:      string(g.State),
		})
	}
	return &pb.PBS2SGMGetRankInstanceListResponse{Groups: pbGroups}, nil
}

// S2SGMGetInstanceRankList GM：通过组实例信息（bizType+actId+groupId）查该组全部排名列表。
func (h *ServerHandler) S2SGMGetInstanceRankList(ctx context.Context, req *pb.PBS2SGMGetInstanceRankListRequest) (resp *pb.PBS2SGMGetInstanceRankListResponse, retErr error) {
	zaplog.LoggerSugar.Infof("[rank][gm] S2SGMGetInstanceRankList req bizType=%s actId=%d groupId=%d",
		req.BizType, req.ActId, req.GroupId)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank][gm] S2SGMGetInstanceRankList resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank][gm] S2SGMGetInstanceRankList resp memberCount=%d", len(resp.Members))
		}
	}()
	svc, err := lookupEngineService(rankservice.BizType(req.BizType), req.ActId)
	if err != nil {
		return nil, err
	}
	g := svc.GetGroup(req.GroupId)
	if g == nil {
		return nil, status.Errorf(codes.NotFound, "group %d not found in bizType=%s actId=%d", req.GroupId, req.BizType, req.ActId)
	}
	snapshots, err := svc.ListGroupRank(ctx, req.GroupId, 0, -1)
	if err != nil {
		return nil, rankErrorToStatus(err)
	}
	return &pb.PBS2SGMGetInstanceRankListResponse{
		GroupId: g.GroupID,
		State:   string(g.State),
		Members: snapshotsToProto(snapshots),
	}, nil
}
