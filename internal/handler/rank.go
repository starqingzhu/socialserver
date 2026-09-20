package handler

import (
	"context"
	"errors"
	"time"

	"common/configmgr"
	cfgtypes "common/configmgr/types"
	"common/rank"
	libdispatch "golib/dispatch"
	dispatchproto "golib/dispatch/proto"
	"golib/zaplog"
	commonMsg "pbcommon/gen/common/msg"
	pb "pbcommon/gen/ss/msg"
	rankservice "socialserver/internal/rank"
	"socialserver/internal/rank/engine"
	"socialserver/internal/dispatch"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func registerRank(d *dispatchproto.Dispatcher) error {
	cmds := []error{
		dispatchproto.Register(d, dispatch.CmdS2SUpsertScore, handleUpsertScore),
		dispatchproto.Register(d, dispatch.CmdS2SGetRankList, handleGetRankList),
		dispatchproto.Register(d, dispatch.CmdS2SGetMemberRank, handleGetMemberRank),
		dispatchproto.Register(d, dispatch.CmdS2SSettle, handleSettle),
		dispatchproto.Register(d, dispatch.CmdS2SGetRewardUsers, handleGetRewardUsers),
		dispatchproto.Register(d, dispatch.CmdS2SClaimReward, handleClaimReward),
		dispatchproto.Register(d, dispatch.CmdS2SGetClaimStatus, handleGetClaimStatus),
		dispatchproto.Register(d, dispatch.CmdS2SListRankBizTypes, handleListRankBizTypes),
		dispatchproto.Register(d, dispatch.CmdS2SCreateRankConfig, handleCreateRankConfig),
		dispatchproto.Register(d, dispatch.CmdS2SGetRankConfig, handleGetRankConfig),
		dispatchproto.Register(d, dispatch.CmdS2SUpdateRankConfig, handleUpdateRankConfig),
		dispatchproto.Register(d, dispatch.CmdS2SDeleteRankConfig, handleDeleteRankConfig),
		dispatchproto.Register(d, dispatch.CmdS2SListRankConfigs, handleListRankConfigs),
		dispatchproto.Register(d, dispatch.CmdS2SGetRankCurRound, handleGetRankCurRound),
		dispatchproto.Register(d, dispatch.CmdS2SGMGetUserRankList, handleGMGetUserRankList),
		dispatchproto.Register(d, dispatch.CmdS2SGMGetGroupRankList, handleGMGetGroupRankList),
		dispatchproto.Register(d, dispatch.CmdS2SGMGetRankInstanceList, handleGMGetRankInstanceList),
		dispatchproto.Register(d, dispatch.CmdS2SGMGetInstanceRankList, handleGMGetInstanceRankList),
	}
	for _, err := range cmds {
		if err != nil {
			return err
		}
	}
	return nil
}

// --- 排行榜玩家接口 ---

func handleUpsertScore(ctx context.Context, meta libdispatch.Meta, req *pb.PBS2SUpsertScoreRequest, resp *pb.PBS2SUpsertScoreResponse) (retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SUpsertScore req bizType=%s actId=%d userId=%d totalScore=%d ts=%d round=%d",
		req.BizType, req.ActId, req.UserId, req.TotalScore, req.Timestamp, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SUpsertScore resp bizType=%s actId=%d userId=%d round=%d err=%v",
				req.BizType, req.ActId, req.UserId, req.Round, retErr)
		} else {
			var myRank int64
			if resp.MyRank != nil {
				myRank = resp.MyRank.Rank
			}
			zaplog.LoggerSugar.Infof("[rank] S2SUpsertScore resp bizType=%s actId=%d userId=%d rank=%d round=%d msgCode=%v",
				req.BizType, req.ActId, req.UserId, myRank, resp.CurrentRound, resp.MsgCode)
		}
	}()

	bizType := rankservice.BizType(req.BizType)
	currentRound := int32(1)
	manager := rankservice.GetGlobalManager()
	if manager != nil {
		if ps := manager.GetPeriodicState(bizType, req.ActId); ps != nil {
			currentRound = ps.GetCurrentRound()
			if req.Round > 0 && req.Round != ps.GetCurrentRound() {
				resp.MsgCode = commonMsg.MsgCode_CODE_RANK_ROUND_CHANGED
				resp.CurrentRound = ps.GetCurrentRound()
				return nil
			}
		}
	}

	svc, err := lookupEngineService(bizType, req.ActId)
	if err != nil {
		return err
	}
	if err := svc.UpsertScore(ctx, req.UserId, req.TotalScore, req.Timestamp, protoAvatarInfoToRank(req.AvatarInfo)); err != nil {
		if errors.Is(err, rank.ErrInstanceClosed) {
			zaplog.LoggerSugar.Infof("[rank] S2SUpsertScore activity closed bizType=%s actId=%d userId=%d round=%d",
				req.BizType, req.ActId, req.UserId, req.Round)
			resp.MsgCode = commonMsg.MsgCode_CODE_RANK_ACTIVITY_CLOSED
			return nil
		}
		return rankErrorToStatus(err)
	}
	snapshot, _, err := svc.GetMemberRank(ctx, req.UserId)
	if err != nil {
		zaplog.LoggerSugar.Warnf("[rank] S2SUpsertScore GetMemberRank bizType=%s actId=%d userId=%d err=%v",
			req.BizType, req.ActId, req.UserId, err)
		resp.MsgCode = commonMsg.MsgCode_CODE_OK
		resp.CurrentRound = currentRound
		return nil
	}

	resp.MsgCode = commonMsg.MsgCode_CODE_OK
	resp.MyRank = snapshotToProto(snapshot)
	resp.CurrentRound = currentRound
	zaplog.LoggerSugar.Infof("[rank] S2SUpsertScore updated score for userId=%d resp:%s", req.UserId, resp.String())
	return nil
}

func handleGetRankList(ctx context.Context, meta libdispatch.Meta, req *pb.PBS2SGetRankListRequest, resp *pb.PBS2SGetRankListResponse) (retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SGetRankList req bizType=%s actId=%d userId=%d start=%d end=%d round=%d",
		req.BizType, req.ActId, req.UserId, req.Start, req.End, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SGetRankList resp bizType=%s actId=%d userId=%d round=%d err=%v",
				req.BizType, req.ActId, req.UserId, req.Round, retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SGetRankList resp bizType=%s actId=%d userId=%d round=%d memberCount=%d currentRound=%d",
				req.BizType, req.ActId, req.UserId, req.Round, len(resp.Members), resp.CurrentRound)
		}
	}()

	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return status.Error(codes.Internal, "rank manager not initialized")
	}
	bizType := rankservice.BizType(req.BizType)
	svc, isHistorical := manager.ResolveEngineService(bizType, req.ActId, req.Round)

	currentRound := int32(1)
	if ps := manager.GetPeriodicState(bizType, req.ActId); ps != nil {
		currentRound = ps.GetCurrentRound()
	}

	if isHistorical {
		snapshots, mySnap, err := manager.GetHistoricalRoundList(ctx, bizType, req.ActId, req.Round, req.UserId, req.Start, req.End)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		resp.MsgCode = commonMsg.MsgCode_CODE_OK
		resp.Members = snapshotsToProto(snapshots)
		resp.MyRank = snapshotToProto(mySnap)
		resp.CurrentRound = currentRound
		return nil
	}

	if svc == nil {
		return status.Errorf(codes.NotFound, "service not found: bizType=%s actId=%d", req.BizType, req.ActId)
	}
	snapshot, groupID, err := svc.GetMemberRank(ctx, req.UserId)
	if err != nil {
		return rankErrorToStatus(err)
	}
	if groupID == 0 {
		groups := svc.ListGroups()
		if len(groups) == 0 {
			return status.Errorf(codes.NotFound, "user %d not found in any group for bizType=%s actId=%d", req.UserId, req.BizType, req.ActId)
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
		return rankErrorToStatus(err)
	}

	resp.MsgCode = commonMsg.MsgCode_CODE_OK
	resp.Members = snapshotsToProto(snapshots)
	resp.MyRank = snapshotToProto(snapshot)
	resp.CreateTime = svc.GetGroupCreateTime(ctx, groupID)
	resp.CurrentRound = currentRound
	return nil
}

func handleGetMemberRank(ctx context.Context, meta libdispatch.Meta, req *pb.PBS2SGetMemberRankRequest, resp *pb.PBS2SGetMemberRankResponse) (retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SGetMemberRank req bizType=%s actId=%d userId=%d round=%d",
		req.BizType, req.ActId, req.UserId, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SGetMemberRank resp userId=%d err=%v bizType=%s actId=%d round=%d",
				req.UserId, retErr, req.BizType, req.ActId, req.Round)
		} else {
			var memberRank, score int64
			if resp.Snapshot != nil {
				memberRank = resp.Snapshot.Rank
				score = resp.Snapshot.Score
			}
			zaplog.LoggerSugar.Infof("[rank] S2SGetMemberRank resp userId=%d bizType=%s actId=%d round=%d rank=%d score=%d settleStage=%d",
				req.UserId, req.BizType, req.ActId, req.Round, memberRank, score, resp.SettleStage)
		}
	}()

	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return status.Error(codes.Internal, "rank manager not initialized")
	}
	bizType := rankservice.BizType(req.BizType)
	svc, isHistorical := manager.ResolveEngineService(bizType, req.ActId, req.Round)
	if isHistorical {
		zaplog.LoggerSugar.Infof("[rank] S2SGetMemberRank route=historical bizType=%s actId=%d userId=%d round=%d",
			req.BizType, req.ActId, req.UserId, req.Round)
		_, mySnap, err := manager.GetHistoricalRoundList(ctx, bizType, req.ActId, req.Round, req.UserId, 0, -1)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if mySnap == nil {
			zaplog.LoggerSugar.Infof("[rank] S2SGetMemberRank user not in historical rank bizType=%s actId=%d userId=%d round=%d",
				req.BizType, req.ActId, req.UserId, req.Round)
		}
		resp.Snapshot = snapshotToProto(mySnap)
		resp.SettleStage = 1
		return nil
	}
	if svc == nil {
		return status.Errorf(codes.NotFound, "service not found: bizType=%s actId=%d", req.BizType, req.ActId)
	}
	snapshot, groupID, err := svc.GetMemberRank(ctx, req.UserId)
	if err != nil {
		return rankErrorToStatus(err)
	}
	if snapshot == nil {
		zaplog.LoggerSugar.Infof("[rank] S2SGetMemberRank user not in rank bizType=%s actId=%d userId=%d round=%d",
			req.BizType, req.ActId, req.UserId, req.Round)
	}
	settleStage := int32(0)
	if groupID > 0 {
		g := svc.GetGroup(groupID)
		if g != nil && g.State == engine.GroupStateSettled {
			settleStage = 1
		}
	}
	resp.Snapshot = snapshotToProto(snapshot)
	resp.SettleStage = settleStage
	return nil
}

func handleSettle(ctx context.Context, meta libdispatch.Meta, req *pb.PBS2SSettleRequest, resp *pb.PBS2SSettleResponse) (retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SSettle req bizType=%s actId=%d timestamp=%d",
		req.BizType, req.ActId, req.Timestamp)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SSettle resp bizType=%s actId=%d err=%v",
				req.BizType, req.ActId, retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SSettle resp bizType=%s actId=%d groupCount=%d",
				req.BizType, req.ActId, len(resp.Groups))
		}
	}()

	svc, err := lookupEngineService(rankservice.BizType(req.BizType), req.ActId)
	if err != nil {
		return err
	}
	results, err := svc.Settle(ctx)
	if err != nil {
		return rankErrorToStatus(err)
	}
	groups := make([]*pb.PBS2SSettleGroupResult, 0, len(results))
	for gid, snapshots := range results {
		groups = append(groups, &pb.PBS2SSettleGroupResult{GroupId: gid, Members: snapshotsToProto(snapshots)})
	}
	resp.Groups = groups
	return nil
}

func handleGetRewardUsers(ctx context.Context, meta libdispatch.Meta, req *pb.PBS2SGetRewardUsersRequest, resp *pb.PBS2SGetRewardUsersResponse) (retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SGetRewardUsers req bizType=%s actId=%d round=%d", req.BizType, req.ActId, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SGetRewardUsers resp bizType=%s actId=%d round=%d err=%v",
				req.BizType, req.ActId, req.Round, retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SGetRewardUsers resp bizType=%s actId=%d round=%d userCount=%d",
				req.BizType, req.ActId, req.Round, len(resp.UserIds))
		}
	}()

	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return status.Error(codes.Internal, "rank manager not initialized")
	}
	bizType := rankservice.BizType(req.BizType)
	svc, isHistorical := manager.ResolveEngineService(bizType, req.ActId, req.Round)
	if isHistorical {
		userIDs, err := manager.GetHistoricalRewardUsers(ctx, bizType, req.ActId, req.Round)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		resp.UserIds = userIDs
		return nil
	}
	if svc == nil {
		return status.Errorf(codes.NotFound, "service not found: bizType=%s actId=%d", req.BizType, req.ActId)
	}
	resp.UserIds = svc.GetOpenRewardUserIDs()
	return nil
}

// --- 奖励领取接口 ---

func handleClaimReward(ctx context.Context, meta libdispatch.Meta, req *pb.PBS2SClaimRewardRequest, resp *pb.PBS2SClaimRewardResponse) (retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SClaimReward req bizType=%s actId=%d userId=%d round=%d",
		req.BizType, req.ActId, req.UserId, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SClaimReward resp bizType=%s actId=%d userId=%d round=%d err=%v",
				req.BizType, req.ActId, req.UserId, req.Round, retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SClaimReward resp bizType=%s actId=%d userId=%d round=%d claimed=%v claimTime=%d",
				req.BizType, req.ActId, req.UserId, req.Round, resp.Claimed, resp.ClaimTime)
		}
	}()

	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return status.Error(codes.Internal, "rank manager not initialized")
	}
	bizType := rankservice.BizType(req.BizType)
	svc, isHistorical := manager.ResolveEngineService(bizType, req.ActId, req.Round)
	if isHistorical {
		claimed, claimTime, err := manager.ClaimHistoricalReward(bizType, req.ActId, req.Round, req.UserId)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		resp.Claimed = claimed
		resp.ClaimTime = claimTime
		return nil
	}
	if svc == nil {
		claimed, claimTime, err := manager.FallbackClaimReward(bizType, req.ActId, req.UserId, time.Now().UnixMilli())
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		resp.Claimed = claimed
		resp.ClaimTime = claimTime
		return nil
	}
	claimed, claimTime, err := svc.ClaimReward(req.UserId, time.Now().UnixMilli())
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	resp.Claimed = claimed
	resp.ClaimTime = claimTime
	return nil
}

func handleGetClaimStatus(ctx context.Context, meta libdispatch.Meta, req *pb.PBS2SGetClaimStatusRequest, resp *pb.PBS2SGetClaimStatusResponse) (retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SGetClaimStatus req bizType=%s actId=%d userId=%d round=%d",
		req.BizType, req.ActId, req.UserId, req.Round)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SGetClaimStatus resp bizType=%s actId=%d userId=%d round=%d err=%v",
				req.BizType, req.ActId, req.UserId, req.Round, retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SGetClaimStatus resp bizType=%s actId=%d userId=%d round=%d claimed=%v claimTime=%d",
				req.BizType, req.ActId, req.UserId, req.Round, resp.Claimed, resp.ClaimTime)
		}
	}()

	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return status.Error(codes.Internal, "rank manager not initialized")
	}
	bizType := rankservice.BizType(req.BizType)
	svc, isHistorical := manager.ResolveEngineService(bizType, req.ActId, req.Round)
	if isHistorical {
		claimed, claimTime, err := manager.GetHistoricalClaimStatus(bizType, req.ActId, req.Round, req.UserId)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		resp.Claimed = claimed
		resp.ClaimTime = claimTime
		return nil
	}
	if svc == nil {
		claimed, claimTime, err := manager.FallbackGetClaimStatus(bizType, req.ActId, req.UserId)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		resp.Claimed = claimed
		resp.ClaimTime = claimTime
		return nil
	}
	claimed, claimTime, err := svc.GetClaimStatus(req.UserId)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	resp.Claimed = claimed
	resp.ClaimTime = claimTime
	return nil
}

// --- GM 管理接口 ---

func handleListRankBizTypes(_ context.Context, _ libdispatch.Meta, _ *pb.PBS2SListRankBizTypesRequest, resp *pb.PBS2SListRankBizTypesResponse) (retErr error) {
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
		resp.MsgCode = commonMsg.MsgCode_CODE_SERVER_INNER
		return nil
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
	resp.MsgCode = commonMsg.MsgCode_CODE_OK
	resp.BizTypes = bizTypes
	return nil
}

func handleCreateRankConfig(ctx context.Context, meta libdispatch.Meta, req *pb.PBS2SCreateRankConfigRequest, resp *pb.PBS2SCreateRankConfigResponse) (retErr error) {
	zaplog.LoggerSugar.Infof("[rank] S2SCreateRankConfig req bizType=%s actId=%d openTime=%d closeTime=%d gameEndTime=%d",
		req.BizType, req.ActId, req.OpenTime, req.CloseTime, req.GameEndTime)
	defer func() {
		if retErr != nil {
			zaplog.LoggerSugar.Warnf("[rank] S2SCreateRankConfig resp err=%v", retErr)
		} else {
			zaplog.LoggerSugar.Infof("[rank] S2SCreateRankConfig resp ok")
		}
	}()

	// CloseTime 与 GameEndTime 不能同时缺省：effectiveSettleAt() 会退化为 0，
	// 导致 Tick 的 settledAt==settleAt 短路在第一次调用就命中，
	// 活动从此既不再 tick 机器人也永不结算（docs/rank_optimization.md 待办 K）。
	if req.CloseTime <= 0 && req.GameEndTime <= 0 {
		return status.Error(codes.InvalidArgument, "closeTime and gameEndTime cannot both be unset")
	}

	manager := rankservice.GetGlobalManager()
	if manager == nil {
		return status.Error(codes.Internal, "rank manager not initialized")
	}
	cfg := engine.Config{
		ActID:       req.ActId,
		OpenTime:    req.OpenTime,
		CloseTime:   req.CloseTime,
		GameEndTime: req.GameEndTime,
	}
	if err := manager.Register(ctx, rankservice.BizType(req.BizType), cfg); err != nil {
		return rankErrorToStatus(err)
	}
	return nil
}

func handleGetRankConfig(_ context.Context, _ libdispatch.Meta, req *pb.PBS2SGetRankConfigRequest, resp *pb.PBS2SGetRankConfigResponse) (retErr error) {
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
		return err
	}
	cfg := svc.GetConfig()
	manager := rankservice.GetGlobalManager()
	var rankType pb.RankType = pb.RankType_RANK_TYPE_ONCE
	var cycleMinutes, currentRound int32
	if manager != nil {
		if state := manager.GetPeriodicState(rankservice.BizType(req.BizType), req.ActId); state != nil {
			rankType = pb.RankType_RANK_TYPE_PERIODIC
			cycleMinutes = state.CycleMinutes
			currentRound = state.GetCurrentRound()
		}
	}

	resp.BizType = cfg.BizType
	resp.ActId = cfg.ActID
	resp.RankCode = cfg.RankCode
	resp.RankPeopleNum = cfg.RankPeopleNum
	resp.OpenToken = cfg.OpenToken
	resp.OpenTime = cfg.OpenTime
	resp.CloseTime = cfg.CloseTime
	resp.GameEndTime = effectiveGameEndTime(cfg.GameEndTime, cfg.CloseTime)
	resp.RobotTiers = cfgRobotTiersToProto(cfg.RobotTiers)
	resp.RobotInfos = cfgRobotInfosToProto(cfg.RobotInfos)
	resp.Settled = svc.IsSettled()
	resp.GroupCount = svc.GroupCount()
	resp.MemberCount = svc.MemberCount()
	resp.RankType = rankType
	resp.CycleMinutes = cycleMinutes
	resp.CurrentRound = currentRound
	return nil
}

func handleUpdateRankConfig(_ context.Context, _ libdispatch.Meta, req *pb.PBS2SUpdateRankConfigRequest, resp *pb.PBS2SUpdateRankConfigResponse) (retErr error) {
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
		return status.Error(codes.Internal, "rank manager not initialized")
	}
	cfg := engine.Config{
		OpenTime:    req.OpenTime,
		CloseTime:   req.CloseTime,
		GameEndTime: req.GameEndTime,
	}
	if err := manager.UpdateService(rankservice.BizType(req.BizType), req.ActId, cfg); err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	return nil
}

func handleDeleteRankConfig(_ context.Context, _ libdispatch.Meta, req *pb.PBS2SDeleteRankConfigRequest, resp *pb.PBS2SDeleteRankConfigResponse) (retErr error) {
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
		return status.Error(codes.Internal, "rank manager not initialized")
	}
	if err := manager.RemoveService(rankservice.BizType(req.BizType), req.ActId); err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	return nil
}

func handleListRankConfigs(_ context.Context, _ libdispatch.Meta, req *pb.PBS2SListRankConfigsRequest, resp *pb.PBS2SListRankConfigsResponse) (retErr error) {
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
		return status.Error(codes.Internal, "rank manager not initialized")
	}
	infos := manager.ListServices(rankservice.BizType(req.BizType))
	ranks := make([]*pb.PBRankConfigSummary, len(infos))
	for i, info := range infos {
		var rankType pb.RankType = pb.RankType_RANK_TYPE_ONCE
		var cycleMinutes, currentRound int32
		openTime := info.Config.OpenTime
		closeTime := info.Config.CloseTime
		gameEndTime := effectiveGameEndTime(info.Config.GameEndTime, info.Config.CloseTime)
		createTime := info.CreateTime
		if state := manager.GetPeriodicState(info.BizType, info.ActID); state != nil {
			rankType = pb.RankType_RANK_TYPE_PERIODIC
			cycleMinutes = state.CycleMinutes
			currentRound = state.GetCurrentRound()
			openTime = state.RoundOpenTime
			closeTime = state.RoundCloseTime
			gameEndTime = state.RoundCloseTime
			createTime = state.TotalOpenTime
		}
		ranks[i] = &pb.PBRankConfigSummary{
			BizType:          string(info.BizType),
			ActId:            info.ActID,
			RankCode:         info.Config.RankCode,
			OpenTime:         openTime,
			CloseTime:        closeTime,
			GameEndTime:      gameEndTime,
			Settled:          info.Settled,
			GroupCount:       info.GroupCount,
			MemberCount:      info.MemberCount,
			CreateTime:       createTime,
			RankType:         rankType,
			CycleMinutes:     cycleMinutes,
			CurrentRound:     currentRound,
			ActivityTypeName: rankservice.GetRankBaseName(info.BizType),
		}
	}
	resp.Ranks = ranks
	return nil
}

func handleGetRankCurRound(_ context.Context, _ libdispatch.Meta, req *pb.PBS2SGetRankCurRoundRequest, resp *pb.PBS2SGetRankCurRoundResponse) (retErr error) {
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
		return status.Error(codes.Internal, "rank manager not initialized")
	}
	info, err := manager.GetCurrentRoundInfo(rankservice.BizType(req.BizType), req.ActId)
	if err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	resp.MsgCode = commonMsg.MsgCode_CODE_OK
	resp.CurrentRound = &pb.PBRoundInfo{
		Round:     info.Round,
		OpenTime:  info.OpenTime,
		CloseTime: info.CloseTime,
		Settled:   info.Settled,
		Current:   info.Current,
	}
	return nil
}

// --- GM 查询接口 ---

func handleGMGetUserRankList(ctx context.Context, meta libdispatch.Meta, req *pb.PBS2SGMGetUserRankListRequest, resp *pb.PBS2SGMGetUserRankListResponse) (retErr error) {
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
		return status.Error(codes.Internal, "rank manager not initialized")
	}
	rankEntries, err := manager.GetMemberRankEntries(ctx, req.UserId)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	entries := make([]*pb.PBGMUserRankEntry, 0, len(rankEntries))
	for _, e := range rankEntries {
		entries = append(entries, &pb.PBGMUserRankEntry{
			BizType:  string(e.BizType),
			ActId:    e.ActID,
			GroupId:  e.GroupID,
			Snapshot: snapshotToProto(e.Snapshot),
		})
	}
	resp.Entries = entries
	return nil
}

func handleGMGetGroupRankList(ctx context.Context, meta libdispatch.Meta, req *pb.PBS2SGMGetGroupRankListRequest, resp *pb.PBS2SGMGetGroupRankListResponse) (retErr error) {
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
		return err
	}
	snapshots, err := svc.ListGroupRank(ctx, req.GroupId, 0, -1)
	if err != nil {
		return rankErrorToStatus(err)
	}
	resp.Members = snapshotsToProto(snapshots)
	resp.MsgCode = commonMsg.MsgCode_CODE_OK
	zaplog.LoggerSugar.Infof("[rank][gm] S2SGMGetGroupRankList bizType=%s actId=%d groupId=%d memberCount=%d",
		req.BizType, req.ActId, req.GroupId, len(resp.Members))
	return nil
}

func handleGMGetRankInstanceList(_ context.Context, _ libdispatch.Meta, req *pb.PBS2SGMGetRankInstanceListRequest, resp *pb.PBS2SGMGetRankInstanceListResponse) (retErr error) {
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
		return err
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
	resp.Groups = pbGroups
	return nil
}

func handleGMGetInstanceRankList(ctx context.Context, meta libdispatch.Meta, req *pb.PBS2SGMGetInstanceRankListRequest, resp *pb.PBS2SGMGetInstanceRankListResponse) (retErr error) {
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
		return err
	}
	g := svc.GetGroup(req.GroupId)
	if g == nil {
		return status.Errorf(codes.NotFound, "group %d not found in bizType=%s actId=%d", req.GroupId, req.BizType, req.ActId)
	}
	snapshots, err := svc.ListGroupRank(ctx, req.GroupId, 0, -1)
	if err != nil {
		return rankErrorToStatus(err)
	}
	resp.GroupId = g.GroupID
	resp.State = string(g.State)
	resp.Members = snapshotsToProto(snapshots)
	return nil
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

func effectiveGameEndTime(gameEndTime, closeTime int64) int64 {
	if gameEndTime == 0 {
		return closeTime
	}
	return gameEndTime
}
