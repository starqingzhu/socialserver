package rankservice

import (
	"context"
	"fmt"
	"time"

	commonrank "common/rank"
	"socialserver/internal/rank/engine"
	"socialserver/internal/rank/periodic"
)

// RegisterRoundService 实现 periodic.ServiceRegistrar 接口，供 Handler 注册子轮服务。
func (m *Manager) RegisterRoundService(ctx context.Context, bizType, logicalKey string, cfg engine.Config) (*engine.Service, error) {
	return m.registerSubService(ctx, BizType(bizType), logicalKey, cfg)
}

// ReplaceRoundService 实现 periodic.ServiceRegistrar 接口，强制替换子轮服务（不走快速路径）。
// 用于轮次推进时创建新一轮服务，绕过 registerSubService 的"已存在则直接返回"保护。
func (m *Manager) ReplaceRoundService(ctx context.Context, bizType, logicalKey string, cfg engine.Config) (*engine.Service, error) {
	return m.replaceSubService(ctx, BizType(bizType), logicalKey, cfg)
}

// GetEngineServiceByKey 实现 periodic.ServiceRegistrar 接口，按 logicalKey 返回引擎服务。
func (m *Manager) GetEngineServiceByKey(logicalKey string) *engine.Service {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.engineServices[logicalKey]
}

// registerSubService 注册一个不写入 MongoDB 配置的 engine.Service，并存入 services/engineServices。
// logicalKey 为 services 和 engineServices 中的查找键（始终是 "{bizType}:{actID}"）。
// BUG5 fix: 双重检查锁，保证并发注册时不覆盖已有服务。
func (m *Manager) registerSubService(ctx context.Context, bizType BizType, logicalKey string, cfg engine.Config) (*engine.Service, error) {
	if cfg.RankCode == "" {
		cfg.RankCode = fmt.Sprintf("%s_score_%d", bizType, cfg.ActID)
	}
	if cfg.CreateTime == 0 {
		cfg.CreateTime = time.Now().UnixMilli()
	}

	// 快速路径：已存在则直接返回
	m.mu.RLock()
	if existing, ok := m.engineServices[logicalKey]; ok {
		m.mu.RUnlock()
		return existing, nil
	}
	m.mu.RUnlock()

	if err := m.rankService.RegisterRank(ctx, commonrank.Rank{
		RankCode:       cfg.RankCode,
		RankName:       fmt.Sprintf("%s_rank_%d", bizType, cfg.ActID),
		ScoreOrder:     commonrank.ScoreOrderDesc,
		TieBreakPolicy: commonrank.TieBreakPolicyFirstEnter,
		CreateTime:     cfg.OpenTime,
		UpdateTime:     cfg.OpenTime,
	}); err != nil {
		return nil, err
	}

	onMemberJoin := func(userID int64, groupID int32) {
		m.memberIndex.Track(userID, MemberEntry{
			BizType: bizType,
			ActID:   cfg.ActID,
			GroupID: groupID,
		})
	}
	service, err := engine.NewService(m.rankService, cfg, m.rdb, m.dao, engine.WithOnMemberJoin(onMemberJoin))
	if err != nil {
		return nil, err
	}

	// 写锁下二次检查，防止并发调用互相覆盖
	m.mu.Lock()
	if existing, ok := m.engineServices[logicalKey]; ok {
		m.mu.Unlock()
		return existing, nil
	}
	m.services[logicalKey] = newBizServiceWrapper(bizType, service)
	m.engineServices[logicalKey] = service
	m.mu.Unlock()

	return service, nil
}

// replaceSubService 与 registerSubService 相同，但不跳过已存在检查，强制替换 engineServices 中的服务。
// 用于轮次推进时将 logicalKey 指向新一轮的 engine.Service。
func (m *Manager) replaceSubService(ctx context.Context, bizType BizType, logicalKey string, cfg engine.Config) (*engine.Service, error) {
	if cfg.RankCode == "" {
		cfg.RankCode = fmt.Sprintf("%s_score_%d", bizType, cfg.ActID)
	}
	if cfg.CreateTime == 0 {
		cfg.CreateTime = time.Now().UnixMilli()
	}

	if err := m.rankService.RegisterRank(ctx, commonrank.Rank{
		RankCode:       cfg.RankCode,
		RankName:       fmt.Sprintf("%s_rank_%d", bizType, cfg.ActID),
		ScoreOrder:     commonrank.ScoreOrderDesc,
		TieBreakPolicy: commonrank.TieBreakPolicyFirstEnter,
		CreateTime:     cfg.OpenTime,
		UpdateTime:     cfg.OpenTime,
	}); err != nil {
		return nil, err
	}

	onMemberJoin := func(userID int64, groupID int32) {
		m.memberIndex.Track(userID, MemberEntry{
			BizType: bizType,
			ActID:   cfg.ActID,
			GroupID: groupID,
		})
	}
	service, err := engine.NewService(m.rankService, cfg, m.rdb, m.dao, engine.WithOnMemberJoin(onMemberJoin))
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.services[logicalKey] = newBizServiceWrapper(bizType, service)
	m.engineServices[logicalKey] = service
	m.mu.Unlock()

	return service, nil
}
func (m *Manager) tickPeriodicActivities(ctx context.Context, now int64) {
	m.periodicHandler.TickAll(ctx, now)
}

// GetPeriodicState 线程安全地读取 PeriodicState（供外部包使用）。
func (m *Manager) GetPeriodicState(bizType BizType, actID int32) *PeriodicState {
	if m == nil {
		return nil
	}
	return m.periodicHandler.GetState(NewBizKey(bizType, actID).String())
}

// GetHistoricalRoundList 查询历史轮次的排行榜。
func (m *Manager) GetHistoricalRoundList(ctx context.Context, bizType BizType, actID int32, round int32, userID int64, start, end int64) ([]commonrank.RankMemberSnapshot, *commonrank.RankMemberSnapshot, error) {
	return m.periodicHandler.GetHistoricalRoundList(ctx, string(bizType), actID, round, userID, start, end)
}

// GetHistoricalRewardUsers 查询历史轮次的入榜真实玩家 ID 列表。
func (m *Manager) GetHistoricalRewardUsers(ctx context.Context, bizType BizType, actID int32, round int32) ([]int64, error) {
	return m.periodicHandler.GetHistoricalRewardUsers(ctx, string(bizType), actID, round)
}

// GetHistoricalClaimStatus 查询历史轮次的奖励领取状态。
func (m *Manager) GetHistoricalClaimStatus(bizType BizType, actID int32, round int32, userID int64) (claimed bool, claimTime int64, err error) {
	return m.periodicHandler.GetHistoricalClaimStatus(string(bizType), actID, round, userID)
}

// ClaimHistoricalReward 为历史轮次记录奖励领取。
func (m *Manager) ClaimHistoricalReward(bizType BizType, actID int32, round int32, userID int64) (claimed bool, claimTime int64, err error) {
	return m.periodicHandler.ClaimHistoricalReward(string(bizType), actID, round, userID)
}

// GetRoundInfos 返回活动的轮次摘要列表（按轮次升序）。
// 一次性排行榜返回虚拟单轮；周期排行榜委托给 periodicHandler。
func (m *Manager) GetRoundInfos(bizType BizType, actID int32) (currentRound int32, rounds []periodic.RoundInfo, err error) {
	key := NewBizKey(bizType, actID).String()
	state := m.periodicHandler.GetState(key)
	if state == nil {
		svc := m.GetEngineService(bizType, actID)
		if svc == nil {
			return 0, nil, fmt.Errorf("service not found: bizType=%s actID=%d", bizType, actID)
		}
		cfg := svc.GetConfig()
		return 1, []periodic.RoundInfo{{
			Round:     1,
			OpenTime:  cfg.OpenTime,
			CloseTime: cfg.CloseTime,
			Settled:   svc.IsSettled(),
			Current:   true,
		}}, nil
	}
	return m.periodicHandler.GetRoundInfos(state)
}

// GetCurrentRoundInfo 直接返回活动当前轮次信息，不枚举所有轮次。
// 一次性排行榜返回虚拟单轮；周期排行榜从 Redis 读取权威轮号后计算时间窗口。
func (m *Manager) GetCurrentRoundInfo(bizType BizType, actID int32) (periodic.RoundInfo, error) {
	key := NewBizKey(bizType, actID).String()
	state := m.periodicHandler.GetState(key)
	if state == nil {
		svc := m.GetEngineService(bizType, actID)
		if svc == nil {
			return periodic.RoundInfo{}, fmt.Errorf("service not found: bizType=%s actID=%d", bizType, actID)
		}
		cfg := svc.GetConfig()
		return periodic.RoundInfo{
			Round:     1,
			OpenTime:  cfg.OpenTime,
			CloseTime: cfg.CloseTime,
			Settled:   svc.IsSettled(),
			Current:   true,
		}, nil
	}
	return m.periodicHandler.GetCurrentRoundInfo(state)
}

// ResolveEngineService 根据 bizType/actID/round 返回对应的引擎服务和是否为历史查询。
// round=0 表示当前轮。
// 一次性排行榜或请求当前轮（且服务活跃）：返回 (svc, false)；
// 周期排行榜历史轮次（round < currentRound）或当前轮已结算/服务不可用：返回 (nil, true)。
func (m *Manager) ResolveEngineService(bizType BizType, actID int32, round int32) (svc *engine.Service, isHistorical bool) {
	key := NewBizKey(bizType, actID).String()
	state := m.periodicHandler.GetState(key)
	if state == nil {
		return m.GetEngineService(bizType, actID), false
	}
	// 同时读本地 state 和 Redis，取较大值保证多节点一致性：
	// advanceRound 先写 Redis 再写 MongoDB，syncFromMongo 基于 MongoDB 更新本地 state，
	// 因此 Redis 值在短期内比本地 state 更新。
	curRound := state.GetCurrentRound()
	if redisCurRound := m.periodicHandler.GetCurRoundFromRedis(key); redisCurRound > curRound {
		curRound = redisCurRound
	}
	effectiveRound := round
	if effectiveRound == 0 {
		effectiveRound = curRound
	}
	if effectiveRound < curRound {
		return nil, true
	}
	// effectiveRound >= curRound：检查当前服务是否可用且未结算。
	// 若服务不存在或已全部结算（活动最后一轮结束后、重启恢复后等场景），
	// 路由到历史 MongoDB 路径以保证数据可靠返回，避免因 Redis 热数据已清理而出错。
	currentSvc := m.GetEngineService(bizType, actID)
	if currentSvc == nil || currentSvc.IsSettled() {
		return nil, true
	}
	return currentSvc, false
}

// 编译期确保 Manager 实现 periodic.ServiceRegistrar。
var _ periodic.ServiceRegistrar = (*Manager)(nil)
