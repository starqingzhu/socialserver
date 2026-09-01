package periodic

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	commonrank "common/rank"
	goredis "golib/redis"
	"golib/zaplog"
	"socialserver/internal/rank/engine"
)

// ServiceRegistrar 由 Manager 实现并注入给 Handler，用于注册新轮次子服务。
// 使用 string 而非 rankservice.BizType，避免循环引用。
type ServiceRegistrar interface {
	RegisterRoundService(ctx context.Context, bizType, logicalKey string, cfg engine.Config) (*engine.Service, error)
	GetEngineServiceByKey(logicalKey string) *engine.Service
}

// Handler 封装周期排行榜的所有运行时逻辑，由 Manager 通过组合持有。
type Handler struct {
	mu        sync.RWMutex
	states    map[string]*PeriodicState
	rdb       *goredis.Redis
	dao       *engine.DAO
	registry  ServiceRegistrar
	warmupSem chan struct{} // 限制并发 WarmUp 数量
}

// NewHandler 创建 Handler。registry 由 Manager 实现并传入。
func NewHandler(rdb *goredis.Redis, dao *engine.DAO, registry ServiceRegistrar) *Handler {
	return &Handler{
		states:    make(map[string]*PeriodicState),
		rdb:       rdb,
		dao:       dao,
		registry:  registry,
		warmupSem: make(chan struct{}, 8),
	}
}

// GetState 线程安全地读取 PeriodicState。
func (h *Handler) GetState(logicalKey string) *PeriodicState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.states[logicalKey]
}

// SetState 线程安全地写入 PeriodicState。
func (h *Handler) SetState(logicalKey string, s *PeriodicState) {
	h.mu.Lock()
	h.states[logicalKey] = s
	h.mu.Unlock()
}

// RemoveState 线程安全地删除 PeriodicState。
func (h *Handler) RemoveState(logicalKey string) {
	h.mu.Lock()
	delete(h.states, logicalKey)
	h.mu.Unlock()
}

// Clear 清空所有状态（用于关闭时清理）。
func (h *Handler) Clear() {
	h.mu.Lock()
	h.states = make(map[string]*PeriodicState)
	h.mu.Unlock()
}

// Register 注册周期排行榜活动：创建第一轮子服务并持久化状态。
func (h *Handler) Register(ctx context.Context, bizType, logicalKey string, cfg engine.Config, cycleMinutes int32) error {
	h.mu.RLock()
	_, alreadyExists := h.states[logicalKey]
	h.mu.RUnlock()
	if alreadyExists {
		return nil
	}

	state := NewPeriodicState(bizType, cfg.ActID, cycleMinutes, cfg.OpenTime, cfg.CloseTime)

	roundCfg := cfg
	roundCfg.OpenTime = state.RoundOpenTime
	roundCfg.CloseTime = state.RoundCloseTime
	roundCfg.GameEndTime = state.RoundCloseTime
	roundCfg.RoundIndex = state.CurrentRound

	if _, err := h.registry.RegisterRoundService(ctx, bizType, logicalKey, roundCfg); err != nil {
		return fmt.Errorf("register periodic round1: %w", err)
	}

	h.mu.Lock()
	h.states[logicalKey] = state
	h.mu.Unlock()

	if h.dao != nil {
		overallCfg := cfg
		overallCfg.RoundIndex = 0
		if err := h.dao.SaveRankConfigWithPeriodic(logicalKey, overallCfg, state.ToSavedState()); err != nil {
			zaplog.LoggerSugar.Errorf("rank periodic: save config+state logicalKey=%s: %v", logicalKey, err)
		}
	}
	zaplog.LoggerSugar.Infof("rank periodic: registered bizType=%s actID=%d cycleMinutes=%d round=1 [%d, %d)",
		bizType, cfg.ActID, cycleMinutes, state.RoundOpenTime, state.RoundCloseTime)
	return nil
}

// TickAll 在每次 tick 时检查所有周期活动，必要时推进轮次。
func (h *Handler) TickAll(ctx context.Context, now int64) {
	h.mu.RLock()
	states := make([]*PeriodicState, 0, len(h.states))
	for _, s := range h.states {
		states = append(states, s)
	}
	h.mu.RUnlock()

	for _, state := range states {
		if !state.isRoundExpired(now) {
			continue
		}
		h.advanceRound(ctx, state, now)
	}
}

// advanceRound 结算当前轮，若活动未结束则注册下一轮。
// 使用 Redis SETNX 保证多节点只有一个节点执行推进。
func (h *Handler) advanceRound(ctx context.Context, state *PeriodicState, now int64) {
	logicalKey := state.StateLogicalKey()
	advanceLock := fmt.Sprintf("rank:periodic_advance:{%s}:r%d", logicalKey, state.CurrentRound)

	locked, err := h.rdb.SetNX(advanceLock, "1", time.Minute)
	if err != nil || !locked {
		return
	}

	svc := h.registry.GetEngineServiceByKey(logicalKey)
	if svc != nil && !svc.IsSettled() {
		if _, err := svc.Settle(ctx); err != nil {
			zaplog.LoggerSugar.Warnf("rank periodic: settle round=%d logicalKey=%s err=%v",
				state.CurrentRound, logicalKey, err)
		}
	}

	if svc != nil {
		h.setSettledTTLForRound(svc, state)
	}

	settledRound := state.CurrentRound
	cleanDelay := time.Duration(cycleDurationMs(state.CycleMinutes)) * time.Millisecond
	if svc != nil {
		svcToClean := svc
		time.AfterFunc(cleanDelay, func() {
			zaplog.LoggerSugar.Infof("rank periodic: cleanup live data logicalKey=%s round=%d", logicalKey, settledRound)
			svcToClean.CleanupLiveData()
		})
	}

	if state.isActivityFinished(now) {
		zaplog.LoggerSugar.Infof("rank periodic: activity finished logicalKey=%s", logicalKey)
		return
	}

	if !state.advanceToNextRound() {
		zaplog.LoggerSugar.Infof("rank periodic: no more rounds logicalKey=%s", logicalKey)
		return
	}

	existCfg := engine.Config{}
	if svc != nil {
		existCfg = svc.GetConfig()
	}

	nextCfg := existCfg
	nextCfg.OpenTime = state.RoundOpenTime
	nextCfg.CloseTime = state.RoundCloseTime
	nextCfg.GameEndTime = state.RoundCloseTime
	nextCfg.RoundIndex = state.CurrentRound
	nextCfg.CreateTime = 0

	if _, err := h.registry.RegisterRoundService(ctx, state.BizType, logicalKey, nextCfg); err != nil {
		zaplog.LoggerSugar.Errorf("rank periodic: register next round=%d logicalKey=%s err=%v",
			state.CurrentRound, logicalKey, err)
		return
	}

	if newSvc := h.registry.GetEngineServiceByKey(logicalKey); newSvc != nil {
		h.warmupSem <- struct{}{}
		go func(svc *engine.Service) {
			defer func() { <-h.warmupSem }()
			svc.WarmUp(ctx)
		}(newSvc)
	}

	if h.dao != nil {
		if err := h.dao.SavePeriodicState(logicalKey, state.ToSavedState()); err != nil {
			zaplog.LoggerSugar.Errorf("rank periodic: save state logicalKey=%s: %v", logicalKey, err)
		}
	}

	zaplog.LoggerSugar.Infof("rank periodic: advanced to round=%d logicalKey=%s [%d, %d)",
		state.CurrentRound, logicalKey, state.RoundOpenTime, state.RoundCloseTime)
}

// setSettledTTLForRound 对历史轮次的 Redis 结算快照键设置 TTL（2 个周期）。
func (h *Handler) setSettledTTLForRound(svc *engine.Service, state *PeriodicState) {
	if h.rdb == nil {
		return
	}
	ttl := roundSettledTTL(state.CycleMinutes)
	groups := svc.ListGroups()
	rankCode := fmt.Sprintf("%s_score_%d", state.BizType, state.ActID)
	bizId := state.roundBizId(state.CurrentRound)
	for _, g := range groups {
		instanceID := commonrank.NewInstanceID(rankCode, bizId, fmt.Sprintf("group_%d", g.GroupID))
		settledKey := fmt.Sprintf("rank:settled:{%s}", instanceID)
		if _, err := h.rdb.Expire(settledKey, ttl); err != nil {
			zaplog.LoggerSugar.Warnf("rank periodic: set TTL key=%s: %v", settledKey, err)
		}
	}
}

// GetHistoricalRoundList 查询历史轮次的排行榜（从 MongoDB/Redis 读取已结算数据）。
func (h *Handler) GetHistoricalRoundList(ctx context.Context, bizType string, actID int32, round int32, userID int64, start, end int64) ([]commonrank.RankMemberSnapshot, *commonrank.RankMemberSnapshot, error) {
	if h.dao == nil {
		return nil, nil, fmt.Errorf("dao not available")
	}
	logicalKey := LogicalKey(bizType, actID)
	state := h.GetState(logicalKey)
	if state == nil {
		return nil, nil, fmt.Errorf("not a periodic activity: bizType=%s actID=%d", bizType, actID)
	}

	bizId := state.roundBizId(round)

	groupID, found, err := h.dao.GetMember(bizId, userID)
	if err != nil {
		return nil, nil, fmt.Errorf("get member: %w", err)
	}
	if !found {
		return nil, nil, nil
	}

	snapshots, err := h.dao.LoadGroupSettled(bizId, groupID)
	if err != nil {
		return nil, nil, fmt.Errorf("load settled: %w", err)
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Rank < snapshots[j].Rank
	})

	var mySnap *commonrank.RankMemberSnapshot
	for i := range snapshots {
		if snapshots[i].MemberId == userID {
			cp := snapshots[i]
			mySnap = &cp
			break
		}
	}

	if start < 0 {
		start = 0
	}
	total := int64(len(snapshots))
	if end < 0 || end >= total {
		end = total - 1
	}
	if start > end {
		return nil, mySnap, nil
	}
	return snapshots[start : end+1], mySnap, nil
}

// GetHistoricalRewardUsers 查询历史轮次的入榜真实玩家 ID 列表。
func (h *Handler) GetHistoricalRewardUsers(ctx context.Context, bizType string, actID int32, round int32) ([]int64, error) {
	if h.dao == nil {
		return nil, fmt.Errorf("dao not available")
	}
	logicalKey := LogicalKey(bizType, actID)
	state := h.GetState(logicalKey)
	if state == nil {
		return nil, fmt.Errorf("not a periodic activity: bizType=%s actID=%d", bizType, actID)
	}
	bizId := state.roundBizId(round)
	members, err := h.dao.LoadAllMembers(bizId)
	if err != nil {
		return nil, err
	}
	result := make([]int64, 0, len(members))
	for uid := range members {
		if uid > 0 {
			result = append(result, uid)
		}
	}
	return result, nil
}

// GetHistoricalClaimStatus 查询历史轮次的奖励领取状态（从 MongoDB 读取）。
func (h *Handler) GetHistoricalClaimStatus(bizType string, actID int32, round int32, userID int64) (claimed bool, claimTime int64, err error) {
	if h.dao == nil {
		return false, 0, fmt.Errorf("dao not available")
	}
	logicalKey := LogicalKey(bizType, actID)
	state := h.GetState(logicalKey)
	if state == nil {
		return false, 0, fmt.Errorf("not a periodic activity: bizType=%s actID=%d", bizType, actID)
	}
	bizId := state.roundBizId(round)
	ct, found, err := h.dao.GetClaim(bizId, userID)
	if err != nil {
		return false, 0, err
	}
	return found, ct, nil
}

// ClaimHistoricalReward 为历史轮次记录奖励领取（写入 MongoDB）。
func (h *Handler) ClaimHistoricalReward(bizType string, actID int32, round int32, userID int64) (claimed bool, claimTime int64, err error) {
	if h.dao == nil {
		return false, 0, fmt.Errorf("dao not available")
	}
	logicalKey := LogicalKey(bizType, actID)
	state := h.GetState(logicalKey)
	if state == nil {
		return false, 0, fmt.Errorf("not a periodic activity: bizType=%s actID=%d", bizType, actID)
	}
	bizId := state.roundBizId(round)
	isFirst, ct, upsertErr := h.dao.SaveClaimIfNotExists(bizId, userID, time.Now().UnixMilli())
	if upsertErr != nil {
		return false, 0, upsertErr
	}
	return !isFirst, ct, nil
}

// GetRoundInfos 返回周期排行榜的轮次摘要列表（按轮次升序）。
// 仅处理周期排行榜；一次性排行榜由 Manager 直接处理。
func (h *Handler) GetRoundInfos(state *PeriodicState) (currentRound int32, rounds []RoundInfo, err error) {
	currentRound = state.CurrentRound
	totalRounds := int32(1)
	if state.CycleMinutes > 0 && state.TotalCloseTime > state.TotalOpenTime {
		dur := state.TotalCloseTime - state.TotalOpenTime
		totalRounds = int32((dur + cycleDurationMs(state.CycleMinutes) - 1) / cycleDurationMs(state.CycleMinutes))
	}

	rounds = make([]RoundInfo, 0, totalRounds)
	for r := int32(1); r <= totalRounds; r++ {
		open, close := state.computeRoundWindow(r)
		if open >= state.TotalCloseTime {
			break
		}
		rounds = append(rounds, RoundInfo{
			Round:     r,
			OpenTime:  open,
			CloseTime: close,
			Settled:   r < state.CurrentRound,
			Current:   r == state.CurrentRound,
		})
	}
	return currentRound, rounds, nil
}

// cycleDurationMs 将分钟数转换为毫秒。
func cycleDurationMs(minutes int32) int64 {
	return int64(minutes) * 60 * 1000
}

// roundSettledTTL 历史轮次 Redis 结算快照的保留时长（2 个周期）。
func roundSettledTTL(cycleMinutes int32) time.Duration {
	return time.Duration(cycleMinutes) * 2 * time.Minute
}

// CleanupHistoricalRounds 在启动恢复时清理所有已结算轮次（< CurrentRound）的 Redis 热数据。
// 幂等操作，可重复执行。
func (h *Handler) CleanupHistoricalRounds(state *PeriodicState) {
	for r := int32(1); r < state.CurrentRound; r++ {
		bizId := state.roundBizId(r)
		store := engine.NewStore(h.rdb, h.dao, bizId)
		store.CleanupLiveData(nil)
	}
}
