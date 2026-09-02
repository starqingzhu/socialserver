package periodic

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	ReplaceRoundService(ctx context.Context, bizType, logicalKey string, cfg engine.Config) (*engine.Service, error)
	GetEngineServiceByKey(logicalKey string) *engine.Service
}

// PeriodicMeta 存储在 Redis 中的周期活动元数据，用于在 MongoDB 数据不可用时从 Redis 恢复活动。
type PeriodicMeta struct {
	TotalOpenTime  int64
	TotalCloseTime int64
	CycleMinutes   int32
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
// 使用 Redis 分布式锁防止多节点并发注册同一活动。
func (h *Handler) Register(ctx context.Context, bizType, logicalKey string, cfg engine.Config, cycleMinutes int32) error {
	// BUG4: 在入口处校验，防止 CycleMinutes=0 导致无限循环推进轮次
	if cycleMinutes <= 0 {
		return fmt.Errorf("register periodic: cycleMinutes must be > 0, got %d", cycleMinutes)
	}

	// 快速路径（内存检查）
	h.mu.RLock()
	_, alreadyExists := h.states[logicalKey]
	h.mu.RUnlock()
	if alreadyExists {
		return nil
	}

	// 多节点分布式锁：防止两个节点同时注册同一活动
	registerLock := fmt.Sprintf("rank:periodic_register:{%s}", logicalKey)
	locked, lockErr := h.rdb.SetNX(registerLock, "1", 30*time.Second)
	if lockErr != nil || !locked {
		// 其他节点正在注册，syncFromMongo 会在 30s 内同步状态到本节点
		zaplog.LoggerSugar.Infof("rank periodic: register lock not acquired logicalKey=%s, will sync via mongo", logicalKey)
		return nil
	}

	state, err := NewPeriodicState(bizType, cfg.ActID, cycleMinutes, cfg.OpenTime, cfg.CloseTime)
	if err != nil {
		return fmt.Errorf("register periodic: %w", err)
	}

	roundCfg := cfg
	roundCfg.OpenTime = state.RoundOpenTime
	roundCfg.CloseTime = state.RoundCloseTime
	roundCfg.GameEndTime = state.RoundCloseTime
	roundCfg.RoundIndex = state.GetCurrentRound()

	if _, err := h.registry.RegisterRoundService(ctx, bizType, logicalKey, roundCfg); err != nil {
		return fmt.Errorf("register periodic round%d: %w", state.GetCurrentRound(), err)
	}

	// 写锁下二次检查，防止并发写覆盖（BUG5）
	h.mu.Lock()
	if _, alreadyExists := h.states[logicalKey]; alreadyExists {
		h.mu.Unlock()
		return nil
	}
	h.states[logicalKey] = state
	h.mu.Unlock()

	// 初始 currentRound 写入 Redis，供其他节点快速同步
	h.setCurRoundInRedis(logicalKey, state.GetCurrentRound())
	// 周期元数据同步写入 Redis，用于重启后 MongoDB 不可用时的降级恢复
	h.setPeriodicMetaInRedis(logicalKey, state)

	if h.dao != nil {
		overallCfg := cfg
		overallCfg.RoundIndex = 0
		if err := h.dao.SaveRankConfigWithPeriodic(logicalKey, overallCfg, state.ToSavedState()); err != nil {
			zaplog.LoggerSugar.Errorf("rank periodic: save config+state logicalKey=%s: %v", logicalKey, err)
		}
	}
	zaplog.LoggerSugar.Infof("rank periodic: registered bizType=%s actID=%d cycleMinutes=%d round=%d [%d, %d)",
		bizType, cfg.ActID, cycleMinutes, state.GetCurrentRound(), state.RoundOpenTime, state.RoundCloseTime)
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
//
// 修复说明：
//   - BUG6: svc==nil 时提前 abort，避免用零值 Config 注册下一轮。
//   - BUG2: Settle 失败时不调度 CleanupLiveData，保留热数据供下次重试。
//   - BUG1: 先 RegisterRoundService 成功后再调用 advanceToNextRound()，
//     注册失败时 state 保持不变，无脏状态。
func (h *Handler) advanceRound(ctx context.Context, state *PeriodicState, now int64) {
	logicalKey := state.StateLogicalKey()
	currentRound := state.GetCurrentRound()

	advanceLock := fmt.Sprintf("rank:periodic_advance:{%s}:r%d", logicalKey, currentRound)
	locked, err := h.rdb.SetNX(advanceLock, "1", time.Minute)
	if err != nil || !locked {
		return
	}

	svc := h.registry.GetEngineServiceByKey(logicalKey)
	// BUG6: svc==nil 无法结算也无法获取配置，提前退出，不修改 state
	if svc == nil {
		zaplog.LoggerSugar.Errorf("rank periodic: advanceRound svc=nil logicalKey=%s round=%d, skipping",
			logicalKey, currentRound)
		return
	}

	// 结算当前轮
	settleOK := svc.IsSettled()
	if !settleOK {
		if _, err := svc.Settle(ctx); err != nil {
			zaplog.LoggerSugar.Warnf("rank periodic: settle round=%d logicalKey=%s err=%v",
				currentRound, logicalKey, err)
			// BUG2: settle 失败 → 不设 TTL、不删热数据，保留数据供后续重试
		} else {
			settleOK = true
		}
	}

	// BUG2: 只有结算成功才安排清理
	if settleOK {
		h.setSettledTTLForRound(svc, state, currentRound)
		cleanDelay := time.Duration(cycleDurationMs(state.CycleMinutes)) * time.Millisecond
		svcToClean := svc
		time.AfterFunc(cleanDelay, func() {
			zaplog.LoggerSugar.Infof("rank periodic: cleanup live data logicalKey=%s round=%d", logicalKey, currentRound)
			svcToClean.CleanupLiveData()
		})
	}

	if state.isActivityFinished(now) {
		zaplog.LoggerSugar.Infof("rank periodic: activity finished logicalKey=%s", logicalKey)
		return
	}

	// BUG1: 先计算下一轮窗口，不修改 state
	nextRound := currentRound + 1
	nextOpen, nextClose := state.computeRoundWindow(nextRound)
	if nextOpen >= state.TotalCloseTime {
		zaplog.LoggerSugar.Infof("rank periodic: no more rounds logicalKey=%s", logicalKey)
		return
	}

	existCfg := svc.GetConfig()
	nextCfg := existCfg
	nextCfg.OpenTime = nextOpen
	nextCfg.CloseTime = nextClose
	nextCfg.GameEndTime = nextClose
	nextCfg.RoundIndex = nextRound
	nextCfg.CreateTime = 0

	// BUG1: 注册成功后才推进 state，失败时 state 保持在 currentRound
	if _, err := h.registry.ReplaceRoundService(ctx, state.BizType, logicalKey, nextCfg); err != nil {
		zaplog.LoggerSugar.Errorf("rank periodic: register next round=%d logicalKey=%s err=%v",
			nextRound, logicalKey, err)
		return
	}

	state.advanceToNextRound()

	// 将新 currentRound 写入 Redis，使其他节点无需等待 syncFromMongo 即可读到最新轮次
	h.setCurRoundInRedis(logicalKey, nextRound)

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
		nextRound, logicalKey, nextOpen, nextClose)
}

// setSettledTTLForRound 对历史轮次的 Redis 结算快照键设置 TTL（2 个周期）。
// round 参数为刚刚结算完成的轮次号，由调用方在 advanceToNextRound 前传入。
func (h *Handler) setSettledTTLForRound(svc *engine.Service, state *PeriodicState, round int32) {
	if h.rdb == nil {
		return
	}
	ttl := roundSettledTTL(state.CycleMinutes)
	groups := svc.ListGroups()
	rankCode := fmt.Sprintf("%s_score_%d", state.BizType, state.ActID)
	bizId := state.roundBizId(round)
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

	// round=0 表示当前轮，解析为实际轮号。
	// 结算后的当前轮会被路由到历史路径，此时 req.Round 可能仍为 0。
	if round == 0 {
		round = state.GetCurrentRound()
		if redisCurRound := h.readCurRoundFromRedis(logicalKey); redisCurRound > round {
			round = redisCurRound
		}
	}

	bizId := state.roundBizId(round)

	// 优先从 Redis 查询（CleanupLiveData 前 1 个周期内仍有效），
	// 再降级到 MongoDB，避免因异步写入延迟导致历史查询短暂失败。
	store := engine.NewStore(h.rdb, h.dao, bizId)
	groupID, found, err := store.GetMember(userID)
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
	if round == 0 {
		round = state.GetCurrentRound()
		if redisCurRound := h.readCurRoundFromRedis(logicalKey); redisCurRound > round {
			round = redisCurRound
		}
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
	if round == 0 {
		round = state.GetCurrentRound()
		if redisCurRound := h.readCurRoundFromRedis(logicalKey); redisCurRound > round {
			round = redisCurRound
		}
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
	if round == 0 {
		round = state.GetCurrentRound()
		if redisCurRound := h.readCurRoundFromRedis(logicalKey); redisCurRound > round {
			round = redisCurRound
		}
	}
	bizId := state.roundBizId(round)
	isFirst, ct, upsertErr := h.dao.SaveClaimIfNotExists(bizId, userID, time.Now().UnixMilli())
	if upsertErr != nil {
		return false, 0, upsertErr
	}
	return !isFirst, ct, nil
}

// GetRoundInfos 返回周期排行榜的轮次摘要列表（按轮次升序）。
// 仅返回已开始的轮次（1..currentRound），不预先枚举未来轮次。
func (h *Handler) GetRoundInfos(state *PeriodicState) (currentRound int32, rounds []RoundInfo, err error) {
	// BUG3: 原子读取一次，整个函数使用同一快照，避免并发 advance 导致前后不一致
	currentRound = state.GetCurrentRound()

	nowMs := time.Now().UnixMilli()
	rounds = make([]RoundInfo, 0, currentRound)
	for r := int32(1); r <= currentRound; r++ {
		open, close := state.computeRoundWindow(r)
		if open >= state.TotalCloseTime {
			break
		}
		settled := r < currentRound || (r == currentRound && close <= nowMs)
		rounds = append(rounds, RoundInfo{
			Round:     r,
			OpenTime:  open,
			CloseTime: close,
			Settled:   settled,
			Current:   r == currentRound,
		})
	}
	return currentRound, rounds, nil
}

// GetCurrentRoundInfo 直接返回当前轮次信息，不枚举所有轮次。
// 优先从 Redis 读取 currentRound 以保证多节点一致性；Redis 不可用时降级到本地原子值。
func (h *Handler) GetCurrentRoundInfo(state *PeriodicState) (RoundInfo, error) {
	logicalKey := state.StateLogicalKey()

	curRound := h.readCurRoundFromRedis(logicalKey)
	if local := state.GetCurrentRound(); local > curRound {
		curRound = local // 本地值更新时以本地为准（Redis 写入可能短暂滞后）
	}

	open, close := state.computeRoundWindow(curRound)
	nowMs := time.Now().UnixMilli()
	return RoundInfo{
		Round:     curRound,
		OpenTime:  open,
		CloseTime: close,
		Settled:   close <= nowMs,
		Current:   true,
	}, nil
}

// cycleDurationMs 将分钟数转换为毫秒。
func cycleDurationMs(minutes int32) int64 {
	return int64(minutes) * 60 * 1000
}

// roundSettledTTL 历史轮次 Redis 结算快照的保留时长（2 个周期）。
func roundSettledTTL(cycleMinutes int32) time.Duration {
	return time.Duration(cycleMinutes) * 2 * time.Minute
}

// curRoundRedisKey 返回存储周期活动当前轮号的 Redis 键。
// 使用 hash tag {logicalKey} 保证在 Redis Cluster 下与其他该活动的键落在同一 slot。
func curRoundRedisKey(logicalKey string) string {
	return fmt.Sprintf("rank:periodic_cur_round:{%s}", logicalKey)
}

// setCurRoundInRedis 将当前轮号写入 Redis，供多节点同步读取。
func (h *Handler) setCurRoundInRedis(logicalKey string, round int32) {
	if h.rdb == nil {
		return
	}
	if err := h.rdb.Set(curRoundRedisKey(logicalKey), strconv.FormatInt(int64(round), 10)); err != nil {
		zaplog.LoggerSugar.Warnf("rank periodic: set cur_round redis key logicalKey=%s: %v", logicalKey, err)
	}
}

// readCurRoundFromRedis 从 Redis 读取当前轮号；失败或不存在时返回 0。
func (h *Handler) readCurRoundFromRedis(logicalKey string) int32 {
	if h.rdb == nil {
		return 0
	}
	val, err := h.rdb.Get(curRoundRedisKey(logicalKey))
	if err != nil || val == "" {
		return 0
	}
	n, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return 0
	}
	return int32(n)
}

// CleanupHistoricalRounds 在启动恢复时清理所有已结算轮次（< CurrentRound）的 Redis 热数据。
// 幂等操作，可重复执行。
func (h *Handler) CleanupHistoricalRounds(state *PeriodicState) {
	curRound := state.GetCurrentRound()
	for r := int32(1); r < curRound; r++ {
		bizId := state.roundBizId(r)
		store := engine.NewStore(h.rdb, h.dao, bizId)
		store.CleanupLiveData(nil)
	}
}

// periodicMetaRedisKey 返回存储周期活动元数据的 Redis 键。
func periodicMetaRedisKey(logicalKey string) string {
	return fmt.Sprintf("rank:periodic_meta:{%s}", logicalKey)
}

// GetCurRoundFromRedis 导出 readCurRoundFromRedis，供 manager 在恢复时调用。
func (h *Handler) GetCurRoundFromRedis(logicalKey string) int32 {
	return h.readCurRoundFromRedis(logicalKey)
}

// setPeriodicMetaInRedis 将周期活动元数据同步写入 Redis。
// 格式："TotalOpenTime,TotalCloseTime,CycleMinutes"，供重启后 MongoDB 不可用时降级恢复。
func (h *Handler) setPeriodicMetaInRedis(logicalKey string, state *PeriodicState) {
	if h.rdb == nil {
		return
	}
	val := fmt.Sprintf("%d,%d,%d", state.TotalOpenTime, state.TotalCloseTime, state.CycleMinutes)
	if err := h.rdb.Set(periodicMetaRedisKey(logicalKey), val); err != nil {
		zaplog.LoggerSugar.Warnf("rank periodic: set periodic_meta redis key logicalKey=%s: %v", logicalKey, err)
	}
}

// GetPeriodicMetaFromRedis 从 Redis 读取周期活动元数据；不存在或解析失败时返回 (PeriodicMeta{}, false)。
func (h *Handler) GetPeriodicMetaFromRedis(logicalKey string) (PeriodicMeta, bool) {
	if h.rdb == nil {
		return PeriodicMeta{}, false
	}
	val, err := h.rdb.Get(periodicMetaRedisKey(logicalKey))
	if err != nil || val == "" {
		return PeriodicMeta{}, false
	}
	parts := strings.Split(val, ",")
	if len(parts) != 3 {
		return PeriodicMeta{}, false
	}
	totalOpen, err1 := strconv.ParseInt(parts[0], 10, 64)
	totalClose, err2 := strconv.ParseInt(parts[1], 10, 64)
	cycleMin, err3 := strconv.ParseInt(parts[2], 10, 32)
	if err1 != nil || err2 != nil || err3 != nil {
		return PeriodicMeta{}, false
	}
	return PeriodicMeta{
		TotalOpenTime:  totalOpen,
		TotalCloseTime: totalClose,
		CycleMinutes:   int32(cycleMin),
	}, true
}
