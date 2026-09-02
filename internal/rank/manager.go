package rankservice

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
	goredis "golib/redis"
	"golib/zaplog"
	"socialserver/internal/rank/engine"
	"socialserver/internal/rank/once"
	"socialserver/internal/rank/periodic"
)

const (
	syncInterval = 30 * time.Second
	tickInterval = time.Second
)

type Manager struct {
	mu             sync.RWMutex
	rdb            *goredis.Redis
	dao            *engine.DAO
	rankService    commonrank.Service
	memberIndex    *MemberIndex
	services       map[string]RankBizService
	engineServices map[string]*engine.Service
	periodicHandler *periodic.Handler
	stopCh         chan struct{}
}

var globalManager *Manager

func InitGlobalManager(rdb *goredis.Redis, dbName string) error {
	if rdb == nil {
		return fmt.Errorf("rank: redis client is required")
	}
	dao := engine.NewDAO(dbName)
	manager := &Manager{
		rdb:            rdb,
		dao:            dao,
		rankService:    commonrank.NewRedisService(rdb),
		memberIndex:    NewMemberIndex(rdb),
		services:       make(map[string]RankBizService),
		engineServices: make(map[string]*engine.Service),
		stopCh:         make(chan struct{}),
	}
	manager.periodicHandler = periodic.NewHandler(rdb, dao, manager)
	if dao != nil {
		dao.EnsureIndexes()
		manager.syncFromMongo(context.Background())
	}
	manager.syncFromRedis(context.Background())
	manager.warmUpAllServices(context.Background())
	globalManager = manager
	zaplog.LoggerSugar.Infof("rank global manager initialized")
	manager.startBackground()
	return nil
}

func GetGlobalManager() *Manager {
	return globalManager
}

func (m *Manager) Close() {
	if m == nil {
		return
	}
	close(m.stopCh)
	m.periodicHandler.Clear()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.services = make(map[string]RankBizService)
	m.engineServices = make(map[string]*engine.Service)
	zaplog.LoggerSugar.Infof("rank global manager closed")
}

func (m *Manager) startBackground() {
	go m.tickLoop()
	go m.syncLoop()
	go m.subscribeDeleteEvents()
	go m.subscribeCreateEvents()
}

func (m *Manager) tickLoop() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case now := <-ticker.C:
			m.tickServices(context.Background(), now.UnixMilli())
		}
	}
}

func (m *Manager) syncLoop() {
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			ctx := context.Background()
			m.syncFromMongo(ctx)
			m.syncFromRedis(ctx)
			m.warmUpAllServices(ctx)
		}
	}
}

func (m *Manager) tickServices(ctx context.Context, now int64) {
	m.mu.RLock()
	svcs := make([]RankBizService, 0, len(m.services))
	for _, svc := range m.services {
		svcs = append(svcs, svc)
	}
	m.mu.RUnlock()
	for _, svc := range svcs {
		if err := svc.Tick(ctx, now); err != nil {
			zaplog.LoggerSugar.Warnf("rank: tick service error: %v", err)
		}
	}
	m.tickPeriodicActivities(ctx, now)
}

// newBizServiceWrapper 为周期排行榜创建业务服务适配器。
// 所有周期排行榜类型（balloon、egg、camper_competition 等）共用同一个通用实现。
func newBizServiceWrapper(bizType BizType, svc *engine.Service) RankBizService {
	return once.NewBizService(string(bizType), svc)
}

func (m *Manager) registerEngine(ctx context.Context, bizType BizType, cfg engine.Config) (*engine.Service, error) {
	if cfg.RankCode == "" {
		cfg.RankCode = fmt.Sprintf("%s_score_%d", bizType, cfg.ActID)
	}

	key := NewBizKey(bizType, cfg.ActID).String()

	m.mu.Lock()
	defer m.mu.Unlock()
	if svc, ok := m.engineServices[key]; ok {
		return svc, nil
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
	m.services[key] = newBizServiceWrapper(bizType, service)
	m.engineServices[key] = service

	if m.dao != nil {
		if err := m.dao.SaveRankConfig(key, cfg); err != nil {
			zaplog.LoggerSugar.Errorf("rank: persist rank config %s to mongodb failed: %v", key, err)
		}
	}

	return service, nil
}

// GetService 返回指定业务类型和活动ID的通用服务接口。
func (m *Manager) GetService(bizType BizType, actID int32) RankBizService {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.services[NewBizKey(bizType, actID).String()]
}

// GetEngineService 返回底层引擎服务，供 handler 直接调用排行榜操作。
func (m *Manager) GetEngineService(bizType BizType, actID int32) *engine.Service {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.engineServices[NewBizKey(bizType, actID).String()]
}

// Register 按业务类型注册排行榜服务，自动从配置文件加载参数。
func (m *Manager) Register(ctx context.Context, bizType BizType, cfg engine.Config) error {
	if err := ValidateBizType(bizType); err != nil {
		return err
	}
	cfg.BizType = string(bizType)
	if err := FillConfigFromFiles(bizType, &cfg); err != nil {
		return err
	}

	rankType, cycleMinutes, err := LoadRankTypeAndCycle(bizType)
	if err != nil {
		return err
	}

	key := NewBizKey(bizType, cfg.ActID).String()

	if rankType == RankTypePeriodic {
		if err := m.periodicHandler.Register(ctx, string(bizType), key, cfg, cycleMinutes); err != nil {
			return err
		}
		var ps *engine.PeriodicSavedState
		if state := m.periodicHandler.GetState(key); state != nil {
			saved := state.ToSavedState()
			ps = &saved
		}
		m.publishRankCreate(key, cfg, ps)
		return nil
	}

	_, err = m.registerEngine(ctx, bizType, cfg)
	if err != nil {
		return err
	}
	m.publishRankCreate(key, cfg, nil)
	return nil
}

// UpdateService 按业务类型更新排行榜配置。
func (m *Manager) UpdateService(bizType BizType, actID int32, cfg engine.Config) error {
	if m == nil {
		return fmt.Errorf("rank manager is nil")
	}
	key := NewBizKey(bizType, actID).String()
	m.mu.RLock()
	svc, ok := m.engineServices[key]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("service not found: %s", key)
	}

	svc.UpdateConfig(cfg)
	updated := svc.GetConfig()

	if m.dao != nil {
		if err := m.dao.SaveRankConfig(key, updated); err != nil {
			zaplog.LoggerSugar.Errorf("rank: persist rank config %s to mongodb failed: %v", key, err)
		}
	}
	return nil
}

// RemoveService 按业务类型移除排行榜服务并清理所有相关持久化数据。
func (m *Manager) RemoveService(bizType BizType, actID int32) error {
	if m == nil {
		return fmt.Errorf("rank manager is nil")
	}
	key := NewBizKey(bizType, actID).String()

	m.mu.Lock()
	svc, ok := m.engineServices[key]
	if ok {
		delete(m.services, key)
		delete(m.engineServices, key)
		m.memberIndex.RemoveByKey(key)
	}
	m.mu.Unlock()

	m.periodicHandler.RemoveState(key)

	if ok {
		if members, err := svc.GetAllMembers(); err == nil && len(members) > 0 {
			m.memberIndex.RemoveUserEntries(bizType, actID, members)
		}
		svc.Cleanup()
	} else {
		m.forceCleanupOrphan(context.Background(), bizType, actID)
	}

	if m.dao != nil {
		m.dao.DeleteRankConfig(key)
	}

	if m.rdb != nil {
		if _, err := m.rdb.Publish(rediskeys.RankDeleteChannel, key); err != nil {
			zaplog.LoggerSugar.Warnf("rank: publish delete event bizKey=%s: %v", key, err)
		}
	}
	return nil
}

// forceCleanupOrphan 清理不在内存中的排行榜服务的所有持久化数据（Redis + MongoDB）。
func (m *Manager) forceCleanupOrphan(ctx context.Context, bizType BizType, actID int32) {
	bizId := fmt.Sprintf("%s_%d", bizType, actID)
	rankCode := fmt.Sprintf("%s_score_%d", bizType, actID)

	store := engine.NewStore(m.rdb, m.dao, bizId)

	if members, err := store.GetAllMembers(); err == nil && len(members) > 0 {
		m.memberIndex.RemoveUserEntries(bizType, actID, members)
	}

	groups, _ := store.LoadGroups()
	store.CleanupAll(groups)

	for _, g := range groups {
		if g == nil {
			continue
		}
		instanceID := commonrank.NewInstanceID(rankCode, bizId, fmt.Sprintf("group_%d", g.GroupID))
		if err := m.rankService.DeleteInstance(ctx, instanceID); err != nil {
			zaplog.LoggerSugar.Warnf("rank: forceCleanup delete rank instance %s: %v", instanceID, err)
		}
	}
	if err := m.rankService.DeleteRankDef(ctx, rankCode); err != nil {
		zaplog.LoggerSugar.Warnf("rank: forceCleanup delete rank def %s: %v", rankCode, err)
	}
}

// ServiceInfo 包含排行榜服务的摘要信息。
type ServiceInfo struct {
	BizType     BizType
	ActID       int32
	Config      engine.Config
	Settled     bool
	GroupCount  int32
	MemberCount int32
	CreateTime  int64
}

// ListServices 返回已注册的排行榜服务摘要，可按 bizType 过滤（空=全部）。
func (m *Manager) ListServices(filterBizType BizType) []ServiceInfo {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]ServiceInfo, 0, len(m.engineServices))
	for _, svc := range m.engineServices {
		cfg := svc.GetConfig()
		bizType := BizType(cfg.BizType)
		if filterBizType != "" && bizType != filterBizType {
			continue
		}
		result = append(result, ServiceInfo{
			BizType:     bizType,
			ActID:       cfg.ActID,
			Config:      cfg,
			Settled:     svc.IsSettled(),
			GroupCount:  svc.GroupCount(),
			MemberCount: svc.MemberCount(),
			CreateTime:  cfg.CreateTime,
		})
	}
	return result
}

// GetMemberEntries 返回用户参与的所有排行榜记录。
func (m *Manager) GetMemberEntries(userID int64) []MemberEntry {
	if m == nil {
		return nil
	}
	return m.memberIndex.Lookup(userID)
}

// GetMemberRankEntries 返回用户在所有排行榜中的名次快照（GM 查询用）。
func (m *Manager) GetMemberRankEntries(ctx context.Context, userID int64) ([]MemberRankEntry, error) {
	if m == nil {
		return nil, nil
	}
	entries := m.memberIndex.Lookup(userID)
	if len(entries) == 0 {
		return nil, nil
	}

	result := make([]MemberRankEntry, 0, len(entries))
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, entry := range entries {
		key := NewBizKey(entry.BizType, entry.ActID).String()
		svc, ok := m.services[key]
		if !ok {
			result = append(result, MemberRankEntry{MemberEntry: entry})
			continue
		}
		snapshot, _, err := svc.GetMemberRank(ctx, userID)
		if err != nil {
			return nil, fmt.Errorf("get rank for %s: %w", key, err)
		}
		result = append(result, MemberRankEntry{
			MemberEntry: entry,
			Snapshot:    snapshot,
		})
	}
	return result, nil
}

func (m *Manager) Tick(ctx context.Context, now int64) error {
	if m == nil {
		return nil
	}
	m.tickServices(ctx, now)
	return nil
}

// syncFromRedis 通过扫描 Redis 运行时数据（rank:meta:*）发现已有活动并补充注册。
func (m *Manager) syncFromRedis(ctx context.Context) {
	if m.rdb == nil {
		return
	}
	prefix := rediskeys.RankMetaKeyPrefix + ":{"
	keys, err := m.rdb.Keys(prefix + "*}")
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: syncFromRedis scan keys: %v", err)
		return
	}
	if len(keys) == 0 {
		return
	}

	now := time.Now().UnixMilli()
	added := 0
	for _, key := range keys {
		bizId := strings.TrimPrefix(key, prefix)
		bizId = strings.TrimSuffix(bizId, "}")
		if bizId == "" {
			continue
		}

		sep := strings.LastIndex(bizId, "_")
		if sep <= 0 {
			continue
		}

		// 周期排行榜子轮次 BizId（格式 "{bizType}_{actID}_r{N}"）：
		// 尝试从 Redis 元数据降级恢复（主路径是 syncFromMongo），然后跳过常规注册流程。
		if periodic.IsRoundBizId(bizId) {
			m.tryRecoverPeriodicFromRedis(ctx, bizId)
			continue
		}

		actIDInt, err := strconv.ParseInt(bizId[sep+1:], 10, 32)
		if err != nil {
			continue
		}
		bizType := BizType(bizId[:sep])
		actID := int32(actIDInt)

		bizKey := NewBizKey(bizType, actID).String()
		m.mu.RLock()
		_, exists := m.engineServices[bizKey]
		m.mu.RUnlock()
		if exists {
			continue
		}

		tmpStore := engine.NewStore(m.rdb, nil, bizId)
		openTime, closeTime, gameEndTime, ok := tmpStore.LoadActivityTimes()
		if !ok {
			continue
		}

		if closeTime > 0 && now > closeTime+7*86400000 {
			continue
		}

		rankCode := fmt.Sprintf("%s_score_%d", bizType, actID)
		cfg := engine.Config{
			BizType:     string(bizType),
			ActID:       actID,
			RankCode:    rankCode,
			OpenTime:    openTime,
			CloseTime:   closeTime,
			GameEndTime: gameEndTime,
		}

		if err := FillConfigFromFiles(bizType, &cfg); err != nil {
			zaplog.LoggerSugar.Warnf("rank: syncFromRedis fill config %s: %v", bizKey, err)
		}

		if err := m.rankService.RegisterRank(ctx, commonrank.Rank{
			RankCode:       cfg.RankCode,
			RankName:       fmt.Sprintf("%s_rank_%d", bizType, actID),
			ScoreOrder:     commonrank.ScoreOrderDesc,
			TieBreakPolicy: commonrank.TieBreakPolicyFirstEnter,
			CreateTime:     cfg.OpenTime,
			UpdateTime:     cfg.OpenTime,
		}); err != nil {
			zaplog.LoggerSugar.Warnf("rank: syncFromRedis register rank def %s: %v", cfg.RankCode, err)
			continue
		}

		localBizType := bizType
		localActID := actID
		onMemberJoin := func(userID int64, groupID int32) {
			m.memberIndex.Track(userID, MemberEntry{
				BizType: localBizType,
				ActID:   localActID,
				GroupID: groupID,
			})
		}

		service, err := engine.NewService(m.rankService, cfg, m.rdb, m.dao, engine.WithOnMemberJoin(onMemberJoin))
		if err != nil {
			zaplog.LoggerSugar.Warnf("rank: syncFromRedis create service %s: %v", bizKey, err)
			continue
		}

		m.mu.Lock()
		if _, dup := m.engineServices[bizKey]; !dup {
			m.services[bizKey] = newBizServiceWrapper(localBizType, service)
			m.engineServices[bizKey] = service
			added++
		}
		m.mu.Unlock()
	}

	if added > 0 {
		zaplog.LoggerSugar.Infof("rank: syncFromRedis completed, added=%d", added)
	}
}

// tryRecoverPeriodicFromRedis 在 syncFromRedis 时遇到周期子轮次 BizId（格式 "{bizType}_{actID}_r{N}"），
// 尝试用 Redis 中的元数据恢复该活动——仅在 syncFromMongo 未能恢复时生效（即 MongoDB 写入在 SIGKILL 前未落盘）。
// 若 Redis 已被清空则静默跳过，依赖 syncFromMongo 从 MongoDB 恢复。
func (m *Manager) tryRecoverPeriodicFromRedis(ctx context.Context, roundBizId string) {
	base, ok := periodic.ExtractRoundBizBase(roundBizId)
	if !ok {
		return
	}
	sep := strings.LastIndex(base, "_")
	if sep <= 0 {
		return
	}
	actIDInt, err := strconv.ParseInt(base[sep+1:], 10, 32)
	if err != nil {
		return
	}
	bizType := BizType(base[:sep])
	actID := int32(actIDInt)
	logicalKey := NewBizKey(bizType, actID).String()

	m.mu.RLock()
	_, exists := m.engineServices[logicalKey]
	m.mu.RUnlock()
	if exists {
		return // syncFromMongo 已恢复，无需降级
	}

	meta, ok := m.periodicHandler.GetPeriodicMetaFromRedis(logicalKey)
	if !ok {
		return // Redis 元数据不存在（Redis 被清空）→ 依赖 syncFromMongo
	}

	curRound := m.periodicHandler.GetCurRoundFromRedis(logicalKey)
	if curRound <= 0 {
		curRound = 1
	}

	cycleDurMs := int64(meta.CycleMinutes) * 60 * 1000
	roundOpen := meta.TotalOpenTime + int64(curRound-1)*cycleDurMs
	roundClose := roundOpen + cycleDurMs
	if roundClose > meta.TotalCloseTime {
		roundClose = meta.TotalCloseTime
	}

	saved := engine.PeriodicSavedState{
		CycleMinutes:   meta.CycleMinutes,
		TotalOpenTime:  meta.TotalOpenTime,
		TotalCloseTime: meta.TotalCloseTime,
		CurrentRound:   curRound,
		RoundOpenTime:  roundOpen,
		RoundCloseTime: roundClose,
	}
	ps := periodic.StateFromSaved(string(bizType), actID, saved)
	m.periodicHandler.SetState(logicalKey, ps)
	m.periodicHandler.CleanupHistoricalRounds(ps)

	cfg := engine.Config{
		BizType:     string(bizType),
		ActID:       actID,
		OpenTime:    roundOpen,
		CloseTime:   roundClose,
		GameEndTime: roundClose,
		RoundIndex:  curRound,
	}
	if err := FillConfigFromFiles(bizType, &cfg); err != nil {
		zaplog.LoggerSugar.Warnf("rank: tryRecoverPeriodicFromRedis fill config %s: %v", logicalKey, err)
	}
	if _, err := m.RegisterRoundService(ctx, string(bizType), logicalKey, cfg); err != nil {
		zaplog.LoggerSugar.Warnf("rank: tryRecoverPeriodicFromRedis register %s round=%d: %v", logicalKey, curRound, err)
		return
	}
	zaplog.LoggerSugar.Infof("rank: tryRecoverPeriodicFromRedis recovered bizType=%s actID=%d curRound=%d", bizType, actID, curRound)
}

// warmUpAllServices 并发调用所有已注册服务的 WarmUp。
func (m *Manager) warmUpAllServices(ctx context.Context) {
	m.mu.RLock()
	svcs := make([]*engine.Service, 0, len(m.engineServices))
	for _, svc := range m.engineServices {
		svcs = append(svcs, svc)
	}
	m.mu.RUnlock()
	if len(svcs) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, svc := range svcs {
		wg.Add(1)
		go func(s *engine.Service) {
			defer wg.Done()
			s.WarmUp(ctx)
		}(svc)
	}
	wg.Wait()
	zaplog.LoggerSugar.Infof("rank: warmUpAllServices completed, services=%d", len(svcs))
}

// subscribeDeleteEvents 订阅排行榜删除广播，收到消息后立即在本节点移除对应 Service。
// 连接断开后自动重连，与 subscribeCreateEvents 保持一致。
func (m *Manager) subscribeDeleteEvents() {
	if m.rdb == nil {
		return
	}
	for {
		select {
		case <-m.stopCh:
			return
		default:
		}

		ps, err := m.rdb.Subscribe(rediskeys.RankDeleteChannel)
		if err != nil {
			zaplog.LoggerSugar.Errorf("rank: subscribe delete channel: %v", err)
			select {
			case <-m.stopCh:
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}

		ch := ps.Channel()
	loop:
		for {
			select {
			case <-m.stopCh:
				ps.Close()
				return
			case msg, ok := <-ch:
				if !ok {
					zaplog.LoggerSugar.Warnf("rank: delete channel closed, reconnecting")
					break loop
				}
				bizKey := msg.Payload
				if bizKey == "" {
					continue
				}
				sep := strings.LastIndex(bizKey, ":")
				if sep <= 0 {
					continue
				}
				bizType := BizType(bizKey[:sep])
				actIDInt, err := strconv.ParseInt(bizKey[sep+1:], 10, 32)
				if err != nil {
					continue
				}
				actID := int32(actIDInt)

				m.mu.Lock()
				svc, ok := m.engineServices[bizKey]
				if ok {
					delete(m.services, bizKey)
					delete(m.engineServices, bizKey)
					m.memberIndex.RemoveByKey(bizKey)
				}
				m.mu.Unlock()

				m.periodicHandler.RemoveState(bizKey)

				if ok {
					zaplog.LoggerSugar.Infof("rank: subscribeDeleteEvents received delete bizKey=%s, cleaning up", bizKey)
					if members, err := svc.GetAllMembers(); err == nil && len(members) > 0 {
						m.memberIndex.RemoveUserEntries(bizType, actID, members)
					}
					svc.Cleanup()
				}
			}
		}
		ps.Close()
	}
}

// syncFromMongo 从 MongoDB 同步活动列表：新增本节点缺少的、移除 MongoDB 中已删除的。
func (m *Manager) syncFromMongo(ctx context.Context) {
	if m.dao == nil {
		return
	}
	m.mu.RLock()
	existingKeys := make(map[string]struct{}, len(m.engineServices))
	for key := range m.engineServices {
		existingKeys[key] = struct{}{}
	}
	m.mu.RUnlock()

	docs, err := m.dao.LoadAllRankConfigs()
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: sync from mongo: %v", err)
		return
	}

	now := time.Now().UnixMilli()

	mongoKeys := make(map[string]struct{})
	added := 0
	for _, doc := range docs {
		cfg := doc.Config

		bizType := BizType(cfg.BizType)
		if bizType == "" {
			bizType = "balloon"
			cfg.BizType = string(bizType)
		}
		key := NewBizKey(bizType, cfg.ActID).String()

		// 超过 7 天的活动不再维护 in-memory 热服务（内存管理）。
		// 其历史轮次查询由 ResolveEngineService 懒加载 MongoDB 周期元数据路由到历史路径，无需恢复状态。
		if cfg.CloseTime > 0 && now > cfg.CloseTime+7*86400000 {
			continue
		}

		mongoKeys[key] = struct{}{}

		// 若文档含周期状态，恢复 PeriodicState（仅有效期内的活动）。
		// 当本节点的内存状态落后于 MongoDB（另一节点已推进了轮次）时，强制刷新。
		if doc.Periodic != nil {
			existing := m.periodicHandler.GetState(key)
			if existing == nil || existing.GetCurrentRound() < doc.Periodic.CurrentRound {
				ps := periodic.StateFromSaved(string(bizType), cfg.ActID, *doc.Periodic)
				m.periodicHandler.SetState(key, ps)
				// 清理历史轮次残留的 Redis 热数据（重启后 in-memory timer 已丢失）。
				m.periodicHandler.CleanupHistoricalRounds(ps)
			}
		}

		m.mu.RLock()
		_, exists := m.engineServices[key]
		m.mu.RUnlock()
		if exists {
			continue
		}

		if cfg.RankCode == "" {
			cfg.RankCode = fmt.Sprintf("%s_score_%d", bizType, cfg.ActID)
		}
		if err := FillConfigFromFiles(bizType, &cfg); err != nil {
			zaplog.LoggerSugar.Warnf("rank: syncFromMongo fill config %s: %v", key, err)
		}

		// 周期排行榜：用当前轮的时间窗口替换整体活动时间
		if doc.Periodic != nil {
			cfg.OpenTime = doc.Periodic.RoundOpenTime
			cfg.CloseTime = doc.Periodic.RoundCloseTime
			cfg.GameEndTime = doc.Periodic.RoundCloseTime
			cfg.RoundIndex = doc.Periodic.CurrentRound
		}

		if err := m.rankService.RegisterRank(ctx, commonrank.Rank{
			RankCode:       cfg.RankCode,
			RankName:       fmt.Sprintf("%s_rank_%d", bizType, cfg.ActID),
			ScoreOrder:     commonrank.ScoreOrderDesc,
			TieBreakPolicy: commonrank.TieBreakPolicyFirstEnter,
			CreateTime:     cfg.OpenTime,
			UpdateTime:     cfg.OpenTime,
		}); err != nil {
			zaplog.LoggerSugar.Warnf("rank: sync register rank def %s: %v", cfg.RankCode, err)
			continue
		}

		localBizType := bizType
		onMemberJoin := func(userID int64, groupID int32) {
			m.memberIndex.Track(userID, MemberEntry{
				BizType: localBizType,
				ActID:   cfg.ActID,
				GroupID: groupID,
			})
		}

		service, err := engine.NewService(m.rankService, cfg, m.rdb, m.dao, engine.WithOnMemberJoin(onMemberJoin))
		if err != nil {
			zaplog.LoggerSugar.Warnf("rank: sync create service %s: %v", key, err)
			continue
		}

		m.mu.Lock()
		if _, dup := m.engineServices[key]; !dup {
			m.services[key] = newBizServiceWrapper(localBizType, service)
			m.engineServices[key] = service
			added++
		}
		m.mu.Unlock()
	}

	m.mu.Lock()
	var removed []string
	for key := range existingKeys {
		if _, inMongo := mongoKeys[key]; !inMongo {
			if _, stillExists := m.engineServices[key]; stillExists {
				removed = append(removed, key)
			}
		}
	}
	type removedEntry struct {
		svc     *engine.Service
		bizType BizType
		actID   int32
	}
	removedSvcs := make([]removedEntry, 0, len(removed))
	for _, key := range removed {
		if svc, ok := m.engineServices[key]; ok {
			cfg := svc.GetConfig()
			removedSvcs = append(removedSvcs, removedEntry{
				svc:     svc,
				bizType: BizType(cfg.BizType),
				actID:   cfg.ActID,
			})
			delete(m.services, key)
			delete(m.engineServices, key)
			m.memberIndex.RemoveByKey(key)
		}
	}
	m.mu.Unlock()

	for _, e := range removedSvcs {
		if members, err := e.svc.GetAllMembers(); err == nil && len(members) > 0 {
			m.memberIndex.RemoveUserEntries(e.bizType, e.actID, members)
		}
		zaplog.LoggerSugar.Infof("rank: syncFromMongo removed service bizType=%s actID=%d (config not in mongo)", e.bizType, e.actID)
	}

	if added > 0 || len(removed) > 0 {
		zaplog.LoggerSugar.Infof("rank: sync from mongo completed, added=%d removed=%d total=%d",
			added, len(removed), len(mongoKeys))
	}
}

// publishRankCreate 广播排行榜创建事件，通知其他节点立即同步。
// 消息为 JSON 编码的 engine.RankConfigDoc（robot 配置由接收节点从配置文件重新加载）。
func (m *Manager) publishRankCreate(bizKey string, cfg engine.Config, ps *engine.PeriodicSavedState) {
	if m.rdb == nil {
		return
	}
	cfg.RobotTiers = nil
	cfg.RobotInfos = nil
	doc := engine.RankConfigDoc{
		BizKey:   bizKey,
		Config:   cfg,
		Periodic: ps,
	}
	data, err := json.Marshal(doc)
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: publishRankCreate marshal bizKey=%s: %v", bizKey, err)
		return
	}
	if _, err := m.rdb.Publish(rediskeys.RankCreateChannel, string(data)); err != nil {
		zaplog.LoggerSugar.Warnf("rank: publishRankCreate bizKey=%s: %v", bizKey, err)
	}
}

// applyRankConfigDoc 从配置文档在本节点注册引擎服务（如已存在则跳过）。
// 供 subscribeCreateEvents 收到广播后调用，逻辑与 syncFromMongo 的单文档处理一致。
func (m *Manager) applyRankConfigDoc(ctx context.Context, doc engine.RankConfigDoc) bool {
	cfg := doc.Config
	bizType := BizType(cfg.BizType)
	if bizType == "" {
		bizType = "balloon"
		cfg.BizType = string(bizType)
	}
	key := NewBizKey(bizType, cfg.ActID).String()

	if doc.Periodic != nil {
		existing := m.periodicHandler.GetState(key)
		if existing == nil || existing.GetCurrentRound() < doc.Periodic.CurrentRound {
			ps := periodic.StateFromSaved(string(bizType), cfg.ActID, *doc.Periodic)
			m.periodicHandler.SetState(key, ps)
			m.periodicHandler.CleanupHistoricalRounds(ps)
		}
	}

	m.mu.RLock()
	_, exists := m.engineServices[key]
	m.mu.RUnlock()
	if exists {
		return false
	}

	if cfg.RankCode == "" {
		cfg.RankCode = fmt.Sprintf("%s_score_%d", bizType, cfg.ActID)
	}
	if err := FillConfigFromFiles(bizType, &cfg); err != nil {
		zaplog.LoggerSugar.Warnf("rank: applyRankConfigDoc fill config %s: %v", key, err)
	}

	if doc.Periodic != nil {
		cfg.OpenTime = doc.Periodic.RoundOpenTime
		cfg.CloseTime = doc.Periodic.RoundCloseTime
		cfg.GameEndTime = doc.Periodic.RoundCloseTime
		cfg.RoundIndex = doc.Periodic.CurrentRound
	}

	if err := m.rankService.RegisterRank(ctx, commonrank.Rank{
		RankCode:       cfg.RankCode,
		RankName:       fmt.Sprintf("%s_rank_%d", bizType, cfg.ActID),
		ScoreOrder:     commonrank.ScoreOrderDesc,
		TieBreakPolicy: commonrank.TieBreakPolicyFirstEnter,
		CreateTime:     cfg.OpenTime,
		UpdateTime:     cfg.OpenTime,
	}); err != nil {
		zaplog.LoggerSugar.Warnf("rank: applyRankConfigDoc register rank def %s: %v", cfg.RankCode, err)
		return false
	}

	localBizType := bizType
	localActID := cfg.ActID
	onMemberJoin := func(userID int64, groupID int32) {
		m.memberIndex.Track(userID, MemberEntry{
			BizType: localBizType,
			ActID:   localActID,
			GroupID: groupID,
		})
	}

	service, err := engine.NewService(m.rankService, cfg, m.rdb, m.dao, engine.WithOnMemberJoin(onMemberJoin))
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: applyRankConfigDoc create service %s: %v", key, err)
		return false
	}

	m.mu.Lock()
	_, dup := m.engineServices[key]
	if !dup {
		m.services[key] = newBizServiceWrapper(localBizType, service)
		m.engineServices[key] = service
	}
	m.mu.Unlock()
	return !dup
}

// subscribeCreateEvents 订阅排行榜创建广播，收到消息后立即在本节点注册对应 Service。
// 连接断开后自动重连，与 subscribeDeleteEvents 保持一致。
func (m *Manager) subscribeCreateEvents() {
	if m.rdb == nil {
		return
	}
	for {
		select {
		case <-m.stopCh:
			return
		default:
		}

		ps, err := m.rdb.Subscribe(rediskeys.RankCreateChannel)
		if err != nil {
			zaplog.LoggerSugar.Errorf("rank: subscribe create channel: %v", err)
			select {
			case <-m.stopCh:
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}

		ch := ps.Channel()
	loop:
		for {
			select {
			case <-m.stopCh:
				ps.Close()
				return
			case msg, ok := <-ch:
				if !ok {
					zaplog.LoggerSugar.Warnf("rank: create channel closed, reconnecting")
					break loop
				}
				var doc engine.RankConfigDoc
				if err := json.Unmarshal([]byte(msg.Payload), &doc); err != nil {
					zaplog.LoggerSugar.Warnf("rank: subscribeCreateEvents unmarshal: %v", err)
					continue
				}
				added := m.applyRankConfigDoc(context.Background(), doc)
				if added {
					zaplog.LoggerSugar.Infof("rank: subscribeCreateEvents applied bizKey=%s", doc.BizKey)
				}
			}
		}
		ps.Close()
	}
}
