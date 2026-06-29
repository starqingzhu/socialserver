package rankservice

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
	goredis "golib/redis"
	"golib/zaplog"
	"socialserver/internal/rank/balloon"
)

const (
	syncInterval = 30 * time.Second // 数据同步间隔（MongoDB/Redis）
	tickInterval = time.Second      // 实例状态推进间隔
)

type Manager struct {
	mu              sync.RWMutex
	rdb             *goredis.Redis
	dao             *balloon.DAO
	rankService     commonrank.Service
	memberIndex     *MemberIndex
	services        map[string]RankBizService
	balloonServices map[string]*balloon.Service
	stopCh          chan struct{}
}

var globalManager *Manager

func InitGlobalManager(rdb *goredis.Redis, dbName string) error {
	if rdb == nil {
		return fmt.Errorf("rank: redis client is required")
	}
	dao := balloon.NewDAO(dbName)
	manager := &Manager{
		rdb:             rdb,
		dao:             dao,
		rankService:     commonrank.NewRedisService(rdb),
		memberIndex:     NewMemberIndex(rdb),
		services:        make(map[string]RankBizService),
		balloonServices: make(map[string]*balloon.Service),
		stopCh:          make(chan struct{}),
	}
	if dao != nil {
		dao.EnsureIndexes()
		manager.syncFromMongo(context.Background())
	}
	// 从 Redis 补充注册（应对 MongoDB 中未存储或节点重启时 Redis 中已有排行榜数据的情况）
	manager.syncFromRedis(context.Background())
	// 预热所有服务：并发触发 ensureLoaded，确保冷启动后 Redis 数据完整恢复，
	// 而非等待各服务收到第一个玩家请求才懒加载。
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.services = make(map[string]RankBizService)
	m.balloonServices = make(map[string]*balloon.Service)
	zaplog.LoggerSugar.Infof("rank global manager closed")
}

// startBackground 启动两个后台 goroutine：
// - tickLoop：每秒推进所有服务实例状态（结算、机器人等）
// - syncLoop：每 30 秒从 MongoDB/Redis 同步活动列表（多节点感知新增/删除）
func (m *Manager) startBackground() {
	go m.tickLoop()
	go m.syncLoop()
	go m.subscribeDeleteEvents()
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

// tickServices 推进所有服务实例的内存状态（每秒调用）。
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
}

func (m *Manager) registerBalloon(ctx context.Context, cfg balloon.Config) (*balloon.Service, error) {
	bizType := BizType(cfg.BizType)
	if cfg.RankCode == "" {
		cfg.RankCode = fmt.Sprintf("%s_score_%d", bizType, cfg.ActID)
	}

	key := NewBizKey(bizType, cfg.ActID).String()

	m.mu.Lock()
	defer m.mu.Unlock()
	if svc, ok := m.balloonServices[key]; ok {
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

	service, err := balloon.NewService(m.rankService, cfg, m.rdb, m.dao, balloon.WithOnMemberJoin(onMemberJoin))
	if err != nil {
		return nil, err
	}
	m.services[key] = &balloonAdapter{Svc: service, bizType: bizType}
	m.balloonServices[key] = service

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

// Register 按业务类型注册排行榜服务，自动从配置文件加载参数。
func (m *Manager) Register(ctx context.Context, bizType BizType, cfg balloon.Config) error {
	if err := ValidateBizType(bizType); err != nil {
		return err
	}
	cfg.BizType = string(bizType)
	if err := FillConfigFromFiles(bizType, &cfg); err != nil {
		return err
	}

	switch bizType {
	case BizTypeBalloon:
		_, err := m.registerBalloon(ctx, cfg)
		return err
	default:
		return fmt.Errorf("unsupported biz type: %s", bizType)
	}
}

// UpdateService 按业务类型更新排行榜配置。
func (m *Manager) UpdateService(bizType BizType, actID int32, cfg balloon.Config) error {
	if m == nil {
		return fmt.Errorf("rank manager is nil")
	}
	key := NewBizKey(bizType, actID).String()
	m.mu.RLock()
	svc, ok := m.balloonServices[key]
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
// 若服务已在内存中，走正常卸载路径；若服务不在内存中（如重启后遗留数据），
// 直接通过 Store/DAO 强制清理 Redis 和 MongoDB 中的全部关联数据。
func (m *Manager) RemoveService(bizType BizType, actID int32) error {
	if m == nil {
		return fmt.Errorf("rank manager is nil")
	}
	key := NewBizKey(bizType, actID).String()

	m.mu.Lock()
	svc, ok := m.balloonServices[key]
	if ok {
		delete(m.services, key)
		delete(m.balloonServices, key)
		m.memberIndex.RemoveByKey(key)
	}
	m.mu.Unlock()

	if ok {
		// 在清理 Redis 数据之前加载所有成员，以便清理各用户的成员索引条目。
		if members, err := svc.GetAllMembers(); err == nil && len(members) > 0 {
			m.memberIndex.RemoveUserEntries(bizType, actID, members)
		}
		svc.Cleanup()
	} else {
		// 服务不在内存中，直接通过 Store/DAO 强制清理持久化数据。
		m.forceCleanupOrphan(context.Background(), bizType, actID)
	}

	if m.dao != nil {
		m.dao.DeleteRankConfig(key)
	}

	// 广播删除事件，通知所有节点立即停止该 Service 的 tick，防止其他节点继续写入 MongoDB。
	if m.rdb != nil {
		if _, err := m.rdb.Publish(rediskeys.RankDeleteChannel, key); err != nil {
			zaplog.LoggerSugar.Warnf("rank: publish delete event bizKey=%s: %v", key, err)
		}
	}
	return nil
}

// forceCleanupOrphan 清理不在内存中的排行榜服务的所有持久化数据（Redis + MongoDB）。
// 用于服务重启后遗留数据、或 GM 强制删除时服务已不在内存的场景。
func (m *Manager) forceCleanupOrphan(ctx context.Context, bizType BizType, actID int32) {
	bizId := fmt.Sprintf("%s_%d", bizType, actID)
	rankCode := fmt.Sprintf("%s_score_%d", bizType, actID)

	store := balloon.NewStore(m.rdb, m.dao, bizId)

	// 清理各用户的全局成员索引（rank:member_index:{userID}）。
	if members, err := store.GetAllMembers(); err == nil && len(members) > 0 {
		m.memberIndex.RemoveUserEntries(bizType, actID, members)
	}

	// 加载分组列表，用于删除各分组的 commonRank 实例。
	groups, _ := store.LoadGroups()

	// 清理 Redis 中所有 rank:* key（含分组级 key）及 MongoDB 中全部关联集合文档。
	store.CleanupAll(groups)

	// 删除每个分组对应的 commonRank 实例 Redis 数据。
	for _, g := range groups {
		if g == nil {
			continue
		}
		instanceID := commonrank.NewInstanceID(rankCode, bizId, fmt.Sprintf("group_%d", g.GroupID))
		if err := m.rankService.DeleteInstance(ctx, instanceID); err != nil {
			zaplog.LoggerSugar.Warnf("rank: forceCleanup delete rank instance %s: %v", instanceID, err)
		}
	}
	// 删除榜单定义。
	if err := m.rankService.DeleteRankDef(ctx, rankCode); err != nil {
		zaplog.LoggerSugar.Warnf("rank: forceCleanup delete rank def %s: %v", rankCode, err)
	}
}

// ServiceInfo 包含排行榜服务的摘要信息。
type ServiceInfo struct {
	BizType     BizType
	ActID       int32
	Config      balloon.Config
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
	result := make([]ServiceInfo, 0, len(m.balloonServices))
	for _, svc := range m.balloonServices {
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

// syncFromRedis 通过扫描 Redis 运行时数据（rank:meta:*）发现已有活动，
// 将内存中缺失的服务重新注册。这是 syncFromMongo 的补充路径：
// 仅在 MongoDB 不可用（或本节点刚重启、数据未同步完）时发挥作用。
// 配置字段从 meta hash 中恢复 openTime/closeTime，无需依赖 rank:inst:{...:main}。
// 仅做新增，不做删除（删除由 syncFromMongo 负责）。
func (m *Manager) syncFromRedis(ctx context.Context) {
	if m.rdb == nil {
		return
	}
	// 通过 rank:meta:* 发现 Redis 中存有运行时数据的活动
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
		// 从 "rank:meta:{balloon_1068}" 中提取 bizId "balloon_1068"
		bizId := strings.TrimPrefix(key, prefix)
		bizId = strings.TrimSuffix(bizId, "}")
		if bizId == "" {
			continue
		}

		// 解析 bizId "balloon_1068" → BizType="balloon", actID=1068
		sep := strings.LastIndex(bizId, "_")
		if sep <= 0 {
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
		_, exists := m.balloonServices[bizKey]
		m.mu.RUnlock()
		if exists {
			continue
		}

		// 从 meta hash 中读取 openTime / closeTime / gameEndTime（由 NewService 写入）
		tmpStore := balloon.NewStore(m.rdb, nil, bizId)
		openTime, closeTime, gameEndTime, ok := tmpStore.LoadActivityTimes()
		if !ok {
			// meta hash 缺少活动时间数据，跳过（数据不完整）
			continue
		}

		// 跳过超过关闭时间 7 天的过期活动
		if closeTime > 0 && now > closeTime+7*86400000 {
			continue
		}

		rankCode := fmt.Sprintf("%s_score_%d", bizType, actID)
		cfg := balloon.Config{
			BizType:     string(bizType),
			ActID:       actID,
			RankCode:    rankCode,
			OpenTime:    openTime,
			CloseTime:   closeTime,
			GameEndTime: gameEndTime,
		}

		// 从本地配置文件填充 RankPeopleNum / OpenToken / RobotTiers / RobotInfos
		if err := FillConfigFromFiles(bizType, &cfg); err != nil {
			zaplog.LoggerSugar.Warnf("rank: syncFromRedis fill config %s: %v", bizKey, err)
		}

		// 确保 rank 定义存在（正常情况下 rank:def:{rankCode} 已在 Redis 中，此处为容错）
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

		service, err := balloon.NewService(m.rankService, cfg, m.rdb, m.dao, balloon.WithOnMemberJoin(onMemberJoin))
		if err != nil {
			zaplog.LoggerSugar.Warnf("rank: syncFromRedis create service %s: %v", bizKey, err)
			continue
		}

		m.mu.Lock()
		if _, dup := m.balloonServices[bizKey]; !dup {
			m.services[bizKey] = &balloonAdapter{Svc: service, bizType: bizType}
			m.balloonServices[bizKey] = service
			added++
		}
		m.mu.Unlock()
	}

	if added > 0 {
		zaplog.LoggerSugar.Infof("rank: syncFromRedis completed, added=%d", added)
	}
}

// warmUpAllServices 并发调用所有已注册服务的 WarmUp，
// 在启动时主动触发 ensureLoaded，确保 Redis 数据完整恢复。
// 在 syncFromMongo + syncFromRedis 之后调用。
func (m *Manager) warmUpAllServices(ctx context.Context) {
	m.mu.RLock()
	svcs := make([]*balloon.Service, 0, len(m.balloonServices))
	for _, svc := range m.balloonServices {
		svcs = append(svcs, svc)
	}
	m.mu.RUnlock()
	if len(svcs) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, svc := range svcs {
		wg.Add(1)
		go func(s *balloon.Service) {
			defer wg.Done()
			s.WarmUp(ctx)
		}(svc)
	}
	wg.Wait()
	zaplog.LoggerSugar.Infof("rank: warmUpAllServices completed, services=%d", len(svcs))
}

// subscribeDeleteEvents 订阅排行榜删除广播，收到消息后立即在本节点移除对应 Service。
// 这解决了多节点场景下节点A删除排行榜、节点B不知情仍持续写入 MongoDB 的问题：
// 节点B收到通知后立即停止 tick（不再产生新写任务），然后执行本地清理。
func (m *Manager) subscribeDeleteEvents() {
	if m.rdb == nil {
		return
	}
	ps, err := m.rdb.Subscribe(rediskeys.RankDeleteChannel)
	if err != nil {
		zaplog.LoggerSugar.Errorf("rank: subscribe delete channel: %v", err)
		return
	}
	defer ps.Close()

	ch := ps.Channel()
	for {
		select {
		case <-m.stopCh:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			bizKey := msg.Payload
			if bizKey == "" {
				continue
			}
			// 解析 bizKey（"balloon:1068"）→ bizType + actID
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
			svc, ok := m.balloonServices[bizKey]
			if ok {
				delete(m.services, bizKey)
				delete(m.balloonServices, bizKey)
				m.memberIndex.RemoveByKey(bizKey)
			}
			m.mu.Unlock()

			if ok {
				zaplog.LoggerSugar.Infof("rank: subscribeDeleteEvents received delete bizKey=%s, cleaning up", bizKey)
				if members, err := svc.GetAllMembers(); err == nil && len(members) > 0 {
					m.memberIndex.RemoveUserEntries(bizType, actID, members)
				}
				svc.Cleanup()
			}
		}
	}
}

// syncFromMongo 从 MongoDB 同步活动列表：新增本节点缺少的、移除 MongoDB 中已删除的。
func (m *Manager) syncFromMongo(ctx context.Context) {
	if m.dao == nil {
		return
	}
	// 快照同步开始时内存中已有的服务 key。
	// 仅删除「本次同步开始前已在内存」且「MongoDB 中已不存在」的服务，
	// 避免与 registerBalloon 并发时误删刚注册的服务（registerBalloon 持锁期间
	// 同时写内存和 MongoDB，但 syncFromMongo 已在锁外读取完 MongoDB）。
	m.mu.RLock()
	existingKeys := make(map[string]struct{}, len(m.balloonServices))
	for key := range m.balloonServices {
		existingKeys[key] = struct{}{}
	}
	m.mu.RUnlock()

	docs, err := m.dao.LoadAllRankConfigs()
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: sync from mongo: %v", err)
		return
	}

	now := time.Now().UnixMilli()

	// 收集 MongoDB 中有效的 key 集合
	mongoKeys := make(map[string]struct{})
	added := 0
	for _, doc := range docs {
		cfg := doc.Config
		if cfg.CloseTime > 0 && now > cfg.CloseTime+7*86400000 {
			continue
		}

		bizType := BizType(cfg.BizType)
		if bizType == "" {
			bizType = BizTypeBalloon
			cfg.BizType = string(bizType)
		}
		key := NewBizKey(bizType, cfg.ActID).String()
		mongoKeys[key] = struct{}{}

		m.mu.RLock()
		_, exists := m.balloonServices[key]
		m.mu.RUnlock()
		if exists {
			continue
		}

		// 新增：本节点没有，从 MongoDB 恢复
		if cfg.RankCode == "" {
			cfg.RankCode = fmt.Sprintf("%s_score_%d", bizType, cfg.ActID)
		}
		// MongoDB 中不存储 RobotTiers/RobotInfos（可从本地配置文件恢复），此处补全。
		if err := FillConfigFromFiles(bizType, &cfg); err != nil {
			zaplog.LoggerSugar.Warnf("rank: syncFromMongo fill config %s: %v", key, err)
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

		onMemberJoin := func(userID int64, groupID int32) {
			m.memberIndex.Track(userID, MemberEntry{
				BizType: bizType,
				ActID:   cfg.ActID,
				GroupID: groupID,
			})
		}

		service, err := balloon.NewService(m.rankService, cfg, m.rdb, m.dao, balloon.WithOnMemberJoin(onMemberJoin))
		if err != nil {
			zaplog.LoggerSugar.Warnf("rank: sync create service %s: %v", key, err)
			continue
		}

		m.mu.Lock()
		if _, dup := m.balloonServices[key]; !dup {
			m.services[key] = &balloonAdapter{Svc: service, bizType: bizType}
			m.balloonServices[key] = service
			added++
		}
		m.mu.Unlock()
	}

	// 移除：本次同步开始时已在内存、但 MongoDB 中已不存在（或已过期）的服务。
	// 注意：此处只做内存移除，不调用 Cleanup() 删除数据。
	// 数据清理由 RemoveService（GM 主动删除）负责，syncFromMongo 只是感知已删除的服务并停止 tick。
	// 若在此处调用 Cleanup()，会因 SaveRankConfig 异步写入延迟（config 暂时不在 MongoDB）
	// 而误删仍在运行的活动的所有业务数据。
	m.mu.Lock()
	var removed []string
	for key := range existingKeys {
		if _, inMongo := mongoKeys[key]; !inMongo {
			if _, stillExists := m.balloonServices[key]; stillExists {
				removed = append(removed, key)
			}
		}
	}
	type removedEntry struct {
		svc     *balloon.Service
		bizType BizType
		actID   int32
	}
	removedSvcs := make([]removedEntry, 0, len(removed))
	for _, key := range removed {
		if svc, ok := m.balloonServices[key]; ok {
			cfg := svc.GetConfig()
			removedSvcs = append(removedSvcs, removedEntry{
				svc:     svc,
				bizType: BizType(cfg.BizType),
				actID:   cfg.ActID,
			})
			delete(m.services, key)
			delete(m.balloonServices, key)
			m.memberIndex.RemoveByKey(key)
		}
	}
	m.mu.Unlock()

	for _, e := range removedSvcs {
		if members, err := e.svc.GetAllMembers(); err == nil && len(members) > 0 {
			m.memberIndex.RemoveUserEntries(e.bizType, e.actID, members)
		}
		// 只停止 tick，不清理数据（数据清理由 RemoveService 负责）
		zaplog.LoggerSugar.Infof("rank: syncFromMongo removed service bizType=%s actID=%d (config not in mongo)", e.bizType, e.actID)
	}

	if added > 0 || len(removed) > 0 {
		zaplog.LoggerSugar.Infof("rank: sync from mongo completed, added=%d removed=%d total=%d",
			added, len(removed), len(mongoKeys))
	}
}
