package rankservice

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
	"golib/gpool"
	goredis "golib/redis"
	"golib/zaplog"
	"socialserver/internal/rank/engine"
	"socialserver/internal/rank/once"
	"socialserver/internal/rank/periodic"
)

const (
	syncInterval   = 30 * time.Second
	tickInterval   = time.Second
	memberIndexTTL = 7 * 24 * time.Hour
	coldDataTTL    = commonrank.ColdDataTTL

	// tickSubmitTimeout 是 tickServices 向协程池提交单个 Service Tick 任务的最长等待时间。
	// tickLoop 运行在 time.Ticker 上（容量为 1 的 channel），若入队阻塞超过一个 tickInterval，
	// 后续 tick 会被静默丢弃；必须远小于 tickInterval 才能保证 tickLoop 及时回到 select 循环
	// （docs/rank_optimization.md 待办 F）。超时后跳过该 Service 本轮 tick，下一轮 tick 幂等补上。
	tickSubmitTimeout = 200 * time.Millisecond
)

type Manager struct {
	mu                   sync.RWMutex
	wg                   sync.WaitGroup
	rdb                  *goredis.Redis
	dao                  *engine.DAO
	rankService          commonrank.Service
	memberIndex          *MemberIndex
	services             map[string]RankBizService
	engineServices       map[string]*engine.Service
	periodicHandler      *periodic.Handler
	stopCh               chan struct{}
	registryBootstrapped atomic.Bool
}

var globalManager *Manager

func InitGlobalManager(rdb *goredis.Redis, dbName string) error {
	if rdb == nil {
		return fmt.Errorf("rank: redis client is required")
	}
	dao := engine.NewDAO(dbName)
	rs := commonrank.NewRedisService(rdb)
	manager := &Manager{
		rdb:            rdb,
		dao:            dao,
		rankService:    rs,
		memberIndex:    NewMemberIndex(rdb, memberIndexTTL),
		services:       make(map[string]RankBizService),
		engineServices: make(map[string]*engine.Service),
		stopCh:         make(chan struct{}),
	}
	// 必须在建好 manager 之后立刻注入：rank:def 一旦到期，RedisService 内部（BatchUpsertScore /
	// loadMembersWithDef 直接调 GetRank）会零 IO 重建，进程外的装饰器拦不住这条热路径（缺陷 1）。
	rs.SetRankDefProvider(manager.rankDefFromMemory)
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
	m.wg.Wait() // 等所有后台 goroutine 退出，再关闭 Redis/MongoDB
	m.periodicHandler.Clear()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.services = make(map[string]RankBizService)
	m.engineServices = make(map[string]*engine.Service)
	zaplog.LoggerSugar.Infof("rank global manager closed")
}

func (m *Manager) startBackground() {
	m.wg.Add(4)
	go func() { defer m.wg.Done(); m.tickLoop() }()
	go func() { defer m.wg.Done(); m.syncLoop() }()
	go func() { defer m.wg.Done(); m.subscribeDeleteEvents() }()
	go func() { defer m.wg.Done(); m.subscribeCreateEvents() }()
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

// tickServices 通过全局协程池并发执行所有 Service 的 Tick（见 docs/rank_optimization.md 第 04 条）。
// 提交入队用带超时的 SubmitWaitTimeout（而非无界 SubmitWait）：tickLoop 运行在 time.Ticker 上，
// 池满时无界阻塞会让 tickLoop 的消费 goroutine 错过下一次 tick（ticker channel 容量为 1，静默丢弃），
// 见待办 F。超时只影响"入队等待"，已入队任务仍用原始 ctx 执行完整 Tick，不会被提前取消。
func (m *Manager) tickServices(ctx context.Context, now int64) {
	m.mu.RLock()
	svcs := make([]RankBizService, 0, len(m.services))
	for _, svc := range m.services {
		svcs = append(svcs, svc)
	}
	m.mu.RUnlock()
	if len(svcs) == 0 {
		m.tickPeriodicActivities(ctx, now)
		return
	}

	var wg sync.WaitGroup
	for _, svc := range svcs {
		s := svc
		wg.Add(1)
		if err := gpool.Global.SubmitWaitTimeout(ctx, func() {
			defer wg.Done()
			if err := s.Tick(ctx, now); err != nil {
				zaplog.LoggerSugar.Warnf("rank: tick service error: %v", err)
			}
		}, tickSubmitTimeout); err != nil {
			wg.Done() // 提交失败/超时必须配平，否则 wg.Wait() 永久阻塞
			zaplog.LoggerSugar.Warnf("rank: tick submit skipped (pool busy), will retry next tick: %v", err)
		}
	}
	wg.Wait()

	m.tickPeriodicActivities(ctx, now) // 仍在全部 Tick 完成后执行
}

// newBizServiceWrapper 为周期排行榜创建业务服务适配器。
// 所有周期排行榜类型（balloon、egg、camper_competition 等）共用同一个通用实现。
func newBizServiceWrapper(bizType BizType, svc *engine.Service) RankBizService {
	return once.NewBizService(string(bizType), svc)
}

// rankDefFor 是 rank:def 的**唯一构造器**：注册路径与恢复路径共用同一份（缺陷 1）。
//
// 之所以必须收敛成一个函数：`RankConfigDoc.Config`（engine.Config）不含 RankName / ScoreOrder /
// TieBreakPolicy / MaxQuerySize 这四个字段，Redis 的 rank:def 是它们的唯一住所，恢复无法从
// MongoDB 无损重建。把 6 个注册点收敛到这里之后，恢复写入与注册写入的 json.Marshal 输入逐字节
// 相同 ⇒ 输出逐字节相同：恢复是无损的，而且不可能随代码演进与注册路径漂移。
//
// 新增注册点时不要内联 commonrank.Rank 字面量，一律走这里。
func rankDefFor(bizType BizType, cfg engine.Config) commonrank.Rank {
	return commonrank.Rank{
		RankCode:       cfg.RankCode,
		RankName:       fmt.Sprintf("%s_rank_%d", bizType, cfg.ActID),
		ScoreOrder:     commonrank.ScoreOrderDesc,
		TieBreakPolicy: commonrank.TieBreakPolicyFirstEnter,
		CreateTime:     cfg.OpenTime,
		UpdateTime:     cfg.OpenTime,
	}
}

// rankDefFromMemory 是 rank:def 到期后的恢复数据源（缺陷 1）：按 rankCode 在内存中已注册的
// engine.Service 里取那份构造期捕获的原始定义（WithRankDef），零 IO 写回。
//
// 直接扫 engineServices、不另建 rankCode → Service 映射：服务被删除时 engineServices 一并删除，
// provider 立刻查不到，不会把 GM 已删除的定义**复活**；另建一份映射就必须在所有删除路径上手工
// 同步，漏一处就是一个删不掉的定义。本函数只在 rank:def 未命中时被调用，线性扫描的代价无关紧要。
func (m *Manager) rankDefFromMemory(rankCode string) (commonrank.Rank, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, svc := range m.engineServices {
		if svc == nil {
			continue
		}
		if def := svc.RankDef(); def.RankCode == rankCode {
			return def, true
		}
	}
	return commonrank.Rank{}, false
}

// snapshotServices 在锁内取一份 engineServices 的切片快照，供调用方在锁外遍历。
//
// 存在的理由（待办 C）：engine.Service 的多数取值器都带 IO（GroupCount/MemberCount 会读
// Redis，GetMemberRank 更是一次成员查询），持着 m.mu 去调它们会让整个注册表的读写
// ——包括热路径上的 Tick/GetEngineService——排队等在这些 IO 后面。
// 快照是浅拷贝：Service 指针本身是并发安全的，锁只保护 map 结构。
func (m *Manager) snapshotServices() []*engine.Service {
	m.mu.RLock()
	defer m.mu.RUnlock()
	svcs := make([]*engine.Service, 0, len(m.engineServices))
	for _, svc := range m.engineServices {
		svcs = append(svcs, svc)
	}
	return svcs
}

func (m *Manager) registerEngine(ctx context.Context, bizType BizType, cfg engine.Config) (*engine.Service, error) {
	if cfg.RankCode == "" {
		cfg.RankCode = fmt.Sprintf("%s_score_%d", bizType, cfg.ActID)
	}

	key := NewBizKey(bizType, cfg.ActID).String()

	// 快速路径：已存在则直接返回，不进任何 IO。
	m.mu.RLock()
	existing, ok := m.engineServices[key]
	m.mu.RUnlock()
	if ok {
		return existing, nil
	}

	if cfg.CreateTime == 0 {
		cfg.CreateTime = time.Now().UnixMilli()
	}
	def := rankDefFor(bizType, cfg)
	if err := m.rankService.RegisterRank(ctx, def); err != nil {
		return nil, err
	}

	onMemberJoin := func(userID int64, groupID int32) {
		m.memberIndex.Track(userID, MemberEntry{
			BizType: bizType,
			ActID:   cfg.ActID,
			GroupID: groupID,
		})
	}

	// Redis 与 Mongo 的 IO 全部放在锁外（待办 C）。这里必然可能做重复工作
	// （两个节点/两个协程同时走到这里会各造一份 Service），但构造本身无副作用：
	// 真正的注册是下面锁内的二次检查，输的一方返回赢家插入的那个实例。
	service, err := engine.NewService(m.rankService, cfg, m.rdb, m.dao,
		engine.WithOnMemberJoin(onMemberJoin), engine.WithRankDef(def))
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	if existing, ok := m.engineServices[key]; ok {
		m.mu.Unlock()
		// 必须返回已插入的那个，不能返回自己刚造的副本：
		// 两个调用方各自持有一份 Service 会让内存状态（分组/成员缓存）从此分叉。
		return existing, nil
	}
	m.services[key] = newBizServiceWrapper(bizType, service)
	m.engineServices[key] = service
	m.mu.Unlock()

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
	}
	m.mu.Unlock()

	m.periodicHandler.RemoveState(key)

	if ok {
		m.cleanupServiceData(svc, bizType, actID)
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

// cleanupServiceData 按固定顺序清理一个服务的全部痕迹（成员索引 + Redis/MongoDB）。
// 顺序不可调换：RemoveUserEntries 依赖 rank:members 仍存在，必须先于 svc.Cleanup()
// （其内部 CleanupAll 会 Del 掉 rank:members），否则索引清理静默失效。
func (m *Manager) cleanupServiceData(svc *engine.Service, bizType BizType, actID int32) {
	switch members, err := svc.GetAllMembers(); {
	case err != nil:
		zaplog.LoggerSugar.Warnf("rank: get members for index cleanup bizType=%s actID=%d: %v (索引由 7 天 TTL 兜底)", bizType, actID, err)
	case len(members) > 0:
		m.memberIndex.RemoveUserEntries(bizType, actID, members)
	}
	svc.Cleanup()
}

// forceCleanupOrphan 清理不在内存中的排行榜服务的所有持久化数据（Redis + MongoDB）。
func (m *Manager) forceCleanupOrphan(ctx context.Context, bizType BizType, actID int32) {
	bizId := fmt.Sprintf("%s_%d", bizType, actID)
	rankCode := fmt.Sprintf("%s_score_%d", bizType, actID)

	store := engine.NewStore(m.rdb, m.dao, bizId, nil)

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
//
// 遍历必须在锁外（待办 C）：IsSettled/GroupCount/MemberCount 各自带 IO
// （GroupCount/MemberCount 会走 Redis），持锁调用会让注册表整体被一个慢节点堵住。
func (m *Manager) ListServices(filterBizType BizType) []ServiceInfo {
	if m == nil {
		return nil
	}
	svcs := m.snapshotServices()
	result := make([]ServiceInfo, 0, len(svcs))
	for _, svc := range svcs {
		if svc == nil {
			continue
		}
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
// Redis 索引键带 TTL，过期后首次查询会从内存中的 engine service 重建。
func (m *Manager) GetMemberEntries(userID int64) []MemberEntry {
	if m == nil {
		return nil
	}
	if entries := m.memberIndex.Lookup(userID); len(entries) > 0 {
		return entries
	}
	return m.rebuildMemberIndex(userID)
}

// rebuildMemberIndex 扫描所有已加载的 engine service，重建指定用户的成员索引。
// engine.Service 的 memberGroup 常驻内存（WarmUp 时加载一次），不产生 Redis/MongoDB 访问。
func (m *Manager) rebuildMemberIndex(userID int64) []MemberEntry {
	svcs := m.snapshotServices()

	var entries []MemberEntry
	for _, svc := range svcs {
		if svc == nil {
			continue
		}
		groupID, ok := svc.GetMemberGroupID(userID)
		if !ok {
			continue
		}
		cfg := svc.GetConfig()
		entry := MemberEntry{
			BizType: BizType(cfg.BizType),
			ActID:   cfg.ActID,
			GroupID: groupID,
		}
		m.memberIndex.Track(userID, entry)
		entries = append(entries, entry)
	}
	return entries
}

// GetMemberRankEntries 返回用户在所有排行榜中的名次快照（GM 查询用）。
//
// 查表与 IO 分离（待办 C）：先在读锁内把「每个 entry 对应的服务」取出来，
// 再在锁外逐个查名次。此前 svc.GetMemberRank 是持着 m.mu.RLock 做的，
// 一个用户的 GM 查询会把 N 个服务的 Redis 往返全压在注册表的读锁上，
// 而 UpsertScore 之外的写路径（注册/删除服务）都在等这把锁。
func (m *Manager) GetMemberRankEntries(ctx context.Context, userID int64) ([]MemberRankEntry, error) {
	if m == nil {
		return nil, nil
	}
	entries := m.GetMemberEntries(userID)
	if len(entries) == 0 {
		return nil, nil
	}

	type entryService struct {
		key string
		svc RankBizService
	}
	lookups := make([]entryService, 0, len(entries))
	m.mu.RLock()
	for _, entry := range entries {
		key := NewBizKey(entry.BizType, entry.ActID).String()
		lookups = append(lookups, entryService{key: key, svc: m.services[key]})
	}
	m.mu.RUnlock()

	result := make([]MemberRankEntry, 0, len(entries))
	for i, entry := range entries {
		svc := lookups[i].svc
		if svc == nil {
			result = append(result, MemberRankEntry{MemberEntry: entry})
			continue
		}
		snapshot, _, err := svc.GetMemberRank(ctx, userID)
		if err != nil {
			return nil, fmt.Errorf("get rank for %s: %w", lookups[i].key, err)
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

// syncFromRedis 通过全局活跃服务注册表（rank:{active_services}）发现已有活动并补充注册，
// 取代原先每 30 秒一次的全库 KEYS 扫描（见 docs/rank_optimization.md 第 01 条）。
//
// 稳态零扫描：每个节点启动后只做一次 bootstrapRegistry（SCAN 建表），之后每轮只 SMEMBERS
// 注册表本身 + 本地按 deadline 过滤，命令数是 O(活跃服务数) 而不是 O(全库 key 数)。
func (m *Manager) syncFromRedis(ctx context.Context) {
	if m.rdb == nil {
		return
	}

	// 启动期一次性 SCAN 建表；失败不置位，下一轮 syncLoop 重试。进程存活后所有新建活动
	// 都经 Store.SaveActivityTimes 实时写注册表，不再需要重复 SCAN。
	if !m.registryBootstrapped.Load() {
		if m.bootstrapRegistry(ctx) {
			m.registryBootstrapped.Store(true)
		}
	}

	members, err := m.rdb.SMembers(rediskeys.RankActiveServicesKey)
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: syncFromRedis read registry: %v", err)
		return
	}
	if len(members) == 0 {
		return
	}

	now := time.Now().UnixMilli()
	live := make([]string, 0, len(members))
	stale := make([]interface{}, 0)
	for _, mem := range members {
		bizId, deadline, ok := parseRegistryMember(mem)
		if !ok || (deadline > 0 && deadline < now) {
			stale = append(stale, mem)
			continue
		}
		live = append(live, bizId)
	}
	if len(stale) > 0 {
		// 删陈旧成员的同时给注册表 key 续期（不加会退化成永久 key，见缺陷 8）。
		pipe := m.rdb.Pipeline()
		pipe.SRem(ctx, rediskeys.RankActiveServicesKey, stale...)
		pipe.Expire(ctx, rediskeys.RankActiveServicesKey, coldDataTTL)
		if _, err := pipe.Exec(ctx); err != nil {
			zaplog.LoggerSugar.Warnf("rank: syncFromRedis prune registry: %v", err)
		}
	}

	added := 0
	for _, bizId := range live {
		if m.registerFromBizId(ctx, bizId, now) {
			added++
		}
	}
	if added > 0 {
		zaplog.LoggerSugar.Infof("rank: syncFromRedis completed, added=%d", added)
	}
}

// registerFromBizId 尝试把注册表发现的单个 bizId 注册为内存中的 Service（若尚未注册）。
// 周期排行榜子轮次 BizId 走 tryRecoverPeriodicFromRedis 的降级恢复路径。
func (m *Manager) registerFromBizId(ctx context.Context, bizId string, now int64) bool {
	sep := strings.LastIndex(bizId, "_")
	if sep <= 0 {
		return false
	}

	// 周期排行榜子轮次 BizId（格式 "{bizType}_{actID}_r{N}"）：
	// 尝试从 Redis 元数据降级恢复（主路径是 syncFromMongo），然后跳过常规注册流程。
	if periodic.IsRoundBizId(bizId) {
		m.tryRecoverPeriodicFromRedis(ctx, bizId)
		return false
	}

	actIDInt, err := strconv.ParseInt(bizId[sep+1:], 10, 32)
	if err != nil {
		return false
	}
	bizType := BizType(bizId[:sep])
	actID := int32(actIDInt)

	bizKey := NewBizKey(bizType, actID).String()
	m.mu.RLock()
	_, exists := m.engineServices[bizKey]
	m.mu.RUnlock()
	if exists {
		return false
	}

	tmpStore := engine.NewStore(m.rdb, nil, bizId, nil)
	openTime, closeTime, gameEndTime, ok := tmpStore.LoadActivityTimes()
	if !ok {
		return false
	}

	if closeTime > 0 && now > closeTime+7*86400000 {
		return false
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

	def := rankDefFor(bizType, cfg)
	if err := m.rankService.RegisterRank(ctx, def); err != nil {
		zaplog.LoggerSugar.Warnf("rank: syncFromRedis register rank def %s: %v", cfg.RankCode, err)
		return false
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

	service, err := engine.NewService(m.rankService, cfg, m.rdb, m.dao,
		engine.WithOnMemberJoin(onMemberJoin), engine.WithRankDef(def))
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: syncFromRedis create service %s: %v", bizKey, err)
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.engineServices[bizKey]; dup {
		return false
	}
	m.services[bizKey] = newBizServiceWrapper(localBizType, service)
	m.engineServices[bizKey] = service
	return true
}

// bootstrapRegistry 启动期一次性 SCAN rank:meta:{*} 建立活跃服务注册表。
// 只在「Redis 有、MongoDB 没有」（进程崩溃于 Mongo 异步写落盘之前）这一种场景下才有必要，
// 进程存活期间的后续创建都经 Store.SaveActivityTimes 实时写注册表，不需要重复 SCAN。
func (m *Manager) bootstrapRegistry(ctx context.Context) bool {
	prefix := rediskeys.RankMetaKeyPrefix + ":{"
	keys, err := m.rdb.ScanAll(ctx, prefix+"*}", 500)
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: bootstrapRegistry scan: %v", err)
		return false
	}
	if len(keys) == 0 {
		return true
	}

	members := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		bizId := strings.TrimPrefix(key, prefix)
		bizId = strings.TrimSuffix(bizId, "}")
		if bizId == "" {
			continue
		}
		members = append(members, fmt.Sprintf("%s:%d", bizId, m.registryDeadline(bizId)))
	}
	if len(members) == 0 {
		return true
	}

	pipe := m.rdb.Pipeline()
	pipe.SAdd(ctx, rediskeys.RankActiveServicesKey, members...)
	pipe.Expire(ctx, rediskeys.RankActiveServicesKey, coldDataTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		zaplog.LoggerSugar.Warnf("rank: bootstrapRegistry seed: %v", err)
		return false
	}
	return true
}

// registryDeadline 计算某 bizId 在注册表中的成员 deadline：settleAt + SettledCacheTTL。
// settleAt 缺失或为 0（配置异常，CloseTime 与 GameEndTime 都未设置）时退化为滑动窗口起点，
// 避免 bootstrap 刚建好的成员因 deadline 落在过去而被下一轮迭代立即判定为 stale。
func (m *Manager) registryDeadline(bizId string) int64 {
	tmpStore := engine.NewStore(m.rdb, nil, bizId, nil)
	_, closeTime, gameEndTime, ok := tmpStore.LoadActivityTimes()
	settleAt := gameEndTime
	if settleAt == 0 {
		settleAt = closeTime
	}
	if !ok || settleAt <= 0 {
		return time.Now().Add(coldDataTTL).UnixMilli()
	}
	return settleAt + commonrank.SettledCacheTTL.Milliseconds()
}

// parseRegistryMember 解析注册表成员 "{bizId}:{deadlineMillis}"。bizId 本身不含冒号
// （格式为 "{bizType}_{actID}" 或 "{bizType}_{actID}_r{N}"），故取最后一个冒号切分是安全的。
func parseRegistryMember(mem string) (bizId string, deadline int64, ok bool) {
	idx := strings.LastIndex(mem, ":")
	if idx <= 0 || idx == len(mem)-1 {
		return "", 0, false
	}
	deadline, err := strconv.ParseInt(mem[idx+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return mem[:idx], deadline, true
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

	// 待办 A：这条降级路径此前只在内存与 Redis 里把活动恢复起来，MongoDB 里始终没有逻辑文档。
	// 补写一次，否则下次重启 syncFromMongo 依旧看不到它，活动会反复"消失"。
	//
	// 只在文档确实缺失时写：文档已存在说明它比 Redis 元数据更权威，
	// 拿降级推断出的轮次去覆盖会让轮号倒退。overallCfg 是逻辑配置（RoundIndex=0、
	// 时间用 Total* 而非本轮窗口），与 periodic.Handler.Register 的落库形状一致。
	if m.dao != nil {
		if _, exists, err := m.dao.LoadRankConfig(logicalKey); err == nil && !exists {
			overallCfg := engine.Config{
				BizType:    string(bizType),
				ActID:      actID,
				OpenTime:   meta.TotalOpenTime,
				CloseTime:  meta.TotalCloseTime,
				RoundIndex: 0,
			}
			if err := FillConfigFromFiles(bizType, &overallCfg); err != nil {
				zaplog.LoggerSugar.Warnf("rank: tryRecoverPeriodicFromRedis fill overall config %s: %v", logicalKey, err)
			}
			if err := m.dao.SaveRankConfigWithPeriodic(logicalKey, overallCfg, saved); err != nil {
				zaplog.LoggerSugar.Warnf("rank: tryRecoverPeriodicFromRedis persist %s: %v", logicalKey, err)
			}
		}
	}
	zaplog.LoggerSugar.Infof("rank: tryRecoverPeriodicFromRedis recovered bizType=%s actID=%d curRound=%d", bizType, actID, curRound)
}

// warmUpAllServices 通过全局协程池并发调用所有已注册服务的 WarmUp（见 docs/rank_optimization.md 第 05 条）。
// 批量场景需要汇总完成，走 SubmitWait + WaitGroup（与 tickServices 同构）。
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
		s := svc
		wg.Add(1)
		if err := gpool.Global.SubmitWait(ctx, func() {
			defer wg.Done()
			s.WarmUp(ctx)
		}); err != nil {
			wg.Done() // 提交失败必须配平，否则 wg.Wait() 永久阻塞
			zaplog.LoggerSugar.Warnf("rank: warmup submit failed: %v", err)
		}
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
				}
				m.mu.Unlock()

				m.periodicHandler.RemoveState(bizKey)

				if ok {
					zaplog.LoggerSugar.Infof("rank: subscribeDeleteEvents received delete bizKey=%s, cleaning up", bizKey)
					m.cleanupServiceData(svc, bizType, actID)
				}
			}
		}
		ps.Close()
	}
}

// activityStillOpen 判断活动是否仍在进行中：CloseTime==0 为常驻活动，now < CloseTime 为未到关闭时间。
// syncFromMongo 用它区分 MongoDB 缺失配置的两种成因——仍在进行中只可能是异步写被丢弃（应修复），
// 已结束则视为 GM 已删除（应删除）。删除不可逆而修复幂等自愈，见 docs/rank_optimization.md 第 09 条。
func activityStillOpen(cfg engine.Config, now int64) bool {
	return cfg.CloseTime == 0 || now < cfg.CloseTime
}

// syncFromMongo 从 MongoDB 同步活动列表：新增本节点缺少的、修复/移除 MongoDB 中缺失配置的。
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

		// 前置检查：服务已存在时快速路径，同时防止 MongoDB 旧文档污染内存 periodic 状态。
		// 必须在 periodic 状态恢复之前执行，以正确处理类型 2→1 切换场景：
		// GM 删除后异步 MongoDB 删除尚未落盘，此时旧文档仍含 Periodic 字段，
		// 若此节点已通过 Redis 广播注册了新的一次性服务，不能用旧 Periodic 覆盖。
		m.mu.RLock()
		existingSvc, exists := m.engineServices[key]
		m.mu.RUnlock()

		// 若文档含周期状态，恢复 PeriodicState（仅有效期内的活动）。
		// 当本节点的内存状态落后于 MongoDB（另一节点已推进了轮次）时，强制刷新。
		// 若服务已存在且为一次性（RoundIndex==0），跳过 periodic 状态恢复，
		// 防止类型从 2→1 切换后 MongoDB 旧文档（异步删除尚未落盘）污染内存状态。
		if doc.Periodic != nil {
			if !exists || (existingSvc != nil && existingSvc.GetConfig().RoundIndex > 0) {
				existing := m.periodicHandler.GetState(key)
				if existing == nil || existing.GetCurrentRound() < doc.Periodic.CurrentRound {
					ps := periodic.StateFromSaved(string(bizType), cfg.ActID, *doc.Periodic)
					m.periodicHandler.SetState(key, ps)
					// 清理历史轮次残留的 Redis 热数据（重启后 in-memory timer 已丢失）。
					m.periodicHandler.CleanupHistoricalRounds(ps)
				}
			}
		}

		if exists {
			// 周期排行榜：检测是否另一节点已推进轮次，本节点服务落后需替换。
			// syncFromMongo 已刷新了 PeriodicState，但 engineServices[key] 仍指向旧轮次服务，
			// 导致 canUpdateScore 用旧 CloseTime 判断，误报 ErrInstanceClosed。
			if doc.Periodic != nil && existingSvc != nil {
				curIdx := existingSvc.GetConfig().RoundIndex
				if curIdx > 0 && curIdx < doc.Periodic.CurrentRound {
					existingCfg := existingSvc.GetConfig()
					roundCfg := existingCfg
					roundCfg.OpenTime = doc.Periodic.RoundOpenTime
					roundCfg.CloseTime = doc.Periodic.RoundCloseTime
					roundCfg.GameEndTime = doc.Periodic.RoundCloseTime
					roundCfg.RoundIndex = doc.Periodic.CurrentRound
					roundCfg.CreateTime = doc.Periodic.RoundOpenTime
					if newSvc, replErr := m.ReplaceRoundService(ctx, string(bizType), key, roundCfg); replErr != nil {
						zaplog.LoggerSugar.Warnf("rank: syncFromMongo replace stale periodic service %s round %d→%d: %v",
							key, curIdx, doc.Periodic.CurrentRound, replErr)
					} else {
						zaplog.LoggerSugar.Infof("rank: syncFromMongo replaced stale periodic service %s round %d→%d",
							key, curIdx, doc.Periodic.CurrentRound)
						localNewSvc := newSvc
						// 单点 WarmUp 可丢弃：池满时由后续懒加载/下一轮 syncLoop 兜底。
						if err := gpool.Global.Submit(func() { localNewSvc.WarmUp(context.Background()) }); err != nil {
							zaplog.LoggerSugar.Warnf("rank: syncFromMongo warmup submit failed key=%s: %v", key, err)
						}
					}
				}
			}
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

		def := rankDefFor(bizType, cfg)
		if err := m.rankService.RegisterRank(ctx, def); err != nil {
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

		service, err := engine.NewService(m.rankService, cfg, m.rdb, m.dao,
			engine.WithOnMemberJoin(onMemberJoin), engine.WithRankDef(def))
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

	// 「不在 MongoDB」有两种成因：异步写被丢弃（活动仍在进行）或 GM 已删除（活动已结束）。
	// 猜错的后果不对称：误删不可逆，误修复只是幂等地把内存态写回 MongoDB，下一轮还能再判一次。
	// 因此按「活动是否仍在进行」区分，而不是见不到就删——见 docs/rank_optimization.md 第 09 条。
	type repairEntry struct {
		key string
		cfg engine.Config
	}
	repaired := 0
	var toRepair []repairEntry
	var toDelete []string
	m.mu.RLock()
	for key := range existingKeys {
		if _, inMongo := mongoKeys[key]; inMongo {
			continue
		}
		svc, stillExists := m.engineServices[key]
		if !stillExists {
			continue
		}
		cfg := svc.GetConfig()
		if activityStillOpen(cfg, now) {
			// 活动仍在进行：MongoDB 缺失只可能是异步写被丢弃，绝不能当作已删除处理，
			// 否则会与 tryRecoverPeriodicFromRedis / applyRankConfigDoc 的恢复路径形成
			// 「30 秒后误删 → 下次又恢复 → 再次误删」的振荡。
			toRepair = append(toRepair, repairEntry{key: key, cfg: cfg})
			continue
		}
		toDelete = append(toDelete, key)
	}
	m.mu.RUnlock()

	for _, r := range toRepair {
		zaplog.LoggerSugar.Warnf("rank: config %s missing in mongo, repairing (activity still open)", r.key)
		if err := m.dao.SaveRankConfig(r.key, r.cfg); err != nil {
			zaplog.LoggerSugar.Errorf("rank: repair rank config %s failed: %v", r.key, err)
			continue
		}
		repaired++
	}

	type removedEntry struct {
		svc     *engine.Service
		bizType BizType
		actID   int32
	}
	removedSvcs := make([]removedEntry, 0, len(toDelete))
	m.mu.Lock()
	for _, key := range toDelete {
		if svc, ok := m.engineServices[key]; ok {
			cfg := svc.GetConfig()
			removedSvcs = append(removedSvcs, removedEntry{
				svc:     svc,
				bizType: BizType(cfg.BizType),
				actID:   cfg.ActID,
			})
			delete(m.services, key)
			delete(m.engineServices, key)
		}
	}
	m.mu.Unlock()

	for _, e := range removedSvcs {
		// 活动已结束且 MongoDB 无记录：视为已删除，走统一清理（含此前漏掉的 svc.Cleanup()）。
		m.cleanupServiceData(e.svc, e.bizType, e.actID)
		zaplog.LoggerSugar.Infof("rank: syncFromMongo removed service bizType=%s actID=%d (activity ended, config not in mongo)", e.bizType, e.actID)
	}

	if added > 0 || repaired > 0 || len(removedSvcs) > 0 {
		zaplog.LoggerSugar.Infof("rank: sync from mongo completed, added=%d repaired=%d removed=%d total=%d",
			added, repaired, len(removedSvcs), len(mongoKeys))
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

	// 落库用的快照必须在下面为「当前轮」改写时间字段**之前**取。
	// SaveRankConfig 只做 $set:{config:...}，若把改写后的 cfg 写进去，逻辑配置会被永久
	// 钉在某一轮的时间窗口上（periodic.Handler.Register 落库时正是显式把 RoundIndex 归零的）。
	persistedCfg := cfg
	persistedCfg.RoundIndex = 0

	if doc.Periodic != nil {
		cfg.OpenTime = doc.Periodic.RoundOpenTime
		cfg.CloseTime = doc.Periodic.RoundCloseTime
		cfg.GameEndTime = doc.Periodic.RoundCloseTime
		cfg.RoundIndex = doc.Periodic.CurrentRound
	}

	def := rankDefFor(bizType, cfg)
	if err := m.rankService.RegisterRank(ctx, def); err != nil {
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

	service, err := engine.NewService(m.rankService, cfg, m.rdb, m.dao,
		engine.WithOnMemberJoin(onMemberJoin), engine.WithRankDef(def))
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

	// 待办 A：只有本节点真的新建了服务才需要补这次落库。广播路径收到的配置此前只活在内存里，
	// 创建节点的写入一旦被丢弃，下一次 syncFromMongo 就会把它当成孤儿走删除分支
	//（serve 端不会为「创建的广播」再写一次 Mongo）。
	// 周期活动写 in-memory state 而不是 doc.Periodic：doc 可能是旧的，
	// 把过期的轮次状态写回去会让轮号倒退。
	if !dup && m.dao != nil {
		if state := m.periodicHandler.GetState(key); state != nil {
			if err := m.dao.SaveRankConfigWithPeriodic(key, persistedCfg, state.ToSavedState()); err != nil {
				zaplog.LoggerSugar.Warnf("rank: applyRankConfigDoc persist periodic config %s: %v", key, err)
			}
		} else if err := m.dao.SaveRankConfig(key, persistedCfg); err != nil {
			zaplog.LoggerSugar.Warnf("rank: applyRankConfigDoc persist config %s: %v", key, err)
		}
	}
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
