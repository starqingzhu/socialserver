package engine

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"common/rank"
	commonrank "common/rank"
	rediskeys "common/redis"
	goredis "golib/redis"
	"golib/zaplog"
)

// Service 通用排行榜业务服务。
// 每个活动对应一个 Service 实例，内部按人数上限自动分组，
// 同一分组共享一个底层榜单实例。
//
// 无状态设计：所有运行时状态（分组、成员映射、机器人）均持久化到 Redis，
// 任意节点可服务任意玩家，节点重启或 hash 偏移后自动从 Redis 恢复。
type Service struct {
	// mu 保护 config / groups / memberGroup / settledGroup 等权威内存状态。
	//
	// 用 RWMutex 而非 Mutex：纯读路径（GetMemberGroupID）只需读一个 map，用写锁会让
	// GM 的批量查询与 UpsertScore（持写锁，内含 Redis 往返）无谓串行化（待办 G-d3）。
	// 其余调用点全部只用 Lock/Unlock，语义不变。
	mu          sync.RWMutex
	config      Config
	rankService rank.Service
	store       *Store

	// 内存缓存（从 Redis 加载，写入时同步回 Redis）
	groups       []*Group
	memberGroup  map[int64]int32
	settledGroup map[int32][]rank.RankMemberSnapshot

	nextGroupID  int32
	onMemberJoin func(userID int64, groupID int32)
	loaded       atomic.Bool

	// rankDef 是构造期注入的 rank:def 原始定义，构造后只读。
	// 它不参与任何存储结构，只作为 rank:def 到期后无损重建的数据源（缺陷 1）。
	rankDef commonrank.Rank

	// tierMap/infoMap 由 config.RobotTiers/RobotInfos 预建，供 findTier/robotAvatarInfo
	// 做 O(1) 查找，替代机器人 tick（每秒×每机器人）时的线性扫描（第 12 条）。
	// 构造后只读：RobotTiers/RobotInfos 当前不会被 UpdateConfig 修改。
	tierMap map[int32]*RobotTierCfg
	infoMap map[int64]*RobotInfoEntry

	// cacheMu 保护以下几个与 s.mu（分组/成员的权威内存状态）无关的"软缓存"：
	// 它们全部允许在有效期内返回陈旧数据，因此单独用一把锁，不与 s.mu 抢占。
	cacheMu sync.RWMutex

	// instanceStates 记录各 instanceID 上次向 Redis 确认存在的时间，供 ensureGroupInstance
	// 跳过冗余 GetInstance（第 03 条）。
	instanceStates map[string]groupInstanceState

	// groupsCache/groupsCacheExpiry 是 tickAllRobots 专用的分组列表缓存（TTL≈2s），
	// 仅用于降低机器人 tick 路径的 LoadGroups 调用频率；UpsertScore 路径的
	// ensureGroupLocked 必须实时读 Redis，不得复用（第 02 条）。
	groupsCache       []*Group
	groupsCacheExpiry time.Time

	// robotsCache 是 tickAllRobots 专用的按分组机器人列表缓存（TTL≈2s）。
	// 缓存内容是 *robotState 指针，tickGroupRobots 对字段的原地修改会直接反映到缓存里，
	// 因此不需要在 SaveRobots 之后显式回写（第 02 条）。
	robotsCache map[int32]robotsCacheEntry

	// scoreCache 缓存每个分组的榜一分数 / 真实玩家榜一分数（TTL≈2s），
	// 把 tickGroupRobots 里唯一未被覆盖的 Range(0,-1) 全量读也纳入同一套 Cache-Aside
	// （第 02 条「遗漏的最大一项」，采用"减少读次"方向）。
	scoreCache map[int32]groupScoreCacheEntry

	// settledAt 记录本节点已完成结算的 settleAt 值；0 表示未结算。
	// 用 settleAt 而非 bool：GM 改配置把 settleAt 推后时自动失效，无需显式重置
	// （见 docs/rank_optimization.md 第 06 条 / 必改-1、必改-4）。
	settledAt atomic.Int64

	// activityEnd 是 effectiveSettleAt() 的原子副本（同一事实来源），供 Store 的 TTL 计算
	// 在 s.mu 之外无竞争读取：懒加载回填可能发生在 UpsertScore（持锁）之外，而 UpdateConfig
	// 会持锁改写 CloseTime/GameEndTime，闭包直接读 s.config 就是一处新的数据竞争。
	// 必须在持有 s.mu 时调用 refreshActivityEndLocked 同步。
	activityEnd atomic.Int64
}

// refreshActivityEndLocked 刷新 activityEnd 原子副本。必须在持有 s.mu 时调用
// （NewService 构造期 / UpdateConfig 尾部），这是 s.config 时间字段仅有的两个写点。
func (s *Service) refreshActivityEndLocked() {
	s.activityEnd.Store(settleAtOf(s.config.CloseTime, s.config.GameEndTime))
}

func NewService(rankService rank.Service, config Config, rdb *goredis.Redis, dao *DAO, opts ...Option) (*Service, error) {
	if rankService == nil || config.RankCode == "" || config.RankPeopleNum <= 0 {
		return nil, rank.ErrInvalidRankSpec
	}
	s := &Service{
		config:       config,
		rankService:  rankService,
		memberGroup:  make(map[int64]int32),
		settledGroup: make(map[int32][]rank.RankMemberSnapshot),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.refreshActivityEndLocked()
	s.store = NewStore(rdb, dao, config.computeBizId(), s.effectiveSettleAt)
	s.buildRobotLookupMaps()
	s.store.SaveActivityTimes(config.OpenTime, config.CloseTime, config.GameEndTime)
	return s, nil
}

// buildRobotLookupMaps 预建 tierMap/infoMap，供 findTier/robotAvatarInfo O(1) 查找。
func (s *Service) buildRobotLookupMaps() {
	if len(s.config.RobotTiers) > 0 {
		m := make(map[int32]*RobotTierCfg, len(s.config.RobotTiers))
		for i := range s.config.RobotTiers {
			m[s.config.RobotTiers[i].TierID] = &s.config.RobotTiers[i]
		}
		s.tierMap = m
	}
	if len(s.config.RobotInfos) > 0 {
		m := make(map[int64]*RobotInfoEntry, len(s.config.RobotInfos))
		for i := range s.config.RobotInfos {
			m[s.config.RobotInfos[i].InfoID] = &s.config.RobotInfos[i]
		}
		s.infoMap = m
	}
}

// ensureLoaded 首次调用时从 Redis/MongoDB 加载运行时状态，并在 Redis 被清理后完整恢复。
// 必须在持有 s.mu 锁时调用。
func (s *Service) ensureLoaded() {
	if s.loaded.Load() || !s.store.available() {
		return
	}
	s.loaded.Store(true)

	if groups, err := s.store.LoadGroups(); err == nil && len(groups) > 0 {
		s.groups = groups
		sort.Slice(s.groups, func(i, j int) bool { return s.groups[i].GroupID < s.groups[j].GroupID })
		for _, g := range s.groups {
			if g.GroupID > s.nextGroupID {
				s.nextGroupID = g.GroupID
			}
		}
		s.store.RestoreNextGroupID(s.nextGroupID)
	}

	if members, err := s.store.GetAllMembers(); err == nil {
		for uid, gid := range members {
			s.memberGroup[uid] = gid
		}
		if s.onMemberJoin != nil {
			for uid, gid := range s.memberGroup {
				s.onMemberJoin(uid, gid)
			}
		}
	}

	for _, g := range s.groups {
		if ids, err := s.store.LoadUsedInfoIDs(g.GroupID); err == nil && len(ids) == 0 {
			if robots, err2 := s.store.LoadRobots(g.GroupID); err2 == nil && len(robots) > 0 {
				idSet := make(map[int64]struct{}, len(robots))
				for _, r := range robots {
					idSet[r.InfoID] = struct{}{}
				}
				_ = s.store.SaveUsedInfoIDs(g.GroupID, idSet)
			}
		}
	}

	ctx := context.Background()
	for _, g := range s.groups {
		instanceID := s.groupInstanceID(g.GroupID)

		mbExists, _ := s.store.RdbExists(rediskeys.GetRankMbKey(instanceID))
		mongoScores, _ := s.store.LoadGroupScores(g.GroupID)

		if mbExists {
			if instExists, _ := s.store.RdbExists(rediskeys.GetRankInstKey(instanceID)); !instExists {
				instToRestore := rank.RankInstance{
					InstanceId:  instanceID,
					RankCode:    s.config.RankCode,
					BizId:       s.bizId(),
					State:       rank.InstanceStateOpen,
					OpenTime:    s.config.OpenTime,
					CloseTime:   s.config.CloseTime,
					GameEndTime: s.config.GameEndTime,
					CreateTime:  s.config.OpenTime,
					UpdateTime:  s.config.OpenTime,
				}
				if mongoInst, err2 := s.store.LoadGroupInst(g.GroupID); err2 == nil && mongoInst != nil {
					instToRestore = *mongoInst
				}
				_ = s.rankService.RestoreInstance(ctx, instToRestore)
				zaplog.LoggerSugar.Infof("rank engine: restored missing rank:inst for group %d instanceID=%s", g.GroupID, instanceID)
			}

			needBackfill := len(mongoScores) == 0
			if needBackfill {
				if snaps, snapErr := s.rankService.Snapshot(ctx, instanceID); snapErr == nil {
					for _, snap := range snaps {
						if IsRobotMemberID(snap.MemberId) {
							continue
						}
						et := snap.EnterTime
						if et == 0 {
							et = snap.UpdateTime
						}
						if wErr := s.store.SaveScore(g.GroupID, snap.MemberId, snap.Score, et, snap.Sequence, snap.UpdateTime, snap.AvatarInfo); wErr != nil {
							zaplog.LoggerSugar.Warnf("rank engine: backfill score group=%d member=%d: %v", g.GroupID, snap.MemberId, wErr)
						}
					}
				}
			}
		} else {
			robotsForGroup, _ := s.store.LoadRobots(g.GroupID)
			items := make([]rank.RankScoreItem, 0, len(mongoScores)+len(robotsForGroup))

			for _, doc := range mongoScores {
				items = append(items, rank.RankScoreItem{
					MemberId:   doc.UserID,
					Score:      doc.Score,
					AtTime:     doc.UpdateTime,
					EnterTime:  doc.EnterTime,
					Sequence:   doc.Sequence,
					AvatarInfo: doc.AvatarInfo,
				})
			}

			for _, r := range robotsForGroup {
				var ai *rank.AvatarInfo
				for _, info := range s.config.RobotInfos {
					if info.InfoID == r.InfoID {
						ai = &rank.AvatarInfo{UserId: info.InfoID, Name: info.Name, Avatar: info.Avatar, Frame: info.Frame}
						break
					}
				}
				items = append(items, rank.RankScoreItem{
					MemberId:   r.MemberID,
					Score:      r.Score,
					AtTime:     r.LastGrowAt,
					EnterTime:  r.LastGrowAt,
					AvatarInfo: ai,
				})
			}

			if len(items) > 0 {
				if err := s.rankService.RestoreMembers(ctx, instanceID, items); err != nil {
					zaplog.LoggerSugar.Warnf("rank engine: restore rank:mb for group %d: %v", g.GroupID, err)
				}
			}
			if instExists, _ := s.store.RdbExists(rediskeys.GetRankInstKey(instanceID)); !instExists {
				instToRestore := rank.RankInstance{
					InstanceId:  instanceID,
					RankCode:    s.config.RankCode,
					BizId:       s.bizId(),
					State:       rank.InstanceStateOpen,
					OpenTime:    s.config.OpenTime,
					CloseTime:   s.config.CloseTime,
					GameEndTime: s.config.GameEndTime,
					CreateTime:  s.config.OpenTime,
					UpdateTime:  s.config.OpenTime,
					MemberCount: int64(len(items)),
				}
				if mongoInst, err2 := s.store.LoadGroupInst(g.GroupID); err2 == nil && mongoInst != nil {
					instToRestore = *mongoInst
				}
				_ = s.rankService.RestoreInstance(ctx, instToRestore)
			}
		}

		if g.State == GroupStateSettled {
			settledExists, _ := s.store.RdbExists(rediskeys.GetRankSettledKey(instanceID))
			if settledExists {
				if mongoSettled, err := s.store.LoadGroupSettled(g.GroupID); err == nil && len(mongoSettled) == 0 {
					if snaps, snapErr := s.rankService.Snapshot(ctx, instanceID); snapErr == nil && len(snaps) > 0 {
						if wErr := s.store.SaveSettled(g.GroupID, snaps, s.config.GameEndTime); wErr != nil {
							zaplog.LoggerSugar.Warnf("rank engine: backfill settled group=%d: %v", g.GroupID, wErr)
						}
					}
				}
			} else {
				if snaps, err := s.store.LoadGroupSettled(g.GroupID); err == nil && len(snaps) > 0 {
					s.settledGroup[g.GroupID] = snaps
					s.store.RestoreSettled(instanceID, snaps)
					if instExists, _ := s.store.RdbExists(rediskeys.GetRankInstKey(instanceID)); !instExists {
						instToRestore := rank.RankInstance{
							InstanceId:  instanceID,
							RankCode:    s.config.RankCode,
							BizId:       s.bizId(),
							State:       rank.InstanceStateSettled,
							OpenTime:    s.config.OpenTime,
							CloseTime:   s.config.CloseTime,
							GameEndTime: s.config.GameEndTime,
							SettleTime:  s.config.GameEndTime,
							CreateTime:  s.config.OpenTime,
							UpdateTime:  s.config.GameEndTime,
							MemberCount: int64(len(snaps)),
							Version:     1,
						}
						if mongoInst, err2 := s.store.LoadGroupInst(g.GroupID); err2 == nil && mongoInst != nil {
							instToRestore = *mongoInst
						}
						_ = s.rankService.RestoreInstance(ctx, instToRestore)
					}
				}
			}
		}

		// 恢复路径写出的 rank:inst/rank:mb/rank:seq/rank:settled 都不带 TTL（RestoreInstance 用
		// 裸 Set、RestoreMembers 的 Lua 只 HSET），这里补一次，覆盖上面所有分支。
		// 用 backfillTTL() 而不是 writeTTL()：这是"重设已有 key 生命周期"的路径，必须带
		// SettledCacheTTL 下限——活动早已结束（writeTTL 退化为 1 分钟钳制值）时若用 writeTTL，
		// 会把这几个 key 连同 rank:settled 一起在 1 分钟内删掉，正是缺陷 4。
		// 对仍活跃的分组这一步是幂等的（同一个绝对到期时刻），也顺带修复历史上遗留的无 TTL key。
		_ = s.rankService.ExpireInstance(ctx, instanceID, s.store.backfillTTL())
	}
}

// WarmUp 主动触发 ensureLoaded，确保该服务的所有分组数据已从 Redis/MongoDB 恢复。
// 原子快路径：稳态下（已加载）不再获取 s.mu，省去 syncLoop 每 30 秒对每个服务的无谓写锁获取
// （见 docs/rank_optimization.md 第 13 条）。
func (s *Service) WarmUp(ctx context.Context) {
	if s.loaded.Load() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLoaded()
}

// recoverGroupData 在运行期间检测到 rank:mb 被 Redis 驱逐后，从 MongoDB 重建指定分组的排行榜数据。
func (s *Service) recoverGroupData(ctx context.Context, groupID int32, instanceID string) {
	if !s.store.available() || !s.store.hasMongo() {
		return
	}
	mongoScores, err := s.store.LoadGroupScores(groupID)
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: runtime recovery load scores group=%d bizId=%s: %v", groupID, s.bizId(), err)
		return
	}

	robotsCopy, _ := s.store.LoadRobots(groupID)
	robotInfos := s.config.RobotInfos

	items := make([]rank.RankScoreItem, 0, len(mongoScores)+len(robotsCopy))

	for _, doc := range mongoScores {
		items = append(items, rank.RankScoreItem{
			MemberId:   doc.UserID,
			Score:      doc.Score,
			AtTime:     doc.UpdateTime,
			EnterTime:  doc.EnterTime,
			Sequence:   doc.Sequence,
			AvatarInfo: doc.AvatarInfo,
		})
	}
	for _, r := range robotsCopy {
		var ai *rank.AvatarInfo
		for _, info := range robotInfos {
			if info.InfoID == r.InfoID {
				ai = &rank.AvatarInfo{UserId: info.InfoID, Name: info.Name, Avatar: info.Avatar, Frame: info.Frame}
				break
			}
		}
		items = append(items, rank.RankScoreItem{
			MemberId:   r.MemberID,
			Score:      r.Score,
			AtTime:     r.LastGrowAt,
			EnterTime:  r.LastGrowAt,
			AvatarInfo: ai,
		})
	}

	if len(items) > 0 {
		if restoreErr := s.rankService.RestoreMembers(ctx, instanceID, items); restoreErr != nil {
			zaplog.LoggerSugar.Warnf("rank engine: runtime recover rank:mb group=%d bizId=%s: %v", groupID, s.bizId(), restoreErr)
			return
		}
	}

	if instExists, _ := s.store.RdbExists(rediskeys.GetRankInstKey(instanceID)); !instExists {
		instToRestore := rank.RankInstance{
			InstanceId:  instanceID,
			RankCode:    s.config.RankCode,
			BizId:       s.bizId(),
			State:       rank.InstanceStateOpen,
			OpenTime:    s.config.OpenTime,
			CloseTime:   s.config.CloseTime,
			GameEndTime: s.config.GameEndTime,
			CreateTime:  s.config.OpenTime,
			UpdateTime:  s.config.OpenTime,
			MemberCount: int64(len(items)),
		}
		if mongoInst, err2 := s.store.LoadGroupInst(groupID); err2 == nil && mongoInst != nil {
			instToRestore = *mongoInst
		}
		_ = s.rankService.RestoreInstance(ctx, instToRestore)
	}

	// 与 ensureLoaded 同理：恢复写出的 rank:inst/rank:mb/rank:seq 都不带 TTL，必须补设，
	// 且必须用 backfillTTL()（带 14d 下限），否则活动已结束时会把刚恢复的数据立刻删掉。
	_ = s.rankService.ExpireInstance(ctx, instanceID, s.store.backfillTTL())

	zaplog.LoggerSugar.Infof("rank engine: runtime recovered rank:mb group=%d bizId=%s members=%d", groupID, s.bizId(), len(items))
}

func (s *Service) stateAt(now int64) rank.InstanceStateType {
	if s.config.OpenTime > 0 && now < s.config.OpenTime {
		return rank.InstanceStateInit
	}
	if s.config.CloseTime > 0 && now >= s.config.CloseTime {
		return rank.InstanceStateClosed
	}
	return rank.InstanceStateOpen
}

func (s *Service) canUpdateScore(now int64) rank.InstanceStateType {
	if s.config.OpenTime > 0 && now < s.config.OpenTime {
		return rank.InstanceStateInit
	}
	if s.config.CloseTime > 0 && now >= s.config.CloseTime {
		return rank.InstanceStateClosed
	}
	if now > s.config.GameEndTime && s.config.GameEndTime > 0 {
		return rank.InstanceStateSettled
	}
	return rank.InstanceStateOpen
}

// effectiveSettleAt 返回本活动的结算时间点：优先 GameEndTime，否则退化为 CloseTime。
// 读的是原子副本，因此可以在 s.mu 之外安全调用（Store 的 TTL 闭包正是这样做的）。
func (s *Service) effectiveSettleAt() int64 { return s.activityEnd.Load() }

// Tick 推进活动时间轴：驱动机器人积分增长，到达 GameEndTime 后自动结算。
func (s *Service) Tick(ctx context.Context, now int64) error {
	s.mu.Lock()
	s.ensureLoaded()
	s.mu.Unlock()

	settleAt := s.effectiveSettleAt()
	if settleAt <= 0 {
		// CloseTime 和 GameEndTime 均未配置：这是非法配置（docs/rank_optimization.md 待办 K），
		// 而不是"常驻活动"。创建入口已校验拒绝此配置，这里只是兜底：显式跳过而不是依赖
		// settledAt 零值巧合短路，避免该行为在未来重构中被意外破坏。
		zaplog.LoggerSugar.Warnf("rank engine: service rankCode=%s has settleAt<=0 (CloseTime=%d GameEndTime=%d), skipping tick",
			s.config.RankCode, s.config.CloseTime, s.config.GameEndTime)
		return nil
	}
	if s.settledAt.Load() == settleAt {
		// 已按当前 settleAt 结算完毕，跳过后续全部 IO（含 LoadGroups）。
		return nil
	}

	if s.config.hasRobots() && now < settleAt {
		// Multi-node guard: only one node ticks robots per second per service.
		if s.store.TryLockRobotTick(now) {
			s.tickAllRobots(ctx, now)
		}
	}
	if now < settleAt {
		return nil
	}

	groups, err := s.store.LoadGroups()
	if err != nil || len(groups) == 0 {
		s.mu.Lock()
		groups = make([]*Group, len(s.groups))
		copy(groups, s.groups)
		s.mu.Unlock()
	}
	for _, g := range groups {
		if g == nil || g.State == GroupStateSettled {
			continue
		}
		if err := s.rankService.CloseInstance(ctx, g.InstanceID, settleAt); err != nil && err != rank.ErrInstanceNotFound {
			zaplog.LoggerSugar.Warnf("rank engine: tick close instance group=%d: %v", g.GroupID, err)
		}
	}

	_, err = s.Settle(ctx)
	return err
}

// UpsertScore 写入用户得分，自动分组，首位真实玩家进入时生成机器人。
func (s *Service) UpsertScore(ctx context.Context, userID int64, totalScore int64, now int64, avatarInfo *rank.AvatarInfo) error {
	switch s.canUpdateScore(now) {
	case rank.InstanceStateOpen:
		// ok
	case rank.InstanceStateClosed, rank.InstanceStateSettled:
		return rank.ErrInstanceClosed
	default:
		return rank.ErrInstanceNotOpen
	}

	s.mu.Lock()
	s.ensureLoaded()

	if _, ok := s.memberGroup[userID]; !ok {
		if gid, found, _ := s.store.GetMember(userID); found {
			s.memberGroup[userID] = gid
		}
	}

	isNewMember := false
	needSpawnRobots := false
	spawnCapacity := int32(0)
	if _, ok := s.memberGroup[userID]; !ok {
		group, err := s.ensureGroupLocked()
		if err != nil {
			s.mu.Unlock()
			return err
		}
		isNewMember = true
		s.memberGroup[userID] = group.GroupID
		newCount, _ := s.store.IncrRealCount(group)
		group.RealCount = newCount
		_ = s.store.SetMember(userID, group.GroupID)
		if s.onMemberJoin != nil {
			s.onMemberJoin(userID, group.GroupID)
		}

		needSpawnRobots = group.RealCount == 1 && s.config.hasRobots()
		spawnCapacity = s.config.RankPeopleNum - group.RealCount
		if needSpawnRobots {
			plan := buildRobotSpawnPlan(s.config.RobotTiers, spawnCapacity)
			group.RobotCount = totalRobotsInPlan(plan)
			_ = s.store.SaveGroup(group)
		}
		if group.totalCount() >= s.config.RankPeopleNum {
			group.State = GroupStateFull
			_ = s.store.SaveGroup(group)
		}
	}
	groupID := s.memberGroup[userID]

	instanceID := s.groupInstanceID(groupID)
	alreadyLoaded := s.loaded.Load()
	s.mu.Unlock()

	if alreadyLoaded && s.store.available() {
		if mbExists, _ := s.store.RdbExists(rediskeys.GetRankMbKey(instanceID)); !mbExists {
			s.recoverGroupData(ctx, groupID, instanceID)
		}
	}

	if err := s.ensureGroupInstance(ctx, instanceID, groupID, now); err != nil {
		return err
	}
	if err := s.rankService.BatchUpsertScore(ctx, instanceID, []rank.RankScoreItem{{
		MemberId:   userID,
		Score:      totalScore,
		AtTime:     now,
		EnterTime:  now,
		AvatarInfo: avatarInfo,
	}}); err != nil {
		return err
	}

	enterTimeForMongo := int64(0)
	seqForMongo := int64(0)
	if isNewMember {
		enterTimeForMongo = now
		if snap, snapErr := s.rankService.GetMemRank(ctx, instanceID, userID); snapErr == nil && snap != nil {
			seqForMongo = snap.Sequence
		}
	}
	if err := s.store.SaveScore(groupID, userID, totalScore, enterTimeForMongo, seqForMongo, now, avatarInfo); err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: save score to mongo group=%d user=%d: %v", groupID, userID, err)
	}

	if needSpawnRobots {
		if err := s.spawnRobotsForGroup(ctx, groupID, spawnCapacity, now); err != nil {
			zaplog.LoggerSugar.Warnf("rank engine: spawn robots for group %d failed: %v", groupID, err)
		}
	}

	return nil
}

// resolveSettled 解析分组的已结算快照：cached 是调用方持锁时读到的内存缓存，命中直接返回；
// 未命中时按分组当前状态（优先查 Store，缺失时退化到内存 s.groups）判断是否需要从
// Redis/MongoDB 加载并回填缓存。ListGroupRank 与 GetMemberRank 共用（第 10 条）。
func (s *Service) resolveSettled(groupID int32, cached []rank.RankMemberSnapshot) []rank.RankMemberSnapshot {
	if len(cached) > 0 {
		return cached
	}
	instanceID := s.groupInstanceID(groupID)
	if g, _ := s.store.LoadGroupByID(groupID); g != nil {
		if g.State == GroupStateSettled {
			return s.loadAndCacheSettled(groupID, g.InstanceID)
		}
		return nil
	}
	needLoad := false
	s.mu.Lock()
	for _, mg := range s.groups {
		if mg != nil && mg.GroupID == groupID {
			if mg.State == GroupStateSettled {
				cached = cloneSnapshots(s.settledGroup[groupID])
				if len(cached) == 0 {
					needLoad = true
				}
			}
			break
		}
	}
	s.mu.Unlock()
	if needLoad {
		return s.loadAndCacheSettled(groupID, instanceID)
	}
	return cached
}

// ListGroupRank 查询指定分组的排行榜区间（0-based 闭区间）。
func (s *Service) ListGroupRank(ctx context.Context, groupID int32, start int64, end int64) ([]rank.RankMemberSnapshot, error) {
	s.mu.Lock()
	s.ensureLoaded()
	settled := cloneSnapshots(s.settledGroup[groupID])
	s.mu.Unlock()

	settled = s.resolveSettled(groupID, settled)

	if len(settled) > 0 {
		return sliceSnapshots(settled, start, end), nil
	}

	instanceID := s.groupInstanceID(groupID)
	members, err := s.rankService.Range(ctx, instanceID, start, end)
	if err != nil {
		if err == rank.ErrInstanceNotFound {
			s.recoverGroupData(ctx, groupID, instanceID)
			members, err = s.rankService.Range(ctx, instanceID, start, end)
		}
		if err != nil {
			return nil, err
		}
	}
	return members, nil
}

// GetMemberRank 查询指定用户的名次快照及所在分组。
func (s *Service) GetMemberRank(ctx context.Context, userID int64) (*rank.RankMemberSnapshot, int32, error) {
	s.mu.Lock()
	s.ensureLoaded()
	groupID, ok := s.memberGroup[userID]
	if !ok {
		if gid, found, _ := s.store.GetMember(userID); found {
			s.memberGroup[userID] = gid
			groupID = gid
			ok = true
		}
	}
	settled := cloneSnapshots(s.settledGroup[groupID])
	s.mu.Unlock()

	if !ok {
		return nil, 0, nil
	}

	settled = s.resolveSettled(groupID, settled)

	if len(settled) > 0 {
		for _, snap := range settled {
			if snap.MemberId == userID {
				c := snap
				if c.AvatarInfo != nil {
					cp := *c.AvatarInfo
					c.AvatarInfo = &cp
				}
				return &c, groupID, nil
			}
		}
		return nil, groupID, nil
	}

	instanceID := s.groupInstanceID(groupID)
	snapshot, err := s.rankService.GetMemRank(ctx, instanceID, userID)
	if err != nil {
		if err == rank.ErrInstanceNotFound {
			s.recoverGroupData(ctx, groupID, instanceID)
			snapshot, err = s.rankService.GetMemRank(ctx, instanceID, userID)
		}
		if err != nil {
			return nil, 0, err
		}
	}
	return snapshot, groupID, nil
}

func (s *Service) loadAndCacheSettled(groupID int32, instanceID string) []rank.RankMemberSnapshot {
	ctx := context.Background()
	if snaps, err := s.rankService.Snapshot(ctx, instanceID); err == nil && len(snaps) > 0 {
		s.mu.Lock()
		if s.settledGroup[groupID] == nil {
			s.settledGroup[groupID] = cloneSnapshots(snaps)
		}
		cached := cloneSnapshots(s.settledGroup[groupID])
		s.mu.Unlock()
		return cached
	}
	if snaps, err := s.store.LoadGroupSettled(groupID); err == nil && len(snaps) > 0 {
		s.mu.Lock()
		if s.settledGroup[groupID] == nil {
			s.settledGroup[groupID] = cloneSnapshots(snaps)
		}
		cached := cloneSnapshots(s.settledGroup[groupID])
		s.mu.Unlock()
		s.store.RestoreSettled(instanceID, snaps)
		return cached
	}
	s.recoverGroupData(ctx, groupID, instanceID)
	if snaps, err := s.rankService.Snapshot(ctx, instanceID); err == nil && len(snaps) > 0 {
		s.store.RestoreSettled(instanceID, snaps)
		s.mu.Lock()
		if s.settledGroup[groupID] == nil {
			s.settledGroup[groupID] = cloneSnapshots(snaps)
		}
		cached := cloneSnapshots(s.settledGroup[groupID])
		s.mu.Unlock()
		return cached
	}
	return nil
}

// Settle 对所有未结算分组执行最终结算，返回各分组快照。幂等。
func (s *Service) Settle(ctx context.Context) (map[int32][]rank.RankMemberSnapshot, error) {
	settleAt := s.effectiveSettleAt()
	if s.settledAt.Load() == settleAt {
		// 已按当前 settleAt 结算完毕，跳过 LoadGroups（消除已结算后的冗余 HGETALL）。
		return nil, nil
	}

	groups, err := s.store.LoadGroups()
	if err != nil || len(groups) == 0 {
		s.mu.Lock()
		s.ensureLoaded()
		groups = make([]*Group, len(s.groups))
		copy(groups, s.groups)
		s.mu.Unlock()
	}

	results := make(map[int32][]rank.RankMemberSnapshot, len(groups))
	// settledGroups 收集本轮「处于已结算状态」的全部分组，循环结束后统一设 TTL（缺陷 6）。
	// 必须同时覆盖三条路径，少一条就会留下永久 key：
	//   - 循环入口就已 settled 的分组（崩溃窗口：上个进程置了状态但没来得及设 TTL）；
	//   - ErrInstanceNotFound 分支（Redis 实例已丢，最容易留永久 key 的一条）；
	//   - 正常结算分支。
	settledGroups := make([]*Group, 0, len(groups))
	for _, group := range groups {
		if group == nil {
			continue
		}
		if group.State == GroupStateSettled {
			settledGroups = append(settledGroups, group)
			continue
		}
		// Multi-node guard: only one node settles each group.
		if !s.store.TryLockSettle(group.GroupID) {
			continue
		}
		if err := s.rankService.CloseInstance(ctx, group.InstanceID, settleAt); err != nil && err != rank.ErrInstanceNotFound {
			return nil, err
		}
		members, err := s.rankService.SettleInstance(ctx, group.InstanceID, settleAt)
		if err != nil {
			if err == rank.ErrInstanceNotFound {
				group.State = GroupStateSettled
				_ = s.store.SaveGroup(group)
				s.mu.Lock()
				s.syncGroupStateLocked(group)
				s.mu.Unlock()
				settledGroups = append(settledGroups, group)
				continue
			}
			return nil, err
		}
		// members 只在此处克隆一次并在三处共享同一份副本（results 返回给调用方只读、
		// settledGroup 是内存缓存只读、SaveSettled 是异步队列 + 只读的 json.Marshal），
		// 避免同一次结算产生三份相同的 snapshot 副本（第 11 条）。
		snapshot := cloneSnapshots(members)
		results[group.GroupID] = snapshot

		group.State = GroupStateSettled
		_ = s.store.SaveGroup(group)

		s.mu.Lock()
		s.syncGroupStateLocked(group)
		s.settledGroup[group.GroupID] = snapshot
		s.mu.Unlock()

		if err := s.store.SaveSettled(group.GroupID, snapshot, settleAt); err != nil {
			zaplog.LoggerSugar.Warnf("rank engine: save settled to mongo group=%d: %v", group.GroupID, err)
		}
		if inst, instErr := s.rankService.GetInstance(ctx, group.InstanceID); instErr == nil && inst != nil {
			_ = s.store.SaveRankInst(group.GroupID, *inst)
		}
		settledGroups = append(settledGroups, group)
	}

	// 已结算分组的活跃期数据在这里统一收敛到「结算时刻 + 保留期」。
	// 用 backfillTTL()（带 14d 下限）而不是 ttlFor(settleAt)：结算可能晚于 settleAt 很久
	// （崩溃恢复补结算），按绝对时刻算会得到已过去的时间，反而把刚结算的数据立刻删掉。
	// 重复设 TTL 永不缩短（同一活动算出同一个绝对时刻，且下限保证不短于 14d），因此 Settle 幂等。
	// 注意只对 settledGroups 设——绝不能转调 s.CleanupLiveData()，那会把尚未结算的分组也一起设上 TTL。
	if len(settledGroups) > 0 {
		ttl := s.store.backfillTTL()
		for _, g := range settledGroups {
			instanceID := s.groupInstanceID(g.GroupID)
			if err := s.rankService.ExpireInstance(ctx, instanceID, ttl); err != nil {
				zaplog.LoggerSugar.Warnf("rank engine: settle expire instance %s: %v", instanceID, err)
			}
			s.invalidateInstanceState(instanceID)
		}
		s.store.ExpireLiveData(settledGroups, ttl)
	}

	// 置位必须同时满足两个条件，缺一不可（见 docs/rank_optimization.md 必改-1、必改-4）：
	//   ① 活动已对写入关闭（canUpdateScore != Open）—— 否则 UpsertScore 仍可能创建新分组；
	//   ② 放在循环之后而非某个分组的成功分支内 —— 否则抢锁失败的节点永远置不上位，
	//      短路只在赢得锁的那个节点生效，其余节点仍会一直付出全量 IO。
	if s.canUpdateScore(time.Now().UnixMilli()) != rank.InstanceStateOpen {
		s.settledAt.Store(settleAt)
	}
	return results, nil
}

func (s *Service) syncGroupStateLocked(updated *Group) {
	for _, g := range s.groups {
		if g != nil && g.GroupID == updated.GroupID {
			*g = *updated
			return
		}
	}
	cp := *updated
	s.groups = append(s.groups, &cp)
}

// GetOpenRewardUserIDs 返回所有已进入排行榜的真实玩家ID。
func (s *Service) GetOpenRewardUserIDs() []int64 {
	allMembers, err := s.store.GetAllMembers()
	if err != nil || len(allMembers) == 0 {
		s.mu.Lock()
		s.ensureLoaded()
		defer s.mu.Unlock()
		users := make([]int64, 0, len(s.memberGroup))
		for userID := range s.memberGroup {
			if userID > 0 {
				users = append(users, userID)
			}
		}
		return users
	}
	users := make([]int64, 0, len(allMembers))
	for userID := range allMembers {
		if userID > 0 {
			users = append(users, userID)
		}
	}
	return users
}

// HasOpenReward 查询指定用户是否具备开启奖励资格。
func (s *Service) HasOpenReward(userID int64) bool {
	s.mu.Lock()
	s.ensureLoaded()
	_, ok := s.memberGroup[userID]
	if !ok {
		if _, found, _ := s.store.GetMember(userID); found {
			ok = true
		}
	}
	s.mu.Unlock()
	return ok && userID > 0
}

// GetGroup 返回指定分组的当前状态副本。
func (s *Service) GetGroup(groupID int32) *Group {
	if g, err := s.store.LoadGroupByID(groupID); err == nil && g != nil {
		s.mu.Lock()
		s.ensureLoaded()
		s.syncGroupStateLocked(g)
		s.mu.Unlock()
		cp := *g
		return &cp
	}
	s.mu.Lock()
	s.ensureLoaded()
	defer s.mu.Unlock()
	for _, group := range s.groups {
		if group != nil && group.GroupID == groupID {
			copyGroup := *group
			return &copyGroup
		}
	}
	return nil
}

// ListGroups 返回所有分组信息的副本列表。
func (s *Service) ListGroups() []Group {
	if latestGroups, err := s.store.LoadGroups(); err == nil && len(latestGroups) > 0 {
		s.mu.Lock()
		s.ensureLoaded()
		for _, g := range latestGroups {
			if g != nil {
				s.syncGroupStateLocked(g)
			}
		}
		result := make([]Group, 0, len(s.groups))
		for _, g := range s.groups {
			if g != nil {
				result = append(result, *g)
			}
		}
		s.mu.Unlock()
		return result
	}
	s.mu.Lock()
	s.ensureLoaded()
	defer s.mu.Unlock()
	result := make([]Group, 0, len(s.groups))
	for _, g := range s.groups {
		if g != nil {
			result = append(result, *g)
		}
	}
	return result
}

// GetGroupCreateTime 返回指定分组底层实例的创建时间（Unix毫秒）。
func (s *Service) GetGroupCreateTime(ctx context.Context, groupID int32) int64 {
	g := s.GetGroup(groupID)
	if g == nil {
		return 0
	}
	inst, err := s.rankService.GetInstance(ctx, g.InstanceID)
	if err != nil || inst == nil {
		return 0
	}
	return inst.CreateTime
}

// IsSettled 返回是否所有分组均已结算。
func (s *Service) IsSettled() bool {
	// 短路的可信前提同样是「活动已对写入关闭」，否则可能有分组尚未创建/结算。
	if s.canUpdateScore(time.Now().UnixMilli()) != rank.InstanceStateOpen && s.settledAt.Load() == s.effectiveSettleAt() {
		return true
	}

	if latestGroups, err := s.store.LoadGroups(); err == nil && len(latestGroups) > 0 {
		s.mu.Lock()
		s.ensureLoaded()
		for _, g := range latestGroups {
			if g != nil {
				s.syncGroupStateLocked(g)
			}
		}
		s.mu.Unlock()
	} else {
		s.mu.Lock()
		s.ensureLoaded()
		s.mu.Unlock()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.groups) == 0 {
		return false
	}
	for _, g := range s.groups {
		if g != nil && g.State != GroupStateSettled {
			return false
		}
	}
	return true
}

func (s *Service) GetConfig() Config {
	return s.config
}

// RankDef 返回构造期注入的 rank:def 原始定义；未注入时返回零值（RankCode 为空）。
// 供 Manager 在 rank:def 到期时零 IO 重建（缺陷 1），调用方必须先判 RankCode != ""。
func (s *Service) RankDef() commonrank.Rank {
	return s.rankDef
}

func (s *Service) GroupCount() int32 {
	if groups, err := s.store.LoadGroups(); err == nil {
		return int32(len(groups))
	}
	s.mu.Lock()
	s.ensureLoaded()
	defer s.mu.Unlock()
	return int32(len(s.groups))
}

func (s *Service) MemberCount() int32 {
	allMembers, err := s.store.GetAllMembers()
	if err != nil {
		s.mu.Lock()
		s.ensureLoaded()
		defer s.mu.Unlock()
		count := int32(0)
		for uid := range s.memberGroup {
			if uid > 0 {
				count++
			}
		}
		return count
	}
	count := int32(0)
	for uid := range allMembers {
		if uid > 0 {
			count++
		}
	}
	return count
}

func (s *Service) UpdateConfig(cfg Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cfg.RankPeopleNum > 0 {
		s.config.RankPeopleNum = cfg.RankPeopleNum
	}
	if cfg.OpenToken >= 0 {
		s.config.OpenToken = cfg.OpenToken
	}
	if cfg.OpenTime > 0 {
		s.config.OpenTime = cfg.OpenTime
	}
	if cfg.CloseTime > 0 {
		s.config.CloseTime = cfg.CloseTime
	}
	if cfg.GameEndTime > 0 {
		s.config.GameEndTime = cfg.GameEndTime
	}
	// 时间字段已被改写，同步 activityEnd 原子副本：Store 的 TTL 闭包读的就是它，
	// 否则 GM 推后截止时间后，后续写入仍按旧的活动结束时刻算过期时间。
	s.refreshActivityEndLocked()
}

// Cleanup 删除本服务在 Redis 与 MongoDB 中的全部数据（含 rank:def）。
//
// **调用顺序不变量（待办 E）**：Cleanup 会 Del 掉 `rank:members`，而 Manager 侧清理
// `rank:member_index` 必须先读到这张成员表（Store 只持有 bizId，索引条目编码
// `{bizType}:{actID}:{groupID}` 在带 `_r{N}` 轮次后缀时不可逆，Store 无法自行清理索引）。
// 因此**不许直接调 Cleanup**：一律走 Manager.cleanupServiceData（先 RemoveUserEntries
// 再 Cleanup）或 forceCleanupOrphan 的同形状写法。顺序反了索引清理会静默失效，
// 只能靠 7 天 TTL 兜底。
func (s *Service) Cleanup() {
	s.mu.Lock()
	groups := s.groups
	s.mu.Unlock()
	if len(groups) == 0 {
		groups, _ = s.store.LoadGroups()
	}

	s.store.CleanupAll(groups)

	ctx := context.Background()
	for _, g := range groups {
		if g == nil {
			continue
		}
		instanceID := s.groupInstanceID(g.GroupID)
		if err := s.rankService.DeleteInstance(ctx, instanceID); err != nil {
			zaplog.LoggerSugar.Warnf("rank engine: cleanup delete rank instance %s: %v", instanceID, err)
		}
	}
	if err := s.rankService.DeleteRankDef(ctx, s.config.RankCode); err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: cleanup delete rank def %s: %v", s.config.RankCode, err)
	}
}

// CleanupLiveData 为该轮次的 Redis 热数据设置 2 周保留 TTL，保留结算快照和 MongoDB 数据。
// 用于周期排行榜历史轮次的延迟设置 TTL（延迟 = 1 个周期，之后数据保留 2 周承接历史查询）。
func (s *Service) CleanupLiveData() {
	s.mu.Lock()
	groups := s.groups
	s.mu.Unlock()
	if len(groups) == 0 {
		groups, _ = s.store.LoadGroups()
	}
	s.store.CleanupLiveData(groups)

	ctx := context.Background()
	for _, g := range groups {
		if g == nil {
			continue
		}
		instanceID := s.groupInstanceID(g.GroupID)
		if err := s.rankService.ExpireInstance(ctx, instanceID, settledDataRetentionTTL); err != nil {
			zaplog.LoggerSugar.Warnf("rank engine: CleanupLiveData expire instance %s: %v", instanceID, err)
		}
		// 实例已被设置 TTL，instanceStates 的正向缓存必须立即失效，否则 Service 仍驻留内存时
		// ensureGroupInstance 可能在 instanceVerifyInterval 内误判实例仍然存在（第 03 条）。
		s.invalidateInstanceState(instanceID)
	}
}

// GetAllMembers 返回该活动所有成员的 userID→groupID 映射。
func (s *Service) GetAllMembers() (map[int64]int32, error) {
	return s.store.GetAllMembers()
}

// GetMemberGroupID 返回用户所在分组 ID；若不在本活动中则 ok=false。
//
// 用 RLock 而非 Lock（待办 G-d3）：本函数只读 s.memberGroup，不调 ensureLoaded，
// map 的并发读是安全的，因此写锁在这里没有任何语义作用，只会让 GM 的批量查询
// 与 UpsertScore（持写锁）无谓争用——一个慢的写操作会把整批查询全部堵住。
func (s *Service) GetMemberGroupID(userID int64) (groupID int32, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	groupID, ok = s.memberGroup[userID]
	return
}

func (s *Service) ClaimReward(userID int64, now int64) (bool, int64, error) {
	return s.store.AtomicClaim(userID, now)
}

func (s *Service) GetClaimStatus(userID int64) (bool, int64, error) {
	claimTime, found, err := s.store.GetClaim(userID)
	if err != nil {
		return false, 0, err
	}
	return found, claimTime, nil
}
