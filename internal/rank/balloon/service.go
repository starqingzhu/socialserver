package balloon

import (
	"context"
	"sort"
	"sync"

	"common/rank"
	goredis "golib/redis"
	"golib/zaplog"
)

// Service 气球活动排行榜业务服务。
// 每个活动对应一个 Service 实例，内部按人数上限自动分组，
// 同一分组共享一个底层榜单实例。
//
// 无状态设计：所有运行时状态（分组、成员映射、机器人）均持久化到 Redis，
// 任意节点可服务任意玩家，节点重启或 hash 偏移后自动从 Redis 恢复。
type Service struct {
	mu           sync.Mutex
	config       Config
	rankService  rank.Service
	timedManager *rank.FixedWindowManager
	store        *Store

	// 内存缓存（从 Redis 加载，写入时同步回 Redis）
	groups       []*Group
	memberGroup  map[int64]int32
	settledGroup map[int32][]rank.RankMemberSnapshot

	// 机器人内存缓存
	groupRobots       map[int32][]*robotState
	groupUsedInfoIDs  map[int32]map[int64]struct{}
	groupMaxRealScore map[int32]int64

	nextGroupID  int32
	onMemberJoin func(userID int64, groupID int32)
	loaded       bool
}

func NewService(rankService rank.Service, config Config, rdb *goredis.Redis, dao *DAO, opts ...Option) (*Service, error) {
	if rankService == nil || config.RankCode == "" || config.RankPeopleNum <= 0 {
		return nil, rank.ErrInvalidRankSpec
	}
	s := &Service{
		config:            config,
		rankService:       rankService,
		store:             NewStore(rdb, dao, config.computeBizId()),
		memberGroup:       make(map[int64]int32),
		settledGroup:      make(map[int32][]rank.RankMemberSnapshot),
		groupRobots:       make(map[int32][]*robotState),
		groupUsedInfoIDs:  make(map[int32]map[int64]struct{}),
		groupMaxRealScore: make(map[int32]int64),
	}
	for _, opt := range opts {
		opt(s)
	}
	manager, err := rank.NewFixedWindowManager(rankService, rank.FixedWindowSpec{
		RankCode:   config.RankCode,
		BizId:      s.bizId(),
		InstanceId: rank.NewInstanceID(config.RankCode, s.bizId(), "main"),
		OpenTime:   config.OpenTime,
		CloseTime:  config.CloseTime,
	})
	if err != nil {
		return nil, err
	}
	s.timedManager = manager
	return s, nil
}

// ensureLoaded 首次调用时从 Redis 加载运行时状态到内存缓存。
func (s *Service) ensureLoaded() {
	if s.loaded || !s.store.available() {
		return
	}
	s.loaded = true

	if groups, err := s.store.LoadGroups(); err == nil && len(groups) > 0 {
		s.groups = groups
		sort.Slice(s.groups, func(i, j int) bool { return s.groups[i].GroupID < s.groups[j].GroupID })
		for _, g := range s.groups {
			if g.GroupID > s.nextGroupID {
				s.nextGroupID = g.GroupID
			}
		}
	}

	if members, err := s.store.GetAllMembers(); err == nil {
		for uid, gid := range members {
			s.memberGroup[uid] = gid
		}
	}

	for _, g := range s.groups {
		if robots, err := s.store.LoadRobots(g.GroupID); err == nil && len(robots) > 0 {
			s.groupRobots[g.GroupID] = robots
		}
		if ids, err := s.store.LoadUsedInfoIDs(g.GroupID); err == nil && len(ids) > 0 {
			s.groupUsedInfoIDs[g.GroupID] = ids
		}
		if score, err := s.store.GetMaxScore(g.GroupID); err == nil {
			s.groupMaxRealScore[g.GroupID] = score
		}
	}
}

// Tick 推进活动时间轴：
// 1. 推进底层时间窗口实例状态。
// 2. 驱动所有活跃分组内的机器人积分增长。
// 3. 到期时若 AutoSettle=true 则自动结算。
func (s *Service) Tick(ctx context.Context, now int64) error {
	if _, err := s.timedManager.EnsureInstance(ctx, now); err != nil {
		return err
	}

	s.mu.Lock()
	s.ensureLoaded()
	s.mu.Unlock()

	if s.config.hasRobots() {
		s.tickAllRobots(ctx, now)
	}

	if now < s.config.CloseTime || !s.config.AutoSettle {
		return nil
	}
	_, err := s.Settle(ctx, now)
	return err
}

// UpsertScore 写入用户得分。
//   - 分数低于 OpenToken 门槛时忽略。
//   - 自动分配用户到当前可用分组（或创建新分组）。
//   - 首位真实玩家进入分组时自动生成机器人。
//   - 活动已关闭或实例未开放时返回 ErrInstanceNotOpen。
func (s *Service) UpsertScore(ctx context.Context, userID int64, totalScore int64, now int64, avatarInfo *rank.AvatarInfo) error {
	if totalScore < s.config.OpenToken {
		return nil
	}
	instance, err := s.timedManager.EnsureInstance(ctx, now)
	if err != nil {
		return err
	}
	if instance.State != rank.InstanceStateOpen {
		return rank.ErrInstanceNotOpen
	}

	s.mu.Lock()
	s.ensureLoaded()

	// 检查 Redis（可能其他节点已经分配了该用户）
	if _, ok := s.memberGroup[userID]; !ok {
		if gid, found, _ := s.store.GetMember(userID); found {
			s.memberGroup[userID] = gid
		}
	}

	group, err := s.ensureGroupLocked()
	if err != nil {
		s.mu.Unlock()
		return err
	}

	isNewMember := false
	if _, ok := s.memberGroup[userID]; !ok {
		isNewMember = true
		s.memberGroup[userID] = group.GroupID
		group.RealCount++
		_ = s.store.SetMember(userID, group.GroupID)
		_ = s.store.SaveGroup(group)
		if s.onMemberJoin != nil {
			s.onMemberJoin(userID, group.GroupID)
		}
	}
	groupID := s.memberGroup[userID]

	if totalScore > s.groupMaxRealScore[groupID] {
		s.groupMaxRealScore[groupID] = totalScore
		_ = s.store.UpdateMaxScore(groupID, totalScore)
	}

	needSpawnRobots := isNewMember && group.RealCount == 1 && s.config.hasRobots()
	spawnCapacity := s.config.RankPeopleNum - group.RealCount
	if needSpawnRobots {
		plan := buildRobotSpawnPlan(s.config.RobotTiers, spawnCapacity)
		group.RobotCount = totalRobotsInPlan(plan)
		_ = s.store.SaveGroup(group)
	}
	if group.totalCount() >= s.config.RankPeopleNum {
		group.State = GroupStateFull
		_ = s.store.SaveGroup(group)
	}

	instanceID := s.groupInstanceID(groupID)
	s.mu.Unlock()

	if err := s.ensureGroupInstance(ctx, instanceID, now); err != nil {
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

	if needSpawnRobots {
		if err := s.spawnRobotsForGroup(ctx, groupID, spawnCapacity, now); err != nil {
			zaplog.LoggerSugar.Warnf("balloon: spawn robots for group %d failed: %v", groupID, err)
		}
	}

	return nil
}

// ListGroupRank 查询指定分组的排行榜区间（0-based 闭区间）。
// 已结算时返回缓存快照，未结算时实时查询。
func (s *Service) ListGroupRank(ctx context.Context, groupID int32, start int64, end int64) ([]rank.RankMemberSnapshot, error) {
	s.mu.Lock()
	s.ensureLoaded()
	settled := cloneSnapshots(s.settledGroup[groupID])
	s.mu.Unlock()
	if len(settled) > 0 {
		return sliceSnapshots(settled, start, end), nil
	}
	return s.rankService.Range(ctx, s.groupInstanceID(groupID), start, end)
}

// GetMemberRank 查询指定用户的名次快照及所在分组。未上榜时返回 (nil, 0, nil)。
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
	s.mu.Unlock()
	if !ok {
		return nil, 0, nil
	}
	snapshot, err := s.rankService.GetMemRank(ctx, s.groupInstanceID(groupID), userID)
	if err != nil {
		return nil, 0, err
	}
	return snapshot, groupID, nil
}

// Settle 对所有未结算分组执行最终结算，返回各分组快照。幂等。
func (s *Service) Settle(ctx context.Context, now int64) (map[int32][]rank.RankMemberSnapshot, error) {
	s.mu.Lock()
	s.ensureLoaded()
	groups := make([]*Group, len(s.groups))
	copy(groups, s.groups)
	s.mu.Unlock()

	results := make(map[int32][]rank.RankMemberSnapshot, len(groups))
	for _, group := range groups {
		if group == nil || group.State == GroupStateSettled {
			continue
		}
		if err := s.rankService.CloseInstance(ctx, group.InstanceID, s.config.CloseTime); err != nil && err != rank.ErrInstanceNotFound {
			return nil, err
		}
		members, err := s.rankService.SettleInstance(ctx, group.InstanceID, now)
		if err != nil {
			if err == rank.ErrInstanceNotFound {
				continue
			}
			return nil, err
		}
		results[group.GroupID] = cloneSnapshots(members)

		s.mu.Lock()
		group.State = GroupStateSettled
		group.SettleTime = now
		s.settledGroup[group.GroupID] = cloneSnapshots(members)
		_ = s.store.SaveGroup(group)
		s.mu.Unlock()
	}
	return results, nil
}

// GetOpenRewardUserIDs 返回所有已进入排行榜的真实玩家ID（开启奖励资格）。
func (s *Service) GetOpenRewardUserIDs() []int64 {
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

// GetGroup 返回指定分组的当前状态副本，未找到时返回 nil。
func (s *Service) GetGroup(groupID int32) *Group {
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

// IsSettled 返回是否所有分组均已结算。无分组时返回 false。
func (s *Service) IsSettled() bool {
	s.mu.Lock()
	s.ensureLoaded()
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

func (s *Service) GroupCount() int32 {
	s.mu.Lock()
	s.ensureLoaded()
	defer s.mu.Unlock()
	return int32(len(s.groups))
}

func (s *Service) MemberCount() int32 {
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
	s.config.AutoSettle = cfg.AutoSettle
}

func (s *Service) Cleanup() {
	// 确保分组列表已从 Redis 加载，避免因懒加载未触发而遗漏分组级 key 的清理。
	s.mu.Lock()
	s.ensureLoaded()
	groups := s.groups
	s.mu.Unlock()

	s.store.CleanupAll(groups)

	// 删除每个分组对应的 commonRank 实例 Redis 数据。
	ctx := context.Background()
	for _, g := range groups {
		if g == nil {
			continue
		}
		instanceID := s.groupInstanceID(g.GroupID)
		if err := s.rankService.DeleteInstance(ctx, instanceID); err != nil {
			zaplog.LoggerSugar.Warnf("balloon: cleanup delete rank instance %s: %v", instanceID, err)
		}
	}
	// 删除榜单定义。
	if err := s.rankService.DeleteRankDef(ctx, s.config.RankCode); err != nil {
		zaplog.LoggerSugar.Warnf("balloon: cleanup delete rank def %s: %v", s.config.RankCode, err)
	}
}

// GetAllMembers 返回该活动所有成员的 userID→groupID 映射，供外部在 Cleanup 前收集用于清理成员索引。
func (s *Service) GetAllMembers() (map[int64]int32, error) {
	return s.store.GetAllMembers()
}

func (s *Service) ClaimReward(userID int64, now int64) (bool, int64, error) {
	claimTime, found, err := s.store.GetClaim(userID)
	if err != nil {
		return false, 0, err
	}
	if found {
		return false, claimTime, nil
	}
	if err := s.store.SetClaim(userID, now); err != nil {
		return false, 0, err
	}
	return true, now, nil
}

func (s *Service) GetClaimStatus(userID int64) (bool, int64, error) {
	claimTime, found, err := s.store.GetClaim(userID)
	if err != nil {
		return false, 0, err
	}
	return found, claimTime, nil
}
