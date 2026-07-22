package engine

import (
	"context"
	"sort"
	"sync"

	"common/rank"
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
	mu          sync.Mutex
	config      Config
	rankService rank.Service
	store       *Store

	// 内存缓存（从 Redis 加载，写入时同步回 Redis）
	groups       []*Group
	memberGroup  map[int64]int32
	settledGroup map[int32][]rank.RankMemberSnapshot

	nextGroupID  int32
	onMemberJoin func(userID int64, groupID int32)
	loaded       bool
}

func NewService(rankService rank.Service, config Config, rdb *goredis.Redis, dao *DAO, opts ...Option) (*Service, error) {
	if rankService == nil || config.RankCode == "" || config.RankPeopleNum <= 0 {
		return nil, rank.ErrInvalidRankSpec
	}
	s := &Service{
		config:       config,
		rankService:  rankService,
		store:        NewStore(rdb, dao, config.computeBizId()),
		memberGroup:  make(map[int64]int32),
		settledGroup: make(map[int32][]rank.RankMemberSnapshot),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.store.SaveActivityTimes(config.OpenTime, config.CloseTime, config.GameEndTime)
	return s, nil
}

// ensureLoaded 首次调用时从 Redis/MongoDB 加载运行时状态，并在 Redis 被清理后完整恢复。
// 必须在持有 s.mu 锁时调用。
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
	}
}

// WarmUp 主动触发 ensureLoaded，确保该服务的所有分组数据已从 Redis/MongoDB 恢复。
func (s *Service) WarmUp(ctx context.Context) {
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

// Tick 推进活动时间轴：驱动机器人积分增长，到达 GameEndTime 后自动结算。
func (s *Service) Tick(ctx context.Context, now int64) error {
	s.mu.Lock()
	s.ensureLoaded()
	s.mu.Unlock()

	if s.config.hasRobots() && now < s.config.GameEndTime {
		s.tickAllRobots(ctx, now)
	}

	settleAt := s.config.GameEndTime
	if settleAt == 0 {
		settleAt = s.config.CloseTime
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
	alreadyLoaded := s.loaded
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

// ListGroupRank 查询指定分组的排行榜区间（0-based 闭区间）。
func (s *Service) ListGroupRank(ctx context.Context, groupID int32, start int64, end int64) ([]rank.RankMemberSnapshot, error) {
	s.mu.Lock()
	s.ensureLoaded()
	settled := cloneSnapshots(s.settledGroup[groupID])
	s.mu.Unlock()

	if len(settled) == 0 {
		instanceID := s.groupInstanceID(groupID)
		if g, _ := s.store.LoadGroupByID(groupID); g != nil {
			if g.State == GroupStateSettled {
				settled = s.loadAndCacheSettled(groupID, g.InstanceID)
			}
		} else {
			needLoad := false
			s.mu.Lock()
			for _, mg := range s.groups {
				if mg != nil && mg.GroupID == groupID {
					if mg.State == GroupStateSettled {
						settled = cloneSnapshots(s.settledGroup[groupID])
						if len(settled) == 0 {
							needLoad = true
						}
					}
					break
				}
			}
			s.mu.Unlock()
			if needLoad {
				settled = s.loadAndCacheSettled(groupID, instanceID)
			}
		}
	}

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

	if len(settled) == 0 {
		instanceID := s.groupInstanceID(groupID)
		if g, _ := s.store.LoadGroupByID(groupID); g != nil {
			if g.State == GroupStateSettled {
				settled = s.loadAndCacheSettled(groupID, g.InstanceID)
			}
		} else {
			needLoad := false
			s.mu.Lock()
			for _, mg := range s.groups {
				if mg != nil && mg.GroupID == groupID {
					if mg.State == GroupStateSettled {
						settled = cloneSnapshots(s.settledGroup[groupID])
						if len(settled) == 0 {
							needLoad = true
						}
					}
					break
				}
			}
			s.mu.Unlock()
			if needLoad {
				settled = s.loadAndCacheSettled(groupID, instanceID)
			}
		}
	}

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
	settleAt := s.config.GameEndTime
	if settleAt == 0 {
		settleAt = s.config.CloseTime
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
	for _, group := range groups {
		if group == nil || group.State == GroupStateSettled {
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
				continue
			}
			return nil, err
		}
		results[group.GroupID] = cloneSnapshots(members)

		group.State = GroupStateSettled
		_ = s.store.SaveGroup(group)

		s.mu.Lock()
		s.syncGroupStateLocked(group)
		s.settledGroup[group.GroupID] = cloneSnapshots(members)
		s.mu.Unlock()

		if err := s.store.SaveSettled(group.GroupID, cloneSnapshots(members), settleAt); err != nil {
			zaplog.LoggerSugar.Warnf("rank engine: save settled to mongo group=%d: %v", group.GroupID, err)
		}
		if inst, instErr := s.rankService.GetInstance(ctx, group.InstanceID); instErr == nil && inst != nil {
			_ = s.store.SaveRankInst(group.GroupID, *inst)
		}
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
}

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

// GetAllMembers 返回该活动所有成员的 userID→groupID 映射。
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
