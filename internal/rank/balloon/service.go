package balloon

import (
	"context"
	"sort"
	"sync"

	"common/rank"
	rediskeys "common/redis"
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
	mu          sync.Mutex
	config      Config
	rankService rank.Service
	store       *Store

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
	// 将活动时间写入 meta hash，以便节点重启后通过 syncFromRedis 恢复。
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

	// 1. 加载分组列表
	if groups, err := s.store.LoadGroups(); err == nil && len(groups) > 0 {
		s.groups = groups
		sort.Slice(s.groups, func(i, j int) bool { return s.groups[i].GroupID < s.groups[j].GroupID })
		for _, g := range s.groups {
			if g.GroupID > s.nextGroupID {
				s.nextGroupID = g.GroupID
			}
		}
		// 恢复 nextGroupID 计数器，防止 Redis 清理后新分组 ID 与已有分组冲突。
		s.store.RestoreNextGroupID(s.nextGroupID)
	}

	// 2. 加载成员→分组映射
	if members, err := s.store.GetAllMembers(); err == nil {
		for uid, gid := range members {
			s.memberGroup[uid] = gid
		}
		// 恢复 rank:member_index：Redis 被清理后每位玩家的分组索引条目会丢失，
		// 通过 SAdd（幂等）重建，避免玩家查询排名时因索引缺失而返回"未上榜"。
		if s.onMemberJoin != nil {
			for uid, gid := range s.memberGroup {
				s.onMemberJoin(uid, gid)
			}
		}
	}

	// 3. 按分组加载机器人、已用 InfoID、最高真实积分
	for _, g := range s.groups {
		if robots, err := s.store.LoadRobots(g.GroupID); err == nil && len(robots) > 0 {
			s.groupRobots[g.GroupID] = robots
		}
		// 若 Redis 被清理导致 InfoID SET 为空，从机器人内存状态重建并写回 Redis。
		if ids, err := s.store.LoadUsedInfoIDs(g.GroupID); err == nil && len(ids) > 0 {
			s.groupUsedInfoIDs[g.GroupID] = ids
		} else if len(s.groupRobots[g.GroupID]) > 0 {
			idSet := make(map[int64]struct{}, len(s.groupRobots[g.GroupID]))
			for _, r := range s.groupRobots[g.GroupID] {
				idSet[r.InfoID] = struct{}{}
			}
			s.groupUsedInfoIDs[g.GroupID] = idSet
			_ = s.store.SaveUsedInfoIDs(g.GroupID, idSet)
		}
		if score, err := s.store.GetMaxScore(g.GroupID); err == nil {
			s.groupMaxRealScore[g.GroupID] = score
		}
	}

	// 4. 双向同步 rank:mb ↔ MongoDB，并在 Redis 清理后恢复 rank:inst / rank:settled。
	//
	// 方向1（Redis → MongoDB backfill）：rank:mb 有数据但 rank_score 为空 → 写入 MongoDB。
	// 方向2（MongoDB → Redis 恢复）：rank:mb 缺失但 MongoDB 有数据 → 重建 Redis。
	ctx := context.Background()
	for _, g := range s.groups {
		instanceID := s.groupInstanceID(g.GroupID)

		mbExists, _ := s.store.RdbExists(rediskeys.GetRankMbKey(instanceID))
		mongoScores, _ := s.store.LoadGroupScores(g.GroupID)

		if mbExists {
			// 方向1：rank:mb 已存在。
			// (a) MongoDB 积分记录为空 → backfill；
			// (b) rank:max_score 缺失（单独清空场景）→ 从 rank:mb 重建，防止机器人增长上限失控。
			// rank:mb 存在但 rank:inst 可能缺失（如仅 rank:inst 被清除）→ 先恢复实例元数据，
			// 否则后续 Snapshot/Range 都会因 GetInstance 返回 ErrInstanceNotFound 而失败。
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
				zaplog.LoggerSugar.Infof("balloon: restored missing rank:inst for group %d instanceID=%s", g.GroupID, instanceID)
			}

			needBackfill := len(mongoScores) == 0
			needMaxScore := s.groupMaxRealScore[g.GroupID] == 0
			if needBackfill || needMaxScore {
				if snaps, snapErr := s.rankService.Snapshot(ctx, instanceID); snapErr == nil {
					var maxRealScore int64
					for _, snap := range snaps {
						if IsRobotMemberID(snap.MemberId) {
							continue
						}
						if snap.Score > maxRealScore {
							maxRealScore = snap.Score
						}
						if needBackfill {
							et := snap.EnterTime
							if et == 0 {
								et = snap.UpdateTime
							}
							if wErr := s.store.SaveScore(g.GroupID, snap.MemberId, snap.Score, et, snap.Sequence, snap.UpdateTime, snap.AvatarInfo); wErr != nil {
								zaplog.LoggerSugar.Warnf("balloon: backfill score group=%d member=%d: %v", g.GroupID, snap.MemberId, wErr)
							}
						}
					}
					if needMaxScore && maxRealScore > 0 {
						s.groupMaxRealScore[g.GroupID] = maxRealScore
						_ = s.store.UpdateMaxScore(g.GroupID, maxRealScore)
					}
				}
			}
		} else {
			// 方向2：rank:mb 缺失 → 从 MongoDB 恢复（真实玩家 + 机器人）
			items := make([]rank.RankScoreItem, 0, len(mongoScores)+len(s.groupRobots[g.GroupID]))

			// 恢复真实玩家积分，同时重建 rank:max_score
			var maxRealScore int64
			for _, doc := range mongoScores {
				if doc.Score > maxRealScore {
					maxRealScore = doc.Score
				}
				items = append(items, rank.RankScoreItem{
					MemberId:   doc.UserID,
					Score:      doc.Score,
					AtTime:     doc.UpdateTime,
					EnterTime:  doc.EnterTime,
					Sequence:   doc.Sequence, // 恢复原始进榜序号，保证同分同 enterTime 时名次顺序不变
					AvatarInfo: doc.AvatarInfo,
				})
			}
			if maxRealScore > s.groupMaxRealScore[g.GroupID] {
				s.groupMaxRealScore[g.GroupID] = maxRealScore
				_ = s.store.UpdateMaxScore(g.GroupID, maxRealScore)
			}

			// 恢复机器人积分（确保排行榜完整）
			for _, r := range s.groupRobots[g.GroupID] {
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
					zaplog.LoggerSugar.Warnf("balloon: restore rank:mb for group %d: %v", g.GroupID, err)
				}
			}
			// rank:mb 是从 MongoDB 恢复的；若 rank:inst 也缺失，同步恢复实例元数据
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
				// 优先使用 MongoDB 持久化的精确实例数据覆盖重建值
				if mongoInst, err2 := s.store.LoadGroupInst(g.GroupID); err2 == nil && mongoInst != nil {
					instToRestore = *mongoInst
				}
				_ = s.rankService.RestoreInstance(ctx, instToRestore)
			}
		}

		// 结算快照双向同步 + rank:inst 恢复
		if g.State == GroupStateSettled {
			settledExists, _ := s.store.RdbExists(rediskeys.GetRankSettledKey(instanceID))
			if settledExists {
				// 方向1：rank:settled 存在，但 MongoDB 可能缺失 → backfill
				if mongoSettled, err := s.store.LoadGroupSettled(g.GroupID); err == nil && len(mongoSettled) == 0 {
					if snaps, snapErr := s.rankService.Snapshot(ctx, instanceID); snapErr == nil && len(snaps) > 0 {
						if wErr := s.store.SaveSettled(g.GroupID, snaps, s.config.GameEndTime); wErr != nil {
							zaplog.LoggerSugar.Warnf("balloon: backfill settled group=%d: %v", g.GroupID, wErr)
						}
					}
				}
			} else {
				// 方向2：rank:settled 缺失 → 从 MongoDB 恢复到内存 + Redis
				if snaps, err := s.store.LoadGroupSettled(g.GroupID); err == nil && len(snaps) > 0 {
					s.settledGroup[g.GroupID] = snaps
					// 写回 Redis，使 rankService.GetMemRank 正常工作
					s.store.RestoreSettled(instanceID, snaps)
					// 若 rank:inst 也缺失，恢复实例元数据：优先使用 MongoDB 持久化的真实数据
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
						// 优先使用 MongoDB 持久化的精确实例数据覆盖重建值
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
// 在节点初始化完成后由 Manager 对所有服务并发调用，解决懒加载导致的冷启动数据缺失问题。
// 可安全并发调用（内部持 s.mu）。
func (s *Service) WarmUp(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLoaded()
}

// recoverGroupData 在运行期间检测到 rank:mb 被 Redis 驱逐后，
// 从 MongoDB 重建指定分组的排行榜数据（rank:mb / rank:inst / rank:max_score）。
// 不持有 s.mu，MongoDB 读和 Redis 写均在锁外执行（操作幂等）。
// 在锁外读取机器人快照前会短暂加锁，避免数据竞争。
func (s *Service) recoverGroupData(ctx context.Context, groupID int32, instanceID string) {
	if !s.store.available() || !s.store.hasMongo() {
		return
	}
	mongoScores, err := s.store.LoadGroupScores(groupID)
	if err != nil {
		zaplog.LoggerSugar.Warnf("balloon: runtime recovery load scores group=%d bizId=%s: %v", groupID, s.bizId(), err)
		return
	}

	// 在锁下读取机器人快照和当前最大积分，避免与 tickAllRobots 并发时的数据竞争
	s.mu.Lock()
	robotsCopy := make([]*robotState, len(s.groupRobots[groupID]))
	copy(robotsCopy, s.groupRobots[groupID])
	curMaxScore := s.groupMaxRealScore[groupID]
	robotInfos := s.config.RobotInfos
	s.mu.Unlock()

	items := make([]rank.RankScoreItem, 0, len(mongoScores)+len(robotsCopy))
	var maxRealScore int64

	for _, doc := range mongoScores {
		if doc.Score > maxRealScore {
			maxRealScore = doc.Score
		}
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
			zaplog.LoggerSugar.Warnf("balloon: runtime recover rank:mb group=%d bizId=%s: %v", groupID, s.bizId(), restoreErr)
			return
		}
	}

	// 重建 rank:max_score hash field
	if maxRealScore > curMaxScore {
		s.mu.Lock()
		if maxRealScore > s.groupMaxRealScore[groupID] {
			s.groupMaxRealScore[groupID] = maxRealScore
		}
		s.mu.Unlock()
		_ = s.store.UpdateMaxScore(groupID, maxRealScore)
	} else if curMaxScore > 0 {
		_ = s.store.UpdateMaxScore(groupID, curMaxScore)
	}

	// 若 rank:inst 也缺失，同步恢复实例元数据（优先使用 MongoDB 中持久化的精确数据）
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

	zaplog.LoggerSugar.Infof("balloon: runtime recovered rank:mb group=%d bizId=%s members=%d", groupID, s.bizId(), len(items))
}

// stateAt 根据活动配置时间返回当前活动状态，无需 Redis 写操作。
// CloseTime=0 表示未设置关闭时间，活动保持 open 直到显式关闭。
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

// Tick 推进活动时间轴：
// 1. 驱动所有活跃分组内的机器人积分增长（CloseTime 前）。
// 2. 到达 GameEndTime（若未设置则退化为 CloseTime）后自动结算。
func (s *Service) Tick(ctx context.Context, now int64) error {

	s.mu.Lock()
	s.ensureLoaded()
	s.mu.Unlock()

	// 活动关闭前推进机器人积分；关闭后机器人停止增长，无需再 tick。
	if s.config.hasRobots() && now < s.config.CloseTime {
		s.tickAllRobots(ctx, now)
	}

	// 到达玩法结束时间后自动结算（幂等，已结算的分组会被跳过）。
	// GameEndTime 为 0 时退化为 CloseTime，保持向后兼容。
	settleAt := s.config.GameEndTime
	if settleAt == 0 {
		settleAt = s.config.CloseTime
	}
	if now < settleAt {
		return nil
	}

	// 到达玩法结束时间：先将所有活跃分组的榜单实例推进到 closed 状态，
	// 确保 rank:inst 中的状态字段及时更新，不再接受新的得分写入。
	s.mu.Lock()
	groups := make([]*Group, len(s.groups))
	copy(groups, s.groups)
	s.mu.Unlock()
	for _, g := range groups {
		if g == nil || g.State == GroupStateSettled {
			continue
		}
		if err := s.rankService.CloseInstance(ctx, g.InstanceID, settleAt); err != nil && err != rank.ErrInstanceNotFound {
			zaplog.LoggerSugar.Warnf("balloon: tick close instance group=%d: %v", g.GroupID, err)
		}
	}

	_, err := s.Settle(ctx)
	return err
}

// UpsertScore 写入用户得分。
//   - 分数低于 OpenToken 门槛时忽略。
//   - 自动分配用户到当前可用分组（或创建新分组）。
//   - 首位真实玩家进入分组时自动生成机器人。
//   - 活动尚未开放时返回 ErrInstanceNotOpen。
//   - 活动已结算或已关闭时返回 ErrInstanceClosed。
func (s *Service) UpsertScore(ctx context.Context, userID int64, totalScore int64, now int64, avatarInfo *rank.AvatarInfo) error {
	// if totalScore < s.config.OpenToken {
	// 	return nil
	// }
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
	alreadyLoaded := s.loaded
	s.mu.Unlock()

	// 运行期间 Redis 驱逐检测：仅当服务已完成初始加载（alreadyLoaded=true）时执行。
	// 若 rank:mb 因 Redis 内存压力被驱逐，从 MongoDB 重建排行榜数据，
	// 防止新写入的分数与历史分数共存的 sorted set 被清空导致排行榜异常。
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

	// 将积分写入 MongoDB（write-through，冷启动后可从 MongoDB 恢复 rank:mb）。
	// enterTime 和 sequence 仅对新成员有效；老成员传 0，dao 层 $setOnInsert 会跳过。
	enterTimeForMongo := int64(0)
	seqForMongo := int64(0)
	if isNewMember {
		enterTimeForMongo = now
		// 读取 Redis 中刚由 BatchUpsertScore 分配的 sequence，确保重启后名次顺序可恢复。
		if snap, snapErr := s.rankService.GetMemRank(ctx, instanceID, userID); snapErr == nil && snap != nil {
			seqForMongo = snap.Sequence
		}
	}
	if err := s.store.SaveScore(groupID, userID, totalScore, enterTimeForMongo, seqForMongo, now, avatarInfo); err != nil {
		zaplog.LoggerSugar.Warnf("balloon: save score to mongo group=%d user=%d: %v", groupID, userID, err)
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
	members, err := s.rankService.Range(ctx, s.groupInstanceID(groupID), start, end)
	if err != nil {
		return nil, err
	}
	return members, nil
}

// GetMemberRank 查询指定用户的名次快照及所在分组。未上榜时返回 (nil, 0, nil)。
// 已结算的分组直接从内存快照查询（与 ListGroupRank 一致），无需依赖 rank:inst 键存在。
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
	// 已结算分组：直接从内存快照查找，避免 Redis 恢复过渡期依赖 rank:inst 键。
	settled := cloneSnapshots(s.settledGroup[groupID])
	s.mu.Unlock()

	if !ok {
		return nil, 0, nil
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

	snapshot, err := s.rankService.GetMemRank(ctx, s.groupInstanceID(groupID), userID)
	if err != nil {
		return nil, 0, err
	}
	return snapshot, groupID, nil
}

// Settle 对所有未结算分组执行最终结算，返回各分组快照。幂等。
// 结算时间固定为配置的 GameEndTime（未设置时退化为 CloseTime），不依赖调用时刻。
func (s *Service) Settle(ctx context.Context) (map[int32][]rank.RankMemberSnapshot, error) {
	settleAt := s.config.GameEndTime
	if settleAt == 0 {
		settleAt = s.config.CloseTime
	}

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
		if err := s.rankService.CloseInstance(ctx, group.InstanceID, settleAt); err != nil && err != rank.ErrInstanceNotFound {
			return nil, err
		}
		members, err := s.rankService.SettleInstance(ctx, group.InstanceID, settleAt)
		if err != nil {
			if err == rank.ErrInstanceNotFound {
				// 底层榜单实例不存在（从未有成员加入），仍需将分组标记为已结算，
				// 避免 Tick 在 GameEndTime 后反复重试。
				s.mu.Lock()
				group.State = GroupStateSettled
				_ = s.store.SaveGroup(group)
				s.mu.Unlock()
				continue
			}
			return nil, err
		}
		results[group.GroupID] = cloneSnapshots(members)

		s.mu.Lock()
		group.State = GroupStateSettled
		s.settledGroup[group.GroupID] = cloneSnapshots(members)
		_ = s.store.SaveGroup(group)
		s.mu.Unlock()

		// 将结算快照写入 MongoDB（write-through，冷启动后可从 MongoDB 恢复 rank:settled）。
		if err := s.store.SaveSettled(group.GroupID, cloneSnapshots(members), settleAt); err != nil {
			zaplog.LoggerSugar.Warnf("balloon: save settled to mongo group=%d: %v", group.GroupID, err)
		}
		// 更新 MongoDB 中的实例元数据（State → Settled，SettleTime 已填入）。
		if inst, instErr := s.rankService.GetInstance(ctx, group.InstanceID); instErr == nil && inst != nil {
			_ = s.store.SaveRankInst(group.GroupID, *inst)
		}
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

// ListGroups 返回所有分组信息的副本列表。
func (s *Service) ListGroups() []Group {
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
// 未找到分组或实例时返回 0。
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
	if cfg.GameEndTime > 0 {
		s.config.GameEndTime = cfg.GameEndTime
	}
}

func (s *Service) Cleanup() {
	// 直接从 Redis 读取分组列表，不调用 ensureLoaded 避免触发 backfill 写任务入队。
	// ensureLoaded 的 backfill 逻辑会把尚未写入 MongoDB 的数据推入写队列，
	// 这些写任务会在 DeleteAllByBizId 的查询之后执行，导致数据在删除后重新出现。
	s.mu.Lock()
	groups := s.groups
	s.mu.Unlock()
	if len(groups) == 0 {
		// 内存中没有分组，从 Redis 读一次（只读 groups key，不触发全量 ensureLoaded）。
		groups, _ = s.store.LoadGroups()
	}

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
