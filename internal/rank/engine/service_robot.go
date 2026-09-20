package engine

import (
	"context"
	"fmt"
	"time"

	"common/rank"
	"golib/zaplog"
)

// tickGroupsCacheTTL 是 tickAllRobots 专用软缓存（分组列表/机器人列表/榜一分数）的有效期。
// 不能拉长：tickRobotScore 是有状态的逐 tick 随机游走，喂给它越陈旧的状态就越会算错增长节拍
// （第 02 条）。
const tickGroupsCacheTTL = 2 * time.Second

// robotsCacheEntry 是某分组机器人列表的缓存条目。
type robotsCacheEntry struct {
	robots []*robotState
	expiry time.Time
}

// groupScoreCacheEntry 是某分组榜一分数 / 真实玩家榜一分数的缓存条目。
type groupScoreCacheEntry struct {
	firstScore     int64
	realFirstScore int64
	expiry         time.Time
}

// tickGroups 返回本轮需要 tick 的分组列表：命中有效期内的缓存直接返回，
// 未命中则回源 Redis 并回填缓存。仅服务 tickAllRobots——UpsertScore 路径的
// ensureGroupLocked 必须实时读 Redis 以感知其他节点新建的分组，不得复用此缓存
// （第 02 条 必改-2：缓存必须带有效期，否则本节点永远看不到其他节点新建的分组）。
func (s *Service) tickGroups() ([]*Group, bool) {
	now := time.Now()
	s.cacheMu.RLock()
	if s.groupsCache != nil && now.Before(s.groupsCacheExpiry) {
		groups := s.groupsCache
		s.cacheMu.RUnlock()
		return groups, true
	}
	s.cacheMu.RUnlock()

	groups, err := s.store.LoadGroups()
	if err != nil {
		return nil, false
	}
	s.cacheMu.Lock()
	s.groupsCache = groups
	s.groupsCacheExpiry = now.Add(tickGroupsCacheTTL)
	s.cacheMu.Unlock()
	return groups, true
}

// tickRobots 返回指定分组的机器人列表：命中有效期内的缓存直接返回，未命中则回源 Redis
// 并回填缓存。缓存持有的是 *robotState 指针，tickGroupRobots 对字段的原地修改会
// 直接反映到缓存里，因此不需要在 SaveRobots 之后显式回写（第 02 条）。
func (s *Service) tickRobots(groupID int32) ([]*robotState, bool) {
	now := time.Now()
	s.cacheMu.RLock()
	if e, ok := s.robotsCache[groupID]; ok && now.Before(e.expiry) {
		robots := e.robots
		s.cacheMu.RUnlock()
		return robots, true
	}
	s.cacheMu.RUnlock()

	robots, err := s.store.LoadRobots(groupID)
	if err != nil {
		return nil, false
	}
	s.cacheMu.Lock()
	if s.robotsCache == nil {
		s.robotsCache = make(map[int32]robotsCacheEntry)
	}
	s.robotsCache[groupID] = robotsCacheEntry{robots: robots, expiry: now.Add(tickGroupsCacheTTL)}
	s.cacheMu.Unlock()
	return robots, true
}

// invalidateRobotsCache 清除指定分组的机器人缓存。
// 用于 spawnRobotsForGroup 新增机器人后让下一次 tick 立即感知新机器人，
// 不必等待 tickGroupsCacheTTL 到期。
func (s *Service) invalidateRobotsCache(groupID int32) {
	s.cacheMu.Lock()
	delete(s.robotsCache, groupID)
	s.cacheMu.Unlock()
}

// tickGroupScores 返回分组的榜一分数与真实玩家榜一分数：命中有效期内的缓存直接返回，
// 未命中则调用 Range(0,-1) 回源并回填缓存。把 tickGroupRobots 里唯一未被覆盖的全量读
// 也纳入同一套 Cache-Aside（第 02 条「遗漏的最大一项」，采用"减少读次"方向）——
// ≤2s 的陈旧度只影响 calcGrowTarget 万分比 clamp 的目标分细微扰动，可接受。
func (s *Service) tickGroupScores(ctx context.Context, groupID int32, instanceID string) (groupScoreCacheEntry, bool) {
	now := time.Now()
	s.cacheMu.RLock()
	if e, ok := s.scoreCache[groupID]; ok && now.Before(e.expiry) {
		s.cacheMu.RUnlock()
		return e, true
	}
	s.cacheMu.RUnlock()

	allSnapshots, err := s.rankService.Range(ctx, instanceID, 0, -1)
	if err != nil || len(allSnapshots) == 0 {
		return groupScoreCacheEntry{}, false
	}
	entry := groupScoreCacheEntry{firstScore: allSnapshots[0].Score, expiry: now.Add(tickGroupsCacheTTL)}
	for _, snap := range allSnapshots {
		if !IsRobotMemberID(snap.MemberId) {
			entry.realFirstScore = snap.Score
			break
		}
	}
	s.cacheMu.Lock()
	if s.scoreCache == nil {
		s.scoreCache = make(map[int32]groupScoreCacheEntry)
	}
	s.scoreCache[groupID] = entry
	s.cacheMu.Unlock()
	return entry, true
}

// spawnRobotsForGroup 为指定分组生成机器人并持久化到 Redis。
// 若距玩法结束时间不足任意一档 LockTokenTime，则跳过该档机器人的生成。
func (s *Service) spawnRobotsForGroup(ctx context.Context, groupID int32, capacity int32, now int64) error {
	plan := buildRobotSpawnPlan(s.config.RobotTiers, capacity)
	if len(plan) == 0 {
		return nil
	}

	instanceID := s.groupInstanceID(groupID)
	if err := s.ensureGroupInstance(ctx, instanceID, groupID, now); err != nil {
		return fmt.Errorf("ensure group instance: %w", err)
	}

	// 多节点：直接从 Redis 读取已用 InfoID，避免内存缓存跨节点不一致。
	usedInfoIDs, _ := s.store.LoadUsedInfoIDs(groupID)
	if usedInfoIDs == nil {
		usedInfoIDs = make(map[int64]struct{})
	}

	// 多节点：从 Redis 已有机器人推算下一个 robotIndex，避免并发 spawn 生成重复 MemberID。
	// 逆向公式：index = -(MemberID) - groupID*stride
	existingRobots, _ := s.store.LoadRobots(groupID)
	var robotIndex int32
	for _, r := range existingRobots {
		idx := int32(-r.MemberID - int64(groupID)*robotIDGroupStride)
		if idx > robotIndex {
			robotIndex = idx
		}
	}
	var scoreItems []rank.RankScoreItem
	var newRobots []*robotState

	gameEndTime := s.config.GameEndTime

	for _, entry := range plan {
		tier := s.findTier(entry.TierID)
		if tier == nil {
			continue
		}
		// 距玩法结束时间不足 LockTokenTime，不初始化该档机器人
		if gameEndTime > 0 && gameEndTime-now <= tier.LockTokenTimeMs {
			continue
		}
		for i := int32(0); i < entry.Count; i++ {
			robotIndex++
			info, ok := pickRobotInfo(s.config.RobotInfos, usedInfoIDs)
			if !ok {
				break
			}
			usedInfoIDs[info.InfoID] = struct{}{}

			memberID := robotMemberID(groupID, robotIndex)
			initScore := initRobotScore(tier.DefaultTokenMin, tier.DefaultTokenMax)
			if initScore > tier.MaxToken {
				initScore = tier.MaxToken
			}

			newRobots = append(newRobots, &robotState{
				MemberID:   memberID,
				TierID:     tier.TierID,
				InfoID:     info.InfoID,
				Score:      initScore,
				LastGrowAt: now,
			})
			scoreItems = append(scoreItems, rank.RankScoreItem{
				MemberId:  memberID,
				Score:     initScore,
				AtTime:    now,
				EnterTime: now,
				AvatarInfo: &rank.AvatarInfo{
					UserId: memberID,
					Name:   info.Name,
					Avatar: info.Avatar,
					Frame:  info.Frame,
				},
			})
		}
	}

	if len(scoreItems) == 0 {
		return nil
	}
	if err := s.rankService.BatchUpsertScore(ctx, instanceID, scoreItems); err != nil {
		return fmt.Errorf("write robot scores: %w", err)
	}

	_ = s.store.SaveRobots(groupID, newRobots)
	_ = s.store.SaveUsedInfoIDs(groupID, usedInfoIDs)
	// 新机器人不在 tickRobots 的缓存快照里；立即失效该分组的缓存，
	// 让下一次 tick 而不是等 tickGroupsCacheTTL 到期才感知它们（第 02 条）。
	s.invalidateRobotsCache(groupID)

	zaplog.LoggerSugar.Infof("rank engine: spawned %d robots for group %d (bizType=%s)", len(newRobots), groupID, s.config.BizType)
	return nil
}

// tickAllRobots 推进所有活跃分组内机器人的积分增长。
// 多节点：分布锁只保证同一秒内单节点执行，但持锁节点可能换手，因此分组列表/机器人状态
// 都走带有效期的软缓存（TTL≈2s）而不是内存永久缓存，未命中时回源 Redis（第 02 条）。
func (s *Service) tickAllRobots(ctx context.Context, nowMs int64) {
	groups, ok := s.tickGroups()
	if !ok || len(groups) == 0 {
		// Redis 不可用时回退到内存缓存
		s.mu.Lock()
		s.ensureLoaded()
		groups = make([]*Group, len(s.groups))
		copy(groups, s.groups)
		s.mu.Unlock()
	}

	type groupEntry struct {
		groupID    int32
		instanceID string
	}
	var targets []groupEntry
	for _, g := range groups {
		if g == nil || g.State == GroupStateSettled {
			continue
		}
		targets = append(targets, groupEntry{groupID: g.GroupID, instanceID: g.InstanceID})
	}

	for _, t := range targets {
		robots, ok := s.tickRobots(t.groupID)
		if !ok || len(robots) == 0 {
			continue
		}
		s.tickGroupRobots(ctx, t.groupID, t.instanceID, robots, nowMs)
	}
}

// tickGroupRobots 推进单个分组内所有机器人的积分并持久化变更。
func (s *Service) tickGroupRobots(ctx context.Context, groupID int32, instanceID string, robots []*robotState, nowMs int64) {
	// 取组内全员榜单（含机器人）的榜一分数，用于计算增长目标分；走 2s Cache-Aside，
	// 覆盖第 02 条中唯一未被 groups/robots 缓存覆盖的 Range(0,-1) 全量读。
	scores, ok := s.tickGroupScores(ctx, groupID, instanceID)
	if !ok {
		return
	}
	firstScore := scores.firstScore
	realFirstScore := scores.realFirstScore

	var updates []rank.RankScoreItem
	var changed []*robotState
	for _, robot := range robots {
		tier := s.findTier(robot.TierID)
		if tier == nil {
			continue
		}
		oldScore := robot.Score
		oldPending := robot.PendingScore
		oldLastGrowAt := robot.LastGrowAt
		newScore := tickRobotScore(robot, tier, firstScore, realFirstScore, nowMs, s.config.GameEndTime)
		scoreChanged := newScore != oldScore
		stateChanged := scoreChanged || robot.PendingScore != oldPending || robot.LastGrowAt != oldLastGrowAt
		if stateChanged {
			changed = append(changed, robot)
		}
		if scoreChanged {
			updates = append(updates, rank.RankScoreItem{
				MemberId:   robot.MemberID,
				Score:      newScore,
				AtTime:     nowMs,
				AvatarInfo: s.robotAvatarInfo(robot),
			})
		}
	}
	if len(updates) > 0 {
		if err := s.rankService.BatchUpsertScore(ctx, instanceID, updates); err != nil {
			zaplog.LoggerSugar.Warnf("rank engine: tick robots for group %d failed: %v", groupID, err)
			return
		}
	}
	if len(changed) > 0 {
		_ = s.store.SaveRobots(groupID, changed)
	}
}

// robotAvatarInfo 根据机器人状态构造正确的 AvatarInfo：userId 使用负数 memberID。
func (s *Service) robotAvatarInfo(robot *robotState) *rank.AvatarInfo {
	if info, ok := s.infoMap[robot.InfoID]; ok {
		return &rank.AvatarInfo{
			UserId: robot.MemberID,
			Name:   info.Name,
			Avatar: info.Avatar,
			Frame:  info.Frame,
		}
	}
	return &rank.AvatarInfo{UserId: robot.MemberID}
}

// findTier 在配置中查找指定档次，未找到时返回 nil。
func (s *Service) findTier(tierID int32) *RobotTierCfg {
	return s.tierMap[tierID]
}
