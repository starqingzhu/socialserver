package balloon

import (
	"context"
	"fmt"

	"common/rank"
	"golib/zaplog"
)

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

	zaplog.LoggerSugar.Infof("balloon: spawned %d robots for group %d (bizType=%s)", len(newRobots), groupID, s.config.BizType)
	return nil
}

// tickAllRobots 推进所有活跃分组内机器人的积分增长。
// 多节点：每次 tick 从 Redis 加载最新分组列表和机器人状态，避免内存缓存跨节点不一致。
func (s *Service) tickAllRobots(ctx context.Context, nowMs int64) {
	// 从 Redis 读取最新分组列表，确保能看到其他节点创建的分组。
	groups, err := s.store.LoadGroups()
	if err != nil || len(groups) == 0 {
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
		// 每次 tick 从 Redis 加载最新机器人状态，确保多节点一致。
		robots, err := s.store.LoadRobots(t.groupID)
		if err != nil || len(robots) == 0 {
			continue
		}
		s.tickGroupRobots(ctx, t.groupID, t.instanceID, robots, nowMs)
	}
}

// tickGroupRobots 推进单个分组内所有机器人的积分并持久化变更。
func (s *Service) tickGroupRobots(ctx context.Context, groupID int32, instanceID string, robots []*robotState, nowMs int64) {
	// 取组内全员榜单（含机器人），用于计算增长目标分
	allSnapshots, err := s.rankService.Range(ctx, instanceID, 0, -1)
	if err != nil || len(allSnapshots) == 0 {
		return
	}
	firstScore := allSnapshots[0].Score

	// 找出真实玩家第一名分值，作为 maxDifferenceToken 的约束基准
	var realFirstScore int64
	for _, snap := range allSnapshots {
		if !IsRobotMemberID(snap.MemberId) {
			realFirstScore = snap.Score
			break
		}
	}

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
			zaplog.LoggerSugar.Warnf("balloon: tick robots for group %d failed: %v", groupID, err)
			return
		}
	}
	if len(changed) > 0 {
		_ = s.store.SaveRobots(groupID, changed)
	}
}

// robotAvatarInfo 根据机器人状态构造正确的 AvatarInfo：userId 使用负数 memberID。
func (s *Service) robotAvatarInfo(robot *robotState) *rank.AvatarInfo {
	for _, info := range s.config.RobotInfos {
		if info.InfoID == robot.InfoID {
			return &rank.AvatarInfo{
				UserId: robot.MemberID,
				Name:   info.Name,
				Avatar: info.Avatar,
				Frame:  info.Frame,
			}
		}
	}
	return &rank.AvatarInfo{UserId: robot.MemberID}
}

// findTier 在配置中查找指定档次，未找到时返回 nil。
func (s *Service) findTier(tierID int32) *RobotTierCfg {
	for i := range s.config.RobotTiers {
		if s.config.RobotTiers[i].TierID == tierID {
			return &s.config.RobotTiers[i]
		}
	}
	return nil
}
