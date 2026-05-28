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

	s.mu.Lock()
	if s.groupUsedInfoIDs[groupID] == nil {
		s.groupUsedInfoIDs[groupID] = make(map[int64]struct{})
	}
	usedInfoIDs := s.groupUsedInfoIDs[groupID]
	s.mu.Unlock()

	var robotIndex int32
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

	s.mu.Lock()
	s.groupRobots[groupID] = append(s.groupRobots[groupID], newRobots...)
	s.mu.Unlock()

	_ = s.store.SaveRobots(groupID, newRobots)
	_ = s.store.SaveUsedInfoIDs(groupID, usedInfoIDs)

	zaplog.LoggerSugar.Infof("balloon: spawned %d robots for group %d (bizType=%s)", len(newRobots), groupID, s.config.BizType)
	return nil
}

// tickAllRobots 推进所有活跃分组内机器人的积分增长。
func (s *Service) tickAllRobots(ctx context.Context, nowMs int64) {
	type groupSnapshot struct {
		groupID    int32
		instanceID string
		robots     []*robotState
	}
	s.mu.Lock()
	s.ensureLoaded()
	var snapshots []groupSnapshot
	for _, group := range s.groups {
		if group == nil || group.State == GroupStateSettled {
			continue
		}
		robots := s.groupRobots[group.GroupID]
		if len(robots) == 0 {
			continue
		}
		snapshots = append(snapshots, groupSnapshot{
			groupID:    group.GroupID,
			instanceID: group.InstanceID,
			robots:     robots,
		})
	}
	s.mu.Unlock()

	for _, snap := range snapshots {
		s.tickGroupRobots(ctx, snap.groupID, snap.instanceID, snap.robots, nowMs)
	}
}

// tickGroupRobots 推进单个分组内所有机器人的积分并持久化变更。
func (s *Service) tickGroupRobots(ctx context.Context, groupID int32, instanceID string, robots []*robotState, nowMs int64) {
	topSnapshots, err := s.rankService.Range(ctx, instanceID, 0, 0)
	if err != nil || len(topSnapshots) == 0 {
		return
	}
	firstScore := topSnapshots[0].Score

	var updates []rank.RankScoreItem
	var changed []*robotState
	for _, robot := range robots {
		tier := s.findTier(robot.TierID)
		if tier == nil {
			continue
		}
		oldScore := robot.Score
		newScore := tickRobotScore(robot, tier, firstScore, nowMs, s.config.GameEndTime)
		if newScore != oldScore {
			changed = append(changed, robot)
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
