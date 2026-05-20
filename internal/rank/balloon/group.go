package balloon

import (
	"context"
	"fmt"

	"common/rank"
)

// ensureGroupLocked 在持有 s.mu 锁时，获取当前可用分组（有空位且处于 Open 状态）。
// 分组容量包含真实玩家和机器人，均计入 RankPeopleNum 上限。
// 若无可用分组则创建新分组并持久化到 Redis。
func (s *Service) ensureGroupLocked() (*Group, error) {
	for _, group := range s.groups {
		if group.State == GroupStateOpen && group.totalCount() < s.config.RankPeopleNum {
			return group, nil
		}
	}

	var newID int32
	if s.store.available() {
		id, err := s.store.NextGroupID()
		if err != nil {
			return nil, fmt.Errorf("next group id: %w", err)
		}
		newID = id
	} else {
		s.nextGroupID++
		newID = s.nextGroupID
	}

	group := &Group{
		GroupID:    newID,
		InstanceID: s.groupInstanceID(newID),
		State:      GroupStateOpen,
	}
	s.groups = append(s.groups, group)
	_ = s.store.SaveGroup(group)
	return group, nil
}

func (s *Service) ensureGroupInstance(ctx context.Context, instanceID string, now int64) error {
	_, err := s.rankService.GetInstance(ctx, instanceID)
	if err == nil {
		return nil
	}
	if err != rank.ErrInstanceNotFound {
		return err
	}
	return s.rankService.OpenInstance(ctx, rank.RankInstance{
		InstanceId: instanceID,
		RankCode:   s.config.RankCode,
		BizId:      s.bizId(),
		State:      rank.InstanceStateOpen,
		OpenTime:   s.config.OpenTime,
		CloseTime:  s.config.CloseTime,
		CreateTime: now,
		UpdateTime: now,
	})
}

func (s *Service) groupInstanceID(groupID int32) string {
	return rank.NewInstanceID(s.config.RankCode, s.bizId(), fmt.Sprintf("group_%d", groupID))
}

func (s *Service) bizId() string {
	return s.config.computeBizId()
}
