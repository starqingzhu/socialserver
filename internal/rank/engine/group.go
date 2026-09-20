package engine

import (
	"context"
	"fmt"
	"sort"

	"common/rank"
)

// ensureGroupLocked 在持有 s.mu 锁时，获取当前可用分组（有空位且处于 Open 状态）。
// 分组容量包含真实玩家和机器人，均计入 RankPeopleNum 上限。
// 若无可用分组则创建新分组并持久化到 Redis。
//
// 多节点：每次从 Redis 读取最新分组列表（同步到内存），确保看到其他节点写入的分组和状态变更。
func (s *Service) ensureGroupLocked() (*Group, error) {
	// 从 Redis 拉取最新分组列表，合并到内存。
	// 这样即使其他节点已将某分组标记为 Full / Settled，本节点也能感知到。
	if s.store.available() {
		if fresh, err := s.store.LoadGroups(); err == nil && len(fresh) > 0 {
			sort.Slice(fresh, func(i, j int) bool { return fresh[i].GroupID < fresh[j].GroupID })
			s.mergeGroupsLocked(fresh)
		}
	}

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

// mergeGroupsLocked 将从 Redis 加载的分组列表合并到内存。
// 对已存在的分组，用 Redis 版本覆盖内存版本（Redis 更新）；
// 对内存中没有的分组，追加进来。
// 必须在持有 s.mu 锁时调用。
func (s *Service) mergeGroupsLocked(fresh []*Group) {
	byID := make(map[int32]*Group, len(s.groups))
	for _, g := range s.groups {
		byID[g.GroupID] = g
	}
	for _, fg := range fresh {
		if existing, ok := byID[fg.GroupID]; ok {
			*existing = *fg // 用 Redis 最新状态覆盖内存
		} else {
			s.groups = append(s.groups, fg)
			byID[fg.GroupID] = fg
			if fg.GroupID > s.nextGroupID {
				s.nextGroupID = fg.GroupID
			}
		}
	}
}

// groupInstanceState 记录某实例上次向 Redis 确认"存在"的时间。
// 只做正向缓存（只记录已确认存在），不做否定缓存，因此不会产生误判：
// 标记缺失或已过期时 ensureGroupInstance 总是回源 GetInstance 重新确认（第 03 条）。
type groupInstanceState struct {
	verifiedAt int64
}

// instanceVerifyInterval 是 instanceStates 正向缓存的有效期（毫秒）。
// 超过这个时长后重新向 Redis 确认，而不是永久信任内存标记——
// 这样 CleanupLiveData 的 TTL 过期、Redis 被清空等场景都能在有效期内自愈，
// 不需要穷举所有可能让实例失效的路径。
const instanceVerifyInterval = 60 * 1000

// ensureGroupInstance 确保 instanceID 对应的 rank 实例已存在（不存在则创建）。
// 命中有效期内的正向缓存时跳过 GetInstance，避免每次 UpsertScore 都产生一次 Redis RTT
// （第 03 条）。
func (s *Service) ensureGroupInstance(ctx context.Context, instanceID string, groupID int32, now int64) error {
	s.cacheMu.RLock()
	st, ok := s.instanceStates[instanceID]
	s.cacheMu.RUnlock()
	if ok && now-st.verifiedAt < instanceVerifyInterval {
		return nil
	}

	inst, err := s.rankService.GetInstance(ctx, instanceID)
	if err == nil {
		// rank:inst 已在 Redis 中；同步持久化到 MongoDB（幂等，若已存在则覆盖更新）。
		_ = s.store.SaveRankInst(groupID, *inst)
		s.markInstanceVerified(instanceID, now)
		return nil
	}
	if err != rank.ErrInstanceNotFound {
		return err
	}
	newInst := rank.RankInstance{
		InstanceId:  instanceID,
		RankCode:    s.config.RankCode,
		BizId:       s.bizId(),
		State:       rank.InstanceStateOpen,
		OpenTime:    s.config.OpenTime,
		CloseTime:   s.config.CloseTime,
		GameEndTime: s.config.GameEndTime,
		CreateTime:  now,
		UpdateTime:  now,
	}
	if openErr := s.rankService.OpenInstance(ctx, newInst); openErr != nil {
		return openErr
	}
	// 持久化新创建的实例元数据到 MongoDB。
	_ = s.store.SaveRankInst(groupID, newInst)
	s.markInstanceVerified(instanceID, now)
	return nil
}

// markInstanceVerified 记录 instanceID 已在 now 时刻向 Redis 确认存在。
func (s *Service) markInstanceVerified(instanceID string, now int64) {
	s.cacheMu.Lock()
	if s.instanceStates == nil {
		s.instanceStates = make(map[string]groupInstanceState)
	}
	s.instanceStates[instanceID] = groupInstanceState{verifiedAt: now}
	s.cacheMu.Unlock()
}

// invalidateInstanceState 清除 instanceID 的正向缓存标记。
// 用于实例被显式设置 TTL 后（CleanupLiveData）立即失效，不必等待 instanceVerifyInterval
// 自然到期——Cleanup() 场景无需调用，因为 Service 本身会随之从内存摘除。
func (s *Service) invalidateInstanceState(instanceID string) {
	s.cacheMu.Lock()
	delete(s.instanceStates, instanceID)
	s.cacheMu.Unlock()
}

func (s *Service) groupInstanceID(groupID int32) string {
	return rank.NewInstanceID(s.config.RankCode, s.bizId(), fmt.Sprintf("group_%d", groupID))
}

func (s *Service) bizId() string {
	return s.config.computeBizId()
}
