package balloon

import (
	"encoding/json"
	"fmt"
	"strconv"

	rediskeys "common/redis"
	goredis "golib/redis"
)

// Store 封装 balloon 业务层的缓存和持久化操作。
// 写操作：先写 Redis 缓存，再写 MongoDB 持久化。
// 读操作：先读 Redis，miss 时从 MongoDB 加载并回填 Redis。
// rdb/dao 为 nil 时退化为 no-op（纯内存模式，用于测试）。
type Store struct {
	rdb   *goredis.Redis
	dao   *DAO
	bizId string
}

func NewStore(rdb *goredis.Redis, dao *DAO, bizId string) *Store {
	return &Store{rdb: rdb, dao: dao, bizId: bizId}
}

func (st *Store) available() bool { return st != nil && st.rdb != nil }
func (st *Store) hasMongo() bool  { return st != nil && st.dao != nil && st.dao.available() }

// --- 分组管理 ---

func (st *Store) SaveGroup(group *Group) error {
	if !st.available() {
		return nil
	}
	data, _ := json.Marshal(group)
	st.rdb.HSet(rediskeys.GetBalloonGroupsKey(st.bizId), strconv.FormatInt(int64(group.GroupID), 10), string(data))

	if st.hasMongo() {
		st.dao.SaveGroup(st.bizId, group)
	}
	return nil
}

func (st *Store) LoadGroups() ([]*Group, error) {
	if !st.available() {
		return nil, nil
	}
	raw, err := st.rdb.HGetAll(rediskeys.GetBalloonGroupsKey(st.bizId))
	if err == nil && len(raw) > 0 {
		groups := make([]*Group, 0, len(raw))
		for _, v := range raw {
			var g Group
			if err := json.Unmarshal([]byte(v), &g); err != nil {
				continue
			}
			groups = append(groups, &g)
		}
		return groups, nil
	}
	if st.hasMongo() {
		groups, err := st.dao.LoadGroups(st.bizId)
		if err != nil {
			return nil, err
		}
		for _, g := range groups {
			data, _ := json.Marshal(g)
			st.rdb.HSet(rediskeys.GetBalloonGroupsKey(st.bizId), strconv.FormatInt(int64(g.GroupID), 10), string(data))
		}
		return groups, nil
	}
	return nil, nil
}

// NextGroupID 原子递增分组 ID 计数器。
func (st *Store) NextGroupID() (int32, error) {
	if !st.available() {
		return 0, fmt.Errorf("store not available")
	}
	val, err := st.rdb.HIncrBy(rediskeys.GetBalloonMetaKey(st.bizId), "nextGroupID", 1)
	if err != nil {
		return 0, err
	}
	return int32(val), nil
}

// --- 成员映射 ---

func (st *Store) SetMember(userID int64, groupID int32) error {
	if !st.available() {
		return nil
	}
	st.rdb.HSet(rediskeys.GetBalloonMembersKey(st.bizId), strconv.FormatInt(userID, 10), strconv.FormatInt(int64(groupID), 10))

	if st.hasMongo() {
		st.dao.SaveMember(st.bizId, userID, groupID)
	}
	return nil
}

func (st *Store) GetMember(userID int64) (int32, bool, error) {
	if !st.available() {
		return 0, false, nil
	}
	raw, err := st.rdb.HGet(rediskeys.GetBalloonMembersKey(st.bizId), strconv.FormatInt(userID, 10))
	if err == nil {
		gid, err := strconv.ParseInt(raw, 10, 32)
		if err == nil {
			return int32(gid), true, nil
		}
	}
	if err != nil && !st.rdb.IsNil(err) {
		return 0, false, err
	}
	if st.hasMongo() {
		gid, found, err := st.dao.GetMember(st.bizId, userID)
		if err != nil {
			return 0, false, err
		}
		if found {
			st.rdb.HSet(rediskeys.GetBalloonMembersKey(st.bizId), strconv.FormatInt(userID, 10), strconv.FormatInt(int64(gid), 10))
			return gid, true, nil
		}
	}
	return 0, false, nil
}

func (st *Store) GetAllMembers() (map[int64]int32, error) {
	if !st.available() {
		return nil, nil
	}
	raw, err := st.rdb.HGetAll(rediskeys.GetBalloonMembersKey(st.bizId))
	if err == nil && len(raw) > 0 {
		result := make(map[int64]int32, len(raw))
		for k, v := range raw {
			uid, _ := strconv.ParseInt(k, 10, 64)
			gid, _ := strconv.ParseInt(v, 10, 32)
			if uid != 0 {
				result[uid] = int32(gid)
			}
		}
		return result, nil
	}
	if st.hasMongo() {
		members, err := st.dao.LoadAllMembers(st.bizId)
		if err != nil {
			return nil, err
		}
		for uid, gid := range members {
			st.rdb.HSet(rediskeys.GetBalloonMembersKey(st.bizId), strconv.FormatInt(uid, 10), strconv.FormatInt(int64(gid), 10))
		}
		return members, nil
	}
	return nil, nil
}

// --- 机器人状态 ---

func (st *Store) SaveRobots(groupID int32, robots []*robotState) error {
	if !st.available() || len(robots) == 0 {
		return nil
	}
	key := rediskeys.GetBalloonRobotsKey(st.bizId, groupID)
	for _, r := range robots {
		data, _ := json.Marshal(r)
		st.rdb.HSet(key, strconv.FormatInt(r.MemberID, 10), string(data))
	}

	if st.hasMongo() {
		st.dao.SaveRobots(st.bizId, groupID, robots)
	}
	return nil
}

func (st *Store) LoadRobots(groupID int32) ([]*robotState, error) {
	if !st.available() {
		return nil, nil
	}
	key := rediskeys.GetBalloonRobotsKey(st.bizId, groupID)
	raw, err := st.rdb.HGetAll(key)
	if err == nil && len(raw) > 0 {
		robots := make([]*robotState, 0, len(raw))
		for _, v := range raw {
			var r robotState
			if err := json.Unmarshal([]byte(v), &r); err != nil {
				continue
			}
			robots = append(robots, &r)
		}
		return robots, nil
	}
	if st.hasMongo() {
		robots, err := st.dao.LoadRobots(st.bizId, groupID)
		if err != nil {
			return nil, err
		}
		for _, r := range robots {
			data, _ := json.Marshal(r)
			st.rdb.HSet(key, strconv.FormatInt(r.MemberID, 10), string(data))
		}
		return robots, nil
	}
	return nil, nil
}

func (st *Store) SaveUsedInfoIDs(groupID int32, ids map[int32]struct{}) error {
	if !st.available() || len(ids) == 0 {
		return nil
	}
	members := make([]interface{}, 0, len(ids))
	for id := range ids {
		members = append(members, strconv.FormatInt(int64(id), 10))
	}
	st.rdb.SAdd(rediskeys.GetBalloonRobotInfosKey(st.bizId, groupID), members...)
	return nil
}

func (st *Store) LoadUsedInfoIDs(groupID int32) (map[int32]struct{}, error) {
	if !st.available() {
		return nil, nil
	}
	raw, err := st.rdb.SMembers(rediskeys.GetBalloonRobotInfosKey(st.bizId, groupID))
	if err != nil {
		return nil, err
	}
	result := make(map[int32]struct{}, len(raw))
	for _, s := range raw {
		id, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			continue
		}
		result[int32(id)] = struct{}{}
	}
	return result, nil
}

// --- 真实玩家最高积分 ---

func (st *Store) UpdateMaxScore(groupID int32, score int64) error {
	if !st.available() {
		return nil
	}
	key := rediskeys.GetBalloonMaxScoreKey(st.bizId)
	field := strconv.FormatInt(int64(groupID), 10)
	cur, err := st.rdb.HGet(key, field)
	if err != nil && !st.rdb.IsNil(err) {
		return err
	}
	if cur != "" {
		if curVal, _ := strconv.ParseInt(cur, 10, 64); curVal >= score {
			return nil
		}
	}
	st.rdb.HSet(key, field, strconv.FormatInt(score, 10))
	return nil
}

func (st *Store) GetMaxScore(groupID int32) (int64, error) {
	if !st.available() {
		return 0, nil
	}
	raw, err := st.rdb.HGet(rediskeys.GetBalloonMaxScoreKey(st.bizId), strconv.FormatInt(int64(groupID), 10))
	if err != nil {
		if st.rdb.IsNil(err) {
			return 0, nil
		}
		return 0, err
	}
	return strconv.ParseInt(raw, 10, 64)
}
