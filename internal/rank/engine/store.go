package engine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
	goredis "golib/redis"
	"golib/zaplog"
)

// Store 封装排行榜业务层的缓存和持久化操作。
// 写操作：先写 Redis 缓存，再写 MongoDB 持久化。
// 读操作：先读 Redis，miss 时从 MongoDB 加载并回填 Redis。
// 恢复策略：若 Redis 和 MongoDB 均为空，写入 mongoChecked 哨兵 key，防止重启后重复打 MongoDB。
// rdb/dao 为 nil 时退化为 no-op（纯内存模式，用于测试）。
type Store struct {
	rdb   *goredis.Redis
	dao   *DAO
	bizId string
}

// nullCacheEntry 负向缓存哨兵字符串。
// 存储在 Redis HASH field 中表示"MongoDB 已查询且未找到"，防止重复查 MongoDB。
const nullCacheEntry = "\x00"

// mongoCheckedTTL 是 mongoDB 已查哨兵 key 的过期时间。
// TTL 内 Redis+MongoDB 均为空的 bizId 将直接返回空，跳过 MongoDB 查询。
const mongoCheckedTTL = 10 * time.Minute

// isMongoChecked 返回 true 表示该 bizId 的 MongoDB 已被查询过且当时为空。
func (st *Store) isMongoChecked() bool {
	ok, _ := st.rdb.Exists(rediskeys.GetRankMongoCheckedKey(st.bizId))
	return ok
}

// setMongoChecked 设置哨兵：表示 MongoDB 已被查询过且为空（即全新活动，尚无数据）。
func (st *Store) setMongoChecked() {
	_ = st.rdb.SetEX(rediskeys.GetRankMongoCheckedKey(st.bizId), "1", mongoCheckedTTL)
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
	st.rdb.HSet(rediskeys.GetRankGroupsKey(st.bizId), strconv.FormatInt(int64(group.GroupID), 10), string(data))

	if st.hasMongo() {
		st.dao.SaveGroup(st.bizId, group)
	}
	return nil
}

func (st *Store) LoadGroups() ([]*Group, error) {
	if !st.available() {
		return nil, nil
	}
	raw, err := st.rdb.HGetAll(rediskeys.GetRankGroupsKey(st.bizId))
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
	if !st.hasMongo() {
		return nil, nil
	}
	// Redis 为空且 MongoDB 已被查询过（为空），跳过查询防止冲击 MongoDB
	if st.isMongoChecked() {
		return nil, nil
	}
	groups, err := st.dao.LoadGroups(st.bizId)
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		// MongoDB 也为空（全新活动），设置哨兵防止重复查询
		st.setMongoChecked()
		return nil, nil
	}
	for _, g := range groups {
		data, _ := json.Marshal(g)
		st.rdb.HSet(rediskeys.GetRankGroupsKey(st.bizId), strconv.FormatInt(int64(g.GroupID), 10), string(data))
	}
	return groups, nil
}

// LoadGroupByID 从 Redis 读取单个分组的最新状态。Redis 无数据时返回 nil, nil。
func (st *Store) LoadGroupByID(groupID int32) (*Group, error) {
	if !st.available() {
		return nil, nil
	}
	raw, err := st.rdb.HGet(rediskeys.GetRankGroupsKey(st.bizId), strconv.FormatInt(int64(groupID), 10))
	if err != nil {
		if st.rdb.IsNil(err) {
			return nil, nil
		}
		return nil, err
	}
	var g Group
	if err := json.Unmarshal([]byte(raw), &g); err != nil {
		return nil, err
	}
	return &g, nil
}

// IncrRealCount 原子递增指定分组的真实玩家计数，并更新 Redis 中的分组 JSON。
// 返回更新后的新计数。多节点并发分配新用户时，防止 read-modify-write 竞争导致 RealCount 偏低。
func (st *Store) IncrRealCount(group *Group) (int32, error) {
	if !st.available() {
		group.RealCount++
		return group.RealCount, nil
	}
	key := rediskeys.GetRankGroupsKey(st.bizId)
	field := strconv.FormatInt(int64(group.GroupID), 10)

	// 先用 HIncrBy 对 realCount 原子加 1，得到最新值。
	// 由于 group 以整体 JSON 存储，需读出最新值更新 struct 再写回。
	newCount, err := st.rdb.HIncrBy(rediskeys.GetRankMetaKey(st.bizId), fmt.Sprintf("realCount_%d", group.GroupID), 1)
	if err != nil {
		// Redis 失败时退化为内存递增
		group.RealCount++
		return group.RealCount, nil
	}
	group.RealCount = int32(newCount)
	data, _ := json.Marshal(group)
	st.rdb.HSet(key, field, string(data))
	if st.hasMongo() {
		st.dao.SaveGroup(st.bizId, group)
	}
	return group.RealCount, nil
}

func (st *Store) NextGroupID() (int32, error) {
	if !st.available() {
		return 0, fmt.Errorf("store not available")
	}
	val, err := st.rdb.HIncrBy(rediskeys.GetRankMetaKey(st.bizId), "nextGroupID", 1)
	if err != nil {
		return 0, err
	}
	return int32(val), nil
}

// SaveActivityTimes 将活动的 openTime / closeTime / gameEndTime 写入 meta hash，用于重启后的 Redis 恢复。
func (st *Store) SaveActivityTimes(openTime, closeTime, gameEndTime int64) {
	if !st.available() {
		return
	}
	key := rediskeys.GetRankMetaKey(st.bizId)
	st.rdb.HSet(key, "openTime", strconv.FormatInt(openTime, 10))
	st.rdb.HSet(key, "closeTime", strconv.FormatInt(closeTime, 10))
	st.rdb.HSet(key, "gameEndTime", strconv.FormatInt(gameEndTime, 10))
}

// LoadActivityTimes 从 meta hash 读取活动的 openTime / closeTime / gameEndTime。
// ok=false 表示数据不存在或不完整。
func (st *Store) LoadActivityTimes() (openTime, closeTime, gameEndTime int64, ok bool) {
	if !st.available() {
		return
	}
	key := rediskeys.GetRankMetaKey(st.bizId)
	fields, err := st.rdb.HGetAll(key)
	if err != nil || len(fields) == 0 {
		return
	}
	openTime, _ = strconv.ParseInt(fields["openTime"], 10, 64)
	closeTime, _ = strconv.ParseInt(fields["closeTime"], 10, 64)
	gameEndTime, _ = strconv.ParseInt(fields["gameEndTime"], 10, 64)
	ok = openTime > 0 && closeTime > 0
	return
}

// RestoreNextGroupID 确保 rank:meta 中的 nextGroupID 计数器 ≥ minID。
// Redis 被清理后 nextGroupID 为 0，新分组 HIncrBy 返回 1，可能与已有分组冲突。
func (st *Store) RestoreNextGroupID(minID int32) {
	if !st.available() || minID <= 0 {
		return
	}
	key := rediskeys.GetRankMetaKey(st.bizId)
	cur, _ := st.rdb.HGet(key, "nextGroupID")
	curID, _ := strconv.ParseInt(cur, 10, 32)
	if int32(curID) < minID {
		st.rdb.HSet(key, "nextGroupID", strconv.FormatInt(int64(minID), 10))
	}
}

// RestoreSettled 将结算快照强制写入 Redis rank:settled 键（冷启动恢复用）。
// 使 rankService.GetMemRank 在 Redis 恢复后能正确读取结算数据。
func (st *Store) RestoreSettled(instanceID string, snaps []commonrank.RankMemberSnapshot) {
	if !st.available() || len(snaps) == 0 {
		return
	}
	data, err := json.Marshal(snaps)
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: marshal settled for restore instanceID=%s: %v", instanceID, err)
		return
	}
	_ = st.rdb.Set(rediskeys.GetRankSettledKey(instanceID), string(data))
}

// --- 成员映射 ---

func (st *Store) SetMember(userID int64, groupID int32) error {
	if !st.available() {
		return nil
	}
	st.rdb.HSet(rediskeys.GetRankMembersKey(st.bizId), strconv.FormatInt(userID, 10), strconv.FormatInt(int64(groupID), 10))

	if st.hasMongo() {
		st.dao.SaveMember(st.bizId, userID, groupID)
	}
	return nil
}

func (st *Store) GetMember(userID int64) (int32, bool, error) {
	if !st.available() {
		return 0, false, nil
	}
	uidStr := strconv.FormatInt(userID, 10)
	raw, err := st.rdb.HGet(rediskeys.GetRankMembersKey(st.bizId), uidStr)
	if err == nil {
		if raw == nullCacheEntry {
			return 0, false, nil // 负向缓存命中：跳过 MongoDB
		}
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
			st.rdb.HSet(rediskeys.GetRankMembersKey(st.bizId), uidStr, strconv.FormatInt(int64(gid), 10))
			return gid, true, nil
		}
	}
	return 0, false, nil
}

func (st *Store) GetAllMembers() (map[int64]int32, error) {
	if !st.available() {
		return nil, nil
	}
	raw, err := st.rdb.HGetAll(rediskeys.GetRankMembersKey(st.bizId))
	if err == nil && len(raw) > 0 {
		result := make(map[int64]int32, len(raw))
		for k, v := range raw {
			if v == nullCacheEntry {
				continue // 跳过负向缓存哨兵
			}
			uid, _ := strconv.ParseInt(k, 10, 64)
			gid, _ := strconv.ParseInt(v, 10, 32)
			if uid != 0 {
				result[uid] = int32(gid)
			}
		}
		if len(result) > 0 {
			return result, nil
		}
	}
	if !st.hasMongo() {
		return nil, nil
	}
	// Redis 为空且 MongoDB 已被查询过（为空），跳过查询防止冲击 MongoDB
	if st.isMongoChecked() {
		return nil, nil
	}
	members, err := st.dao.LoadAllMembers(st.bizId)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		st.setMongoChecked()
		return nil, nil
	}
	for uid, gid := range members {
		st.rdb.HSet(rediskeys.GetRankMembersKey(st.bizId), strconv.FormatInt(uid, 10), strconv.FormatInt(int64(gid), 10))
	}
	return members, nil
}

// --- 机器人状态 ---

func (st *Store) SaveRobots(groupID int32, robots []*robotState) error {
	if !st.available() || len(robots) == 0 {
		return nil
	}
	key := rediskeys.GetRankRobotsKey(st.bizId, groupID)
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
	key := rediskeys.GetRankRobotsKey(st.bizId, groupID)
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

func (st *Store) SaveUsedInfoIDs(groupID int32, ids map[int64]struct{}) error {
	if !st.available() || len(ids) == 0 {
		return nil
	}
	members := make([]interface{}, 0, len(ids))
	for id := range ids {
		members = append(members, strconv.FormatInt(int64(id), 10))
	}
	st.rdb.SAdd(rediskeys.GetRankRobotInfosKey(st.bizId, groupID), members...)
	return nil
}

func (st *Store) LoadUsedInfoIDs(groupID int32) (map[int64]struct{}, error) {
	if !st.available() {
		return nil, nil
	}
	raw, err := st.rdb.SMembers(rediskeys.GetRankRobotInfosKey(st.bizId, groupID))
	if err != nil {
		return nil, err
	}
	result := make(map[int64]struct{}, len(raw))
	for _, s := range raw {
		id, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			continue
		}
		result[id] = struct{}{}
	}
	return result, nil
}

// settledDataRetentionTTL 已结算轮次 Redis 热数据的保留时长（2 周）。
// 轮次结束后热数据不立即清理，而是保留 2 周承接历史查询；到期后由 Redis 异步回收，避免大 key 阻塞。
const settledDataRetentionTTL = commonrank.SettledCacheTTL

// CleanupLiveData 为该轮次的 Redis 数据设置 2 周保留 TTL（meta/分组/成员/机器人/查询哨兵）。
// 用于周期排行榜历史轮次的延迟清理（清理窗口 = 1 个周期，之后保留 2 周）。
// 不删除 rank:settled（同样 2 周 TTL）和 MongoDB（永久保留）。
func (st *Store) CleanupLiveData(groups []*Group) {
	if !st.available() {
		return
	}
	if len(groups) == 0 {
		if loaded, err := st.LoadGroups(); err == nil {
			groups = loaded
		}
	}
	st.rdb.Expire(rediskeys.GetRankMetaKey(st.bizId), settledDataRetentionTTL)
	st.rdb.Expire(rediskeys.GetRankGroupsKey(st.bizId), settledDataRetentionTTL)
	st.rdb.Expire(rediskeys.GetRankMembersKey(st.bizId), settledDataRetentionTTL)
	st.rdb.Expire(rediskeys.GetRankClaimsKey(st.bizId), settledDataRetentionTTL)
	st.rdb.Expire(rediskeys.GetRankMongoCheckedKey(st.bizId), settledDataRetentionTTL)
	for _, g := range groups {
		if g == nil {
			continue
		}
		st.rdb.Expire(rediskeys.GetRankRobotsKey(st.bizId, g.GroupID), settledDataRetentionTTL)
		st.rdb.Expire(rediskeys.GetRankRobotInfosKey(st.bizId, g.GroupID), settledDataRetentionTTL)
	}
}

func (st *Store) CleanupAll(groups []*Group) {
	if !st.available() {
		return
	}
	// 若调用方未传入分组列表（懒加载未触发），从 Redis 加载以确保分组级 key 被清除。
	if len(groups) == 0 {
		if loaded, err := st.LoadGroups(); err == nil {
			groups = loaded
		}
	}

	// 在清除 Redis 之前，先根据 Redis 中现有数据计算所有可能的 MongoDB docID，
	// 通过写队列推送 DeleteOne 任务（hashFactor = docID，与写任务一致），
	// 确保删除任务排在队列中所有同 docID 的写任务之后执行，覆盖尚未落库的 upsert。
	if st.hasMongo() {
		st.queueDeleteAllDocIDs(groups)
	}

	st.rdb.Del(rediskeys.GetRankMetaKey(st.bizId))
	st.rdb.Del(rediskeys.GetRankGroupsKey(st.bizId))
	st.rdb.Del(rediskeys.GetRankMembersKey(st.bizId))
	st.rdb.Del(rediskeys.GetRankClaimsKey(st.bizId))
	st.rdb.Del(rediskeys.GetRankMongoCheckedKey(st.bizId))
	for _, g := range groups {
		if g == nil {
			continue
		}
		st.rdb.Del(rediskeys.GetRankRobotsKey(st.bizId, g.GroupID))
		st.rdb.Del(rediskeys.GetRankRobotInfosKey(st.bizId, g.GroupID))
	}

	// 同步 DeleteMany 清除调用时 MongoDB 中已存在的文档。
	// 与 queueDeleteAllDocIDs 组合：前者覆盖写任务后写入的数据，后者覆盖当前已有数据。
	if st.hasMongo() {
		if err := st.dao.DeleteAllByBizId(st.bizId); err != nil {
			zaplog.LoggerSugar.Errorf("rank engine: CleanupAll delete mongo bizId=%s: %v", st.bizId, err)
		}
	}
}

// queueDeleteAllDocIDs 在清 Redis 之前，根据 Redis 中 groups/members/robots 数据
// 计算出所有可能的 MongoDB docID，逐条推入写队列（hashFactor = docID）。
// 这保证即使写任务在 DeleteMany 之后执行写入了数据，后入队的删除任务也会将其清除。
func (st *Store) queueDeleteAllDocIDs(groups []*Group) {
	if !st.hasMongo() {
		return
	}

	// 读取 members（userID→groupID）
	members, _ := st.GetAllMembers()

	// 按分组计算并推送所有集合的删除任务
	for _, g := range groups {
		if g == nil {
			continue
		}
		gid := g.GroupID
		bizId := st.bizId

		// rank_group
		st.dao.QueueDeleteDocIDs(commonrank.CT_RANK_GROUP, []string{
			fmt.Sprintf("%s:%d", bizId, gid),
		})
		// rank_inst
		st.dao.QueueDeleteDocIDs(commonrank.CT_RANK_INST, []string{
			fmt.Sprintf("%s:%d", bizId, gid),
		})
		// rank_settled
		st.dao.QueueDeleteDocIDs(commonrank.CT_RANK_SETTLED, []string{
			fmt.Sprintf("%s:%d", bizId, gid),
		})

		// rank_robot：从 Redis 读取该 group 的所有机器人 memberID
		robotRaw, err := st.rdb.HGetAll(rediskeys.GetRankRobotsKey(bizId, gid))
		if err == nil {
			robotIDs := make([]string, 0, len(robotRaw))
			for memberIDStr := range robotRaw {
				robotIDs = append(robotIDs, fmt.Sprintf("%s:%d:%s", bizId, gid, memberIDStr))
			}
			st.dao.QueueDeleteDocIDs(commonrank.CT_RANK_ROBOT, robotIDs)
		}
	}

	// rank_member 和 rank_score：遍历所有 members
	memberDocIDs := make([]string, 0, len(members))
	scoreDocIDs := make([]string, 0, len(members))
	claimDocIDs := make([]string, 0, len(members))
	for uid, gid := range members {
		uidStr := strconv.FormatInt(uid, 10)
		bizId := st.bizId
		memberDocIDs = append(memberDocIDs, fmt.Sprintf("%s:%s", bizId, uidStr))
		scoreDocIDs = append(scoreDocIDs, fmt.Sprintf("%s:%d:%s", bizId, gid, uidStr))
		claimDocIDs = append(claimDocIDs, fmt.Sprintf("%s:%s", bizId, uidStr))
	}
	st.dao.QueueDeleteDocIDs(commonrank.CT_RANK_MEMBER, memberDocIDs)
	st.dao.QueueDeleteDocIDs(commonrank.CT_RANK_SCORE, scoreDocIDs)
	st.dao.QueueDeleteDocIDs(commonrank.CT_RANK_CLAIM, claimDocIDs)
}

// --- 奖励领取记录 ---

// atomicClaimScript atomically claims a reward for a user.
// It handles the null cache sentinel ("\x00") transparently:
//
//	KEYS[1]: rank:claims hash key
//	ARGV[1]: userID field name
//	ARGV[2]: current timestamp (string)
//	ARGV[3]: null sentinel value
//
// Returns {0, now} on first claim, {1, existing_timestamp} if already claimed.
const atomicClaimScript = `
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur and cur ~= ARGV[3] then
    return {1, cur}
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return {0, ARGV[2]}
`

// AtomicClaim atomically marks a user's reward as claimed.
// Returns claimed=false on first claim (caller should distribute reward),
// or claimed=true with the recorded timestamp if already claimed.
// Uses a Lua script for Redis atomicity and falls back to MongoDB to handle Redis eviction.
func (st *Store) AtomicClaim(userID int64, now int64) (claimed bool, claimTime int64, err error) {
	uidStr := strconv.FormatInt(userID, 10)
	nowStr := strconv.FormatInt(now, 10)

	if !st.available() {
		if st.hasMongo() {
			ct, found, e := st.dao.GetClaim(st.bizId, userID)
			if e != nil {
				return false, 0, e
			}
			if found {
				return true, ct, nil
			}
			if e = st.dao.SaveClaim(st.bizId, userID, now); e != nil {
				return false, 0, e
			}
		}
		return false, now, nil
	}

	claimsKey := rediskeys.GetRankClaimsKey(st.bizId)

	// Atomic Redis check-and-set; handles null sentinel and concurrent claims.
	result, evalErr := st.rdb.Eval("", atomicClaimScript, []string{claimsKey}, uidStr, nowStr, nullCacheEntry)
	if evalErr != nil {
		return false, 0, evalErr
	}

	ret, ok := result.([]interface{})
	if !ok || len(ret) < 2 {
		return false, 0, fmt.Errorf("AtomicClaim: unexpected result type %T", result)
	}
	claimedInt, _ := ret[0].(int64)
	ctStr, _ := ret[1].(string)
	ct, _ := strconv.ParseInt(ctStr, 10, 64)

	if claimedInt == 1 {
		return true, ct, nil
	}

	// Lua said first claim — verify against MongoDB to handle the Redis eviction case
	// (Redis was flushed after a previous successful claim).
	if st.hasMongo() {
		mongoTime, found, mongoErr := st.dao.GetClaim(st.bizId, userID)
		if mongoErr != nil {
			return false, 0, mongoErr
		}
		if found {
			// Prior claim exists in MongoDB; Redis was evicted. Restore the cache entry.
			st.rdb.HSet(claimsKey, uidStr, strconv.FormatInt(mongoTime, 10))
			return true, mongoTime, nil
		}
		// Genuinely first claim — persist atomically via $setOnInsert so Redis eviction can't cause
		// a duplicate (SaveClaim uses an async write queue and silently drops errors, which is unsafe here).
		isFirst, savedTime, e := st.dao.SaveClaimIfNotExists(st.bizId, userID, now)
		if e != nil {
			return false, 0, e
		}
		if !isFirst {
			// Concurrent write from another node won the race; update Redis and report as already claimed.
			st.rdb.HSet(claimsKey, uidStr, strconv.FormatInt(savedTime, 10))
			return true, savedTime, nil
		}
	}
	return false, now, nil
}

// TryLockSettle tries to atomically acquire a per-group settle lock.
// Returns true if this node won the race to settle the group.
// TTL of 10 minutes handles the case where the winning node crashes before completing.
func (st *Store) TryLockSettle(groupID int32) bool {
	if !st.available() {
		return true
	}
	lockKey := fmt.Sprintf("rank:settle:{%s}:%d", st.bizId, groupID)
	locked, err := st.rdb.SetNX(lockKey, "1", 10*time.Minute)
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: TryLockSettle bizId=%s group=%d: %v", st.bizId, groupID, err)
		return true // treat as locked on error so settle still runs
	}
	return locked
}

// TryLockRobotTick tries to acquire the per-second robot tick lock for this service.
// Returns true if this node should run the robot tick for this second.
// Using nowMs/1000 as the tick key means each wall-clock second has a distinct lock.
func (st *Store) TryLockRobotTick(nowMs int64) bool {
	if !st.available() {
		return true
	}
	tickSec := nowMs / 1000
	lockKey := fmt.Sprintf("rank:robot_tick:{%s}:%d", st.bizId, tickSec)
	locked, err := st.rdb.SetNX(lockKey, "1", 3*time.Second)
	if err != nil {
		return true // treat as unlocked on error so robots still progress
	}
	return locked
}

func (st *Store) SetClaim(userID int64, claimTime int64) error {
	if !st.available() {
		return nil
	}
	st.rdb.HSet(rediskeys.GetRankClaimsKey(st.bizId),
		strconv.FormatInt(userID, 10),
		strconv.FormatInt(claimTime, 10))

	if st.hasMongo() {
		st.dao.SaveClaim(st.bizId, userID, claimTime)
	}
	return nil
}

func (st *Store) GetClaim(userID int64) (int64, bool, error) {
	if !st.available() {
		return 0, false, nil
	}
	uidStr := strconv.FormatInt(userID, 10)
	raw, err := st.rdb.HGet(rediskeys.GetRankClaimsKey(st.bizId), uidStr)
	if err == nil {
		if raw == nullCacheEntry {
			return 0, false, nil // 负向缓存命中：跳过 MongoDB
		}
		t, err := strconv.ParseInt(raw, 10, 64)
		if err == nil {
			return t, true, nil
		}
	}
	if err != nil && !st.rdb.IsNil(err) {
		return 0, false, err
	}
	if st.hasMongo() {
		t, found, err := st.dao.GetClaim(st.bizId, userID)
		if err != nil {
			return 0, false, err
		}
		if found {
			st.rdb.HSet(rediskeys.GetRankClaimsKey(st.bizId), uidStr, strconv.FormatInt(t, 10))
			return t, true, nil
		}
		// MongoDB 未找到：写入负向缓存，防止同一用户重复查 MongoDB
		st.rdb.HSet(rediskeys.GetRankClaimsKey(st.bizId), uidStr, nullCacheEntry)
	}
	return 0, false, nil
}

// --- 成员积分持久化 ---

// SaveScore 将成员积分写入 MongoDB（write-through）。
// enterTime 和 sequence 仅在首次插入时设置（$setOnInsert），避免恢复时覆盖原始进榜时间和序号。
func (st *Store) SaveScore(groupID int32, userID int64, score int64, enterTime int64, sequence int64, updateTime int64, avatarInfo *commonrank.AvatarInfo) error {
	if !st.hasMongo() {
		return nil
	}
	return st.dao.SaveScore(st.bizId, groupID, userID, score, enterTime, sequence, updateTime, avatarInfo)
}

// LoadGroupScores 从 MongoDB 加载指定分组的全部成员积分。
func (st *Store) LoadGroupScores(groupID int32) ([]ScoreDoc, error) {
	if !st.hasMongo() {
		return nil, nil
	}
	return st.dao.LoadGroupScores(st.bizId, groupID)
}

// RdbExists 检查 Redis 键是否存在（用于冷启动恢复时判断 rank:mb 是否缺失）。
func (st *Store) RdbExists(key string) (bool, error) {
	if !st.available() {
		return false, nil
	}
	return st.rdb.Exists(key)
}

// --- 结算快照持久化 ---

// SaveSettled 将结算快照写入 MongoDB。
func (st *Store) SaveSettled(groupID int32, snaps []commonrank.RankMemberSnapshot, settleTime int64) error {
	if !st.hasMongo() {
		return nil
	}
	return st.dao.SaveSettled(st.bizId, groupID, snaps, settleTime)
}

// LoadGroupSettled 从 MongoDB 加载指定分组的结算快照。
func (st *Store) LoadGroupSettled(groupID int32) ([]commonrank.RankMemberSnapshot, error) {
	if !st.hasMongo() {
		return nil, nil
	}
	return st.dao.LoadGroupSettled(st.bizId, groupID)
}

// LoadGroupSettledCached 读取指定分组结算快照：优先 Redis rank:settled，
// miss 时从 MongoDB 加载并写回 Redis（2 周 TTL），避免历史查询反复打 Mongo。
// instanceID 为对应分组实例 ID（rankCode:bizId:group_N），与 RestoreSettled 同 key。
func (st *Store) LoadGroupSettledCached(instanceID string, groupID int32) ([]commonrank.RankMemberSnapshot, error) {
	if st.available() {
		key := rediskeys.GetRankSettledKey(instanceID)
		if raw, err := st.rdb.Get(key); err == nil {
			var snaps []commonrank.RankMemberSnapshot
			if json.Unmarshal([]byte(raw), &snaps) == nil && len(snaps) > 0 {
				_, _ = st.rdb.Expire(key, commonrank.SettledCacheTTL)
				return snaps, nil
			}
		} else if !st.rdb.IsNil(err) {
			return nil, err
		}
	}
	if !st.hasMongo() {
		return nil, nil
	}
	snaps, err := st.dao.LoadGroupSettled(st.bizId, groupID)
	if err != nil {
		return nil, err
	}
	if st.available() && len(snaps) > 0 {
		key := rediskeys.GetRankSettledKey(instanceID)
		if data, err := json.Marshal(snaps); err == nil {
			_ = st.rdb.SetEX(key, string(data), commonrank.SettledCacheTTL)
		}
	}
	return snaps, nil
}

// --- 榜单实例元数据持久化 ---

// SaveRankInst 异步将榜单实例元数据写入 MongoDB（rank:inst 持久化备份）。
func (st *Store) SaveRankInst(groupID int32, inst commonrank.RankInstance) error {
	if !st.hasMongo() {
		return nil
	}
	return st.dao.SaveRankInst(st.bizId, groupID, inst)
}

// LoadGroupInst 从 MongoDB 加载指定分组的榜单实例元数据。未找到时返回 nil, nil。
func (st *Store) LoadGroupInst(groupID int32) (*commonrank.RankInstance, error) {
	if !st.hasMongo() {
		return nil, nil
	}
	return st.dao.LoadGroupInst(st.bizId, groupID)
}
