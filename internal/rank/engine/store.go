package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
	goredis "golib/redis"
	"golib/zaplog"
)

// storeMongo 是 Store 用到的 MongoDB 读写方法集合。
// 存在的唯一目的是让 Store 的 Mongo 分支可注入替身——否则 dao 是具体类型 *DAO
// （持有 *mongodbmodule.Session），懒加载回填路径无法在单测里被驱动，
// 而「key 过期后重新回填不会重建成永久 key」正是缺陷 3 唯一重要的断言。
// 生产代码只用 *DAO 实现此接口，不引入任何行为变化。
type storeMongo interface {
	available() bool

	SaveGroup(bizId string, group *Group) error
	LoadGroups(bizId string) ([]*Group, error)

	SaveMember(bizId string, userID int64, groupID int32) error
	GetMember(bizId string, userID int64) (int32, bool, error)
	LoadAllMembers(bizId string) (map[int64]int32, error)

	SaveRobots(bizId string, groupID int32, robots []*robotState) error
	LoadRobots(bizId string, groupID int32) ([]*robotState, error)

	SaveClaimIfNotExists(bizId string, userID int64, claimTime int64) (bool, int64, error)
	SaveClaim(bizId string, userID int64, claimTime int64) error
	GetClaim(bizId string, userID int64) (int64, bool, error)

	SaveScore(bizId string, groupID int32, userID int64, score int64, enterTime int64, sequence int64, updateTime int64, avatarInfo *commonrank.AvatarInfo) error
	LoadGroupScores(bizId string, groupID int32) ([]ScoreDoc, error)

	SaveSettled(bizId string, groupID int32, snaps []commonrank.RankMemberSnapshot, settleTime int64) error
	LoadGroupSettled(bizId string, groupID int32) ([]commonrank.RankMemberSnapshot, error)

	SaveRankInst(bizId string, groupID int32, inst commonrank.RankInstance) error
	LoadGroupInst(bizId string, groupID int32) (*commonrank.RankInstance, error)

	DeleteAllByBizId(bizId string) error
	QueueDeleteDocIDs(coll string, docIDs []string)
}

// Store 封装排行榜业务层的缓存和持久化操作。
// 写操作：先写 Redis 缓存，再写 MongoDB 持久化。
// 读操作：先读 Redis，miss 时从 MongoDB 加载并回填 Redis。
// 恢复策略：若 Redis 和 MongoDB 均为空，写入 mongoChecked 哨兵 key，防止重启后重复打 MongoDB。
// rdb/dao 为 nil 时退化为 no-op（纯内存模式，用于测试）。
type Store struct {
	rdb   *goredis.Redis
	dao   storeMongo
	bizId string

	// activityEnd 返回所属活动的结算时刻（Unix 毫秒），由 engine.NewService 注入
	// （直接传 s.effectiveSettleAt，读的是原子副本）。用闭包而非快照值：GM 通过 UpdateConfig
	// 改写 CloseTime/GameEndTime 后，这里立即读到新值，结构上不可能陈旧。
	// 为 nil 表示该 Store 不属于任何活动实例（历史查询、孤儿清理、周期元数据读取、claim 兜底），
	// 此时所有 TTL 计算退化为 backfillTTLFor(0) == SettledCacheTTL，恰好等于这些路径改造前
	// 手工设的 2 周常量，因此对它们零行为变化。
	activityEnd func() int64
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
//
// 用 SetNX 而不是 SetEX：SET 会连带清掉 key 上已有的 TTL，而本 key 同时也在
// CleanupLiveData / ExpireLiveData 的 key 集合里（被设成 2 周保留期）。若在这里用 SetEX，
// 一次「重新判定 Mongo 为空」就会把那 2 周削成 10 分钟，等于在保留期结束前把哨兵删掉。
// SetNX 只在 key 不存在时写入，永远不会缩短既有 TTL——本哨兵只表达「查过了且是空的」，
// 完全没有必要覆盖一个已经存在的同义哨兵。
func (st *Store) setMongoChecked() {
	_, _ = st.rdb.SetNX(rediskeys.GetRankMongoCheckedKey(st.bizId), "1", mongoCheckedTTL)
}

func NewStore(rdb *goredis.Redis, dao *DAO, bizId string, activityEnd func() int64) *Store {
	return &Store{rdb: rdb, dao: dao, bizId: bizId, activityEnd: activityEnd}
}

// newStoreWithDAO 供包内测试以 storeMongo 替身构造 Store（生产代码一律走 NewStore）。
func newStoreWithDAO(rdb *goredis.Redis, dao storeMongo, bizId string, activityEnd func() int64) *Store {
	return &Store{rdb: rdb, dao: dao, bizId: bizId, activityEnd: activityEnd}
}

func (st *Store) available() bool { return st != nil && st.rdb != nil }
func (st *Store) hasMongo() bool  { return st != nil && st.dao != nil && st.dao.available() }

// activityEndMs 返回活动结算时刻，nil 闭包（非活动上下文的 Store）返回 0。
func (st *Store) activityEndMs() int64 {
	if st == nil || st.activityEnd == nil {
		return 0
	}
	return st.activityEnd()
}

// writeTTL 是写路径的 TTL：活动数据 key 的绝对过期时刻 = activityEnd + SettledCacheTTL。
func (st *Store) writeTTL() time.Duration { return ttlFor(st.activityEndMs()) }

// backfillTTL 是读路径的 TTL：在 writeTTL 之上加 SettledCacheTTL 下限（见 backfillTTLFor）。
func (st *Store) backfillTTL() time.Duration { return backfillTTLFor(st.activityEndMs()) }

// backfill 把「先写数据、再 EXPIRE」固定成懒加载回填的唯一写法。
// 顺序不能反：EXPIRE 作用在不存在的 key 上会被 Redis 静默丢弃（pipeline 内亦然），
// 那样就等于把 key 留成永久的——正是缺陷 3。
func (st *Store) backfill(key string, write func()) {
	if !st.available() {
		return
	}
	write()
	if _, err := st.rdb.Expire(key, st.backfillTTL()); err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: backfill expire key=%s: %v", key, err)
	}
}

// expireKey 是写路径设置活跃期 TTL 的唯一出口（第 02 条）。写路径全部走这里，
// 「不允许存在永久 key」这条不变量就只有一份实现，不会有谁漏掉。
//
// 必须在写命令之后调用：EXPIRE 作用在不存在的 key 上会被 Redis 静默丢弃，
// 顺序反了就等于把 key 留成永久的（与 backfill 同一条不变量，理由也相同）。
// 返回值只在 key 不存在时为 false——那说明写没生效或 key 已到期，与 TTL 设置本身无关，故不告警。
func (st *Store) expireKey(key string) {
	if !st.available() {
		return
	}
	if _, err := st.rdb.Expire(key, st.writeTTL()); err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: write expire key=%s bizId=%s: %v", key, st.bizId, err)
	}
}

// --- 分组管理 ---

func (st *Store) SaveGroup(group *Group) error {
	if !st.available() {
		return nil
	}
	data, _ := json.Marshal(group)
	key := rediskeys.GetRankGroupsKey(st.bizId)
	st.rdb.HSet(key, strconv.FormatInt(int64(group.GroupID), 10), string(data))
	st.expireKey(key)

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
	key := rediskeys.GetRankGroupsKey(st.bizId)
	st.backfill(key, func() {
		for _, g := range groups {
			data, _ := json.Marshal(g)
			st.rdb.HSet(key, strconv.FormatInt(int64(g.GroupID), 10), string(data))
		}
	})
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
	metaKey := rediskeys.GetRankMetaKey(st.bizId)

	// 先用 HIncrBy 对 realCount 原子加 1，得到最新值。
	// 由于 group 以整体 JSON 存储，需读出最新值更新 struct 再写回。
	newCount, err := st.rdb.HIncrBy(metaKey, fmt.Sprintf("realCount_%d", group.GroupID), 1)
	if err != nil {
		// Redis 失败时退化为内存递增
		group.RealCount++
		return group.RealCount, nil
	}
	// HIncrBy 会在 meta 不存在时创建它，所以 meta 的 TTL 也要在这里补。
	st.expireKey(metaKey)
	group.RealCount = int32(newCount)
	data, _ := json.Marshal(group)
	st.rdb.HSet(key, field, string(data))
	st.expireKey(key)
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
	// 计数器所在的 meta key 可能刚被 HIncrBy 创建出来，同样要带上活跃期 TTL。
	st.expireKey(rediskeys.GetRankMetaKey(st.bizId))
	return int32(val), nil
}

// SaveActivityTimes 将活动的 openTime / closeTime / gameEndTime 写入 meta hash，用于重启后的 Redis 恢复。
// 这是 meta key 的唯一创建入口（由 engine.NewService 构造时调用），因此也是全局活跃服务注册表
// （rank:{active_services}，见 docs/rank_optimization.md 第 01 条）的唯一注册入口。
func (st *Store) SaveActivityTimes(openTime, closeTime, gameEndTime int64) {
	if !st.available() {
		return
	}
	key := rediskeys.GetRankMetaKey(st.bizId)
	st.rdb.HSet(key, "openTime", strconv.FormatInt(openTime, 10))
	st.rdb.HSet(key, "closeTime", strconv.FormatInt(closeTime, 10))
	st.rdb.HSet(key, "gameEndTime", strconv.FormatInt(gameEndTime, 10))

	// 第 02 条：meta key 是活跃期数据，必须带 TTL。用本函数收到的活动时间而不是 writeTTL()，
	// 是为了让 TTL 的来源与刚写进去的三个字段同源，不依赖 activityEnd 原子副本是否已刷新。
	// 绝对时刻语义（activityEnd + SettledCacheTTL）⇒ 只需在这里设一次，tick 热路径不刷新；
	// 重复调用是幂等的（设到同一个绝对时刻不改变剩余时间）。
	activityEnd := settleAtOf(closeTime, gameEndTime)
	if _, err := st.rdb.Expire(key, ttlFor(activityEnd)); err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: saveActivityTimes expire bizId=%s: %v", st.bizId, err)
	}

	st.registerActive(closeTime, gameEndTime)
	st.logPlannedActiveTTL(closeTime, gameEndTime)
}

// ttlFor 按「距活动结束 + 结算保留期」公式计算活跃期数据 key 应设的绝对过期时长
// （docs/rank_optimization.md 第 02 条）。实现在 common/rank，与 OpenInstance 写
// rank:inst/mb/seq 时用的是同一份，避免两个模块各写一遍公式后漂移。
func ttlFor(activityEnd int64) time.Duration {
	return commonrank.TTLForActivityEnd(activityEnd)
}

// settleAtOf 收敛「结算时刻 = GameEndTime 优先，缺失时退化为 CloseTime」这同一个判断。
// 此前该逻辑散在三处（logPlannedActiveTTL、registerActive、engine.effectiveSettleAt），
// 三者若漂移会导致保留期算错（GameEndTime < CloseTime 的活动被提前过期）。
func settleAtOf(closeTime, gameEndTime int64) int64 {
	return commonrank.SettleAtOf(closeTime, gameEndTime)
}

// backfillTTLFor 是「读路径懒加载回填 / 已有 key 的 TTL 重设」专用规则：在 ttlFor 之上加一个
// SettledCacheTTL 下限。写路径用 ttlFor，读路径用本函数——两个具名函数，调用方不需要挑分支。
//
// 下限是必需的，三个方向都成立：
//   - 不产生永久 key：结果恒 >= SettledCacheTTL(14d) > 0（ttlFor 自身也保证非正数有兜底）。
//   - 不在保留期结束前删除：活动仍在未来时 ttlFor 就是精确的剩余绝对时长，max 原样保留
//     （与写入路径设的绝对时刻一致 ⇒ 幂等）；已过绝对到期时刻时下限给出「从此刻起 14d」，
//     严格长于剩余保留期。
//   - 不让读路径打 Mongo：裸用 ttlFor 对已过期的活动返回 1 分钟夹紧值，一次历史查询就会把 key
//     写成 1 分钟后过期，下次再 miss 再打 Mongo，反复震荡。下限消除震荡——这正是
//     LoadGroupSettledCached 今天刻意用固定 2 周的原因。
//
// 注意 backfillTTLFor(0) == SettledCacheTTL：所有不属于活动实例的 Store（历史查询、孤儿清理、
// 周期元数据读取、claim 兜底，共 9 处传 nil activityEnd）行为与改造前逐位相同。
func backfillTTLFor(activityEnd int64) time.Duration {
	if ttl := ttlFor(activityEnd); ttl > commonrank.SettledCacheTTL {
		return ttl
	}
	return commonrank.SettledCacheTTL
}

// logPlannedActiveTTL 记录活跃期数据 key 实际设置的绝对过期时刻（第 02 条）。TTL 已由
// SaveActivityTimes 在同一 pipeline 内真实生效，这条日志保留用于线上核对 expireAt 是否等于
// activityEnd+SettledCacheTTL，以及是否不早于 CleanupLiveData 设置的时刻。
func (st *Store) logPlannedActiveTTL(closeTime, gameEndTime int64) {
	activityEnd := settleAtOf(closeTime, gameEndTime)
	ttl := ttlFor(activityEnd)
	zaplog.LoggerSugar.Infof("rank ttl plan bizId=%s activityEnd=%d ttl=%s expireAt=%d",
		st.bizId, activityEnd, ttl, time.Now().Add(ttl).UnixMilli())
}

// registerActive 把 bizId 以 "{bizId}:{deadlineMillis}" 的形式写入全局活跃服务注册表，
// 并刷新注册表 key 自身的滑动 TTL。deadline 是服务的有效截止时间（settleAt + SettledCacheTTL），
// Manager.syncFromRedis 据此做纯本地过滤，取代原先每 30 秒一次的全库 KEYS/SCAN。
func (st *Store) registerActive(closeTime, gameEndTime int64) {
	settleAt := settleAtOf(closeTime, gameEndTime)
	var deadline int64
	if settleAt <= 0 {
		// 配置异常（CloseTime=0 且 GameEndTime=0，理论上会在 1 秒内被结算掉，见缺陷 7）：
		// settleAt + SettledCacheTTL 无意义，退化为滑动窗口起点，避免成员一写入就被判定为 stale。
		deadline = time.Now().Add(commonrank.ColdDataTTL).UnixMilli()
	} else {
		deadline = settleAt + commonrank.SettledCacheTTL.Milliseconds()
	}

	ctx := context.Background()
	member := fmt.Sprintf("%s:%d", st.bizId, deadline)
	pipe := st.rdb.Pipeline()
	pipe.SAdd(ctx, rediskeys.RankActiveServicesKey, member)
	pipe.Expire(ctx, rediskeys.RankActiveServicesKey, commonrank.ColdDataTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: registerActive bizId=%s: %v", st.bizId, err)
	}
}

// unregisterActive 把该 bizId 对应的成员从活跃服务注册表中剔除。
// 用真实值 SMEMBERS 后按 "{bizId}:" 前缀筛选而不是重建 deadline 后精确 SRem，
// 是因为调用方（如 forceCleanupOrphan）可能只持有一个新建的临时 Store、不知道
// registerActive 当初写入的确切 deadline，前缀筛选与调用方状态无关，始终正确。
// 注册表大小有界（见文档「注册表规模」），SMEMBERS 成本可控，且只发生在服务删除这种低频路径上。
func (st *Store) unregisterActive() {
	members, err := st.rdb.SMembers(rediskeys.RankActiveServicesKey)
	if err != nil || len(members) == 0 {
		return
	}
	prefix := st.bizId + ":"
	stale := make([]interface{}, 0, len(members))
	for _, mem := range members {
		if strings.HasPrefix(mem, prefix) {
			stale = append(stale, mem)
		}
	}
	if len(stale) == 0 {
		return
	}
	ctx := context.Background()
	pipe := st.rdb.Pipeline()
	pipe.SRem(ctx, rediskeys.RankActiveServicesKey, stale...)
	pipe.Expire(ctx, rediskeys.RankActiveServicesKey, commonrank.ColdDataTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: unregisterActive bizId=%s: %v", st.bizId, err)
	}
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
	// 无需写入的分支意味着 key 已存在，此时设 TTL 只是刷到同一个绝对时刻，不会缩短。
	st.expireKey(key)
}

// RestoreSettled 将结算快照强制写入 Redis rank:settled 键（冷启动恢复用）。
// 使 rankService.GetMemRank 在 Redis 恢复后能正确读取结算数据。
//
// 必须带 TTL（缺陷 4）：这里原本用裸 Set，是三个 rank:settled 写入方里唯一会留下永久 key 的。
// 用 backfillTTL() 而非 SettledCacheTTL：这是"重设已有 key 生命周期"的路径，要带 14d 下限，
// 否则恢复一个早已结束的活动时会把刚恢复的快照设成 1 分钟后过期。
func (st *Store) RestoreSettled(instanceID string, snaps []commonrank.RankMemberSnapshot) {
	if !st.available() || len(snaps) == 0 {
		return
	}
	data, err := json.Marshal(snaps)
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: marshal settled for restore instanceID=%s: %v", instanceID, err)
		return
	}
	_ = st.rdb.SetEX(rediskeys.GetRankSettledKey(instanceID), string(data), st.backfillTTL())
}

// --- 成员映射 ---

func (st *Store) SetMember(userID int64, groupID int32) error {
	if !st.available() {
		return nil
	}
	key := rediskeys.GetRankMembersKey(st.bizId)
	st.rdb.HSet(key, strconv.FormatInt(userID, 10), strconv.FormatInt(int64(groupID), 10))
	st.expireKey(key)

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
			key := rediskeys.GetRankMembersKey(st.bizId)
			st.backfill(key, func() {
				st.rdb.HSet(key, uidStr, strconv.FormatInt(int64(gid), 10))
			})
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
	membersKey := rediskeys.GetRankMembersKey(st.bizId)
	st.backfill(membersKey, func() {
		for uid, gid := range members {
			st.rdb.HSet(membersKey, strconv.FormatInt(uid, 10), strconv.FormatInt(int64(gid), 10))
		}
	})
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
	st.expireKey(key)

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
		st.backfill(key, func() {
			for _, r := range robots {
				data, _ := json.Marshal(r)
				st.rdb.HSet(key, strconv.FormatInt(r.MemberID, 10), string(data))
			}
		})
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
	st.expireKey(rediskeys.GetRankRobotInfosKey(st.bizId, groupID))
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

// ExpireLiveData 为这批分组的活跃期 Redis 数据设置 TTL（meta/分组/成员/claim/查询哨兵 + 每分组
// 机器人/机器人信息），与 CleanupLiveData 共用同一份 key 集合定义——TTL 值不同，key 集合必须相同，
// 否则某条路径会漏掉一个 key，而漏掉的 key 就是永久 key。
//
// 调用方决定 TTL 语义：
//   - 周期轮次的延迟清理用固定 settledDataRetentionTTL（见 CleanupLiveData 的说明）；
//   - 结算（Settle）用 backfillTTLFor(settleAt)，让已结算分组的 key 与活动保留期对齐。
func (st *Store) ExpireLiveData(groups []*Group, ttl time.Duration) {
	if !st.available() {
		return
	}
	if len(groups) == 0 {
		if loaded, err := st.LoadGroups(); err == nil {
			groups = loaded
		}
	}
	st.rdb.Expire(rediskeys.GetRankMetaKey(st.bizId), ttl)
	st.rdb.Expire(rediskeys.GetRankGroupsKey(st.bizId), ttl)
	st.rdb.Expire(rediskeys.GetRankMembersKey(st.bizId), ttl)
	st.rdb.Expire(rediskeys.GetRankClaimsKey(st.bizId), ttl)
	st.rdb.Expire(rediskeys.GetRankMongoCheckedKey(st.bizId), ttl)
	for _, g := range groups {
		if g == nil {
			continue
		}
		st.rdb.Expire(rediskeys.GetRankRobotsKey(st.bizId, g.GroupID), ttl)
		st.rdb.Expire(rediskeys.GetRankRobotInfosKey(st.bizId, g.GroupID), ttl)
	}
}

// CleanupLiveData 为该轮次的 Redis 数据设置 2 周保留 TTL（meta/分组/成员/机器人/查询哨兵）。
// 用于周期排行榜历史轮次的延迟清理（清理窗口 = 1 个周期，之后保留 2 周）。
// 不删除 rank:settled（同样 2 周 TTL）和 MongoDB（永久保留）。
//
// 周期路径必须继续用固定的 2 周，不能改成按轮次关闭时刻算的绝对值：轮次清理发生在结算之后一个
// 完整周期，改成 ttlFor(roundClose) 会让轮次数据的总保留期从「1 周期 + 2 周」缩到 2 周，
// 是真实的功能退化。统一的是实现（ExpireLiveData），不是 TTL 值。
func (st *Store) CleanupLiveData(groups []*Group) {
	st.ExpireLiveData(groups, settledDataRetentionTTL)
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
	st.unregisterActive()

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

	// claimedInt==0 ⟺ Lua 脚本确实执行了 HSET（cur 为空或负缓存哨兵），即 key 刚被创建。
	// 脚本本身不设过期，必须在这里补 TTL，否则 rank:claims 会成为永久 key（缺陷 3）。
	// 只在这一处 Expire 即可覆盖下面全部三个写入分支：HSET 只改值、不会清除 key 上已有的 TTL
	// （会清 TTL 的是不带 KEEPTTL 的 SET）。选 Go 侧 Expire 而非改 Lua，是为保持脚本 ARGV
	// 契约不变，并让它能被 miniredis 断言。
	if _, err := st.rdb.Expire(claimsKey, st.backfillTTL()); err != nil {
		zaplog.LoggerSugar.Warnf("rank engine: claim expire bizId=%s: %v", st.bizId, err)
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
	key := rediskeys.GetRankClaimsKey(st.bizId)
	st.rdb.HSet(key,
		strconv.FormatInt(userID, 10),
		strconv.FormatInt(claimTime, 10))
	st.expireKey(key)

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
		key := rediskeys.GetRankClaimsKey(st.bizId)
		if found {
			st.backfill(key, func() {
				st.rdb.HSet(key, uidStr, strconv.FormatInt(t, 10))
			})
			return t, true, nil
		}
		// MongoDB 未找到：写入负向缓存，防止同一用户重复查 MongoDB。
		// 哨兵同样必须带 TTL——否则"Mongo 中没有该 claim"这个事实会永久驻留，
		// 一旦之后真的产生了 claim（例如 Redis 被清空后从 Mongo 恢复），负缓存会一直压着它。
		st.backfill(key, func() {
			st.rdb.HSet(key, uidStr, nullCacheEntry)
		})
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
// miss 时从 MongoDB 加载并写回 Redis，避免历史查询反复打 Mongo。
// instanceID 为对应分组实例 ID（rankCode:bizId:group_N），与 RestoreSettled 同 key。
//
// 两条路径都用 backfillTTL() 而不是固定的 SettledCacheTTL（缺陷 4）：它是 ttlFor 加上 14d 下限，
// 因此 (a) 永远不会设出 0（不产生永久 key）、(b) 永远不短于 14d（不会提前删除历史数据）、
// (c) 活动尚未结束时与活跃期数据用同一个绝对到期时刻（与写路径一致，幂等）。
// 这里同时也是 rank:settled 丢失后的自愈点：Redis 被清掉后从 Mongo 重建。
func (st *Store) LoadGroupSettledCached(instanceID string, groupID int32) ([]commonrank.RankMemberSnapshot, error) {
	if st.available() {
		key := rediskeys.GetRankSettledKey(instanceID)
		if raw, err := st.rdb.Get(key); err == nil {
			var snaps []commonrank.RankMemberSnapshot
			if json.Unmarshal([]byte(raw), &snaps) == nil && len(snaps) > 0 {
				_, _ = st.rdb.Expire(key, st.backfillTTL())
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
			_ = st.rdb.SetEX(key, string(data), st.backfillTTL())
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
