package engine

import (
	"testing"
	"time"

	commonrank "common/rank"
	goredis "golib/redis"

	"github.com/alicebob/miniredis/v2"
)

// newMiniRedisStore 用 miniredis 起一个具备真实 Redis 语义（含 TTL/EXPIRE/Lua）的 Store。
// rdb 非 nil ⇒ available()==true；dao 传 fakeMongo ⇒ hasMongo()==true，懒加载回填路径可被驱动。
// activityEnd 以闭包注入，测试中可随时改变以模拟 GM 改配置；传 nil 表示"非活动上下文"
// （历史查询 / 孤儿清理 / claim 兜底），此时 TTL 恒为 SettledCacheTTL 下限。
//
// 之所以能这样替身：golib/redis.NewRedis 用 redis.NewUniversalClient，单地址时退化为普通
// （非 cluster）客户端，miniredis 是 drop-in。这也意味着 miniredis 不校验 CROSSSLOT，
// 涉及多 key 的 Lua 在本地全绿也不能证明生产 Cluster 可用。
func newMiniRedisStore(t *testing.T, bizId string, activityEnd func() int64, dao storeMongo) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewRedis(&goredis.RedisConfig{RedisAddrs: []string{mr.Addr()}})
	if rdb == nil {
		t.Fatalf("miniredis client: NewRedis returned nil for addr %s", mr.Addr())
	}
	return newStoreWithDAO(rdb, dao, bizId, activityEnd), mr
}

// mustTTL 断言 key 存在并返回其剩余 TTL。
// miniredis 的 TTL 对"未设过期"和"key 不存在"都返回 0，因此必须先用 Exists 区分——
// 缺陷断言（存在但 TTL==0）与"key 已到期消失"是两种完全不同的结论。
func mustTTL(t *testing.T, mr *miniredis.Miniredis, key string) time.Duration {
	t.Helper()
	if !mr.Exists(key) {
		t.Fatalf("key %s: expected to exist", key)
	}
	return mr.TTL(key)
}

// fakeMongo 是 storeMongo 的内存替身，只实现 Store 懒加载回填路径会走到的方法。
type fakeMongo struct {
	groups  map[int32]*Group
	members map[int64]int32
	robots  map[int32][]*robotState
	claims  map[int64]int64
	settled map[int32][]commonrank.RankMemberSnapshot
	insts   map[int32]commonrank.RankInstance
	scores  map[int32][]ScoreDoc
}

func newFakeMongo() *fakeMongo {
	return &fakeMongo{
		groups:  map[int32]*Group{},
		members: map[int64]int32{},
		robots:  map[int32][]*robotState{},
		claims:  map[int64]int64{},
		settled: map[int32][]commonrank.RankMemberSnapshot{},
		insts:   map[int32]commonrank.RankInstance{},
		scores:  map[int32][]ScoreDoc{},
	}
}

func (f *fakeMongo) available() bool { return true }

func (f *fakeMongo) SaveGroup(_ string, g *Group) error { f.groups[g.GroupID] = g; return nil }

func (f *fakeMongo) LoadGroups(string) ([]*Group, error) {
	out := make([]*Group, 0, len(f.groups))
	for _, g := range f.groups {
		out = append(out, g)
	}
	return out, nil
}

func (f *fakeMongo) SaveMember(_ string, uid int64, gid int32) error { f.members[uid] = gid; return nil }

func (f *fakeMongo) GetMember(_ string, uid int64) (int32, bool, error) {
	gid, ok := f.members[uid]
	return gid, ok, nil
}

func (f *fakeMongo) LoadAllMembers(string) (map[int64]int32, error) {
	out := make(map[int64]int32, len(f.members))
	for k, v := range f.members {
		out[k] = v
	}
	return out, nil
}

func (f *fakeMongo) SaveRobots(_ string, gid int32, r []*robotState) error { f.robots[gid] = r; return nil }

func (f *fakeMongo) LoadRobots(_ string, gid int32) ([]*robotState, error) { return f.robots[gid], nil }

func (f *fakeMongo) SaveClaim(_ string, uid int64, ct int64) error { f.claims[uid] = ct; return nil }

func (f *fakeMongo) SaveClaimIfNotExists(_ string, uid int64, ct int64) (bool, int64, error) {
	if v, ok := f.claims[uid]; ok {
		return false, v, nil
	}
	f.claims[uid] = ct
	return true, ct, nil
}

func (f *fakeMongo) GetClaim(_ string, uid int64) (int64, bool, error) {
	ct, ok := f.claims[uid]
	return ct, ok, nil
}

func (f *fakeMongo) SaveScore(string, int32, int64, int64, int64, int64, int64, *commonrank.AvatarInfo) error {
	return nil
}

func (f *fakeMongo) LoadGroupScores(_ string, gid int32) ([]ScoreDoc, error) { return f.scores[gid], nil }

func (f *fakeMongo) SaveSettled(_ string, gid int32, s []commonrank.RankMemberSnapshot, _ int64) error {
	f.settled[gid] = s
	return nil
}

func (f *fakeMongo) LoadGroupSettled(_ string, gid int32) ([]commonrank.RankMemberSnapshot, error) {
	return f.settled[gid], nil
}

func (f *fakeMongo) SaveRankInst(_ string, gid int32, i commonrank.RankInstance) error {
	f.insts[gid] = i
	return nil
}

func (f *fakeMongo) LoadGroupInst(_ string, gid int32) (*commonrank.RankInstance, error) {
	i, ok := f.insts[gid]
	if !ok {
		return nil, nil
	}
	return &i, nil
}

func (f *fakeMongo) DeleteAllByBizId(string) error { return nil }

func (f *fakeMongo) QueueDeleteDocIDs(string, []string) {}
