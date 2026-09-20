package engine

import (
	"context"
	"testing"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
	goredis "golib/redis"

	"github.com/alicebob/miniredis/v2"
)

// newActivePeriodService 起一个走真实 RedisService（miniredis 后端）的 engine.Service，
// 用于验证「活跃期写路径」——不经过任何恢复/结算分支的那条最普通的路径。
//
// 之所以必须用真实 RedisService 而不是引擎测试里常用的 fake：本文件断言的是
// common/rank 那一侧写入时有没有带 TTL，fake 会把这一点原样吞掉。
func newActivePeriodService(t *testing.T, bizType, rankCode string, closeIn time.Duration) (*Service, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewRedis(&goredis.RedisConfig{RedisAddrs: []string{mr.Addr()}})
	if rdb == nil {
		t.Fatalf("miniredis client: NewRedis returned nil for addr %s", mr.Addr())
	}
	rs := commonrank.NewRedisService(rdb)

	now := time.Now().UnixMilli()
	cfg := Config{
		BizType:       bizType,
		ActID:         1,
		RankCode:      rankCode,
		RankPeopleNum: 10,
		OpenTime:      now - int64(time.Hour/time.Millisecond),
		CloseTime:     now + closeIn.Milliseconds(),
	}
	def := commonrank.Rank{
		RankCode:       cfg.RankCode,
		RankName:       bizType + "_rank",
		ScoreOrder:     commonrank.ScoreOrderDesc,
		TieBreakPolicy: commonrank.TieBreakPolicyFirstEnter,
		CreateTime:     cfg.OpenTime,
		UpdateTime:     cfg.OpenTime,
	}
	if err := rs.RegisterRank(context.Background(), def); err != nil {
		t.Fatalf("RegisterRank: %v", err)
	}
	svc, err := NewService(rs, cfg, rdb, nil, WithRankDef(def))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	svc.WarmUp(context.Background())
	return svc, mr
}

// TestActivePeriodWritePathLeavesNoPermanentKey 覆盖「活跃期写路径」这个此前唯一的漏网场景：
// 开组 + 普通得分写入。这条路径既不经过 Restore*（那之后的 ExpireInstance 会补 TTL），
// 也不经过 Settle（那之后的 ExpireLiveData 会补 TTL），所以 TTL 必须在写入当次就带上。
//
// 此前这里有两个真实缺口，且都在最热的写路径上：
//   - common/rank.BatchUpsertScore 更新实例元数据用的是裸 SET，会清掉 OpenInstance 刚设上的
//     rank:inst TTL（而该分支几乎每写必进——LastScoreUpdate 每次得分都在推进）；
//   - rank:mb / rank:seq 由 upsert 的 Lua 用 HSET/INCR 现建，那一刻无从设 TTL。
//
// 断言方式是全键扫描而不是逐个点名：逐个点名只能证明"我想到的那几个 key 没问题"，
// 而这条原则要的是"不存在永久 key"，后者只有扫描能证明。
func TestActivePeriodWritePathLeavesNoPermanentKey(t *testing.T) {
	svc, mr := newActivePeriodService(t, "actttl", "actttl_score_1", time.Hour)

	now := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		if err := svc.UpsertScore(context.Background(), int64(2000+i), int64(100*i), now+int64(i), nil); err != nil {
			t.Fatalf("UpsertScore #%d: %v", i, err)
		}
	}

	keys := mr.Keys()
	if len(keys) == 0 {
		t.Fatal("前置失败：UpsertScore 之后 Redis 里一个 key 都没有，本用例证明不了任何事")
	}
	for _, k := range keys {
		if !mr.Exists(k) {
			continue
		}
		if ttl := mr.TTL(k); ttl <= 0 {
			t.Errorf("活跃期写路径留下了永久 key：%s（TTL=%v）", k, ttl)
		}
	}

	// 点名断言使失败信息可读：全键扫描只会说"某个 key 没有 TTL"，不会说清是哪一个环节漏的。
	instID := svc.groupInstanceID(svc.memberGroup[2000])
	for _, tc := range []struct{ name, key string }{
		{"rank:inst", rediskeys.GetRankInstKey(instID)},
		{"rank:mb", rediskeys.GetRankMbKey(instID)},
		{"rank:seq", rediskeys.GetRankSeqKey(instID)},
	} {
		if !mr.Exists(tc.key) {
			t.Fatalf("%s 不存在（%s）⇒ 前置不成立，无法断言其 TTL", tc.name, tc.key)
		}
		if ttl := mr.TTL(tc.key); ttl <= 0 {
			t.Errorf("%s(%s) 无 TTL ⇒ 永久 key", tc.name, tc.key)
		}
	}

	// 幂等性：写入的是「绝对过期时刻」而不是「从现在起 N 秒」，所以快进 30 分钟之后
	// 剩余 TTL 必须真的少 30 分钟，重新写一次又要回到原值——既不续期，也不重算。
	instKey := rediskeys.GetRankInstKey(instID)
	before := mr.TTL(instKey)
	mr.FastForward(30 * time.Minute)
	mid := mr.TTL(instKey)
	if diff := (before - 30*time.Minute) - mid; diff > time.Second || diff < -time.Second {
		t.Fatalf("前置失败：FastForward(30m) 后 TTL %v → %v，未按预期递减", before, mid)
	}
	if err := svc.UpsertScore(context.Background(), 2000, 999, now+int64(5*time.Minute/time.Millisecond), nil); err != nil {
		t.Fatalf("UpsertScore(重复写): %v", err)
	}
	if diff := mr.TTL(instKey) - before; diff > time.Second || diff < -time.Second {
		t.Errorf("重复写入没有回到同一个绝对过期时刻：%v → %v（期望 ≈ %v）；"+
			"偏差为正说明续了期，为负说明重算成了「从现在起」", mid, mr.TTL(instKey), before)
	}
}
