package rankservice

import (
	"context"
	"testing"
	"time"

	commonrank "common/rank"
	rediskeys "common/redis"
	goredis "golib/redis"
	"socialserver/internal/rank/engine"

	"github.com/alicebob/miniredis/v2"
)

// 本文件覆盖缺陷 1 在 socialserver 侧的落点：rank:def 到期后，Manager 必须能凭内存里
// 已注册的 engine.Service（构造期 WithRankDef 捕获的定义）把定义零 IO 重建回来。
// common/rank 侧只证明了"注入了 provider 就能恢复"，这里证明的是"provider 真的被注入、
// 且注入的内容确实来自内存中的服务"。

// newDefRecoveryManager 起一个 miniredis 支撑的 Manager，并按 InitGlobalManager 的顺序
// 注入 provider。刻意不抽成生产代码里的构造函数：这层接线正是本文件要验证的对象。
func newDefRecoveryManager(t *testing.T) (*Manager, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewRedis(&goredis.RedisConfig{RedisAddrs: []string{mr.Addr()}})
	if rdb == nil {
		t.Fatalf("miniredis client: NewRedis returned nil for addr %s", mr.Addr())
	}
	rs := commonrank.NewRedisService(rdb)
	m := &Manager{
		rdb:            rdb,
		rankService:    rs,
		memberIndex:    NewMemberIndex(rdb, memberIndexTTL),
		services:       make(map[string]RankBizService),
		engineServices: make(map[string]*engine.Service),
	}
	rs.SetRankDefProvider(m.rankDefFromMemory)
	return m, mr
}

// TestRankDefFromMemoryReturnsInjectedDef 钉住 rankDefFor 与 WithRankDef 用的是同一份定义：
// 注册写进 Redis 的那份、和 Service 内存里留着的那份，必须逐字段相同，
// 否则恢复出来的定义与注册时的定义不一致，恢复就不是无损的了。
func TestRankDefFromMemoryReturnsInjectedDef(t *testing.T) {
	m, _ := newDefRecoveryManager(t)
	ctx := context.Background()
	cfg := engine.Config{
		BizType: "defrec", ActID: 7, RankCode: "defrec_score_7",
		RankPeopleNum: 10,
		OpenTime:      1700000000000,
		CloseTime:     1700003600000,
	}
	svc, err := m.registerSubService(ctx, "defrec", "defrec:7", cfg)
	if err != nil {
		t.Fatalf("registerSubService: %v", err)
	}

	inMemory := svc.RankDef()
	if inMemory.RankCode == "" {
		t.Fatalf("Service 没有持有 rank:def（WithRankDef 未接线）⇒ 定义到期后无从恢复")
	}
	if want := rankDefFor("defrec", cfg); inMemory != want {
		t.Fatalf("内存中的定义与注册构造器不一致：\ngot  %+v\nwant %+v", inMemory, want)
	}

	got, ok := m.rankDefFromMemory(cfg.RankCode)
	if !ok {
		t.Fatalf("rankDefFromMemory(%s) 未命中已注册的服务", cfg.RankCode)
	}
	if got != inMemory {
		t.Fatalf("provider 返回的定义与 Service 持有的不一致：\ngot  %+v\nwant %+v", got, inMemory)
	}
}

// TestRankDefFromMemorySkipsServiceWithoutDef 覆盖"服务在内存里但没有定义"的降级态
// （例如构造时未注入）：此时必须返回 false，而不是把一份零值定义当成有效定义交出去。
func TestRankDefFromMemorySkipsServiceWithoutDef(t *testing.T) {
	m, _ := newDefRecoveryManager(t)
	svc, err := engine.NewService(&fakeCleanupRankService{}, engine.Config{
		BizType: "nodef", ActID: 1, RankCode: "nodef_score_1", RankPeopleNum: 10,
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	m.engineServices["nodef:1"] = svc

	if def, ok := m.rankDefFromMemory("nodef_score_1"); ok {
		t.Fatalf("未注入定义的 Service 被当成了有效来源，返回 %+v", def)
	}
}

// TestRankDefFromMemoryDoesNotResurrectRemovedService 钉住"不另建 rankCode → Service 映射"
// 这个选择的正确性：服务被删除（从 engineServices 移除）后，provider 必须立刻查不到，
// 否则 GM 删掉的定义会被下一次写入**复活**。
func TestRankDefFromMemoryDoesNotResurrectRemovedService(t *testing.T) {
	m, _ := newDefRecoveryManager(t)
	ctx := context.Background()
	cfg := engine.Config{
		BizType: "removed", ActID: 3, RankCode: "removed_score_3", RankPeopleNum: 10,
	}
	if _, err := m.registerSubService(ctx, "removed", "removed:3", cfg); err != nil {
		t.Fatalf("registerSubService: %v", err)
	}
	if _, ok := m.rankDefFromMemory(cfg.RankCode); !ok {
		t.Fatalf("前置失败：注册后 rankDefFromMemory 应能命中")
	}

	m.mu.Lock()
	delete(m.engineServices, "removed:3")
	m.mu.Unlock()

	if def, ok := m.rankDefFromMemory(cfg.RankCode); ok {
		t.Fatalf("服务已删除但 provider 仍返回 %+v ⇒ 已删除的定义会被写入复活", def)
	}
}

// TestUpsertScoreSurvivesExpiredDef 是缺陷 1 的端到端断言：
// 定义被删除后，走 engine.Service.UpsertScore 的真实写入链路仍必须成功。
//
// 这条链路里 GetRank 由 RedisService 在**内部**调用（BatchUpsertScore），
// 进程外再包一层恢复逻辑是拦不住的——这正是恢复能力必须做进 RedisService 的原因。
func TestUpsertScoreSurvivesExpiredDef(t *testing.T) {
	m, mr := newDefRecoveryManager(t)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	cfg := engine.Config{
		BizType:       "survive",
		ActID:         9,
		RankCode:      "survive_score_9",
		RankPeopleNum: 10,
		OpenTime:      now - int64(time.Hour/time.Millisecond),
		CloseTime:     now + int64(24*time.Hour/time.Millisecond),
	}
	svc, err := m.registerSubService(ctx, "survive", "survive:9", cfg)
	if err != nil {
		t.Fatalf("registerSubService: %v", err)
	}

	// 先正常写一次，确保分组/实例都已就绪，随后的问题只可能来自定义。
	if err := svc.UpsertScore(ctx, 1001, 10, now, nil); err != nil {
		t.Fatalf("定义未过期时的 UpsertScore 就失败了: %v", err)
	}

	defKey := rediskeys.GetRankDefKey(cfg.RankCode)
	mr.Del(defKey)
	if mr.Exists(defKey) {
		t.Fatalf("前置失败：%s 未被删除", defKey)
	}

	if err := svc.UpsertScore(ctx, 1002, 20, now+1, nil); err != nil {
		t.Fatalf("rank:def 到期后 UpsertScore 失败: %v ⇒ 每个玩家的每次得分写入都会失败（缺陷 1）", err)
	}
	if !mr.Exists(defKey) {
		t.Fatalf("UpsertScore 成功了但 %s 没被重建 ⇒ 恢复能力未生效，得分写入只是碰巧绕过了定义校验", defKey)
	}
}
