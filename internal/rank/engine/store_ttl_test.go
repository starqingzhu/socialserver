package engine

import (
	"testing"
	"time"

	commonrank "common/rank"
)

// TestTTLForAbsoluteExpiryIsIndependentOfSetTime 验证第 02 条 TTL 公式的关键性质：
// 绝对过期时刻 == activityEnd + SettledCacheTTL，与"在什么时刻计算"无关。
// 这正是"只需在首次写入时设一次、不必在 tick 热路径刷新"的依据。
func TestTTLForAbsoluteExpiryIsIndependentOfSetTime(t *testing.T) {
	activityEnd := time.Now().Add(3 * time.Hour).UnixMilli()
	want := time.UnixMilli(activityEnd).Add(commonrank.SettledCacheTTL)

	// 同一个 activityEnd，在两个不同的"当前时刻"调用 ttlFor，得到的绝对过期时刻必须都是
	// end + SettledCacheTTL。这就是"活跃期回填与结算后回填可以共用同一个函数、不必分支"的依据。
	for _, label := range []string{"at t0", "at t0+20ms"} {
		now := time.Now()
		expireAt := now.Add(ttlFor(activityEnd))
		if diff := expireAt.Sub(want); diff > time.Second || diff < -time.Second {
			t.Errorf("%s: expireAt=%v want≈%v (activityEnd=%d)", label, expireAt, want, activityEnd)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 期望值：距结束 + 结算保留期，而不是"活动周期 + 1 天 buffer"（那会在保留期内提前销毁历史数据）。
	if ttl, wantTTL := ttlFor(activityEnd), time.Until(time.UnixMilli(activityEnd))+commonrank.SettledCacheTTL; ttl < wantTTL-time.Second || ttl > wantTTL+time.Second {
		t.Errorf("ttlFor(future end)=%v want≈%v", ttl, wantTTL)
	}
}

// TestTTLForDegradesToColdDataWhenActivityEndMissing 验证缺陷 7：effectiveSettleAt()==0
// （CloseTime 与 GameEndTime 都未配置）时公式无定义，必须退化为 ColdDataTTL 滑动值兜底，
// 而不是产生负数或跳过 Expire —— 跳过等于把 key 留成永久的，违反「不允许存在永久 key」。
func TestTTLForDegradesToColdDataWhenActivityEndMissing(t *testing.T) {
	for _, activityEnd := range []int64{0, -1, -time.Now().UnixMilli()} {
		ttl := ttlFor(activityEnd)
		if ttl != commonrank.ColdDataTTL {
			t.Errorf("ttlFor(activityEnd=%d)=%v want ColdDataTTL(%v)", activityEnd, ttl, commonrank.ColdDataTTL)
		}
	}
}

// TestTTLForNeverReturnsNonPositive 验证缺陷 7 的第二个落点：当活动早已超出保留期
// （绝对过期时刻已在过去），ttlFor 必须返回一个"极短的、正的" TTL 让它被回收，
// 既不能返回 <= 0（Expire 收到非正数会立即删除，比不设更危险从而诱使调用方写"跳过"分支），
// 也不能依赖调用方跳过 —— 跳过的结果就是永久 key。
func TestTTLForNeverReturnsNonPositive(t *testing.T) {
	cases := []struct {
		name string
		end  int64
	}{
		{"exactly at retention boundary minus one second", time.Now().Add(-commonrank.SettledCacheTTL + time.Second).UnixMilli()},
		{"just past retention", time.Now().Add(-commonrank.SettledCacheTTL - time.Second).UnixMilli()},
		{"long past retention", time.Now().Add(-30 * 24 * time.Hour).UnixMilli()},
		{"zero", 0},
		{"negative", -1},
	}
	for _, c := range cases {
		ttl := ttlFor(c.end)
		if ttl <= 0 {
			t.Errorf("%s: ttlFor(%d)=%v, must be positive (a non-positive TTL must never be handed to Expire)", c.name, c.end, ttl)
		}
	}

	// 超出保留期的一律退化为极短 TTL（1 分钟），而不是 ColdDataTTL 或负值。
	past := time.Now().Add(-commonrank.SettledCacheTTL - time.Hour).UnixMilli()
	if ttl := ttlFor(past); ttl != time.Minute {
		t.Errorf("ttlFor(activityEnd beyond retention)=%v want %v", ttl, time.Minute)
	}
}

// TestSettleAtOfPrefersGameEndTime 验证 settleAtOf 收敛了此前散在三处的同一段判断
// （logPlannedActiveTTL、registerActive、effectiveSettleAt）。三者若漂移，保留期就会算错。
func TestSettleAtOfPrefersGameEndTime(t *testing.T) {
	const closeTime, gameEndTime = int64(1000), int64(2000)
	cases := []struct {
		name                         string
		closeTime, gameEndTime, want int64
	}{
		{"both set: GameEndTime wins even when earlier", closeTime, gameEndTime, gameEndTime},
		{"GameEndTime later than CloseTime", gameEndTime, 3000, 3000},
		{"GameEndTime unset falls back to CloseTime", closeTime, 0, closeTime},
		{"both unset yields 0 (illegal config, 待办 K 拦截)", 0, 0, 0},
	}
	for _, c := range cases {
		if got := settleAtOf(c.closeTime, c.gameEndTime); got != c.want {
			t.Errorf("%s: settleAtOf(%d, %d)=%d want %d", c.name, c.closeTime, c.gameEndTime, got, c.want)
		}
	}
}

// TestBackfillTTLForNeverShorterThanRetention 验证缺陷 3 的核心不变量：读路径回填用的 TTL
// 恒 >= SettledCacheTTL。这是"不产生永久 key"（!=0）与"不退化成 1 分钟震荡"（下限生效）
// 两个方向的共同保证。ttlFor 对已过保留期的活动返回 1 分钟，直接用在回填上会让一次历史查询
// 把 key 写成 1 分钟后过期，下次再 miss 再打 Mongo，反复震荡。
func TestBackfillTTLForNeverShorterThanRetention(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name        string
		activityEnd int64
	}{
		{"zero (非法配置)", 0},
		{"negative", -1},
		{"ended 30 days ago (远超保留期)", now.Add(-30 * 24 * time.Hour).UnixMilli()},
		{"ended 1 second ago", now.Add(-time.Second).UnixMilli()},
		{"ends in 1 hour", now.Add(time.Hour).UnixMilli()},
		{"ends in 90 days", now.Add(90 * 24 * time.Hour).UnixMilli()},
	}
	for _, c := range cases {
		ttl := backfillTTLFor(c.activityEnd)
		if ttl < commonrank.SettledCacheTTL {
			t.Errorf("%s: backfillTTLFor(%d)=%v, must be >= SettledCacheTTL(%v) so a backfill never deletes before retention ends and never thrashes Mongo",
				c.name, c.activityEnd, ttl, commonrank.SettledCacheTTL)
		}
	}

	// 活动仍在未来时，ttlFor 已 >= 下限，max 必须原样保留它——这样回填设的绝对时刻与写入路径
	// 完全一致（幂等），而不是被下限截断成"从此刻起 14d"。
	future := now.Add(24 * time.Hour).UnixMilli()
	if got, want := backfillTTLFor(future), ttlFor(future); got != want {
		t.Errorf("backfillTTLFor(future)=%v want exactly ttlFor(future)=%v (floor must not truncate)", got, want)
	}

	// 已过保留期的活动恰好退化为下限本身，而不是 ttlFor 的 1 分钟夹紧值。
	past := now.Add(-30 * 24 * time.Hour).UnixMilli()
	if got := backfillTTLFor(past); got != commonrank.SettledCacheTTL {
		t.Errorf("backfillTTLFor(long past)=%v want exactly SettledCacheTTL(%v)", got, commonrank.SettledCacheTTL)
	}

	// 兼容性红利：传 nil activityEnd 的 9 个 Store 走的就是这个分支，结果必须逐位等于这些路径
	// 改造前手工设的 2 周常量。
	if got := backfillTTLFor(0); got != commonrank.SettledCacheTTL {
		t.Errorf("backfillTTLFor(0)=%v want SettledCacheTTL(%v) — nil-activityEnd stores must be behaviour-identical to before", got, commonrank.SettledCacheTTL)
	}
}

// TestEffectiveSettleAtPrefersGameEndTime 验证第 02 条与第 06 条共用同一个时间基准：
// GameEndTime 优先，缺失时才退化为 CloseTime。两者若不一致，保留期就会算错
// （GameEndTime < CloseTime 的活动会被提前过期）。
func TestEffectiveSettleAtPrefersGameEndTime(t *testing.T) {
	now := time.Now().UnixMilli()
	gameEnd := now + 2*3600*1000
	closeTime := now + 6*3600*1000

	svc, _ := newDegradedSettleService(t, Config{
		OpenTime:    now - 3600*1000,
		CloseTime:   closeTime,
		GameEndTime: gameEnd,
	})
	if got := svc.effectiveSettleAt(); got != gameEnd {
		t.Errorf("effectiveSettleAt()=%d want GameEndTime=%d (must not fall back to the later CloseTime)", got, gameEnd)
	}

	// GameEndTime 缺省时退化为 CloseTime。
	svc2, _ := newDegradedSettleService(t, Config{
		OpenTime:  now - 3600*1000,
		CloseTime: closeTime,
	})
	if got := svc2.effectiveSettleAt(); got != closeTime {
		t.Errorf("effectiveSettleAt()=%d want CloseTime=%d", got, closeTime)
	}

	// 两者都缺省 ⇒ 0（非法配置，由待办 K 的创建入口校验拦截，Tick 另有显式兜底）。
	svc3, _ := newDegradedSettleService(t, Config{OpenTime: now - 3600*1000})
	if got := svc3.effectiveSettleAt(); got != 0 {
		t.Errorf("effectiveSettleAt()=%d want 0 when both CloseTime and GameEndTime are unset", got)
	}
}
