package periodic

import (
	"testing"
	"time"
)

func TestComputeRoundAt(t *testing.T) {
	// cycleMinutes=5 → 周期 300000ms；总时长 1000000ms 容纳 4 轮：
	// [0,300000) [300000,600000) [600000,900000) [900000,1000000)
	p := &PeriodicState{
		CycleMinutes:   5,
		TotalOpenTime:  0,
		TotalCloseTime: 1_000_000,
	}
	cases := []struct {
		now  int64
		want int32
	}{
		{-1, 1},        // 未开始
		{0, 1},         // 恰好开始
		{299999, 1},    // 第 1 轮末
		{300000, 2},    // 第 2 轮起
		{599999, 2},    // 第 2 轮末
		{600000, 3},    // 第 3 轮起
		{899999, 3},    // 第 3 轮末
		{900000, 4},    // 第 4 轮起（最后一轮，截断）
		{999999, 4},    // 第 4 轮末
		{1_000_000, 4}, // 活动结束 → 截断到最后一轮
		{5_000_000, 4}, // 远超结束时间 → 仍截断到最后一轮
	}
	for _, c := range cases {
		if got := p.computeRoundAt(c.now); got != c.want {
			t.Errorf("computeRoundAt(%d) = %d, want %d", c.now, got, c.want)
		}
	}
}

func TestMaxRound(t *testing.T) {
	cases := []struct {
		open, close int64
		want        int32
	}{
		{0, 300000, 1},            // 恰好一轮
		{0, 300001, 2},            // 越过一轮边界
		{0, 600000, 2},            // 两整轮
		{0, 1_000_000, 4},         // 3 整轮 + 余量
		{0, 0, 1},                 // 无效窗口
		{1000, 0, 1},              // close < open
	}
	for _, c := range cases {
		p := &PeriodicState{CycleMinutes: 5, TotalOpenTime: c.open, TotalCloseTime: c.close}
		if got := p.maxRound(); got != c.want {
			t.Errorf("maxRound(open=%d close=%d) = %d, want %d", c.open, c.close, got, c.want)
		}
	}
}

func TestNewPeriodicStatePositionsToCurrentRound(t *testing.T) {
	const cycleMin int32 = 10
	now := time.Now().UnixMilli()
	// now 位于活动开始 25 分钟处（10 分钟一轮 → 第 3 轮），
	// 轮窗口边界在 now 前后各 5 分钟，远离边界避免毫秒级抖动。
	open := now - 25*60*1000
	close := now + 35*60*1000
	state, err := NewPeriodicState("balloon", 1, cycleMin, open, close)
	if err != nil {
		t.Fatal(err)
	}
	r := state.GetCurrentRound()
	if r != 3 {
		t.Fatalf("currentRound = %d, want 3", r)
	}
	wantOpen, wantClose := state.computeRoundWindow(r)
	if state.RoundOpenTime != wantOpen || state.RoundCloseTime != wantClose {
		t.Fatalf("window mismatch: got [%d,%d], want [%d,%d]", state.RoundOpenTime, state.RoundCloseTime, wantOpen, wantClose)
	}
	if state.RoundOpenTime > now || now > state.RoundCloseTime {
		t.Fatalf("round %d window [%d,%d] does not contain now %d", r, state.RoundOpenTime, state.RoundCloseTime, now)
	}
}

func TestNewPeriodicStateClampsToLastRoundAfterClose(t *testing.T) {
	now := time.Now().UnixMilli()
	open := now - 60*60*1000 // 1 小时前开始
	close := now - 30*60*1000 // 30 分钟前已结束
	state, err := NewPeriodicState("balloon", 1, 10, open, close)
	if err != nil {
		t.Fatal(err)
	}
	want := state.maxRound()
	if r := state.GetCurrentRound(); r != want {
		t.Fatalf("currentRound = %d, want last round %d", r, want)
	}
	if state.RoundCloseTime != state.TotalCloseTime {
		t.Fatalf("last round close = %d, want TotalCloseTime %d", state.RoundCloseTime, state.TotalCloseTime)
	}
}
