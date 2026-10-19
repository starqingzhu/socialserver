package periodic

import (
	"testing"
	"time"

	"socialserver/internal/rank/engine"
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

// TestNextAdvanceRoundJumpsToRoundAtNow 验证 state 落后墙钟多轮时一跳到位，
// 而不是每 tick 只前进一轮（服务停跑 / tick 卡顿 / 时钟跳变场景）。
func TestNextAdvanceRoundJumpsToRoundAtNow(t *testing.T) {
	const cycleMin int32 = 15
	cycleMs := int64(cycleMin) * 60 * 1000
	// 活动开始后已过 294 轮，但 state 停在 79 轮（Epoch 落后 ~12 天）。
	open := int64(1788855360000)
	state := &PeriodicState{
		BizType:        "camper_competition",
		ActID:          1210,
		CycleMinutes:   cycleMin,
		TotalOpenTime:  open,
		TotalCloseTime: open + 30*24*60*60*1000,
	}
	state.setCurrentRound(79)
	state.RoundOpenTime, state.RoundCloseTime = state.computeRoundWindow(79)

	now := open + 293*cycleMs + 1000 // 落在第 294 轮开头
	if got := state.nextAdvanceRound(now); got != 294 {
		t.Fatalf("nextAdvanceRound = %d, want 294（应直接跳到 now 所在轮，而不是 80）", got)
	}

	if !state.advanceToRound(294) {
		t.Fatal("advanceToRound(294) = false, want true")
	}
	if r := state.GetCurrentRound(); r != 294 {
		t.Fatalf("currentRound = %d, want 294", r)
	}
	wantOpen, wantClose := state.computeRoundWindow(294)
	if state.RoundOpenTime != wantOpen || state.RoundCloseTime != wantClose {
		t.Fatalf("window mismatch: got [%d,%d], want [%d,%d]",
			state.RoundOpenTime, state.RoundCloseTime, wantOpen, wantClose)
	}
}

// TestNextAdvanceRoundMonotonic 验证目标轮可能不大于当前轮（maxRound 截断 / 历史 state
// 与周期配置不自洽）时仍至少前进一轮，避免原地打转或回退重开已结算轮。
func TestNextAdvanceRoundMonotonic(t *testing.T) {
	// 活动已结束：computeRoundAt 截断到 maxRound=4，而 state 已被一个更长的旧周期推到 9。
	p := &PeriodicState{
		CycleMinutes:   5,
		TotalOpenTime:  0,
		TotalCloseTime: 1_000_000,
	}
	p.setCurrentRound(9)
	if got := p.nextAdvanceRound(5_000_000); got != 10 {
		t.Fatalf("nextAdvanceRound = %d, want 10（目标轮被截断时仍须 > 当前轮）", got)
	}
	// 轮次只增不减：回退会重开已结算轮次的计分窗口。
	if p.advanceToRound(4) {
		t.Fatal("advanceToRound(4) = true, want false（不允许轮次回退）")
	}
	if r := p.GetCurrentRound(); r != 9 {
		t.Fatalf("currentRound = %d, want 9（回退被拒绝后 state 不得改变）", r)
	}

	// 目标轮开始时间超出有效期（open(5)=1200000 >= TotalCloseTime=1000000）时拒绝推进。
	p2 := &PeriodicState{CycleMinutes: 5, TotalOpenTime: 0, TotalCloseTime: 1_000_000}
	p2.setCurrentRound(1)
	if p2.advanceToRound(5) {
		t.Fatal("advanceToRound(5) = true, want false（超出有效期）")
	}
	if r := p2.GetCurrentRound(); r != 1 {
		t.Fatalf("currentRound = %d, want 1（拒绝推进后 state 不得改变）", r)
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

// TestComputeRoundWindowAfterCycleChange 验证周期变更后，未来轮次从历史轮次结束时间开始推算，
// 而不是始终从 TotalOpenTime 计算。
func TestComputeRoundWindowAfterCycleChange(t *testing.T) {
	// 场景：周期从 30 分钟变到 60 分钟，当前第 2 轮结束于 60 分钟
	state := &PeriodicState{
		BizType:          "test",
		ActID:            1,
		CycleMinutes:     60, // 新周期
		BaseCycleMinutes: 30, // 初始周期
		TotalOpenTime:    0,
		TotalCloseTime:   240 * 60 * 1000, // 4 小时
	}
	state.setCurrentRound(2)
	state.RoundOpenTime = 30 * 60 * 1000  // 第 2 轮实际 30-60（旧周期）
	state.RoundCloseTime = 60 * 60 * 1000 //

	// 当前轮应返回保存的窗口
	open, close := state.computeRoundWindow(2)
	if open != state.RoundOpenTime || close != state.RoundCloseTime {
		t.Fatalf("current round window = [%d,%d], want [%d,%d]", open, close, state.RoundOpenTime, state.RoundCloseTime)
	}

	// 未来轮次：第 3 轮应从当前轮结束时间 60 开始，持续 60 分钟
	open, close = state.computeRoundWindow(3)
	wantOpen := int64(60 * 60 * 1000)
	wantClose := int64(120 * 60 * 1000)
	if open != wantOpen || close != wantClose {
		t.Fatalf("future round 3 window = [%d,%d], want [%d,%d]", open, close, wantOpen, wantClose)
	}

	// 历史轮次：第 1 轮应用初始周期 30 分钟计算
	open, close = state.computeRoundWindow(1)
	wantOpen = 0
	wantClose = 30 * 60 * 1000
	if open != wantOpen || close != wantClose {
		t.Fatalf("historical round 1 window = [%d,%d], want [%d,%d]", open, close, wantOpen, wantClose)
	}
}

// TestComputeRoundWindowShortenCycle 验证周期从长变短后，下一轮紧接当前轮结束。
func TestComputeRoundWindowShortenCycle(t *testing.T) {
	// 场景：周期从 60 分钟变到 30 分钟，当前第 2 轮结束于 120 分钟
	state := &PeriodicState{
		BizType:          "test",
		ActID:            1,
		CycleMinutes:     30, // 新周期
		BaseCycleMinutes: 60, // 初始周期
		TotalOpenTime:    0,
		TotalCloseTime:   300 * 60 * 1000,
	}
	state.setCurrentRound(2)
	state.RoundOpenTime = 60 * 60 * 1000   // 第 2 轮实际 60-120（旧周期）
	state.RoundCloseTime = 120 * 60 * 1000 //

	// 未来轮次：第 3 轮应从 120 开始，持续 30 分钟
	open, close := state.computeRoundWindow(3)
	wantOpen := int64(120 * 60 * 1000)
	wantClose := int64(150 * 60 * 1000)
	if open != wantOpen || close != wantClose {
		t.Fatalf("future round 3 window = [%d,%d], want [%d,%d]", open, close, wantOpen, wantClose)
	}
}

// TestBaseCycleMinutesCompat 验证 BaseCycleMinutes 缺失（旧数据）时退化为 CycleMinutes。
func TestBaseCycleMinutesCompat(t *testing.T) {
	saved := engine.PeriodicSavedState{
		CycleMinutes:   15,
		TotalOpenTime:  0,
		TotalCloseTime: 1000000,
		CurrentRound:   2,
		RoundOpenTime:  300000,
		RoundCloseTime: 600000,
	}
	ps := StateFromSaved("balloon", 1, saved)
	if ps.BaseCycleMinutes != 15 {
		t.Fatalf("BaseCycleMinutes = %d, want 15 (fallback to CycleMinutes)", ps.BaseCycleMinutes)
	}
}

// TestAdvanceRoundAfterCycleChange 验证周期变更后 advanceToRound 使用正确窗口。
func TestAdvanceRoundAfterCycleChange(t *testing.T) {
	state := &PeriodicState{
		BizType:          "test",
		ActID:            1,
		CycleMinutes:     60,
		BaseCycleMinutes: 30,
		TotalOpenTime:    0,
		TotalCloseTime:   240 * 60 * 1000,
	}
	state.setCurrentRound(2)
	state.RoundOpenTime = 30 * 60 * 1000
	state.RoundCloseTime = 60 * 60 * 1000

	// 推进到第 3 轮
	if !state.advanceToRound(3) {
		t.Fatal("advanceToRound(3) = false, want true")
	}
	if state.GetCurrentRound() != 3 {
		t.Fatalf("currentRound = %d, want 3", state.GetCurrentRound())
	}
	wantOpen := int64(60 * 60 * 1000)
	wantClose := int64(120 * 60 * 1000)
	if state.RoundOpenTime != wantOpen || state.RoundCloseTime != wantClose {
		t.Fatalf("round 3 window = [%d,%d], want [%d,%d]", state.RoundOpenTime, state.RoundCloseTime, wantOpen, wantClose)
	}
}
