package engine

import (
	"context"
	"testing"
	"time"

	"common/rank"
)

// fakeSettleRankService is a minimal rank.Service stub used to exercise Settle()'s
// per-group loop (docs/rank_optimization.md 第 06 条) without a real Redis/RedisService
// backend. Embedding a nil rank.Service satisfies the rest of the interface; those
// methods are never invoked by Settle/Tick/IsSettled in these tests.
type fakeSettleRankService struct {
	rank.Service
	closeCalls  int
	settleCalls int
	// expireTTLs 记录 ExpireInstance 收到的每个 TTL，供缺陷 6 的断言使用
	// （Settle 必须给已结算分组的 rank:inst/mb/seq/settled 设上非零且有下限的 TTL）。
	expireTTLs []time.Duration
}

func (f *fakeSettleRankService) CloseInstance(ctx context.Context, instanceId string, closeTime int64) error {
	f.closeCalls++
	return nil
}

func (f *fakeSettleRankService) SettleInstance(ctx context.Context, instanceId string, settleTime int64) ([]rank.RankMemberSnapshot, error) {
	f.settleCalls++
	return nil, nil
}

func (f *fakeSettleRankService) ExpireInstance(ctx context.Context, instanceId string, d time.Duration) error {
	f.expireTTLs = append(f.expireTTLs, d)
	return nil
}

func (f *fakeSettleRankService) GetInstance(ctx context.Context, instanceId string) (*rank.RankInstance, error) {
	return nil, rank.ErrInstanceNotFound
}

// newDegradedSettleService builds a Service backed by an unavailable Store (rdb=nil,
// dao=nil ⇒ store.available()==false), matching the existing "nil rdb/dao = no-op"
// degraded-store test convention used elsewhere in this package.
func newDegradedSettleService(t *testing.T, cfg Config) (*Service, *fakeSettleRankService) {
	t.Helper()
	if cfg.RankCode == "" {
		cfg.RankCode = "test_score"
	}
	if cfg.RankPeopleNum == 0 {
		cfg.RankPeopleNum = 10
	}
	fake := &fakeSettleRankService{}
	svc, err := NewService(fake, cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, fake
}

// TestTickShortCircuitsOnceSettled 验证第 06 条的核心收益：一旦按当前 settleAt 完成结算，
// Tick 必须直接短路返回，不再重新处理（哪怕之后又冒出一个未被短路感知的分组）。
func TestTickShortCircuitsOnceSettled(t *testing.T) {
	now := time.Now().UnixMilli()
	svc, _ := newDegradedSettleService(t, Config{
		OpenTime:  now - int64(time.Hour/time.Millisecond),
		CloseTime: now - int64(time.Minute/time.Millisecond), // 已关闭
	})

	if err := svc.Tick(context.Background(), now); err != nil {
		t.Fatalf("first Tick: %v", err)
	}
	settleAt := svc.effectiveSettleAt()
	if svc.settledAt.Load() != settleAt {
		t.Fatalf("expected settledAt=%d after closed Tick, got %d", settleAt, svc.settledAt.Load())
	}

	// 短路生效后手动塞入一个 Open 分组：若 Tick 没有真正短路，会尝试结算它并把它标记为 settled。
	sentinel := &Group{GroupID: 999, InstanceID: "sentinel-instance", State: GroupStateOpen}
	svc.mu.Lock()
	svc.groups = append(svc.groups, sentinel)
	svc.mu.Unlock()

	if err := svc.Tick(context.Background(), now+1000); err != nil {
		t.Fatalf("second Tick: %v", err)
	}
	if sentinel.State != GroupStateOpen {
		t.Fatalf("short-circuited Tick must not touch groups, but sentinel state changed to %q", sentinel.State)
	}
}

// TestSettleDoesNotStickBeforeWritesClosed 验证必改-1：GM 手动提前结算（Settle 无时间闸门，
// 唯一调用方是 GM「结算」按钮）时，只要 canUpdateScore 仍是 Open，就绝不能把 settledAt 钉死——
// 否则活动仍在开放期内产生的新分组会被短路直接吞掉，玩家奖励永久丢失。
func TestSettleDoesNotStickBeforeWritesClosed(t *testing.T) {
	now := time.Now().UnixMilli()
	svc, fake := newDegradedSettleService(t, Config{
		OpenTime:  now - int64(time.Hour/time.Millisecond),
		CloseTime: now + int64(time.Hour/time.Millisecond), // 仍在开放期内
	})

	group := &Group{GroupID: 1, InstanceID: svc.groupInstanceID(1), State: GroupStateOpen}
	svc.mu.Lock()
	svc.groups = append(svc.groups, group)
	svc.mu.Unlock()

	if _, err := svc.Settle(context.Background()); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if fake.settleCalls != 1 {
		t.Fatalf("expected the open group to actually be settled once, got %d calls", fake.settleCalls)
	}
	if svc.settledAt.Load() != 0 {
		t.Fatalf("必改-1 violated: settledAt must stay 0 while canUpdateScore==Open, got %d", svc.settledAt.Load())
	}

	// 新玩家在提前结算之后、活动真正关闭之前又创建了一个新分组：由于 settledAt 未被钉死，
	// 后续 Tick 必须继续正常处理它，而不是被短路跳过。
	freshGroup := &Group{GroupID: 2, InstanceID: svc.groupInstanceID(2), State: GroupStateOpen}
	svc.mu.Lock()
	svc.groups = append(svc.groups, freshGroup)
	svc.mu.Unlock()

	if err := svc.Tick(context.Background(), now); err != nil {
		t.Fatalf("Tick after premature settle: %v", err)
	}
	if fake.closeCalls == 0 {
		t.Fatalf("Tick must still be able to reach the group-processing path (not short-circuited)")
	}
}

// TestSettleGateFiresAfterLoopRegardlessOfPerGroupOutcome 验证必改-4：settledAt 的写入必须
// 放在整个分组循环之后、且不依赖任何单个分组的处理分支——即便循环体内每个分组都是
// continue（例如全部早已 settled），只要写入已对外关闭，短路标志仍必须被设置。
func TestSettleGateFiresAfterLoopRegardlessOfPerGroupOutcome(t *testing.T) {
	now := time.Now().UnixMilli()
	svc, fake := newDegradedSettleService(t, Config{
		OpenTime:  now - int64(time.Hour/time.Millisecond),
		CloseTime: now - int64(time.Minute/time.Millisecond), // 已关闭
	})

	// 所有分组均已 settled ⇒ 循环体对每个分组都会走 continue，不做任何实际结算调用。
	svc.mu.Lock()
	svc.groups = append(svc.groups,
		&Group{GroupID: 1, InstanceID: svc.groupInstanceID(1), State: GroupStateSettled},
		&Group{GroupID: 2, InstanceID: svc.groupInstanceID(2), State: GroupStateSettled},
	)
	svc.mu.Unlock()

	if _, err := svc.Settle(context.Background()); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if fake.settleCalls != 0 {
		t.Fatalf("expected no SettleInstance calls (all groups already settled), got %d", fake.settleCalls)
	}
	settleAt := svc.effectiveSettleAt()
	if svc.settledAt.Load() != settleAt {
		t.Fatalf("必改-4 violated: settledAt must be set after the loop even when every group continue'd, got %d want %d", svc.settledAt.Load(), settleAt)
	}
}

// TestIsSettledFastPath 验证短路命中时 IsSettled 直接返回 true，无需重新 LoadGroups/扫描分组。
func TestIsSettledFastPath(t *testing.T) {
	now := time.Now().UnixMilli()
	svc, _ := newDegradedSettleService(t, Config{
		OpenTime:  now - int64(time.Hour/time.Millisecond),
		CloseTime: now - int64(time.Minute/time.Millisecond), // 已关闭
	})

	// 短路命中前：无分组时 IsSettled 走原有逻辑，返回 false。
	if svc.IsSettled() {
		t.Fatalf("expected false before any settlement has happened")
	}

	settleAt := svc.effectiveSettleAt()
	svc.settledAt.Store(settleAt)

	if !svc.IsSettled() {
		t.Fatalf("expected fast-path true once settledAt matches effectiveSettleAt and writes are closed")
	}
}

// TestGMExtendingCloseTimeAutoInvalidatesShortCircuit 验证「用 settleAt 而非 bool」的完整性论证：
// GM 把 CloseTime/GameEndTime 推后后，effectiveSettleAt() 的返回值改变，settledAt 与其自动不再相等，
// 短路无需任何显式重置代码即可失效。
func TestGMExtendingCloseTimeAutoInvalidatesShortCircuit(t *testing.T) {
	now := time.Now().UnixMilli()
	svc, _ := newDegradedSettleService(t, Config{
		OpenTime:  now - int64(time.Hour/time.Millisecond),
		CloseTime: now - int64(time.Minute/time.Millisecond),
	})

	if err := svc.Tick(context.Background(), now); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	oldSettleAt := svc.effectiveSettleAt()
	if svc.settledAt.Load() != oldSettleAt {
		t.Fatalf("expected settled after closed Tick")
	}

	svc.UpdateConfig(Config{CloseTime: now + int64(time.Hour/time.Millisecond)})

	newSettleAt := svc.effectiveSettleAt()
	if newSettleAt == oldSettleAt {
		t.Fatalf("test setup error: CloseTime extension did not change effectiveSettleAt")
	}
	if svc.settledAt.Load() == newSettleAt {
		t.Fatalf("settledAt must not already match the new settleAt")
	}
	if svc.IsSettled() {
		t.Fatalf("short circuit must auto-invalidate after GM extends CloseTime, with zero explicit reset code")
	}
}

// TestSettleShortCircuitsAfterGateSet 验证第 06 条的另一半收益：`Settle` 的短路必须放在
// `LoadGroups` 之前。Tick 的短路（TestTickShortCircuitsOnceSettled）只覆盖三个调用点中的一个，
// 另外两个是 GM 手动结算与周期轮次推进；活动结束后每秒仍被调用的正是这条路径。
func TestSettleShortCircuitsAfterGateSet(t *testing.T) {
	now := time.Now().UnixMilli()
	svc, fake := newDegradedSettleService(t, Config{
		OpenTime:  now - int64(time.Hour/time.Millisecond),
		CloseTime: now - int64(time.Minute/time.Millisecond), // 已关闭
	})
	svc.settledAt.Store(svc.effectiveSettleAt())

	// 即便内存里存在一个未结算的分组，短路也必须让它原封不动。
	sentinel := &Group{GroupID: 1, InstanceID: svc.groupInstanceID(1), State: GroupStateOpen}
	svc.mu.Lock()
	svc.groups = append(svc.groups, sentinel)
	svc.mu.Unlock()

	results, err := svc.Settle(context.Background())
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if fake.settleCalls != 0 || fake.closeCalls != 0 {
		t.Fatalf("short-circuited Settle must not touch Redis, got closeCalls=%d settleCalls=%d", fake.closeCalls, fake.settleCalls)
	}
	if sentinel.State != GroupStateOpen {
		t.Fatalf("short-circuited Settle must not touch groups, but sentinel state changed to %q", sentinel.State)
	}
	if results != nil {
		t.Fatalf("short-circuited Settle must return nil results (no groups were settled), got %v", results)
	}
}

// TestTickSkipsWhenSettleAtIsZero 验证待办 K「附带发现」：CloseTime 与 GameEndTime 均未配置时
// effectiveSettleAt() 退化为 0，这是非法配置而不是"常驻活动"。Tick 必须显式识别并跳过
// （既不 tick 机器人也不结算），不能依赖 settledAt 零值与 settleAt 恰好相等的巧合短路。
func TestTickSkipsWhenSettleAtIsZero(t *testing.T) {
	now := time.Now().UnixMilli()
	svc, fake := newDegradedSettleService(t, Config{
		OpenTime: now - int64(time.Hour/time.Millisecond),
		// CloseTime 和 GameEndTime 均缺省 ⇒ effectiveSettleAt() == 0。
	})

	sentinel := &Group{GroupID: 1, InstanceID: svc.groupInstanceID(1), State: GroupStateOpen}
	svc.mu.Lock()
	svc.groups = append(svc.groups, sentinel)
	svc.mu.Unlock()

	if err := svc.Tick(context.Background(), now); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if sentinel.State != GroupStateOpen {
		t.Fatalf("Tick must not settle groups when settleAt<=0, but sentinel state changed to %q", sentinel.State)
	}
	if fake.closeCalls != 0 || fake.settleCalls != 0 {
		t.Fatalf("Tick must not call CloseInstance/SettleInstance when settleAt<=0, got closeCalls=%d settleCalls=%d",
			fake.closeCalls, fake.settleCalls)
	}
}
