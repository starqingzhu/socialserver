package balloon

import (
	"context"
	"testing"

	"common/rank"
)

func TestBalloonServiceAssignsGroupAndRanks(t *testing.T) {
	ctx := context.Background()
	rankService := rank.NewMemoryService()
	if err := rankService.RegisterRank(ctx, rank.Rank{
		RankCode:   "balloon_score",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}

	service, err := NewService(rankService, Config{
		BizType:       "balloon",
		ActID:         1001,
		RankCode:      "balloon_score",
		RankPeopleNum: 2,
		OpenToken:     100,
		OpenTime:      1000,
		CloseTime:     999999,
	}, nil, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	if err := service.UpsertScore(ctx, 1001, 120, 1100, nil); err != nil {
		t.Fatalf("upsert first user: %v", err)
	}
	if err := service.UpsertScore(ctx, 1002, 130, 1200, nil); err != nil {
		t.Fatalf("upsert second user: %v", err)
	}
	list, err := service.ListGroupRank(ctx, 1, 0, 9)
	if err != nil {
		t.Fatalf("list group rank: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected two members, got %d", len(list))
	}
	if list[0].MemberId != 1002 || list[1].MemberId != 1001 {
		t.Fatalf("unexpected sort order: %+v", list)
	}

	self, groupID, err := service.GetMemberRank(ctx, 1002)
	if err != nil {
		t.Fatalf("get member rank: %v", err)
	}
	if groupID != 1 || self == nil || self.Rank != 1 {
		t.Fatalf("unexpected member rank result: group=%d rank=%+v", groupID, self)
	}
}

func TestBalloonServiceCreatesNewGroupWhenFull(t *testing.T) {
	ctx := context.Background()
	rankService := rank.NewMemoryService()
	if err := rankService.RegisterRank(ctx, rank.Rank{
		RankCode:   "balloon_score",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}

	service, err := NewService(rankService, Config{
		BizType:       "balloon",
		ActID:         1002,
		RankCode:      "balloon_score",
		RankPeopleNum: 2,
		OpenToken:     100,
		OpenTime:      1000,
		CloseTime:     999999,
	}, nil, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	for idx, userID := range []int64{2001, 2002, 2003} {
		if err := service.UpsertScore(ctx, userID, int64(100+idx), int64(1100+idx), nil); err != nil {
			t.Fatalf("upsert user %d: %v", userID, err)
		}
	}

	group1 := service.GetGroup(1)
	group2 := service.GetGroup(2)
	if group1 == nil || group2 == nil {
		t.Fatalf("expected two groups, got group1=%+v group2=%+v", group1, group2)
	}
	if group1.State != GroupStateFull {
		t.Fatalf("expected first group full when capacity reached, got %s", group1.State)
	}
	self, groupID, err := service.GetMemberRank(ctx, 2003)
	if err != nil {
		t.Fatalf("get third member rank: %v", err)
	}
	if self == nil || groupID != 2 {
		t.Fatalf("expected third member in second group, group=%d rank=%+v", groupID, self)
	}
}

func TestBalloonServiceRejectsClosedActivityAndSettles(t *testing.T) {
	ctx := context.Background()
	rankService := rank.NewMemoryService()
	if err := rankService.RegisterRank(ctx, rank.Rank{
		RankCode:   "balloon_score",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}

	service, err := NewService(rankService, Config{
		BizType:       "balloon",
		ActID:         1003,
		RankCode:      "balloon_score",
		RankPeopleNum: 10,
		OpenToken:     100,
		OpenTime:      1000,
		CloseTime:     2000,
	}, nil, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	if err := service.UpsertScore(ctx, 3001, 150, 1100, nil); err != nil {
		t.Fatalf("upsert before close: %v", err)
	}
	if err := service.Tick(ctx, 2500); err != nil {
		t.Fatalf("tick settle: %v", err)
	}

	group := service.GetGroup(1)
	if group == nil || group.State != GroupStateSettled {
		t.Fatalf("expected settled group, got %+v", group)
	}
	settledList, err := service.ListGroupRank(ctx, 1, 0, 9)
	if err != nil {
		t.Fatalf("list settled rank: %v", err)
	}
	if len(settledList) != 1 || settledList[0].MemberId != 3001 {
		t.Fatalf("unexpected settled list: %+v", settledList)
	}
	if err := service.UpsertScore(ctx, 3002, 180, 2600, nil); err != rank.ErrInstanceNotOpen {
		t.Fatalf("expected closed activity to reject writes, got %v", err)
	}
}

func TestBalloonServiceOnMemberJoinCallback(t *testing.T) {
	ctx := context.Background()
	rankService := rank.NewMemoryService()
	if err := rankService.RegisterRank(ctx, rank.Rank{
		RankCode:   "balloon_cb",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	var joined []struct {
		userID  int64
		groupID int32
	}
	onJoin := func(userID int64, groupID int32) {
		joined = append(joined, struct {
			userID  int64
			groupID int32
		}{userID, groupID})
	}

	service, err := NewService(rankService, Config{
		BizType:       "balloon",
		ActID:         5001,
		RankCode:      "balloon_cb",
		RankPeopleNum: 10,
		OpenToken:     100,
		OpenTime:      1000,
		CloseTime:     99999,
	}, nil, nil, WithOnMemberJoin(onJoin))
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	if err := service.UpsertScore(ctx, 1001, 150, 1100, nil); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := service.UpsertScore(ctx, 1001, 200, 1200, nil); err != nil {
		t.Fatalf("upsert again: %v", err)
	}
	if err := service.UpsertScore(ctx, 1002, 180, 1300, nil); err != nil {
		t.Fatalf("upsert second user: %v", err)
	}

	if len(joined) != 2 {
		t.Fatalf("expected 2 join callbacks (once per user), got %d", len(joined))
	}
	if joined[0].userID != 1001 || joined[1].userID != 1002 {
		t.Fatalf("unexpected callback order: %+v", joined)
	}
}

func TestBalloonServiceIsSettled(t *testing.T) {
	ctx := context.Background()
	rankService := rank.NewMemoryService()
	if err := rankService.RegisterRank(ctx, rank.Rank{
		RankCode:   "balloon_settled",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	service, err := NewService(rankService, Config{
		BizType:       "balloon",
		ActID:         6001,
		RankCode:      "balloon_settled",
		RankPeopleNum: 10,
		OpenToken:     100,
		OpenTime:      1000,
		CloseTime:     2000,
	}, nil, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	if service.IsSettled() {
		t.Fatalf("expected not settled with no groups")
	}

	if err := service.UpsertScore(ctx, 1001, 150, 1100, nil); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if service.IsSettled() {
		t.Fatalf("expected not settled before close")
	}

	if err := service.Tick(ctx, 2500); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if !service.IsSettled() {
		t.Fatalf("expected settled after tick past close time")
	}
}

// TestRestoreMembersPreservesSequence 验证 MemoryService.RestoreMembers 正确使用
// 存储序号：三名成员积分和进榜时间完全相同，序号是唯一决胜因子，
// 恢复后名次顺序应与原始顺序一致。
func TestRestoreMembersPreservesSequence(t *testing.T) {
	ctx := context.Background()
	rankService := rank.NewMemoryService()
	if err := rankService.RegisterRank(ctx, rank.Rank{
		RankCode:   "seq_rank",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register rank: %v", err)
	}
	const instanceID = "seq_rank:{balloon_7001}:group_1"
	if err := rankService.OpenInstance(ctx, rank.RankInstance{
		InstanceId: instanceID,
		RankCode:   "seq_rank",
		State:      rank.InstanceStateOpen,
		OpenTime:   1000,
		CloseTime:  999999,
	}); err != nil {
		t.Fatalf("open instance: %v", err)
	}

	// 三名成员积分和 enterTime 完全相同，序号是唯一名次决胜因子。
	// 期望：seq=1 → rank1，seq=2 → rank2，seq=3 → rank3
	items := []rank.RankScoreItem{
		{MemberId: 101, Score: 500, AtTime: 1000, EnterTime: 1000, Sequence: 1},
		{MemberId: 102, Score: 500, AtTime: 1000, EnterTime: 1000, Sequence: 2},
		{MemberId: 103, Score: 500, AtTime: 1000, EnterTime: 1000, Sequence: 3},
	}
	if err := rankService.RestoreMembers(ctx, instanceID, items); err != nil {
		t.Fatalf("restore members: %v", err)
	}

	snaps, err := rankService.Range(ctx, instanceID, 0, 2)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(snaps) != 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(snaps))
	}
	// 验证名次顺序：seq 小的排在前面（先进榜者占优）
	for i, want := range []int64{101, 102, 103} {
		if snaps[i].MemberId != want {
			t.Errorf("rank %d: expected memberId=%d, got=%d (seq order not preserved)", i+1, want, snaps[i].MemberId)
		}
	}

	// 验证 nextSeq 推进：新加入的第4名成员应获得序号 4（大于已有最大序号 3）
	if err := rankService.BatchUpsertScore(ctx, instanceID, []rank.RankScoreItem{
		{MemberId: 104, Score: 500, AtTime: 1001, EnterTime: 1001},
	}); err != nil {
		t.Fatalf("upsert new member after restore: %v", err)
	}
	snap104, err := rankService.GetMemRank(ctx, instanceID, 104)
	if err != nil || snap104 == nil {
		t.Fatalf("get member rank: %v snap=%v", err, snap104)
	}
	if snap104.Sequence <= 3 {
		t.Errorf("new member should get sequence > 3, got %d", snap104.Sequence)
	}
	// 验证排列顺序：member104 进榜时间最晚(1001 > 1000)，应排在最后一位
	allSnaps, err := rankService.Range(ctx, instanceID, 0, 3)
	if err != nil {
		t.Fatalf("range all 4 members: %v", err)
	}
	if len(allSnaps) != 4 {
		t.Fatalf("expected 4 snapshots, got %d", len(allSnaps))
	}
	if allSnaps[3].MemberId != 104 {
		t.Errorf("member 104 (later enterTime) should be last, got memberId=%d at rank4", allSnaps[3].MemberId)
	}
}

// TestBalloonServiceSequencePersistAndRecover 验证 balloon Service 的 sequence 端到端恢复：
// 模拟三名同分、同进榜时间成员，写分后记录 sequence，然后用 ScoreDoc 数据（含 sequence）
// 恢复到新 rankService，验证名次顺序与原始一致。
func TestBalloonServiceSequencePersistAndRecover(t *testing.T) {
	ctx := context.Background()

	// --- 阶段1：写分，读取分配的 sequence ---
	rs1 := rank.NewMemoryService()
	if err := rs1.RegisterRank(ctx, rank.Rank{
		RankCode:   "balloon_seq",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	svc1, err := NewService(rs1, Config{
		BizType:       "balloon",
		ActID:         7002,
		RankCode:      "balloon_seq",
		RankPeopleNum: 10,
		OpenTime:      1000,
		CloseTime:     999999,
	}, nil, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	const enterTime = int64(1100)
	users := []int64{201, 202, 203}
	for _, uid := range users {
		if err := svc1.UpsertScore(ctx, uid, 500, enterTime, nil); err != nil {
			t.Fatalf("upsert user %d: %v", uid, err)
		}
	}

	// 读取各成员的序号（通过 rankService 直接查，绕过 balloon 层）
	groupID := int32(1)
	instanceID := svc1.groupInstanceID(groupID)
	type memberSeq struct {
		uid int64
		seq int64
	}
	var originalOrder []memberSeq
	for _, uid := range users {
		snap, err := rs1.GetMemRank(ctx, instanceID, uid)
		if err != nil || snap == nil {
			t.Fatalf("get rank uid=%d: %v snap=%v", uid, err, snap)
		}
		originalOrder = append(originalOrder, memberSeq{uid: uid, seq: snap.Sequence})
	}

	// 获取原始名次列表
	origSnaps, err := rs1.Range(ctx, instanceID, 0, 9)
	if err != nil {
		t.Fatalf("range original: %v", err)
	}

	// --- 阶段2：模拟 Redis 清空 → 用 ScoreDoc（含 sequence）重建新 rankService ---
	rs2 := rank.NewMemoryService()
	if err := rs2.RegisterRank(ctx, rank.Rank{
		RankCode:   "balloon_seq",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register rs2: %v", err)
	}
	// 模拟新实例（等同于 ensureLoaded 中的 RestoreInstance）
	if err := rs2.RestoreInstance(ctx, rank.RankInstance{
		InstanceId: instanceID,
		RankCode:   "balloon_seq",
		State:      rank.InstanceStateOpen,
		OpenTime:   1000,
		CloseTime:  999999,
	}); err != nil {
		t.Fatalf("restore instance: %v", err)
	}

	// 构造恢复数据（携带 sequence，模拟从 MongoDB rank_score 读取的 ScoreDoc）
	restoreItems := make([]rank.RankScoreItem, 0, len(originalOrder))
	for _, ms := range originalOrder {
		restoreItems = append(restoreItems, rank.RankScoreItem{
			MemberId:  ms.uid,
			Score:     500,
			AtTime:    enterTime,
			EnterTime: enterTime,
			Sequence:  ms.seq,
		})
	}
	if err := rs2.RestoreMembers(ctx, instanceID, restoreItems); err != nil {
		t.Fatalf("restore members: %v", err)
	}

	// 获取恢复后名次列表
	restoredSnaps, err := rs2.Range(ctx, instanceID, 0, 9)
	if err != nil {
		t.Fatalf("range restored: %v", err)
	}

	// 验证：名次顺序完全一致
	if len(origSnaps) != len(restoredSnaps) {
		t.Fatalf("snapshot count mismatch: orig=%d restored=%d", len(origSnaps), len(restoredSnaps))
	}
	for i := range origSnaps {
		if origSnaps[i].MemberId != restoredSnaps[i].MemberId {
			t.Errorf("rank %d mismatch: original=%d restored=%d",
				i+1, origSnaps[i].MemberId, restoredSnaps[i].MemberId)
		}
	}
}

// TestLastEnterTieBreak 验证同分时后进榜者名次靠前（TieBreakPolicyLastEnter）。
func TestLastEnterTieBreak(t *testing.T) {
	ctx := context.Background()
	rs := rank.NewMemoryService()
	if err := rs.RegisterRank(ctx, rank.Rank{
		RankCode:       "tie_test",
		ScoreOrder:     rank.ScoreOrderDesc,
		TieBreakPolicy: rank.TieBreakPolicyLastEnter,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	svc, err := NewService(rs, Config{
		BizType:       "balloon",
		ActID:         9001,
		RankCode:      "tie_test",
		RankPeopleNum: 10,
		OpenToken:     0,
		OpenTime:      1000,
		CloseTime:     999999,
	}, nil, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	// 3 人同分 100，进榜顺序：uid 11（enterTime=1100）, 12（1200）, 13（1300）
	// LastEnter 策略：enterTime 越大排名越靠前，即 13 第 1 名，12 第 2，11 第 3。
	for _, tc := range []struct{ uid, score, enterTime int64 }{
		{11, 100, 1100},
		{12, 100, 1200},
		{13, 100, 1300},
	} {
		if err := svc.UpsertScore(ctx, tc.uid, tc.score, tc.enterTime, nil); err != nil {
			t.Fatalf("upsert uid=%d: %v", tc.uid, err)
		}
	}

	list, err := svc.ListGroupRank(ctx, 1, 0, 9)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 members, got %d", len(list))
	}
	// 验证排序：enterTime 大的排前
	wantOrder := []int64{13, 12, 11}
	for i, uid := range wantOrder {
		if list[i].MemberId != uid {
			t.Errorf("rank[%d] expected uid=%d got uid=%d", i, uid, list[i].MemberId)
		}
	}
	// 验证名次唯一（位置排名，无并列）
	for i, snap := range list {
		if snap.Rank != int64(i+1) {
			t.Errorf("rank[%d] expected Rank=%d got Rank=%d", i, i+1, snap.Rank)
		}
	}
}

// TestUniqueRanksNoTies 验证不同分时名次同样唯一，无并列。
func TestUniqueRanksNoTies(t *testing.T) {
	ctx := context.Background()
	rs := rank.NewMemoryService()
	if err := rs.RegisterRank(ctx, rank.Rank{
		RankCode:       "unique_rank_test",
		ScoreOrder:     rank.ScoreOrderDesc,
		TieBreakPolicy: rank.TieBreakPolicyLastEnter,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	svc, err := NewService(rs, Config{
		BizType:       "balloon",
		ActID:         9002,
		RankCode:      "unique_rank_test",
		RankPeopleNum: 10,
		OpenToken:     0,
		OpenTime:      1000,
		CloseTime:     999999,
	}, nil, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	// 5 人，前 2 人同分 200，后 3 人各不同分
	scores := []struct{ uid, score, enterTime int64 }{
		{21, 200, 1100},
		{22, 200, 1200}, // 同分，enterTime 大，排 rank 1
		{23, 150, 1300},
		{24, 100, 1400},
		{25, 50, 1500},
	}
	for _, tc := range scores {
		if err := svc.UpsertScore(ctx, tc.uid, tc.score, tc.enterTime, nil); err != nil {
			t.Fatalf("upsert uid=%d: %v", tc.uid, err)
		}
	}

	list, err := svc.ListGroupRank(ctx, 1, 0, 9)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	// 验证无并列：名次 1..n 各不相同
	seen := make(map[int64]bool)
	for _, snap := range list {
		if seen[snap.Rank] {
			t.Errorf("duplicate rank %d found", snap.Rank)
		}
		seen[snap.Rank] = true
	}
	// 验证位置排名连续
	for i, snap := range list {
		if snap.Rank != int64(i+1) {
			t.Errorf("rank[%d] expected Rank=%d got Rank=%d", i, i+1, snap.Rank)
		}
	}
	// 验证 uid22 排在 uid21 之前（同分，enterTime 大）
	var r22, r21 int64
	for _, snap := range list {
		if snap.MemberId == 22 {
			r22 = snap.Rank
		}
		if snap.MemberId == 21 {
			r21 = snap.Rank
		}
	}
	if r22 >= r21 {
		t.Errorf("uid22 (later enter) should rank before uid21: r22=%d r21=%d", r22, r21)
	}
}

// TestWarmUpIdempotent 验证 WarmUp 在无 Redis/MongoDB 的场景下可安全重复调用，
// 且服务在 WarmUp 前后均能正常处理 UpsertScore 请求。
func TestWarmUpIdempotent(t *testing.T) {
	ctx := context.Background()
	rs := rank.NewMemoryService()
	if err := rs.RegisterRank(ctx, rank.Rank{
		RankCode:   "warmup_test",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	svc, err := NewService(rs, Config{
		BizType:       "balloon",
		ActID:         8001,
		RankCode:      "warmup_test",
		RankPeopleNum: 10,
		OpenTime:      1000,
		CloseTime:     999999,
	}, nil, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	// WarmUp 可在写分前调用（无 Redis，直接返回）
	svc.WarmUp(ctx)
	svc.WarmUp(ctx) // 幂等：可安全重复调用

	if err := svc.UpsertScore(ctx, 8001, 100, 1100, nil); err != nil {
		t.Fatalf("upsert before WarmUp effect: %v", err)
	}

	// WarmUp 可在写分后调用
	svc.WarmUp(ctx)

	list, err := svc.ListGroupRank(ctx, 1, 0, 9)
	if err != nil {
		t.Fatalf("list after WarmUp: %v", err)
	}
	if len(list) != 1 || list[0].MemberId != 8001 {
		t.Fatalf("unexpected list after WarmUp: %+v", list)
	}
}

// TestRuntimeRecoverySkippedWithoutRedis 验证在无 Redis 环境下（store.available()=false），
// UpsertScore 中的运行期恢复检测被跳过，业务正常进行，不会产生 nil 指针或 panic。
func TestRuntimeRecoverySkippedWithoutRedis(t *testing.T) {
	ctx := context.Background()
	rs := rank.NewMemoryService()
	if err := rs.RegisterRank(ctx, rank.Rank{
		RankCode:   "recover_skip_test",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	svc, err := NewService(rs, Config{
		BizType:       "balloon",
		ActID:         8002,
		RankCode:      "recover_skip_test",
		RankPeopleNum: 5,
		OpenTime:      1000,
		CloseTime:     999999,
	}, nil, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	// 写入多次，每次 UpsertScore 都经过运行期恢复检测（因 store 不可用而跳过）
	for i, uid := range []int64{9001, 9002, 9003} {
		if err := svc.UpsertScore(ctx, uid, int64(100+i*10), int64(1100+int64(i)), nil); err != nil {
			t.Fatalf("upsert uid=%d: %v", uid, err)
		}
	}

	list, err := svc.ListGroupRank(ctx, 1, 0, 9)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 members, got %d", len(list))
	}
	// 高分在前
	if list[0].MemberId != 9003 {
		t.Errorf("expected uid 9003 at rank1, got %d", list[0].MemberId)
	}
}
