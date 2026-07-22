package engine

import (
	"context"
	"testing"

	"common/rank"
)

func TestEngineServiceAssignsGroupAndRanks(t *testing.T) {
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

func TestEngineServiceCreatesNewGroupWhenFull(t *testing.T) {
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

func TestEngineServiceRejectsClosedActivityAndSettles(t *testing.T) {
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
	if err := service.UpsertScore(ctx, 3002, 180, 2600, nil); err != rank.ErrInstanceClosed {
		t.Fatalf("expected closed activity to reject writes, got %v", err)
	}
}

func TestEngineServiceOnMemberJoinCallback(t *testing.T) {
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

func TestEngineServiceIsSettled(t *testing.T) {
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
	for i, want := range []int64{101, 102, 103} {
		if snaps[i].MemberId != want {
			t.Errorf("rank %d: expected memberId=%d, got=%d (seq order not preserved)", i+1, want, snaps[i].MemberId)
		}
	}

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

func TestEngineServiceSequencePersistAndRecover(t *testing.T) {
	ctx := context.Background()

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

	origSnaps, err := rs1.Range(ctx, instanceID, 0, 9)
	if err != nil {
		t.Fatalf("range original: %v", err)
	}

	rs2 := rank.NewMemoryService()
	if err := rs2.RegisterRank(ctx, rank.Rank{
		RankCode:   "balloon_seq",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register rs2: %v", err)
	}
	if err := rs2.RestoreInstance(ctx, rank.RankInstance{
		InstanceId: instanceID,
		RankCode:   "balloon_seq",
		State:      rank.InstanceStateOpen,
		OpenTime:   1000,
		CloseTime:  999999,
	}); err != nil {
		t.Fatalf("restore instance: %v", err)
	}

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

	restoredSnaps, err := rs2.Range(ctx, instanceID, 0, 9)
	if err != nil {
		t.Fatalf("range restored: %v", err)
	}

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
	wantOrder := []int64{13, 12, 11}
	for i, uid := range wantOrder {
		if list[i].MemberId != uid {
			t.Errorf("rank[%d] expected uid=%d got uid=%d", i, uid, list[i].MemberId)
		}
	}
	// All three have equal scores, so all get rank 1 (tied ranking).
	for i, snap := range list {
		if snap.Rank != 1 {
			t.Errorf("rank[%d] expected Rank=1 (tied) got Rank=%d", i, snap.Rank)
		}
	}
}

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

	scores := []struct{ uid, score, enterTime int64 }{
		{21, 200, 1100},
		{22, 200, 1200},
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
	// uid22 and uid21 both have score=200; LastEnter means uid22 (enterTime=1200) appears first.
	if len(list) != 5 {
		t.Fatalf("expected 5 members, got %d", len(list))
	}
	if list[0].MemberId != 22 || list[1].MemberId != 21 {
		t.Errorf("uid22 (later enter) should appear before uid21: list[0]=%d list[1]=%d", list[0].MemberId, list[1].MemberId)
	}
	// uid22 and uid21 both tied at rank 1; uid23 rank 3, uid24 rank 4, uid25 rank 5.
	wantRanks := map[int64]int64{22: 1, 21: 1, 23: 3, 24: 4, 25: 5}
	for _, snap := range list {
		want, ok := wantRanks[snap.MemberId]
		if !ok {
			t.Errorf("unexpected member %d in list", snap.MemberId)
			continue
		}
		if snap.Rank != want {
			t.Errorf("uid%d expected Rank=%d got Rank=%d", snap.MemberId, want, snap.Rank)
		}
	}
}

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

	svc.WarmUp(ctx)
	svc.WarmUp(ctx)

	if err := svc.UpsertScore(ctx, 8001, 100, 1100, nil); err != nil {
		t.Fatalf("upsert before WarmUp effect: %v", err)
	}

	svc.WarmUp(ctx)

	list, err := svc.ListGroupRank(ctx, 1, 0, 9)
	if err != nil {
		t.Fatalf("list after WarmUp: %v", err)
	}
	if len(list) != 1 || list[0].MemberId != 8001 {
		t.Fatalf("unexpected list after WarmUp: %+v", list)
	}
}

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
	if list[0].MemberId != 9003 {
		t.Errorf("expected uid 9003 at rank1, got %d", list[0].MemberId)
	}
}
