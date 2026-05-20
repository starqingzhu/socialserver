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
		ActID:         1001,
		RankCode:      "balloon_score",
		RankPeopleNum: 2,
		OpenToken:     100,
		OpenTime:      1000,
		CloseTime:     999999,
		AutoSettle:    true,
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
	if list[0].Extra["groupId"] != 1 {
		t.Fatalf("expected group info in extra: %+v", list[0].Extra)
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
		ActID:         1003,
		RankCode:      "balloon_score",
		RankPeopleNum: 10,
		OpenToken:     100,
		OpenTime:      1000,
		CloseTime:     2000,
		AutoSettle:    true,
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

func TestBalloonServiceWithRound(t *testing.T) {
	ctx := context.Background()
	rankService := rank.NewMemoryService()
	if err := rankService.RegisterRank(ctx, rank.Rank{
		RankCode:   "balloon_round",
		ScoreOrder: rank.ScoreOrderDesc,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	svc1, err := NewService(rankService, Config{
		ActID: 4001, Round: 1, RankCode: "balloon_round",
		RankPeopleNum: 10, OpenToken: 100, OpenTime: 1000, CloseTime: 5000,
	}, nil, nil)
	if err != nil {
		t.Fatalf("new service round 1: %v", err)
	}
	svc2, err := NewService(rankService, Config{
		ActID: 4001, Round: 2, RankCode: "balloon_round",
		RankPeopleNum: 10, OpenToken: 100, OpenTime: 6000, CloseTime: 10000,
	}, nil, nil)
	if err != nil {
		t.Fatalf("new service round 2: %v", err)
	}

	if err := svc1.UpsertScore(ctx, 1001, 200, 1100, nil); err != nil {
		t.Fatalf("upsert r1: %v", err)
	}
	if err := svc2.UpsertScore(ctx, 1001, 500, 6100, nil); err != nil {
		t.Fatalf("upsert r2: %v", err)
	}

	snap1, _, _ := svc1.GetMemberRank(ctx, 1001)
	snap2, _, _ := svc2.GetMemberRank(ctx, 1001)
	if snap1 == nil || snap1.Score != 200 {
		t.Fatalf("expected r1 score 200, got %+v", snap1)
	}
	if snap2 == nil || snap2.Score != 500 {
		t.Fatalf("expected r2 score 500, got %+v", snap2)
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
		ActID: 5001, RankCode: "balloon_cb",
		RankPeopleNum: 10, OpenToken: 100, OpenTime: 1000, CloseTime: 99999,
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
		ActID: 6001, RankCode: "balloon_settled",
		RankPeopleNum: 10, OpenToken: 100, OpenTime: 1000, CloseTime: 2000, AutoSettle: true,
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
