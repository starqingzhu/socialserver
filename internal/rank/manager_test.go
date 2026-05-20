package rankservice

import (
	"context"
	"fmt"
	"testing"

	"common/rank"
	"socialserver/internal/rank/balloon"
)

func newTestManager() *Manager {
	return &Manager{
		rankService:     rank.NewMemoryService(),
		memberIndex:     NewMemberIndex(nil),
		services:        make(map[BizKey]RankBizService),
		balloonServices: make(map[BizKey]*balloon.Service),
	}
}

func TestManagerBalloonScenario(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager()
	defer manager.Close()

	service, err := manager.RegisterBalloon(ctx, balloon.Config{
		ActID:         2001,
		RankCode:      "balloon_score",
		RankPeopleNum: 2,
		OpenToken:     100,
		OpenTime:      1000,
		CloseTime:     5000,
		AutoSettle:    true,
	})
	if err != nil {
		t.Fatalf("register balloon service: %v", err)
	}

	input := []struct {
		userID int64
		score  int64
		now    int64
	}{
		{10001, 120, 1100},
		{10002, 150, 1200},
		{10003, 180, 1300},
		{10004, 160, 1400},
		{10002, 220, 1500},
	}
	for _, item := range input {
		if err := service.UpsertScore(ctx, item.userID, item.score, item.now, map[string]int64{"zoneId": 7}); err != nil {
			t.Fatalf("upsert score user=%d: %v", item.userID, err)
		}
	}

	group1, err := service.ListGroupRank(ctx, 1, 0, 9)
	if err != nil {
		t.Fatalf("list group1: %v", err)
	}
	if len(group1) != 2 || group1[0].MemberId != 10002 || group1[1].MemberId != 10001 {
		t.Fatalf("unexpected group1 rank: %+v", group1)
	}

	group2, err := service.ListGroupRank(ctx, 2, 0, 9)
	if err != nil {
		t.Fatalf("list group2: %v", err)
	}
	if len(group2) != 2 || group2[0].MemberId != 10003 || group2[1].MemberId != 10004 {
		t.Fatalf("unexpected group2 rank: %+v", group2)
	}

	self, groupID, err := service.GetMemberRank(ctx, 10003)
	if err != nil {
		t.Fatalf("get member rank: %v", err)
	}
	if self == nil || groupID != 2 || self.Rank != 1 {
		t.Fatalf("unexpected member rank: group=%d snapshot=%+v", groupID, self)
	}

	if err := manager.Tick(ctx, 5500); err != nil {
		t.Fatalf("tick settle: %v", err)
	}

	settledGroup1, err := service.ListGroupRank(ctx, 1, 0, 9)
	if err != nil {
		t.Fatalf("list settled group1: %v", err)
	}
	if len(settledGroup1) != 2 || settledGroup1[0].Rank != 1 || settledGroup1[1].Rank != 2 {
		t.Fatalf("unexpected settled group1: %+v", settledGroup1)
	}

	if err := service.UpsertScore(ctx, 10005, 190, 5600, nil); err != rank.ErrInstanceNotOpen {
		t.Fatalf("expected writes rejected after close, got %v", err)
	}
}

func BenchmarkManagerBalloonScenario(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		manager := newTestManager()
		service, err := manager.RegisterBalloon(ctx, balloon.Config{
			ActID:         int32(3000 + i),
			RankCode:      fmt.Sprintf("balloon_bench_%d", i),
			RankPeopleNum: 50,
			OpenToken:     100,
			OpenTime:      1000,
			CloseTime:     100000,
			AutoSettle:    true,
		})
		if err != nil {
			b.Fatalf("register balloon: %v", err)
		}

		for userID := int64(1); userID <= 1000; userID++ {
			if err := service.UpsertScore(ctx, userID, 100+userID, 1100+userID, map[string]int64{"zoneId": userID % 8}); err != nil {
				b.Fatalf("upsert score: %v", err)
			}
		}

		if err := manager.Tick(ctx, 100001); err != nil {
			b.Fatalf("tick settle: %v", err)
		}
		list, err := service.ListGroupRank(ctx, 1, 0, 49)
		if err != nil {
			b.Fatalf("list group rank: %v", err)
		}
		if len(list) == 0 {
			b.Fatalf("expected settled rank list")
		}
	}
}

func TestManagerMultiRound(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager()
	defer manager.Close()

	svc1, err := manager.RegisterBalloon(ctx, balloon.Config{
		ActID: 5001, Round: 1, RankCode: "balloon_mr",
		RankPeopleNum: 2, OpenToken: 100, OpenTime: 1000, CloseTime: 5000, AutoSettle: true,
	})
	if err != nil {
		t.Fatalf("register round 1: %v", err)
	}
	svc2, err := manager.RegisterBalloon(ctx, balloon.Config{
		ActID: 5001, Round: 2, RankCode: "balloon_mr",
		RankPeopleNum: 2, OpenToken: 100, OpenTime: 6000, CloseTime: 10000, AutoSettle: true,
	})
	if err != nil {
		t.Fatalf("register round 2: %v", err)
	}
	if svc1 == svc2 {
		t.Fatalf("expected different services for different rounds")
	}

	if err := svc1.UpsertScore(ctx, 1001, 200, 1100, nil); err != nil {
		t.Fatalf("upsert r1: %v", err)
	}
	if err := svc2.UpsertScore(ctx, 1001, 300, 6100, nil); err != nil {
		t.Fatalf("upsert r2: %v", err)
	}

	snap1, g1, _ := svc1.GetMemberRank(ctx, 1001)
	snap2, g2, _ := svc2.GetMemberRank(ctx, 1001)
	if snap1 == nil || snap2 == nil {
		t.Fatalf("expected snapshots for both rounds")
	}
	if snap1.Score != 200 || snap2.Score != 300 {
		t.Fatalf("unexpected scores: r1=%d r2=%d", snap1.Score, snap2.Score)
	}
	if g1 != 1 || g2 != 1 {
		t.Fatalf("unexpected groups: r1=%d r2=%d", g1, g2)
	}

	if got := manager.GetBalloonServiceByRound(5001, 1); got != svc1 {
		t.Fatalf("GetBalloonServiceByRound(5001,1) returned wrong service")
	}
	if got := manager.GetBalloonServiceByRound(5001, 2); got != svc2 {
		t.Fatalf("GetBalloonServiceByRound(5001,2) returned wrong service")
	}
}

func TestManagerMemberIndex(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager()
	defer manager.Close()

	_, err := manager.RegisterBalloon(ctx, balloon.Config{
		ActID: 6001, Round: 1, RankCode: "balloon_idx",
		RankPeopleNum: 10, OpenToken: 100, OpenTime: 1000, CloseTime: 99999,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	svc := manager.GetBalloonServiceByRound(6001, 1)

	if err := svc.UpsertScore(ctx, 2001, 150, 1100, nil); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := svc.UpsertScore(ctx, 2002, 160, 1200, nil); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	entries := manager.GetMemberEntries(2001)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry for user 2001, got %d", len(entries))
	}
	if entries[0].ActID != 6001 || entries[0].Round != 1 || entries[0].GroupID != 1 {
		t.Fatalf("unexpected entry: %+v", entries[0])
	}

	if manager.GetMemberEntries(9999) != nil {
		t.Fatalf("expected nil for unknown user")
	}
}

func TestManagerGetMemberRankEntries(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager()
	defer manager.Close()

	for _, round := range []int32{1, 2} {
		_, err := manager.RegisterBalloon(ctx, balloon.Config{
			ActID: 7001, Round: round, RankCode: "balloon_agg",
			RankPeopleNum: 10, OpenToken: 100,
			OpenTime: int64(round) * 10000, CloseTime: int64(round)*10000 + 5000,
		})
		if err != nil {
			t.Fatalf("register round %d: %v", round, err)
		}
	}

	svc1 := manager.GetBalloonServiceByRound(7001, 1)
	svc2 := manager.GetBalloonServiceByRound(7001, 2)

	if err := svc1.UpsertScore(ctx, 3001, 200, 10100, nil); err != nil {
		t.Fatalf("upsert r1: %v", err)
	}
	if err := svc2.UpsertScore(ctx, 3001, 400, 20100, nil); err != nil {
		t.Fatalf("upsert r2: %v", err)
	}

	rankEntries, err := manager.GetMemberRankEntries(ctx, 3001)
	if err != nil {
		t.Fatalf("get member rank entries: %v", err)
	}
	if len(rankEntries) != 2 {
		t.Fatalf("expected 2 rank entries, got %d", len(rankEntries))
	}

	scoreByRound := make(map[int32]int64)
	for _, re := range rankEntries {
		if re.Snapshot != nil {
			scoreByRound[re.Round] = re.Snapshot.Score
		}
	}
	if scoreByRound[1] != 200 || scoreByRound[2] != 400 {
		t.Fatalf("unexpected scores by round: %+v", scoreByRound)
	}
}

func TestManagerBackwardCompat(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager()
	defer manager.Close()

	svc, err := manager.RegisterBalloon(ctx, balloon.Config{
		ActID: 8001, RankCode: "balloon_compat",
		RankPeopleNum: 10, OpenToken: 100, OpenTime: 1000, CloseTime: 99999,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	got := manager.GetBalloonService(8001)
	if got != svc {
		t.Fatalf("GetBalloonService should return Round=0 service")
	}
	if manager.GetBalloonService(9999) != nil {
		t.Fatalf("expected nil for unregistered actID")
	}
}
