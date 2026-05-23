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
		services:        make(map[string]RankBizService),
		balloonServices: make(map[string]*balloon.Service),
	}
}

func TestManagerBalloonScenario(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager()
	defer manager.Close()

	service, err := manager.registerBalloon(ctx, balloon.Config{
		BizType:       "balloon",
		ActID:         2001,
		RankCode:      "balloon_score",
		RankPeopleNum: 2,
		OpenToken:     100,
		OpenTime:      1000,
		CloseTime:     5000,
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
		if err := service.UpsertScore(ctx, item.userID, item.score, item.now, nil); err != nil {
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
		service, err := manager.registerBalloon(ctx, balloon.Config{
			BizType:       fmt.Sprintf("balloon_bench_%d", i),
			ActID:         int32(3000 + i),
			RankCode:      fmt.Sprintf("balloon_bench_%d", i),
			RankPeopleNum: 50,
			OpenToken:     100,
			OpenTime:      1000,
			CloseTime:     100000,
		})
		if err != nil {
			b.Fatalf("register balloon: %v", err)
		}

		for userID := int64(1); userID <= 1000; userID++ {
			if err := service.UpsertScore(ctx, userID, 100+userID, 1100+userID, nil); err != nil {
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

func TestManagerMemberIndex(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager()
	defer manager.Close()

	_, err := manager.registerBalloon(ctx, balloon.Config{
		BizType:       "balloon",
		ActID:         6001,
		RankCode:      "balloon_idx",
		RankPeopleNum: 10,
		OpenToken:     100,
		OpenTime:      1000,
		CloseTime:     99999,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	svc := manager.GetService(BizTypeBalloon, 6001)
	bsvc := svc.(*BalloonBizService)

	if err := bsvc.Svc.UpsertScore(ctx, 2001, 150, 1100, nil); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := bsvc.Svc.UpsertScore(ctx, 2002, 160, 1200, nil); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	entries := manager.GetMemberEntries(2001)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry for user 2001, got %d", len(entries))
	}
	if entries[0].BizType != BizTypeBalloon || entries[0].GroupID != 1 {
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

	_, err := manager.registerBalloon(ctx, balloon.Config{
		BizType:       "balloon",
		ActID:         7001,
		RankCode:      "balloon_agg",
		RankPeopleNum: 10,
		OpenToken:     100,
		OpenTime:      10000,
		CloseTime:     15000,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	svc := manager.GetService(BizTypeBalloon, 7001)
	bsvc := svc.(*BalloonBizService)

	if err := bsvc.Svc.UpsertScore(ctx, 3001, 200, 10100, nil); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	rankEntries, err := manager.GetMemberRankEntries(ctx, 3001)
	if err != nil {
		t.Fatalf("get member rank entries: %v", err)
	}
	if len(rankEntries) != 1 {
		t.Fatalf("expected 1 rank entry, got %d", len(rankEntries))
	}
	if rankEntries[0].Snapshot == nil || rankEntries[0].Snapshot.Score != 200 {
		t.Fatalf("unexpected rank entry: %+v", rankEntries[0])
	}
}

func TestManagerGetService(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager()
	defer manager.Close()

	_, err := manager.registerBalloon(ctx, balloon.Config{
		BizType:       "balloon",
		ActID:         8001,
		RankCode:      "balloon_compat",
		RankPeopleNum: 10,
		OpenToken:     100,
		OpenTime:      1000,
		CloseTime:     99999,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	got := manager.GetService(BizTypeBalloon, 8001)
	if got == nil {
		t.Fatalf("GetService should return registered service")
	}
	if manager.GetService("nonexistent", 0) != nil {
		t.Fatalf("expected nil for unregistered bizType")
	}
}
