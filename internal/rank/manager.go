package rankservice

import (
	"context"
	"fmt"
	"sync"
	"time"

	commonrank "common/rank"
	goredis "golib/redis"
	"golib/zaplog"
	"socialserver/internal/rank/balloon"
)

type Manager struct {
	mu              sync.RWMutex
	rdb             *goredis.Redis
	dao             *balloon.DAO
	rankService     commonrank.Service
	memberIndex     *MemberIndex
	services        map[BizKey]RankBizService
	balloonServices map[BizKey]*balloon.Service
}

var globalManager *Manager

func InitGlobalManager(rdb *goredis.Redis, dbName string) error {
	if rdb == nil {
		return fmt.Errorf("rank: redis client is required")
	}
	dao := balloon.NewDAO(dbName)
	manager := &Manager{
		rdb:             rdb,
		dao:             dao,
		rankService:     commonrank.NewRedisService(rdb),
		memberIndex:     NewMemberIndex(rdb),
		services:        make(map[BizKey]RankBizService),
		balloonServices: make(map[BizKey]*balloon.Service),
	}
	if dao != nil {
		dao.EnsureIndexes()
		manager.recoverFromMongo(context.Background())
	}
	globalManager = manager
	zaplog.LoggerSugar.Infof("rank global manager initialized")
	return nil
}

func GetGlobalManager() *Manager {
	return globalManager
}

func (m *Manager) Close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.services = make(map[BizKey]RankBizService)
	m.balloonServices = make(map[BizKey]*balloon.Service)
	zaplog.LoggerSugar.Infof("rank global manager closed")
}

func (m *Manager) RegisterBalloon(ctx context.Context, cfg balloon.Config) (*balloon.Service, error) {
	if m == nil {
		return nil, fmt.Errorf("rank manager is nil")
	}
	if cfg.RankCode == "" {
		cfg.RankCode = fmt.Sprintf("balloon_score_%d", cfg.ActID)
	}

	key := BizKey{BizType: BizTypeBalloon, ActID: cfg.ActID, Round: cfg.Round}

	m.mu.Lock()
	defer m.mu.Unlock()
	if svc, ok := m.balloonServices[key]; ok {
		return svc, nil
	}
	if err := m.rankService.RegisterRank(ctx, commonrank.Rank{
		RankCode:       cfg.RankCode,
		RankName:       fmt.Sprintf("balloon_rank_%d", cfg.ActID),
		ScoreOrder:     commonrank.ScoreOrderDesc,
		TieBreakPolicy: commonrank.TieBreakPolicyFirstEnter,
		CreateTime:     cfg.OpenTime,
		UpdateTime:     cfg.OpenTime,
	}); err != nil {
		return nil, err
	}

	onMemberJoin := func(userID int64, groupID int32) {
		m.memberIndex.Track(userID, MemberEntry{
			BizType: BizTypeBalloon,
			ActID:   cfg.ActID,
			Round:   cfg.Round,
			GroupID: groupID,
		})
	}

	service, err := balloon.NewService(m.rankService, cfg, m.rdb, m.dao, balloon.WithOnMemberJoin(onMemberJoin))
	if err != nil {
		return nil, err
	}
	m.services[key] = &balloonAdapter{svc: service, bizType: BizTypeBalloon}
	m.balloonServices[key] = service

	// 持久化活动注册到 MongoDB
	bizKeyStr := fmt.Sprintf("%s:%d:%d", key.BizType, key.ActID, key.Round)
	if m.dao != nil {
		m.dao.SaveActivity(bizKeyStr, cfg)
	}

	return service, nil
}

// GetBalloonService 返回指定活动 Round=0 的 balloon 服务（向后兼容）。
func (m *Manager) GetBalloonService(actID int32) *balloon.Service {
	return m.GetBalloonServiceByRound(actID, 0)
}

// GetBalloonServiceByRound 返回指定活动和期数的 balloon 服务。
func (m *Manager) GetBalloonServiceByRound(actID int32, round int32) *balloon.Service {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.balloonServices[BizKey{BizType: BizTypeBalloon, ActID: actID, Round: round}]
}

// GetMemberEntries 返回用户参与的所有排行榜记录。
func (m *Manager) GetMemberEntries(userID int64) []MemberEntry {
	if m == nil {
		return nil
	}
	return m.memberIndex.Lookup(userID)
}

// GetMemberRankEntries 返回用户在所有排行榜中的名次快照（GM 查询用）。
func (m *Manager) GetMemberRankEntries(ctx context.Context, userID int64) ([]MemberRankEntry, error) {
	if m == nil {
		return nil, nil
	}
	entries := m.memberIndex.Lookup(userID)
	if len(entries) == 0 {
		return nil, nil
	}

	result := make([]MemberRankEntry, 0, len(entries))
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, entry := range entries {
		key := BizKey{BizType: entry.BizType, ActID: entry.ActID, Round: entry.Round}
		svc, ok := m.services[key]
		if !ok {
			result = append(result, MemberRankEntry{MemberEntry: entry})
			continue
		}
		snapshot, _, err := svc.GetMemberRank(ctx, userID)
		if err != nil {
			return nil, fmt.Errorf("get rank for %v: %w", key, err)
		}
		result = append(result, MemberRankEntry{
			MemberEntry: entry,
			Snapshot:    snapshot,
		})
	}
	return result, nil
}

func (m *Manager) Tick(ctx context.Context, now int64) error {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	svcs := make([]RankBizService, 0, len(m.services))
	for _, svc := range m.services {
		svcs = append(svcs, svc)
	}
	m.mu.RUnlock()
	for _, svc := range svcs {
		if err := svc.Tick(ctx, now); err != nil {
			return err
		}
	}
	return nil
}

// recoverFromMongo 启动时从 MongoDB 恢复活跃的排行榜活动。
func (m *Manager) recoverFromMongo(ctx context.Context) {
	if m.dao == nil {
		return
	}
	docs, err := m.dao.LoadAllActivities()
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: load activity registry from mongo: %v", err)
		return
	}

	now := time.Now().UnixMilli()
	recovered := 0
	for _, doc := range docs {
		cfg := doc.Config
		// 跳过已过期且已结算的活动（预留一天查询窗口）
		if cfg.CloseTime > 0 && now > cfg.CloseTime+86400_000 {
			continue
		}

		key := BizKey{BizType: BizTypeBalloon, ActID: cfg.ActID, Round: cfg.Round}
		if _, ok := m.balloonServices[key]; ok {
			continue
		}
		if cfg.RankCode == "" {
			cfg.RankCode = fmt.Sprintf("balloon_score_%d", cfg.ActID)
		}

		onMemberJoin := func(userID int64, groupID int32) {
			m.memberIndex.Track(userID, MemberEntry{
				BizType: BizTypeBalloon,
				ActID:   cfg.ActID,
				Round:   cfg.Round,
				GroupID: groupID,
			})
		}

		service, err := balloon.NewService(m.rankService, cfg, m.rdb, m.dao, balloon.WithOnMemberJoin(onMemberJoin))
		if err != nil {
			zaplog.LoggerSugar.Warnf("rank: recover balloon %s: %v", doc.BizKey, err)
			continue
		}
		m.services[key] = &balloonAdapter{svc: service, bizType: BizTypeBalloon}
		m.balloonServices[key] = service
		recovered++
	}
	if recovered > 0 {
		zaplog.LoggerSugar.Infof("rank: recovered %d balloon services from mongodb", recovered)
	}
}
