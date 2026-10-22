package rankservice

import (
	"fmt"
	"strconv"
	"strings"

	"common/configmgr"
	cfgtypes "common/configmgr/types"
	"common/gameid"
	"golib/zaplog"
	"socialserver/internal/rank/engine"
)

func LoadRobotTiers(gameId gameid.GameId, bizType BizType) []engine.RobotTierCfg {
	cf, err := configmgr.GetGameConfigByFileName(gameId, "RobotRank")
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: load RobotRank config for gameId=%d: %v", gameId, err)
		return nil
	}

	var tiers []engine.RobotTierCfg
	for _, item := range cf.GetData() {
		cfg, ok := item.(cfgtypes.RobotRankConfig)
		if !ok {
			continue
		}
		if cfg.BizType != string(bizType) {
			continue
		}

		defaultMin, defaultMax := parseRange(cfg.DefaultTokenRange)
		growMin, growMax := parseRange(cfg.GrowTokenRange)

		tiers = append(tiers, engine.RobotTierCfg{
			TierID:             cfg.Id,
			Num:                cfg.Num,
			DefaultTokenMin:    defaultMin,
			DefaultTokenMax:    defaultMax,
			GrowTokenCdMs:      int64(cfg.GrowTokenCd) * 1000,
			GrowTokenMinBps:    growMin,
			GrowTokenMaxBps:    growMax,
			MaxToken:           int64(cfg.MaxToken),
			MaxDifferenceToken: int64(cfg.MaxDifferenceToken),
			LockTokenTimeMs:    int64(cfg.LockTokenTime) * 1000,
			OvertakeTimeMs:     int64(cfg.OvertakeTime) * 1000,
			OvertakeIntervalMs: int64(cfg.OvertakeInterval) * 1000,
		})
	}
	return tiers
}

func LoadRobotInfos(gameId gameid.GameId) []engine.RobotInfoEntry {
	cf, err := configmgr.GetGameConfigByFileName(gameId, "RobotName")
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: load RobotName config for gameId=%d: %v", gameId, err)
		return nil
	}

	var infos []engine.RobotInfoEntry
	for _, item := range cf.GetData() {
		cfg, ok := item.(cfgtypes.RobotNameConfig)
		if !ok {
			continue
		}
		infos = append(infos, engine.RobotInfoEntry{
			InfoID: int64(cfg.Id),
			Name:   cfg.Name,
			Avatar: cfg.Avatar,
			Frame:  cfg.Frame,
		})
	}
	return infos
}

func parseRange(s string) (int64, int64) {
	if s == "" {
		return 0, 0
	}
	parts := strings.Split(s, ",")
	if len(parts) < 2 {
		if len(parts) == 0 {
			return 0, 0
		}
		v, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			zaplog.LoggerSugar.Warnf("rank: parseRange single value failed: %q", s)
			return 0, 0
		}
		return v, v
	}
	a, err1 := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	b, err2 := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err1 != nil || err2 != nil {
		zaplog.LoggerSugar.Warnf("rank: parseRange failed: %q", s)
		return 0, 0
	}
	return a, b
}

func LoadRobotConfig(gameId gameid.GameId, bizType BizType) ([]engine.RobotTierCfg, []engine.RobotInfoEntry) {
	tiers := LoadRobotTiers(gameId, bizType)
	if len(tiers) == 0 {
		return nil, nil
	}
	infos := LoadRobotInfos(gameId)
	if len(infos) == 0 {
		zaplog.LoggerSugar.Warnf("rank: no robot infos found for gameId=%d, bizType=%s, tiers will be ignored", gameId, bizType)
		return nil, nil
	}
	zaplog.LoggerSugar.Infof("rank: loaded %d robot tiers and %d robot infos for gameId=%d, bizType=%s", len(tiers), len(infos), gameId, bizType)
	return tiers, infos
}

// FillConfigFromFiles 根据 gameId 和 biz_type 从配置文件加载排行榜基础参数和机器人参数，填充到 Config 中。
func FillConfigFromFiles(gameId gameid.GameId, bizType BizType, cfg *engine.Config) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	base, err := loadRankBase(gameId, bizType)
	if err != nil {
		return err
	}
	cfg.RankPeopleNum = base.RankPeopleNum
	cfg.OpenToken = int64(base.BalloonRankOpenToken)

	tiers, infos := LoadRobotConfig(gameId, bizType)
	cfg.RobotTiers = tiers
	cfg.RobotInfos = infos

	return nil
}

func loadRankBase(gameId gameid.GameId, bizType BizType) (*cfgtypes.RankBaseConfig, error) {
	cf, err := configmgr.GetGameConfigByFileName(gameId, "RankBase")
	if err != nil {
		return nil, fmt.Errorf("load RankBase config for gameId=%d: %w", gameId, err)
	}
	for _, item := range cf.GetData() {
		cfg, ok := item.(cfgtypes.RankBaseConfig)
		if !ok {
			continue
		}
		if cfg.BizType == string(bizType) {
			return &cfg, nil
		}
	}
	return nil, fmt.Errorf("RankBase config not found for gameId=%d, bizType=%s", gameId, bizType)
}

// GetRankBaseName 返回指定游戏和业务类型在 RankBase 配置中的名称（活动类型名）。
// 若未找到配置则返回空字符串。
func GetRankBaseName(gameId gameid.GameId, bizType BizType) string {
	base, err := loadRankBase(gameId, bizType)
	if err != nil {
		return ""
	}
	return base.Name
}

// ValidateBizType 验证业务类型是否支持。
// 通过检查 RankBase.json 配置文件中是否存在该业务类型的配置。
// 使用 gameId=0 的公共配置做验证（向后兼容）。
func ValidateBizType(bizType BizType) error {
	_, err := loadRankBase(0, bizType)
	if err != nil {
		return fmt.Errorf("unsupported biz type: %s - %w", bizType, err)
	}
	return nil
}

// LoadRankTypeAndCycle 从 RankBase.json 配置读取排行榜类型和周期分钟数。
// rankType=1 一次性排行榜，rankType=2 周期排行榜；cycleMinutes 仅在 rankType=2 时有效。
func LoadRankTypeAndCycle(gameId gameid.GameId, bizType BizType) (rankType int32, cycleMinutes int32, err error) {
	base, err := loadRankBase(gameId, bizType)
	if err != nil {
		return 0, 0, err
	}
	return base.RankType, base.CycleMinutes, nil
}
