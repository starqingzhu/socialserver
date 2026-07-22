package rankservice

import (
	"fmt"
	"strconv"
	"strings"

	"common/configmgr"
	cfgtypes "common/configmgr/types"
	"golib/zaplog"
	"socialserver/internal/rank/engine"
)

func LoadRobotTiers(bizType BizType) []engine.RobotTierCfg {
	cf, err := configmgr.GetConfigByFileName("RobotRank")
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: load RobotRank config: %v", err)
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

func LoadRobotInfos() []engine.RobotInfoEntry {
	cf, err := configmgr.GetConfigByFileName("RobotName")
	if err != nil {
		zaplog.LoggerSugar.Warnf("rank: load RobotName config: %v", err)
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

func LoadRobotConfig(bizType BizType) ([]engine.RobotTierCfg, []engine.RobotInfoEntry) {
	tiers := LoadRobotTiers(bizType)
	if len(tiers) == 0 {
		return nil, nil
	}
	infos := LoadRobotInfos()
	if len(infos) == 0 {
		zaplog.LoggerSugar.Warnf("rank: no robot infos found for bizType=%s, tiers will be ignored", bizType)
		return nil, nil
	}
	zaplog.LoggerSugar.Infof("rank: loaded %d robot tiers and %d robot infos for bizType=%s", len(tiers), len(infos), bizType)
	return tiers, infos
}

// FillConfigFromFiles 根据 biz_type 从配置文件加载排行榜基础参数和机器人参数，填充到 Config 中。
func FillConfigFromFiles(bizType BizType, cfg *engine.Config) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	base, err := loadRankBase(bizType)
	if err != nil {
		return err
	}
	cfg.RankPeopleNum = base.RankPeopleNum
	cfg.OpenToken = int64(base.BalloonRankOpenToken)

	tiers, infos := LoadRobotConfig(bizType)
	cfg.RobotTiers = tiers
	cfg.RobotInfos = infos

	return nil
}

func loadRankBase(bizType BizType) (*cfgtypes.RankBaseConfig, error) {
	cf, err := configmgr.GetConfigByFileName("RankBase")
	if err != nil {
		return nil, fmt.Errorf("load RankBase config: %w", err)
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
	return nil, fmt.Errorf("RankBase config not found for bizType=%s", bizType)
}

// ValidateBizType 验证业务类型是否支持。
func ValidateBizType(bizType BizType) error {
	switch bizType {
	case BizTypeBalloon:
		return nil
	case BizTypeEgg:
		return nil
	default:
		return fmt.Errorf("unsupported biz type: %s", bizType)
	}
}
