package periodic

import "fmt"

// FormatLogicalKey 返回逻辑活动的 map 键格式 "g{gameId}_{bizType}:{actID}"。
func FormatLogicalKey(gameId int32, bizType string, actID int32) string {
	return fmt.Sprintf("g%d_%s:%d", gameId, bizType, actID)
}

// FormatRankCode 生成榜单定义的唯一标识（rank:def 的 Redis Key 组成部分）。
func FormatRankCode(gameId int32, bizType string, actID int32) string {
	return fmt.Sprintf("g%d_%s_score_%d", gameId, bizType, actID)
}

// FormatBizId 生成业务分组 ID（engine.Store 的 Redis Key 前缀组成部分）。
func FormatBizId(gameId int32, bizType string, actID int32) string {
	return fmt.Sprintf("g%d_%s_%d", gameId, bizType, actID)
}

// FormatRoundBizId 生成周期子轮次的业务分组 ID。
func FormatRoundBizId(gameId int32, bizType string, actID, round int32) string {
	return fmt.Sprintf("g%d_%s_%d_r%d", gameId, bizType, actID, round)
}

// FormatRankName 生成榜单展示名称。
func FormatRankName(gameId int32, bizType string, actID int32) string {
	return fmt.Sprintf("g%d_%s_rank_%d", gameId, bizType, actID)
}
