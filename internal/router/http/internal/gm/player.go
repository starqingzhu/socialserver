/**
 * @ Author: sunbin
 * @ Create Time: 2026-11-12
 * @ Description: GM 删除玩家排行榜数据接口
 */

package gm

import (
	"common/defines"
	"common/gameid"
	credis "common/redis"
	"fmt"
	"net/http"
	"strconv"

	rankservice "socialserver/internal/rank"
	goredis "golib/redis"

	"github.com/gin-gonic/gin"
	"golib/zaplog"
)

// DeletePlayerRankDataRequest 删除玩家排行榜数据请求
type DeletePlayerRankDataRequest struct {
	UserId string `json:"userId" binding:"required"` // 用户ID
	GameId int32  `json:"gameId" binding:"required"` // 游戏ID
}

// DeletePlayerRankData 删除指定玩家在指定游戏的所有排行榜数据
func DeletePlayerRankData(ctx *gin.Context) {
	var req DeletePlayerRankDataRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		zaplog.LoggerSugar.Errorf("DeletePlayerRankData invalid request: %v", err)
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code": defines.HttpCodeFailed,
			"msg":  fmt.Sprintf("请求参数错误: %v", err),
		})
		return
	}

	userId, err := strconv.ParseInt(req.UserId, 10, 64)
	if err != nil || userId <= 0 {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code": defines.HttpCodeFailed,
			"msg":  "userId 格式错误",
		})
		return
	}

	gameId := gameid.GameId(req.GameId)
	if !gameId.IsValid() {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code": defines.HttpCodeFailed,
			"msg":  "gameId 必须大于 0",
		})
		return
	}

	manager := rankservice.GetGlobalManager()
	if manager == nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"code": defines.HttpCodeFailed,
			"msg":  "排行榜管理器未初始化",
		})
		return
	}

	// 1. 查询用户参与的所有排行榜
	entries := manager.GetMemberEntries(gameId.Int32(), userId)
	if len(entries) == 0 {
		ctx.JSON(http.StatusOK, gin.H{
			"code": defines.HttpCodeSuccess,
			"msg":  "success",
			"data": gin.H{
				"deletedCount": 0,
			},
		})
		return
	}

	// 2. 逐个移除
	deletedCount := 0
	for _, entry := range entries {
		svc := manager.GetEngineService(gameId.Int32(), entry.BizType, entry.ActID)
		if svc == nil {
			zaplog.LoggerSugar.Warnf("DeletePlayerRankData: engine service not found, gameId=%d, bizType=%s, actID=%d",
				gameId, entry.BizType, entry.ActID)
			continue
		}
		if err := svc.RemoveMember(userId); err != nil {
			zaplog.LoggerSugar.Errorf("DeletePlayerRankData: remove member failed, userId=%d, gameId=%d, bizType=%s, actID=%d, err=%v",
				userId, gameId, entry.BizType, entry.ActID, err)
			continue
		}
		deletedCount++
	}

	// 3. 清理成员索引（直接删除该用户的 member_index key）
	redisKey := credis.GetGameRankMemberIndexKey(gameId.Int32(), userId)
	if _, err := goredis.GetRedisCli().Del(redisKey); err != nil {
		zaplog.LoggerSugar.Warnf("DeletePlayerRankData: del member_index key failed, userId=%d, gameId=%d, key=%s, err=%v",
			userId, gameId, redisKey, err)
	}

	zaplog.LoggerSugar.Infof("DeletePlayerRankData success, userId=%d, gameId=%d, entries=%d, deleted=%d",
		userId, gameId, len(entries), deletedCount)

	ctx.JSON(http.StatusOK, gin.H{
		"code": defines.HttpCodeSuccess,
		"msg":  "success",
		"data": gin.H{
			"deletedCount": deletedCount,
		},
	})
}
