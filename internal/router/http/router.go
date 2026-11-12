package http

import (
	"common/defines"
	"fmt"
	"golib/ginm"
	"golib/ginpprof"
	"golib/zaplog"
	"net/http"
	"socialserver/internal/router/http/internal/gm"

	"github.com/gin-gonic/gin"
)

func InitRouter(engine *gin.Engine) {
	engine.Use(
		ginm.Recovery(),
		ginm.AccessLogWithZap(zaplog.Logger),
	)

	engine.NoRoute(func(c *gin.Context) {
		c.AbortWithStatus(http.StatusNotFound)
	})
	engine.NoMethod(func(c *gin.Context) {
		c.AbortWithStatus(http.StatusMethodNotAllowed)
	})

	// pprof
	ginpprof.Wrap(engine)

	// 程序运转状态
	engine.Any("/health", func(ctx *gin.Context) {
		ctx.String(http.StatusOK, "OK")
	})

	// GM 接口
	gmGroup := engine.Group(fmt.Sprintf("gm/%s", defines.SocialServer.String()))
	{
		gmGroup.POST("/deletePlayerRankData", gm.DeletePlayerRankData) // 删除玩家排行榜数据
	}
}
