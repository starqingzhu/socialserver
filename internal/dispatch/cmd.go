/**
 * @ Description: 业务命令名常量（跨传输层共享的唯一标识）
 *
 * 同一命令可同时由 gRPC / HTTP / 未来 TCP 触发，传输层只负责把「命令名 + 载荷」送到
 * dispatch.Dispatcher，因此命令名在这里统一定义，避免各处手写字符串。
 */

package dispatch

const (
	// === 排行榜玩家接口 ===
	CmdS2SUpsertScore      = "rank.upsertScore"   // 更新用户积分
	CmdS2SGetRankList      = "rank.getRankList"   // 获取排行榜列表
	CmdS2SGetMemberRank    = "rank.getMemberRank" // 获取单用户排名
	CmdS2SSettle           = "rank.settle"        // 结算排行榜
	CmdS2SGetRewardUsers   = "rank.getRewardUsers" // 获取奖励用户列表
	CmdS2SClaimReward      = "rank.claimReward"   // 领取排行奖励
	CmdS2SGetClaimStatus   = "rank.getClaimStatus" // 查询领奖状态

	// === 排行榜配置管理接口 ===
	CmdS2SListRankBizTypes   = "rank.listBizTypes"  // 查询支持的业务类型
	CmdS2SCreateRankConfig   = "rank.createConfig"  // 创建排行榜配置
	CmdS2SGetRankConfig      = "rank.getConfig"     // 获取排行榜配置
	CmdS2SUpdateRankConfig   = "rank.updateConfig"  // 更新排行榜配置
	CmdS2SDeleteRankConfig   = "rank.deleteConfig"  // 删除排行榜配置
	CmdS2SListRankConfigs    = "rank.listConfigs"   // 列出所有排行榜配置
	CmdS2SGetRankCurRound    = "rank.getCurRound"   // 查询当前轮次信息

	// === GM 查询接口 ===
	CmdS2SGMGetUserRankList     = "rank.gm.userRankList"     // GM：查用户所在排行榜
	CmdS2SGMGetGroupRankList    = "rank.gm.groupRankList"    // GM：查分组排行榜
	CmdS2SGMGetRankInstanceList = "rank.gm.instanceList"     // GM：查排行榜实例列表
	CmdS2SGMGetInstanceRankList = "rank.gm.instanceRankList" // GM：查实例内排名

	// === S2S 完成通知 ===
	CmdS2SCompleted = "s2s.completed" // S2S 完成通知
)
