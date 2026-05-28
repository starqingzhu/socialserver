package balloon

import (
	"math/rand"
)

// robotMemberIDBase 机器人成员ID基准值（负数，与真实玩家正数ID不冲突）。
// 机器人ID格式：-(groupID * robotIDGroupStride + robotIndex)
const robotMemberIDBase = int64(-1_000_000_000)
const robotIDGroupStride = int64(10_000)

// IsRobotMemberID 判断成员ID是否为机器人。
func IsRobotMemberID(memberID int64) bool {
	return memberID < 0
}

// robotMemberID 根据分组和组内序号生成机器人成员ID。
func robotMemberID(groupID int32, index int32) int64 {
	return robotMemberIDBase - int64(groupID)*robotIDGroupStride - int64(index)
}

// RobotTierCfg 单档机器人已解析配置，供 Service 运行时使用。
type RobotTierCfg struct {
	TierID               int32
	Num                  int32 // 每组生成数量
	DefaultTokenMin      int64 // 初始积分下限
	DefaultTokenMax      int64 // 初始积分上限
	GrowTokenCdMs        int64 // 增长冷却时间（毫秒）
	GrowTokenMinBps int64 // 增长目标最小万分比（配置值 / 10000 * firstScore = 目标积分）
	GrowTokenMaxBps int64 // 增长目标最大万分比
	MaxToken             int64 // 积分上限
	MaxDifferenceToken   int64 // 超过玩家第一名的最大分差（超过则停止增长）
	LockTokenTimeMs      int64 // 结束前停止增长的时间窗口（毫秒）
}

// RobotInfoEntry 机器人展示信息条目。
type RobotInfoEntry struct {
	InfoID int64
	Name   string
	Avatar int32
	Frame  int32
}

// robotState 运行时机器人状态，每分组每机器人一个实例。
type robotState struct {
	MemberID   int64 `json:"memberId" bson:"memberId"`
	TierID     int32 `json:"tierId" bson:"tierId"`
	InfoID     int64 `json:"infoId" bson:"infoId"`
	Score      int64 `json:"score" bson:"score"`
	LastGrowAt int64 `json:"lastGrowAt" bson:"lastGrowAt"`
}

// robotSpawnEntry 机器人生成计划条目。
type robotSpawnEntry struct {
	TierID int32
	Count  int32
}

// buildRobotSpawnPlan 根据档次配置和可用容量生成机器人生成计划。
// capacity 为剩余可用名额（= RankPeopleNum - 已有真实玩家数）。
func buildRobotSpawnPlan(tiers []RobotTierCfg, capacity int32) []robotSpawnEntry {
	var plan []robotSpawnEntry
	remaining := capacity
	for _, tier := range tiers {
		if remaining <= 0 {
			break
		}
		count := tier.Num
		if count > remaining {
			count = remaining
		}
		if count > 0 {
			plan = append(plan, robotSpawnEntry{TierID: tier.TierID, Count: count})
			remaining -= count
		}
	}
	return plan
}

// totalRobotsInPlan 计算生成计划中机器人总数。
func totalRobotsInPlan(plan []robotSpawnEntry) int32 {
	var total int32
	for _, e := range plan {
		total += e.Count
	}
	return total
}

// pickRobotInfo 从信息池中随机选取一条未使用的机器人信息。
// usedIDs 为当前组已使用的 InfoID 集合。
// 若信息池全部耗尽则返回 false。
func pickRobotInfo(infos []RobotInfoEntry, usedIDs map[int64]struct{}) (RobotInfoEntry, bool) {
	// 构建可用列表
	avail := make([]RobotInfoEntry, 0, len(infos))
	for _, info := range infos {
		if _, used := usedIDs[info.InfoID]; !used {
			avail = append(avail, info)
		}
	}
	if len(avail) == 0 {
		return RobotInfoEntry{}, false
	}
	return avail[rand.Intn(len(avail))], true
}

// initRobotScore 在 [min, max] 范围内随机初始化积分。
func initRobotScore(min, max int64) int64 {
	if max <= min {
		return min
	}
	return min + rand.Int63n(max-min+1)
}

// tickRobotScore 按机器人分数变化流程图推进一步。
// 参数：
//   - robot          机器人状态（in/out）
//   - tier           档次配置
//   - firstScore     组内当前第一名积分（含机器人），用于计算增长目标分
//   - realFirstScore 真实玩家第一名积分，用于 maxDifferenceToken 约束（0 表示无真实玩家，不约束）
//   - nowMs          当前时间（Unix 毫秒）
//   - gameEndTimeMs  玩法结束时间（Unix 毫秒），LockTokenTime 以此为基准
//
// 返回实际写入的新积分（若未变动则返回当前积分）。
func tickRobotScore(robot *robotState, tier *RobotTierCfg, firstScore int64, realFirstScore int64, nowMs int64, gameEndTimeMs int64) int64 {
	// 1. 距玩法结束剩余时间 ≤ LockTokenTime → 停止增长
	if gameEndTimeMs > 0 && gameEndTimeMs-nowMs <= tier.LockTokenTimeMs {
		return robot.Score
	}

	// 2. 未到增长CD → 等待
	if nowMs-robot.LastGrowAt < tier.GrowTokenCdMs {
		return robot.Score
	}

	// 3. 基于组内第一名积分，在万分比范围内随机目标分
	targetScore := calcGrowTarget(firstScore, tier.GrowTokenMinBps, tier.GrowTokenMaxBps)

	// 4. 随机结果不高于当前积分 → 本次不变（更新 CD 时间）
	if targetScore <= robot.Score {
		robot.LastGrowAt = nowMs
		return robot.Score
	}

	// 5. 依次应用两条上限：
	//    a. 真实玩家第一名 + maxDifferenceToken（有真实玩家时生效）
	//    b. 积分绝对上限 MaxToken
	newScore := targetScore
	if realFirstScore > 0 && newScore > realFirstScore+tier.MaxDifferenceToken {
		newScore = realFirstScore + tier.MaxDifferenceToken
	}
	if newScore > tier.MaxToken {
		newScore = tier.MaxToken
	}

	robot.LastGrowAt = nowMs
	if newScore > robot.Score {
		robot.Score = newScore
	}
	return robot.Score
}

// calcGrowTarget 计算增长目标积分：firstScore * randBps / 10000。
func calcGrowTarget(firstScore, minBps, maxBps int64) int64 {
	if firstScore <= 0 || minBps >= maxBps {
		return firstScore * minBps / 10000
	}
	r := minBps + rand.Int63n(maxBps-minBps+1)
	return firstScore * r / 10000
}
