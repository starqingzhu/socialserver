package engine

import "fmt"

// GroupState 业务层分组状态，独立于底层榜单实例状态。
type GroupState string

const (
	GroupStateOpen    GroupState = "open"    // 接受新成员加入
	GroupStateFull    GroupState = "full"    // 已达人数上限，不再接受新成员
	GroupStateSettled GroupState = "settled" // 结算，排名固化
)

// Config 排行榜业务配置。
type Config struct {
	BizType       string // 业务类型标识
	ActID         int32  // 活动ID
	RankCode      string
	RankPeopleNum int32 // 每组最大人数（含机器人）
	OpenToken     int64 // 进榜最低积分门槛（大于等于，不受商店消耗影响）
	OpenTime      int64 // 活动开放时间（Unix 毫秒，UTC+0）
	CloseTime     int64 // 活动关闭时间（Unix 毫秒，UTC+0）：超过此时间停止接受积分
	GameEndTime   int64 // 玩法结束时间（Unix 毫秒，UTC+0）：超过此时间触发结算（0 则退化为 CloseTime）
	CreateTime    int64 // 配置创建时间（Unix 毫秒，$setOnInsert 写入 MongoDB，不随更新覆盖）

	// 机器人配置（可选；为空则不生成机器人）
	RobotTiers []RobotTierCfg   // 各档次机器人配置
	RobotInfos []RobotInfoEntry // 机器人展示信息池
}

// hasRobots 判断是否配置了机器人。
func (c *Config) hasRobots() bool {
	return len(c.RobotTiers) > 0 && len(c.RobotInfos) > 0
}

func (c *Config) computeBizId() string {
	return fmt.Sprintf("%s_%d", c.BizType, c.ActID)
}

// Option 用于配置 Service 的可选参数。
type Option func(*Service)

// WithOnMemberJoin 设置新成员首次加入分组时的回调。
func WithOnMemberJoin(fn func(userID int64, groupID int32)) Option {
	return func(s *Service) { s.onMemberJoin = fn }
}

// Group 业务层分组信息。
type Group struct {
	GroupID    int32      `json:"groupId" bson:"groupId"`
	InstanceID string     `json:"instanceId" bson:"instanceId"`
	RealCount  int32      `json:"realCount" bson:"realCount"`
	RobotCount int32      `json:"robotCount" bson:"robotCount"`
	State      GroupState `json:"state" bson:"state"`
}

// totalCount 返回分组的总人数（真实玩家 + 机器人）。
func (g *Group) totalCount() int32 {
	return g.RealCount + g.RobotCount
}
