package periodic

import (
	"fmt"

	"socialserver/internal/rank/engine"
)

// PeriodicState 周期排行榜的运行时状态，由 Handler 维护。
// 逻辑活动 (bizType+actID) 对应一个 PeriodicState；
// 每轮对应一个 engine.Service，内部 BizId 格式为 "{bizType}_{actID}_r{round}"。
type PeriodicState struct {
	BizType        string
	ActID          int32
	CycleDays      int32
	TotalOpenTime  int64
	TotalCloseTime int64
	CurrentRound   int32
	RoundOpenTime  int64
	RoundCloseTime int64
}

// RoundInfo 轮次摘要。
type RoundInfo struct {
	Round     int32
	OpenTime  int64
	CloseTime int64
	Settled   bool
	Current   bool
}

// LogicalKey 返回逻辑活动的 map 键格式 "{bizType}:{actID}"。
// 与 rankservice.BizKey.String() 保持一致。
func LogicalKey(bizType string, actID int32) string {
	return fmt.Sprintf("%s:%d", bizType, actID)
}

// IsRoundBizId 判断一个 BizId 是否为周期子轮次 BizId，格式 "{bizType}_{actID}_r{N}"。
func IsRoundBizId(bizId string) bool {
	for i := len(bizId) - 1; i >= 0; i-- {
		if bizId[i] == '_' {
			suffix := bizId[i+1:]
			if len(suffix) >= 2 && suffix[0] == 'r' {
				for _, ch := range suffix[1:] {
					if ch < '0' || ch > '9' {
						return false
					}
				}
				return true
			}
			return false
		}
	}
	return false
}

// StateLogicalKey 返回该状态的 map 键。
func (p *PeriodicState) StateLogicalKey() string {
	return LogicalKey(p.BizType, p.ActID)
}

// roundBizId 返回第 round 轮的 engine 内部 BizId（用于 Redis/MongoDB 键）。
func (p *PeriodicState) roundBizId(round int32) string {
	return fmt.Sprintf("%s_%d_r%d", p.BizType, p.ActID, round)
}

// currentBizId 返回当前轮的 engine 内部 BizId。
func (p *PeriodicState) currentBizId() string {
	return p.roundBizId(p.CurrentRound)
}

// computeRoundWindow 根据总开始时间和周期天数计算第 round 轮的时间窗口。
// 最后一轮的结束时间截断至 TotalCloseTime。
func (p *PeriodicState) computeRoundWindow(round int32) (openTime, closeTime int64) {
	cycleDur := int64(p.CycleDays) * 24 * 60 * 60 * 1000
	openTime = p.TotalOpenTime + int64(round-1)*cycleDur
	closeTime = openTime + cycleDur
	if closeTime > p.TotalCloseTime {
		closeTime = p.TotalCloseTime
	}
	return
}

// isRoundExpired 判断当前轮是否已到结束时间。
func (p *PeriodicState) isRoundExpired(now int64) bool {
	return now >= p.RoundCloseTime
}

// isActivityFinished 判断整个活动有效期是否结束。
func (p *PeriodicState) isActivityFinished(now int64) bool {
	return now >= p.TotalCloseTime
}

// advanceToNextRound 推进到下一轮，更新 CurrentRound/RoundOpenTime/RoundCloseTime。
// 若下一轮开始时间超出有效期则返回 false。
func (p *PeriodicState) advanceToNextRound() bool {
	nextRound := p.CurrentRound + 1
	open, close := p.computeRoundWindow(nextRound)
	if open >= p.TotalCloseTime {
		return false
	}
	p.CurrentRound = nextRound
	p.RoundOpenTime = open
	p.RoundCloseTime = close
	return true
}

// ToSavedState 转换为用于持久化的 DAO 结构。
func (p *PeriodicState) ToSavedState() engine.PeriodicSavedState {
	return engine.PeriodicSavedState{
		CycleDays:      p.CycleDays,
		TotalOpenTime:  p.TotalOpenTime,
		TotalCloseTime: p.TotalCloseTime,
		CurrentRound:   p.CurrentRound,
		RoundOpenTime:  p.RoundOpenTime,
		RoundCloseTime: p.RoundCloseTime,
	}
}

// StateFromSaved 从持久化结构和逻辑活动信息恢复 PeriodicState。
func StateFromSaved(bizType string, actID int32, s engine.PeriodicSavedState) *PeriodicState {
	return &PeriodicState{
		BizType:        bizType,
		ActID:          actID,
		CycleDays:      s.CycleDays,
		TotalOpenTime:  s.TotalOpenTime,
		TotalCloseTime: s.TotalCloseTime,
		CurrentRound:   s.CurrentRound,
		RoundOpenTime:  s.RoundOpenTime,
		RoundCloseTime: s.RoundCloseTime,
	}
}

// NewPeriodicState 创建新的 PeriodicState（首次注册时使用）。
func NewPeriodicState(bizType string, actID int32, cycleDays int32, totalOpenTime, totalCloseTime int64) *PeriodicState {
	p := &PeriodicState{
		BizType:        bizType,
		ActID:          actID,
		CycleDays:      cycleDays,
		TotalOpenTime:  totalOpenTime,
		TotalCloseTime: totalCloseTime,
		CurrentRound:   1,
	}
	p.RoundOpenTime, p.RoundCloseTime = p.computeRoundWindow(1)
	return p
}
