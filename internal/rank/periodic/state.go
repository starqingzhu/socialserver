package periodic

import (
	"fmt"
	"sync/atomic"
	"time"

	"socialserver/internal/rank/engine"
)

// PeriodicState 周期排行榜的运行时状态，由 Handler 维护。
// 逻辑活动 (bizType+actID) 对应一个 PeriodicState；
// 每轮对应一个 engine.Service，内部 BizId 格式为 "{bizType}_{actID}_r{round}"。
//
// 并发规则：
//   - BizType/ActID/CycleMinutes/TotalOpenTime/TotalCloseTime 创建后不可变，可直接读。
//   - currentRound 由 tick 单 goroutine 写，多 RPC goroutine 读：通过 atomic 保护。
//   - RoundOpenTime/RoundCloseTime 仅由 tick goroutine 读写，无需额外同步。
type PeriodicState struct {
	BizType        string
	ActID          int32
	CycleMinutes   int32
	TotalOpenTime  int64
	TotalCloseTime int64

	currentRound   int32 // 仅通过 GetCurrentRound/setCurrentRound 访问
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

// GetCurrentRound 线程安全地返回当前轮次号。
func (p *PeriodicState) GetCurrentRound() int32 {
	return atomic.LoadInt32(&p.currentRound)
}

// setCurrentRound 线程安全地设置当前轮次号（仅供 tick goroutine 调用）。
func (p *PeriodicState) setCurrentRound(r int32) {
	atomic.StoreInt32(&p.currentRound, r)
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

// ExtractRoundBizBase 从轮次 BizId（格式 "{bizType}_{actID}_r{N}"）中提取基础部分（"{bizType}_{actID}"）。
// 若 bizId 不符合轮次格式则返回 ("", false)。
func ExtractRoundBizBase(roundBizId string) (base string, ok bool) {
	for i := len(roundBizId) - 1; i >= 0; i-- {
		if roundBizId[i] == '_' {
			suffix := roundBizId[i+1:]
			if len(suffix) >= 2 && suffix[0] == 'r' {
				for _, ch := range suffix[1:] {
					if ch < '0' || ch > '9' {
						return "", false
					}
				}
				return roundBizId[:i], true
			}
			return "", false
		}
	}
	return "", false
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
	return p.roundBizId(p.GetCurrentRound())
}

// computeRoundWindow 根据总开始时间和周期分钟数计算第 round 轮的时间窗口。
// 最后一轮的结束时间截断至 TotalCloseTime。
// CycleMinutes<=0 时退化为整体活动窗口（防御性保护）。
func (p *PeriodicState) computeRoundWindow(round int32) (openTime, closeTime int64) {
	if p.CycleMinutes <= 0 {
		return p.TotalOpenTime, p.TotalCloseTime
	}
	cycleDur := int64(p.CycleMinutes) * 60 * 1000
	openTime = p.TotalOpenTime + int64(round-1)*cycleDur
	closeTime = openTime + cycleDur
	if closeTime > p.TotalCloseTime {
		closeTime = p.TotalCloseTime
	}
	return
}

// computeRoundAt 返回时间点 now 所属的轮次（从 1 开始）。
// 按 (now - TotalOpenTime) / 周期时长 + 1 计算，并截断到活动有效轮次内。
func (p *PeriodicState) computeRoundAt(now int64) int32 {
	if p.CycleMinutes <= 0 {
		return 1
	}
	cycleDur := int64(p.CycleMinutes) * 60 * 1000
	if now <= p.TotalOpenTime {
		return 1
	}
	round := int32((now-p.TotalOpenTime)/cycleDur) + 1
	if max := p.maxRound(); round > max {
		return max
	}
	return round
}

// maxRound 返回该活动可容纳的最大轮次号。
func (p *PeriodicState) maxRound() int32 {
	if p.CycleMinutes <= 0 {
		return 1
	}
	span := p.TotalCloseTime - p.TotalOpenTime
	if span <= 0 {
		return 1
	}
	cycleDur := int64(p.CycleMinutes) * 60 * 1000
	return int32((span-1)/cycleDur) + 1
}

// isRoundExpired 判断当前轮是否已到结束时间。
// 仅由 tick goroutine 调用，RoundCloseTime 无需 atomic 保护。
func (p *PeriodicState) isRoundExpired(now int64) bool {
	return now >= p.RoundCloseTime
}

// isActivityFinished 判断整个活动有效期是否结束。
func (p *PeriodicState) isActivityFinished(now int64) bool {
	return now >= p.TotalCloseTime
}

// advanceToNextRound 推进到下一轮，更新 currentRound/RoundOpenTime/RoundCloseTime。
// 若下一轮开始时间超出有效期则返回 false。
// 仅由 tick goroutine 调用；先写时间窗口，再原子更新 currentRound，
// 保证外部 goroutine 读到新 round 号时窗口已就绪。
func (p *PeriodicState) advanceToNextRound() bool {
	nextRound := p.GetCurrentRound() + 1
	open, close := p.computeRoundWindow(nextRound)
	if open >= p.TotalCloseTime {
		return false
	}
	p.RoundOpenTime = open
	p.RoundCloseTime = close
	p.setCurrentRound(nextRound)
	return true
}

// ToSavedState 转换为用于持久化的 DAO 结构。
func (p *PeriodicState) ToSavedState() engine.PeriodicSavedState {
	return engine.PeriodicSavedState{
		CycleMinutes:   p.CycleMinutes,
		TotalOpenTime:  p.TotalOpenTime,
		TotalCloseTime: p.TotalCloseTime,
		CurrentRound:   p.GetCurrentRound(),
		RoundOpenTime:  p.RoundOpenTime,
		RoundCloseTime: p.RoundCloseTime,
	}
}

// StateFromSaved 从持久化结构和逻辑活动信息恢复 PeriodicState。
func StateFromSaved(bizType string, actID int32, s engine.PeriodicSavedState) *PeriodicState {
	p := &PeriodicState{
		BizType:        bizType,
		ActID:          actID,
		CycleMinutes:   s.CycleMinutes,
		TotalOpenTime:  s.TotalOpenTime,
		TotalCloseTime: s.TotalCloseTime,
		RoundOpenTime:  s.RoundOpenTime,
		RoundCloseTime: s.RoundCloseTime,
	}
	p.currentRound = s.CurrentRound // 创建阶段未共享，直接赋值
	return p
}

// NewPeriodicState 创建新的 PeriodicState（首次注册时使用）。
// 创建时直接按当前时间定位到所在轮次，而不是从第 1 轮开始逐步 tick 推进。
// cycleMinutes 必须 > 0，否则返回 error。
func NewPeriodicState(bizType string, actID int32, cycleMinutes int32, totalOpenTime, totalCloseTime int64) (*PeriodicState, error) {
	if cycleMinutes <= 0 {
		return nil, fmt.Errorf("periodic: cycleMinutes must be > 0, got %d", cycleMinutes)
	}
	p := &PeriodicState{
		BizType:        bizType,
		ActID:          actID,
		CycleMinutes:   cycleMinutes,
		TotalOpenTime:  totalOpenTime,
		TotalCloseTime: totalCloseTime,
	}
	curRound := p.computeRoundAt(time.Now().UnixMilli())
	p.currentRound = curRound
	p.RoundOpenTime, p.RoundCloseTime = p.computeRoundWindow(curRound)
	return p, nil
}
