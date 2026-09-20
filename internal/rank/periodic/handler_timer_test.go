package periodic

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestHandlerClearStopsTrackedTimers 验证第 07 条：Clear() 必须 Stop() 所有仍在等待触发的
// 延迟清理 timer，防止 Server 关闭后 timer 到期仍调用已关闭的 Redis 连接。
func TestHandlerClearStopsTrackedTimers(t *testing.T) {
	h := NewHandler(nil, nil, nil)

	var fired int32
	timer := time.AfterFunc(50*time.Millisecond, func() {
		atomic.AddInt32(&fired, 1)
	})
	h.trackTimer(timer)

	h.mu.RLock()
	trackedCount := len(h.timers)
	h.mu.RUnlock()
	if trackedCount != 1 {
		t.Fatalf("expected 1 tracked timer before Clear, got %d", trackedCount)
	}

	h.Clear()

	h.mu.RLock()
	afterClearCount := len(h.timers)
	h.mu.RUnlock()
	if afterClearCount != 0 {
		t.Fatalf("expected 0 tracked timers after Clear, got %d", afterClearCount)
	}

	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&fired) != 0 {
		t.Fatalf("timer fired after Clear() stopped it; Clear must prevent pending cleanup from running against a closed Handler")
	}
}

// TestUntrackTimerRemovesFiredTimer 验证正常触发的 timer 会从追踪列表中自行移除，
// 不会无限增长占用内存。
func TestUntrackTimerRemovesFiredTimer(t *testing.T) {
	h := NewHandler(nil, nil, nil)

	done := make(chan struct{})
	var timer *time.Timer
	timer = time.AfterFunc(10*time.Millisecond, func() {
		h.untrackTimer(timer)
		close(done)
	})
	h.trackTimer(timer)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timer callback did not fire in time")
	}

	h.mu.RLock()
	remaining := len(h.timers)
	h.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("expected fired timer to be untracked, got %d remaining", remaining)
	}
}
