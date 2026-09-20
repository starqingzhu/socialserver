// Package taskpool provides a single bounded worker pool shared by the whole
// process, replacing unbounded `go func()` call sites (see docs/rank_optimization.md 第 00 条).
package taskpool

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"golib/zaplog"
)

var (
	// ErrPoolFull is returned by Submit/SubmitWaitTimeout when the task queue has no room.
	ErrPoolFull = errors.New("taskpool: queue is full")
	// ErrPoolClosed is returned when a task is submitted after Close has been called.
	ErrPoolClosed = errors.New("taskpool: pool is closed")
)

// Global is the process-wide pool singleton, initialized in server.OnInit
// and closed in server.OnClose before Redis/MongoDB are closed.
var Global *Pool

// Pool is a bounded worker pool with panic-isolated task execution.
type Pool struct {
	name      string
	queue     chan func()
	stopped   chan struct{}
	closing   atomic.Bool
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// New creates a Pool with the given number of workers and queue capacity, and
// starts the workers immediately.
func New(name string, workers, queueLen int) *Pool {
	p := &Pool{
		name:    name,
		queue:   make(chan func(), queueLen),
		stopped: make(chan struct{}),
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		select {
		case fn := <-p.queue:
			runSafely(p.name, fn)
		case <-p.stopped:
			p.drain()
			return
		}
	}
}

// drain runs any tasks left in the queue buffer without blocking, so that
// SubmitWait callers that already enqueued before Close began still complete.
func (p *Pool) drain() {
	for {
		select {
		case fn := <-p.queue:
			runSafely(p.name, fn)
		default:
			return
		}
	}
}

func runSafely(poolName string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			zaplog.LoggerSugar.Errorf("taskpool[%s]: task panicked: %v\n%s", poolName, r, debug.Stack())
		}
	}()
	fn()
}

// Submit enqueues fn without blocking. Returns ErrPoolFull if the queue is
// full, or ErrPoolClosed if the pool has been closed. Use for droppable or
// retryable work (e.g. WarmUp).
func (p *Pool) Submit(fn func()) error {
	if p.closing.Load() {
		return ErrPoolClosed
	}
	select {
	case p.queue <- fn:
		return nil
	default:
		return ErrPoolFull
	}
}

// SubmitWait blocks until fn is enqueued (never dropped), or until ctx is
// done, or until the pool is closed. Use for per-task work inside a batch
// that must not be dropped (e.g. a single Service's Tick).
func (p *Pool) SubmitWait(ctx context.Context, fn func()) error {
	if p.closing.Load() {
		return ErrPoolClosed
	}
	select {
	case p.queue <- fn:
		return nil
	case <-p.stopped:
		return ErrPoolClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SubmitWaitTimeout blocks until fn is enqueued, up to timeout, then returns
// ErrPoolFull. Use for a resident ticker loop submitting a whole batch, where
// skipping a tick is safer than blocking the ticker (Go's time.Ticker channel
// has capacity 1 and silently drops ticks if the consumer blocks).
func (p *Pool) SubmitWaitTimeout(ctx context.Context, fn func(), timeout time.Duration) error {
	if p.closing.Load() {
		return ErrPoolClosed
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case p.queue <- fn:
		return nil
	case <-p.stopped:
		return ErrPoolClosed
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ErrPoolFull
	}
}

// Close stops accepting new tasks, drains the queue, and waits for all
// workers to exit or ctx to expire, whichever comes first.
func (p *Pool) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.closing.Store(true)
		close(p.stopped)
	})

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
