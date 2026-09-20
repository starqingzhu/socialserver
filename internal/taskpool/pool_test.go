package taskpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolExecutesSubmittedTask(t *testing.T) {
	p := New("test", 2, 4)
	defer p.Close(context.Background())

	var ran atomic.Bool
	done := make(chan struct{})
	if err := p.Submit(func() {
		ran.Store(true)
		close(done)
	}); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("task did not run in time")
	}
	if !ran.Load() {
		t.Fatal("expected task to have run")
	}
}

func TestPoolSubmitReturnsErrPoolFullWhenSaturated(t *testing.T) {
	// 0 workers so nothing drains the queue; queue length 1 so the second Submit overflows.
	p := New("test", 0, 1)
	defer p.Close(context.Background())

	if err := p.Submit(func() {}); err != nil {
		t.Fatalf("first Submit should succeed, got: %v", err)
	}
	if err := p.Submit(func() {}); err != ErrPoolFull {
		t.Fatalf("expected ErrPoolFull, got: %v", err)
	}
}

func TestPoolSubmitWaitBlocksUntilRoomThenSucceeds(t *testing.T) {
	p := New("test", 0, 1)
	defer p.Close(context.Background())

	if err := p.Submit(func() {}); err != nil {
		t.Fatalf("first Submit should succeed, got: %v", err)
	}

	submitted := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := p.SubmitWait(ctx, func() {}); err != nil {
			t.Errorf("SubmitWait failed: %v", err)
		}
		close(submitted)
	}()

	select {
	case <-submitted:
		t.Fatal("SubmitWait returned before queue had room")
	case <-time.After(100 * time.Millisecond):
	}

	// Drain the queue manually (no workers running) to free up room.
	<-p.queue

	select {
	case <-submitted:
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitWait did not unblock after room freed up")
	}
}

func TestPoolSubmitWaitTimeoutExpiresWhenSaturated(t *testing.T) {
	p := New("test", 0, 1)
	defer p.Close(context.Background())

	if err := p.Submit(func() {}); err != nil {
		t.Fatalf("first Submit should succeed, got: %v", err)
	}

	start := time.Now()
	err := p.SubmitWaitTimeout(context.Background(), func() {}, 100*time.Millisecond)
	elapsed := time.Since(start)

	if err != ErrPoolFull {
		t.Fatalf("expected ErrPoolFull, got: %v", err)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("expected to wait at least the timeout, elapsed: %v", elapsed)
	}
}

func TestPoolWorkerSurvivesPanickingTask(t *testing.T) {
	p := New("test", 1, 4)
	defer p.Close(context.Background())

	if err := p.Submit(func() { panic("boom") }); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	var ran atomic.Bool
	done := make(chan struct{})
	// Give the panic time to be recovered, then confirm the worker still runs tasks.
	time.Sleep(50 * time.Millisecond)
	if err := p.Submit(func() {
		ran.Store(true)
		close(done)
	}); err != nil {
		t.Fatalf("Submit after panic failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not survive panic")
	}
	if !ran.Load() {
		t.Fatal("expected task after panic to have run")
	}
}

func TestPoolCloseDrainsQueueThenRejectsNewSubmissions(t *testing.T) {
	p := New("test", 2, 8)

	var completed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		if err := p.Submit(func() {
			defer wg.Done()
			completed.Add(1)
		}); err != nil {
			wg.Done()
			t.Fatalf("Submit failed: %v", err)
		}
	}
	wg.Wait()

	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if completed.Load() != 5 {
		t.Fatalf("expected all 5 tasks to complete, got %d", completed.Load())
	}

	if err := p.Submit(func() {}); err != ErrPoolClosed {
		t.Fatalf("expected ErrPoolClosed after Close, got: %v", err)
	}
	if err := p.SubmitWait(context.Background(), func() {}); err != ErrPoolClosed {
		t.Fatalf("expected ErrPoolClosed from SubmitWait after Close, got: %v", err)
	}
	if err := p.SubmitWaitTimeout(context.Background(), func() {}, time.Second); err != ErrPoolClosed {
		t.Fatalf("expected ErrPoolClosed from SubmitWaitTimeout after Close, got: %v", err)
	}
}

func TestPoolCloseRespectsContextDeadline(t *testing.T) {
	p := New("test", 1, 4)

	blockForever := make(chan struct{})
	if err := p.Submit(func() { <-blockForever }); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	// Let the worker pick up the blocking task before closing.
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := p.Close(ctx)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got: %v", err)
	}
	close(blockForever)
}
