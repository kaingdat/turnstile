package turnstile

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBackpressure_NewController_InvalidPanics(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		t.Run("maxInFlight="+itoa(n), func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for maxInFlight=%d", n)
				}
			}()
			newBackpressureController(n)
		})
	}
}

func TestBackpressure_AcquireRelease_Basic(t *testing.T) {
	bp := newBackpressureController(3)
	ctx := context.Background()

	for i := range 3 {
		if err := bp.Acquire(ctx); err != nil {
			t.Fatalf("Acquire %d: unexpected error: %v", i, err)
		}
	}
	if bp.current != 3 {
		t.Fatalf("expected current=3, got %d", bp.current)
	}

	for range 3 {
		bp.Release()
	}
	if bp.current != 0 {
		t.Fatalf("expected current=0 after releases, got %d", bp.current)
	}
}

func TestBackpressure_Acquire_BlocksUntilRelease(t *testing.T) {
	bp := newBackpressureController(1)
	ctx := context.Background()

	if err := bp.Acquire(ctx); err != nil {
		t.Fatal(err)
	}

	acquired := make(chan struct{})
	go func() {
		if err := bp.Acquire(ctx); err != nil {
			return
		}
		close(acquired)
		bp.Release()
	}()

	// Give the goroutine time to block.
	select {
	case <-acquired:
		t.Fatal("second Acquire should have blocked")
	case <-time.After(20 * time.Millisecond):
	}

	bp.Release()

	select {
	case <-acquired:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second Acquire did not unblock after Release")
	}
}

func TestBackpressure_Acquire_ContextAlreadyCanceled(t *testing.T) {
	bp := newBackpressureController(1)

	if err := bp.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := bp.Acquire(ctx); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	bp.Release()
}

func TestBackpressure_Acquire_ContextCanceledWhileBlocked(t *testing.T) {
	bp := newBackpressureController(1)
	ctx := context.Background()

	if err := bp.Acquire(ctx); err != nil {
		t.Fatal(err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- bp.Acquire(ctx2)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel2()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Acquire did not return after context cancel")
	}

	bp.Release()
}

func TestBackpressure_Release_PanicWithoutAcquire(t *testing.T) {
	bp := newBackpressureController(1)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on Release without Acquire")
		}
	}()
	bp.Release()
}

func TestBackpressure_Concurrent(t *testing.T) {
	const maxInFlight = 5
	const goroutines = 20
	bp := newBackpressureController(maxInFlight)
	ctx := context.Background()

	var (
		mu      sync.Mutex
		current int
		maxSeen int
	)

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := bp.Acquire(ctx); err != nil {
				t.Errorf("Acquire error: %v", err)
				return
			}
			mu.Lock()
			current++
			if current > maxSeen {
				maxSeen = current
			}
			if current > maxInFlight {
				t.Errorf("concurrent=%d exceeded maxInFlight=%d", current, maxInFlight)
			}
			mu.Unlock()

			time.Sleep(2 * time.Millisecond)

			mu.Lock()
			current--
			mu.Unlock()
			bp.Release()
		}()
	}
	wg.Wait()

	if maxSeen == 0 {
		t.Error("expected at least one goroutine to run")
	}
	if bp.current != 0 {
		t.Fatalf("expected current=0 after all goroutines done, got %d", bp.current)
	}
}

// itoa converts small ints to string for subtest names without importing strconv.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	b := make([]byte, 0, 10)
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}
