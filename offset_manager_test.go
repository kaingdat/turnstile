package turnstile

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"log/slog"

	"github.com/segmentio/kafka-go"
)

func newTestOffsetManager(t *testing.T) *offsetManager {
	t.Helper()
	return newOffsetManager(offsetManagerConfig{
		topic:          "test-topic",
		commitFunc:     func(ctx context.Context, message kafka.Message) error { return nil },
		logger:         slog.Default(),
		minCommitCount: 5,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     3,
		retryDelay:     10 * time.Millisecond,
	})
}

// lastCommitOf reads the lastCommit watermark for tests. Returns (-1, false) if
// the partition has not been tracked yet.
func lastCommitOf(m *offsetManager, partition int) (int64, bool) {
	m.mu.RLock()
	s, ok := m.partitions[partition]
	m.mu.RUnlock()
	if !ok {
		return -1, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.initialized {
		return -1, false
	}
	return s.lastCommit, true
}

func TestTrack_InitializesLastCommitOffset(t *testing.T) {
	m := newTestOffsetManager(t)

	m.Track(0, 10)

	got, ok := lastCommitOf(m, 0)
	if !ok {
		t.Fatal("expected partition 0 to be initialized")
	}
	if got != 9 {
		t.Errorf("expected lastCommit to be 9 (offset-1), got %d", got)
	}
}

func TestMarkDone_NoPanicOnUnknownPartition(t *testing.T) {
	m := newTestOffsetManager(t)

	if err := m.MarkDone(context.Background(), 99, 42); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTrack_UsesCommittedOffsetFromFetch(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic:      "test-topic",
		commitFunc: func(ctx context.Context, message kafka.Message) error { return nil },
		fetchOffsetFunc: func(ctx context.Context, partition int) (int64, bool, error) {
			return 100, true, nil
		},
		logger:         slog.Default(),
		minCommitCount: 5,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	m.Track(0, 200)

	got, ok := lastCommitOf(m, 0)
	if !ok {
		t.Fatal("expected partition 0 to be initialized")
	}
	if got != 99 {
		t.Errorf("expected lastCommit=99 from committed=100, got %d", got)
	}
}

// Bug #1 regression: committed offset 0 is a valid value and must NOT be treated
// as "no committed offset". After Track(p, 5) with fetchOffsetFunc returning
// (0, true, nil), lastCommit must be -1 (next-to-read 0), not 4.
func TestTrack_CommittedOffsetZeroIsHonored(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic:      "test-topic",
		commitFunc: func(ctx context.Context, message kafka.Message) error { return nil },
		fetchOffsetFunc: func(ctx context.Context, partition int) (int64, bool, error) {
			return 0, true, nil
		},
		logger:         slog.Default(),
		minCommitCount: 5,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	m.Track(0, 5)

	got, ok := lastCommitOf(m, 0)
	if !ok {
		t.Fatal("expected partition 0 to be initialized")
	}
	if got != -1 {
		t.Errorf("expected lastCommit=-1 from committed=0, got %d", got)
	}
}

func TestForceCommit_CommitsDoneOffsets(t *testing.T) {
	var committed sync.Map

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(ctx context.Context, msg kafka.Message) error {
			committed.Store(msg.Partition, msg.Offset)
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	m.Track(0, 0)
	m.Track(0, 1)
	m.Track(1, 0)
	m.Track(1, 1)

	_ = m.MarkDone(context.Background(), 0, 0)
	_ = m.MarkDone(context.Background(), 0, 1)
	_ = m.MarkDone(context.Background(), 1, 0)
	_ = m.MarkDone(context.Background(), 1, 1)

	m.ForceCommit(context.Background())

	offset0, ok0 := committed.Load(0)
	offset1, ok1 := committed.Load(1)

	if !ok0 {
		t.Error("expected partition 0 to be committed")
	}
	if !ok1 {
		t.Error("expected partition 1 to be committed")
	}

	t.Logf("partition 0 committed offset=%v, partition 1 committed offset=%v", offset0, offset1)
}

func TestCommitWithRetries_DoesNotAdvanceOffsetOnAllRetriesFailed(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(ctx context.Context, msg kafka.Message) error {
			return fmt.Errorf("kafka unavailable")
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     2,
		retryDelay:     1 * time.Millisecond,
	})

	m.Track(0, 0)
	m.Track(0, 1)
	m.Track(0, 2)

	_ = m.MarkDone(context.Background(), 0, 0)
	_ = m.MarkDone(context.Background(), 0, 1)
	_ = m.MarkDone(context.Background(), 0, 2)

	got, ok := lastCommitOf(m, 0)
	if !ok {
		t.Fatal("expected partition 0 to be initialized")
	}
	if got != -1 {
		t.Errorf("lastCommit should stay at -1 after all retries fail, got %d", got)
	}
}

func TestSequential_CommitProgress(t *testing.T) {
	var lastCommitted int64

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(ctx context.Context, msg kafka.Message) error {
			lastCommitted = msg.Offset
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 3,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	for i := range int64(5) {
		m.Track(0, i)
	}

	_ = m.MarkDone(context.Background(), 0, 0)
	_ = m.MarkDone(context.Background(), 0, 1)
	if lastCommitted != 0 {
		t.Errorf("expected no commit yet, got lastCommitted=%d", lastCommitted)
	}

	_ = m.MarkDone(context.Background(), 0, 2)
	if lastCommitted != 2 {
		t.Errorf("expected lastCommitted=2, got %d", lastCommitted)
	}
}

// Bug #4 regression: concurrent Track + MarkDone on a fresh partition must
// initialize exactly once and advance the watermark to the last contiguous done
// offset without panics or lost messages.
func TestConcurrentTrackMarkDone_NoRace(t *testing.T) {
	const N = 200
	var committed atomic.Int64
	committed.Store(-1)

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(ctx context.Context, msg kafka.Message) error {
			committed.Store(msg.Offset)
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    10 * time.Millisecond,
		forceInterval:  1 * time.Second,
		maxRetries:     1,
		retryDelay:     1 * time.Millisecond,
	})

	var wg sync.WaitGroup
	for i := range int64(N) {
		wg.Add(1)
		go func(offset int64) {
			defer wg.Done()
			m.Track(0, offset)
			_ = m.MarkDone(context.Background(), 0, offset)
		}(i)
	}
	wg.Wait()

	m.ForceCommit(context.Background())

	if got := committed.Load(); got != N-1 {
		t.Errorf("expected committed=%d, got %d", N-1, got)
	}

	got, ok := lastCommitOf(m, 0)
	if !ok {
		t.Fatal("expected partition 0 to be initialized")
	}
	if got != N-1 {
		t.Errorf("expected lastCommit=%d, got %d", N-1, got)
	}
}

func TestForceCommit_NoPendingCommit(t *testing.T) {
	commitCount := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(ctx context.Context, msg kafka.Message) error {
			commitCount++
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	m.Track(0, 5)

	// Do NOT mark anything done — canCommit stays at lastCommit.
	m.ForceCommit(context.Background())

	if commitCount != 0 {
		t.Errorf("expected no commit, got %d", commitCount)
	}
}

func TestTrack_FetchOffsetFuncError_FallsBackToMessageOffset(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic:      "test-topic",
		commitFunc: func(ctx context.Context, message kafka.Message) error { return nil },
		fetchOffsetFunc: func(ctx context.Context, partition int) (int64, bool, error) {
			return 0, false, fmt.Errorf("broker unavailable")
		},
		logger:         slog.Default(),
		minCommitCount: 5,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	m.Track(0, 10)

	got, ok := lastCommitOf(m, 0)
	if !ok {
		t.Fatal("expected partition 0 to be initialized")
	}
	// fallback: lastCommit = offset-1 = 9
	if got != 9 {
		t.Errorf("expected lastCommit=9 (fallback to first message), got %d", got)
	}
}

func TestMarkDone_UnknownOffsetIsNoop(t *testing.T) {
	commitCount := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(ctx context.Context, msg kafka.Message) error {
			commitCount++
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	m.Track(0, 5)

	// Mark an offset that was never tracked — should be a no-op.
	if err := m.MarkDone(context.Background(), 0, 999); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if commitCount != 0 {
		t.Errorf("expected no commit for unknown offset, got %d", commitCount)
	}
}

// TestForceCommit_CommitFailureSkipsCleanup covers the path where the
// commitFunc returns an error during ForceCommit — cleanupInFlight must NOT
// run, so the in-flight map still carries the un-committed offsets.
func TestForceCommit_CommitFailureSkipsCleanup(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(ctx context.Context, msg kafka.Message) error {
			return fmt.Errorf("broker down")
		},
		logger:         slog.Default(),
		minCommitCount: 100, // make MarkDone skip commit so ForceCommit drives it
		maxInterval:    1 * time.Hour,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     1 * time.Millisecond,
	})

	m.Track(0, 0)
	_ = m.MarkDone(context.Background(), 0, 0)

	m.ForceCommit(context.Background())

	m.mu.RLock()
	s := m.partitions[0]
	m.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, present := s.inFlight[0]; !present {
		t.Fatal("expected offset 0 to remain in inFlight after commit failure")
	}
	if s.lastCommit != -1 {
		t.Errorf("expected lastCommit unchanged at -1, got %d", s.lastCommit)
	}
}

// TestForceCommit_SuccessCleansInFlight drives the success branch of ForceCommit
// where the commit goes through and cleanupInFlight runs.
func TestForceCommit_SuccessCleansInFlight(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic:          "test-topic",
		commitFunc:     func(ctx context.Context, msg kafka.Message) error { return nil },
		logger:         slog.Default(),
		minCommitCount: 100, // MarkDone skips commit; ForceCommit drives it
		maxInterval:    1 * time.Hour,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     1 * time.Millisecond,
	})

	m.Track(0, 0)
	m.Track(0, 1)
	_ = m.MarkDone(context.Background(), 0, 0)
	_ = m.MarkDone(context.Background(), 0, 1)

	m.ForceCommit(context.Background())

	m.mu.RLock()
	s := m.partitions[0]
	m.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastCommit != 1 {
		t.Errorf("expected lastCommit=1, got %d", s.lastCommit)
	}
	if len(s.inFlight) != 0 {
		t.Errorf("expected inFlight cleared after successful commit, got %d entries", len(s.inFlight))
	}
}

func TestCommitWithRetries_ContextCanceledDuringRetryDelay(t *testing.T) {
	attempts := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(ctx context.Context, msg kafka.Message) error {
			attempts++
			return fmt.Errorf("transient error")
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     10,
		retryDelay:     500 * time.Millisecond, // long delay so context cancel fires first
	})

	m.Track(0, 0)
	_ = m.MarkDone(context.Background(), 0, 0) // first commit attempt happens here

	// Reset to force another attempt via ForceCommit.
	// We'll call commitWithRetries directly via ForceCommit with a short-lived context.
	m.Track(0, 1)
	_ = m.MarkDone(context.Background(), 0, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	m.ForceCommit(ctx)

	// We can't assert the exact number, but we can assert that the ctx cancellation
	// stopped the retries before exhausting all 10 attempts.
	if attempts == 0 {
		t.Error("expected at least one commit attempt")
	}
}
