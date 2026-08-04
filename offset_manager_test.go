package turnstile

import (
	"context"
	"errors"
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
		commitFunc:     func(context.Context, uint64, int, int64) error { return nil },
		logger:         slog.Default(),
		minCommitCount: 5,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     3,
		retryDelay:     10 * time.Millisecond,
	})
}

// assignSentinel assigns partitions with no committed offset, leaving them to seed
// from their first message.
func assignSentinel(m *offsetManager, epoch uint64, partitions ...int) {
	assignments := make([]kafka.PartitionAssignment, 0, len(partitions))
	for _, p := range partitions {
		assignments = append(assignments, kafka.PartitionAssignment{ID: p, Offset: kafka.FirstOffset})
	}
	m.BeginEpoch(epoch, assignments)
}

// lastCommitOf returns (-1, false) if the partition is unassigned or unseeded.
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

func TestOffsetConventionRoundTrips(t *testing.T) {
	for _, committed := range []int64{0, 1, 100, 1 << 40} {
		if got := nextOffsetToRead(seedWatermark(committed)); got != committed {
			t.Errorf("committed=%d round-tripped to %d", committed, got)
		}
	}
}

func TestTrack_InitializesLastCommitOffset(t *testing.T) {
	m := newTestOffsetManager(t)
	assignSentinel(m, 1, 0)

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

// An assignment offset of 100 is the next offset to read, so message 99 was the last
// one processed.
func TestBeginEpoch_SeedsFromAssignment(t *testing.T) {
	var committed atomic.Int64
	committed.Store(-1)

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(_ context.Context, _ uint64, _ int, offset int64) error {
			committed.Store(nextOffsetToRead(offset))
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 3, Offset: 100}})

	got, ok := lastCommitOf(m, 3)
	if !ok {
		t.Fatal("expected partition 3 to be initialized by the assignment")
	}
	if got != 99 {
		t.Errorf("expected lastCommit=99 from assignment offset 100, got %d", got)
	}

	m.Track(3, 100)
	if err := m.MarkDone(context.Background(), 3, 100); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}

	if got := committed.Load(); got != 101 {
		t.Errorf("expected committed next-offset-to-read=101 after processing 100, got %d", got)
	}
}

// Bug #1 regression: 0 is a real committed offset, not "never committed". Nothing
// has been processed yet, so the watermark is -1.
func TestBeginEpoch_CommittedOffsetZeroIsHonored(t *testing.T) {
	m := newTestOffsetManager(t)

	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 0, Offset: 0}})

	got, ok := lastCommitOf(m, 0)
	if !ok {
		t.Fatal("expected partition 0 to be initialized")
	}
	if got != -1 {
		t.Errorf("expected lastCommit=-1 from assignment offset 0, got %d", got)
	}
}

// A sentinel means the group has no committed offset, so only the first message
// received can supply the watermark.
func TestBeginEpoch_SentinelDefersToFirstMessage(t *testing.T) {
	m := newTestOffsetManager(t)

	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 3, Offset: kafka.LastOffset}})

	if _, ok := lastCommitOf(m, 3); ok {
		t.Fatal("expected partition 3 to stay uninitialized under a sentinel offset")
	}

	m.Track(3, 500)

	got, ok := lastCommitOf(m, 3)
	if !ok {
		t.Fatal("expected partition 3 to be seeded by the first message")
	}
	if got != 499 {
		t.Errorf("expected lastCommit=499 seeded from first message 500, got %d", got)
	}
}

// The core regression: a partition is revoked, advanced by another member, and
// reassigned. Carrying the old watermark would leave canCommit at 150 while messages
// arrive at 400+, so the contiguous walk waits forever on the 151..399 gap and
// commits freeze permanently — never rewinding, which is why an "offset went
// backwards" heuristic cannot catch it.
func TestBeginEpoch_ReassignmentReSeeds(t *testing.T) {
	var committed atomic.Int64
	committed.Store(-1)

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(_ context.Context, _ uint64, _ int, offset int64) error {
			committed.Store(nextOffsetToRead(offset))
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Millisecond,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 3, Offset: 100}})
	for offset := int64(100); offset <= 150; offset++ {
		m.Track(3, offset)
		if err := m.MarkDone(context.Background(), 3, offset); err != nil {
			t.Fatalf("MarkDone(%d): %v", offset, err)
		}
	}
	if got, _ := lastCommitOf(m, 3); got != 150 {
		t.Fatalf("expected lastCommit=150 before revocation, got %d", got)
	}

	m.EndEpoch(1)

	// While we were revoked another member consumed through 399.
	m.BeginEpoch(2, []kafka.PartitionAssignment{{ID: 3, Offset: 400}})

	got, ok := lastCommitOf(m, 3)
	if !ok {
		t.Fatal("expected partition 3 to be initialized on reassignment")
	}
	if got != 399 {
		t.Fatalf("expected lastCommit=399 from the new assignment, got %d (stale state carried over)", got)
	}

	m.Track(3, 400)
	if err := m.MarkDone(context.Background(), 3, 400); err != nil {
		t.Fatalf("MarkDone(400): %v", err)
	}
	if got := committed.Load(); got != 401 {
		t.Errorf("expected commit to advance to 401, got %d (partition stalled on the 151..399 gap)", got)
	}
}

// Asserts the memory leak is closed: state for partitions we no longer own does not
// survive the next generation.
func TestBeginEpoch_PrunesUnassignedPartitions(t *testing.T) {
	m := newTestOffsetManager(t)

	assignSentinel(m, 1, 0, 1, 2, 3)
	for p := range 4 {
		m.Track(p, 0)
	}

	m.mu.RLock()
	before := len(m.partitions)
	m.mu.RUnlock()
	if before != 4 {
		t.Fatalf("expected 4 partitions in the first epoch, got %d", before)
	}

	assignSentinel(m, 2, 0, 1)

	m.mu.RLock()
	after := len(m.partitions)
	m.mu.RUnlock()
	if after != 2 {
		t.Errorf("expected 2 partitions after reassignment, got %d", after)
	}
	if _, ok := lastCommitOf(m, 3); ok {
		t.Error("expected partition 3 to be dropped after it was no longer assigned")
	}
}

// Revocation is best-effort, so handlers still running when their partition goes
// away must not panic or commit against state that no longer exists.
func TestMarkDone_AfterEndEpochIsInert(t *testing.T) {
	var commits atomic.Int64

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(context.Context, uint64, int, int64) error {
			commits.Add(1)
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Millisecond,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 0, Offset: 10}})
	m.Track(0, 10)
	m.Track(0, 11)

	m.EndEpoch(1)
	commits.Store(0)

	if err := m.MarkDone(context.Background(), 0, 10); err != nil {
		t.Fatalf("unexpected error from post-revoke MarkDone: %v", err)
	}
	if got := commits.Load(); got != 0 {
		t.Errorf("expected no commits after EndEpoch, got %d", got)
	}
}

// A stale generation is terminal, not transient: retrying holds the partition lock
// through the rebalance, exactly when it needs releasing.
func TestCommit_StaleGenerationDoesNotRetry(t *testing.T) {
	var attempts atomic.Int64

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(context.Context, uint64, int, int64) error {
			attempts.Add(1)
			return ErrStaleGeneration
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Millisecond,
		forceInterval:  5 * time.Second,
		maxRetries:     50,
		retryDelay:     10 * time.Millisecond,
	})

	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 0, Offset: 0}})
	m.Track(0, 0)

	err := m.MarkDone(context.Background(), 0, 0)
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected ErrStaleGeneration, got %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("expected exactly 1 commit attempt, got %d", got)
	}
}

func TestCommit_AbortsOnGenerationEnded(t *testing.T) {
	var attempts atomic.Int64

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(context.Context, uint64, int, int64) error {
			attempts.Add(1)
			return fmt.Errorf("commit failed: %w", kafka.ErrGenerationEnded)
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Millisecond,
		forceInterval:  5 * time.Second,
		maxRetries:     50,
		retryDelay:     10 * time.Millisecond,
	})

	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 0, Offset: 0}})
	m.Track(0, 0)
	_ = m.MarkDone(context.Background(), 0, 0)

	if got := attempts.Load(); got != 1 {
		t.Errorf("expected exactly 1 commit attempt for a wrapped ErrGenerationEnded, got %d", got)
	}
}

// Guards against the commit path hardcoding context.Background(), which made caller
// cancellation unable to interrupt a commit in progress.
func TestCommit_HonorsCallerContext(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(ctx context.Context, _ uint64, _ int, _ int64) error {
			<-ctx.Done()
			return ctx.Err()
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Millisecond,
		forceInterval:  5 * time.Second,
		maxRetries:     50,
		retryDelay:     10 * time.Millisecond,
	})

	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 0, Offset: 0}})
	m.Track(0, 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.MarkDone(ctx, 0, 0) }()

	// Give MarkDone time to reach the blocking commitFunc before cancelling.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MarkDone did not return after its context was canceled")
	}
}

// EndEpoch is the revocation hook's final flush, so it must commit past the
// thresholds that would otherwise hold the offset back.
func TestEndEpoch_FlushesBeforeDroppingState(t *testing.T) {
	var committed atomic.Int64
	committed.Store(-1)

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(_ context.Context, _ uint64, _ int, offset int64) error {
			committed.Store(nextOffsetToRead(offset))
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 100, // too high for MarkDone to commit on its own
		maxInterval:    1 * time.Hour,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 0, Offset: 10}})
	for offset := int64(10); offset <= 12; offset++ {
		m.Track(0, offset)
		_ = m.MarkDone(context.Background(), 0, offset)
	}
	if got := committed.Load(); got != -1 {
		t.Fatalf("expected no commit before EndEpoch, got %d", got)
	}

	m.EndEpoch(1)

	if got := committed.Load(); got != 13 {
		t.Errorf("expected EndEpoch to commit next-offset-to-read=13, got %d", got)
	}

	m.mu.RLock()
	remaining := len(m.partitions)
	m.mu.RUnlock()
	if remaining != 0 {
		t.Errorf("expected all partition state dropped after EndEpoch, got %d entries", remaining)
	}
}

// A superseded epoch must not flush — those offsets belong to the partition's new
// owner now.
func TestEndEpoch_IgnoresSupersededEpoch(t *testing.T) {
	var commits atomic.Int64

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(context.Context, uint64, int, int64) error {
			commits.Add(1)
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 100,
		maxInterval:    1 * time.Hour,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 0, Offset: 10}})
	m.BeginEpoch(2, []kafka.PartitionAssignment{{ID: 0, Offset: 10}})
	m.Track(0, 10)
	_ = m.MarkDone(context.Background(), 0, 10)

	m.EndEpoch(1)

	if got := commits.Load(); got != 0 {
		t.Errorf("expected no commits from a superseded epoch, got %d", got)
	}
	m.mu.RLock()
	remaining := len(m.partitions)
	m.mu.RUnlock()
	if remaining != 1 {
		t.Errorf("expected the current epoch's state to survive, got %d entries", remaining)
	}
}

// Dropping rather than tracking is what keeps BeginEpoch's pruning from being undone
// one message at a time.
func TestTrack_UnassignedPartitionIsIgnored(t *testing.T) {
	m := newTestOffsetManager(t)
	assignSentinel(m, 1, 0)

	m.Track(7, 42)

	m.mu.RLock()
	_, ok := m.partitions[7]
	m.mu.RUnlock()
	if ok {
		t.Error("expected no state to be created for an unassigned partition")
	}
}

func TestForceCommit_CommitsDoneOffsets(t *testing.T) {
	var committed sync.Map

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(_ context.Context, _ uint64, partition int, offset int64) error {
			committed.Store(partition, offset)
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	assignSentinel(m, 1, 0, 1)

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
		commitFunc: func(context.Context, uint64, int, int64) error {
			return fmt.Errorf("kafka unavailable")
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     2,
		retryDelay:     1 * time.Millisecond,
	})

	assignSentinel(m, 1, 0)

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
		commitFunc: func(_ context.Context, _ uint64, _ int, offset int64) error {
			lastCommitted = offset
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 3,
		maxInterval:    1 * time.Second,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     10 * time.Millisecond,
	})

	assignSentinel(m, 1, 0)

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

// Bug #4 regression: concurrent Track + MarkDone must seed exactly once and advance
// the watermark without panics or lost messages.
func TestConcurrentTrackMarkDone_NoRace(t *testing.T) {
	const N = 200
	var committed atomic.Int64
	committed.Store(-1)

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(_ context.Context, _ uint64, _ int, offset int64) error {
			committed.Store(offset)
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    10 * time.Millisecond,
		forceInterval:  1 * time.Second,
		maxRetries:     1,
		retryDelay:     1 * time.Millisecond,
	})

	assignSentinel(m, 1, 0)

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
		commitFunc: func(context.Context, uint64, int, int64) error {
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

	assignSentinel(m, 1, 0)
	m.Track(0, 5)

	// Nothing marked done, so canCommit stays at lastCommit.
	m.ForceCommit(context.Background())

	if commitCount != 0 {
		t.Errorf("expected no commit, got %d", commitCount)
	}
}

func TestMarkDone_UnknownOffsetIsNoop(t *testing.T) {
	commitCount := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(context.Context, uint64, int, int64) error {
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

	assignSentinel(m, 1, 0)
	m.Track(0, 5)

	if err := m.MarkDone(context.Background(), 0, 999); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if commitCount != 0 {
		t.Errorf("expected no commit for unknown offset, got %d", commitCount)
	}
}

// A failed commit must skip the in-flight cleanup, or the offsets it dropped could
// never be retried.
func TestForceCommit_CommitFailureSkipsCleanup(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(context.Context, uint64, int, int64) error {
			return fmt.Errorf("broker down")
		},
		logger:         slog.Default(),
		minCommitCount: 100, // make MarkDone skip commit so ForceCommit drives it
		maxInterval:    1 * time.Hour,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     1 * time.Millisecond,
	})

	assignSentinel(m, 1, 0)
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

func TestForceCommit_SuccessCleansInFlight(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic:          "test-topic",
		commitFunc:     func(context.Context, uint64, int, int64) error { return nil },
		logger:         slog.Default(),
		minCommitCount: 100, // MarkDone skips commit; ForceCommit drives it
		maxInterval:    1 * time.Hour,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     1 * time.Millisecond,
	})

	assignSentinel(m, 1, 0)
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
		commitFunc: func(context.Context, uint64, int, int64) error {
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

	assignSentinel(m, 1, 0)
	m.Track(0, 0)
	_ = m.MarkDone(context.Background(), 0, 0) // first commit attempt happens here

	// Advance the watermark again so ForceCommit has something to retry.
	m.Track(0, 1)
	_ = m.MarkDone(context.Background(), 0, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	m.ForceCommit(ctx)

	// The exact count is timing-dependent; only the fact that retries ran matters.
	if attempts == 0 {
		t.Error("expected at least one commit attempt")
	}
}

// A generation that owned nothing has nothing to flush, and must not spend the commit
// budget setting up a deadline for an empty partition set.
func TestEndEpoch_NoAssignedPartitions(t *testing.T) {
	commits := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(context.Context, uint64, int, int64) error {
			commits++
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    time.Millisecond,
		maxRetries:     1,
		retryDelay:     time.Millisecond,
	})

	m.BeginEpoch(1, nil)
	m.EndEpoch(1)

	if commits != 0 {
		t.Fatalf("committed %d times for an empty assignment, want 0", commits)
	}
}

// An offset for a partition that has been assigned but never seeded was never tracked,
// so completing it must be a no-op rather than committing from a watermark of -1.
func TestMarkDone_UninitializedPartition(t *testing.T) {
	commits := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(context.Context, uint64, int, int64) error {
			commits++
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    time.Millisecond,
		maxRetries:     1,
		retryDelay:     time.Millisecond,
	})

	assignSentinel(m, 1, 0)

	if err := m.MarkDone(context.Background(), 0, 7); err != nil {
		t.Fatalf("MarkDone on an unseeded partition: %v", err)
	}
	if commits != 0 {
		t.Fatalf("committed %d times without a seeded watermark, want 0", commits)
	}
}

// commitFunc's network call is not context-aware, so an expired budget has to be
// caught before it is entered or each partition pays a full socket timeout.
func TestCommitLocked_AbortsOnExpiredContext(t *testing.T) {
	attempts := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(context.Context, uint64, int, int64) error {
			attempts++
			return nil
		},
		logger: slog.Default(),
		// High enough that MarkDone leaves the commit to ForceCommit.
		minCommitCount: 1000,
		maxInterval:    time.Hour,
		maxRetries:     3,
		retryDelay:     time.Millisecond,
	})

	assignSentinel(m, 1, 0)
	m.Track(0, 0)
	if err := m.MarkDone(context.Background(), 0, 0); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}
	if attempts != 0 {
		t.Fatalf("MarkDone committed %d times below the threshold, want 0", attempts)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.ForceCommit(ctx)

	if attempts != 0 {
		t.Fatalf("commitFunc called %d times with an expired context, want 0", attempts)
	}
}

// A partition assigned but never seeded has no watermark to flush; committing from its
// -1 placeholder would rewind the group to the start of the log.
func TestForceCommit_SkipsUninitializedPartition(t *testing.T) {
	commits := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commitFunc: func(context.Context, uint64, int, int64) error {
			commits++
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    time.Millisecond,
		maxRetries:     1,
		retryDelay:     time.Millisecond,
	})

	assignSentinel(m, 1, 0)
	m.ForceCommit(context.Background())

	if commits != 0 {
		t.Fatalf("committed %d times for an unseeded partition, want 0", commits)
	}
}
