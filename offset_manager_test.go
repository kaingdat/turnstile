package turnstile

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
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
		commit:         func(context.Context, uint64, map[int]int64) error { return nil },
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

// drainKick runs the flush Run would perform for a pending kick, keeping tests
// synchronous. It returns nil if MarkDone did not kick.
func drainKick(ctx context.Context, m *offsetManager) error {
	select {
	case <-m.kick:
		return m.flush(ctx, false)
	default:
		return nil
	}
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
	return s.lastCommitOffset, true
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

	m.MarkDone(99, 42)
}

// An assignment offset of 100 is the next offset to read, so message 99 was the last
// one processed.
func TestBeginEpoch_SeedsFromAssignment(t *testing.T) {
	var committed atomic.Int64
	committed.Store(-1)

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(_ context.Context, _ uint64, offsets map[int]int64) error {
			for _, offset := range offsets {
				committed.Store(offset + 1)
			}
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
	m.MarkDone(3, 100)
	if err := drainKick(context.Background(), m); err != nil {
		t.Fatalf("flush: %v", err)
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
// reassigned. Carrying the old watermark would leave it at 150 while messages
// arrive at 400+, so the contiguous walk waits forever on the 151..399 gap and
// commits freeze permanently — never rewinding, which is why an "offset went
// backwards" heuristic cannot catch it.
func TestBeginEpoch_ReassignmentReSeeds(t *testing.T) {
	var committed atomic.Int64
	committed.Store(-1)

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(_ context.Context, _ uint64, offsets map[int]int64) error {
			for _, offset := range offsets {
				committed.Store(offset + 1)
			}
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
		m.MarkDone(3, offset)
		if err := drainKick(context.Background(), m); err != nil {
			t.Fatalf("flush after %d: %v", offset, err)
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
	m.MarkDone(3, 400)
	if err := drainKick(context.Background(), m); err != nil {
		t.Fatalf("flush: %v", err)
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
		commit: func(context.Context, uint64, map[int]int64) error {
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

	m.MarkDone(0, 10)
	_ = drainKick(context.Background(), m)
	if got := commits.Load(); got != 0 {
		t.Errorf("expected no commits after EndEpoch, got %d", got)
	}
}

// A stale generation is terminal, not transient: retrying holds commitMu through the
// rebalance, stalling EndEpoch's final flush.
func TestCommit_StaleGenerationDoesNotRetry(t *testing.T) {
	var attempts atomic.Int64

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(context.Context, uint64, map[int]int64) error {
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

	m.MarkDone(0, 0)
	err := drainKick(context.Background(), m)
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
		commit: func(context.Context, uint64, map[int]int64) error {
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
	m.MarkDone(0, 0)
	_ = drainKick(context.Background(), m)

	if got := attempts.Load(); got != 1 {
		t.Errorf("expected exactly 1 commit attempt for a wrapped ErrGenerationEnded, got %d", got)
	}
}

// Guards against the commit path hardcoding context.Background(), which made caller
// cancellation unable to interrupt a commit in progress.
func TestCommit_HonorsCallerContext(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(ctx context.Context, _ uint64, _ map[int]int64) error {
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

	m.MarkDone(0, 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- drainKick(ctx, m) }()

	// Give the flush time to reach the blocking commitFunc before cancelling.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not return after its context was canceled")
	}
}

// The point of committing off the hot path: a commit stuck on the network must not
// block Track or MarkDone for the partition it is committing.
func TestCommit_DoesNotBlockTrackOrMarkDone(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(context.Context, uint64, map[int]int64) error {
			close(entered)
			<-release
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 1,
		maxInterval:    time.Hour,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     time.Millisecond,
	})

	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 0, Offset: 0}})
	m.Track(0, 0)
	m.MarkDone(0, 0)

	done := make(chan error, 1)
	go func() { done <- drainKick(context.Background(), m) }()
	<-entered

	hot := make(chan struct{})
	go func() {
		m.Track(0, 1)
		m.MarkDone(0, 1)
		close(hot)
	}()
	select {
	case <-hot:
	case <-time.After(2 * time.Second):
		t.Fatal("Track/MarkDone blocked behind an in-progress commit")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("flush: %v", err)
	}
	// The commit snapshot was taken before offset 1 finished, so only 0 is recorded.
	if got, _ := lastCommitOf(m, 0); got != 0 {
		t.Errorf("expected lastCommit=0 from the snapshot, got %d", got)
	}
}

// One request carries every due partition, rather than one round-trip each.
func TestFlush_BatchesPartitionsIntoOneCommit(t *testing.T) {
	var calls []map[int]int64
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(_ context.Context, _ uint64, offsets map[int]int64) error {
			calls = append(calls, offsets)
			return nil
		},
		logger:         slog.Default(),
		minCommitCount: 100,
		maxInterval:    time.Hour,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     time.Millisecond,
	})

	assignSentinel(m, 1, 0, 1, 2)
	for p := range 3 {
		m.Track(p, 10)
		m.MarkDone(p, 10)
	}

	if err := m.flush(context.Background(), true); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if len(calls) != 1 {
		t.Fatalf("expected 1 commit request, got %d", len(calls))
	}
	want := map[int]int64{0: 10, 1: 10, 2: 10}
	if !maps.Equal(calls[0], want) {
		t.Errorf("committed %v, want %v", calls[0], want)
	}
}

// EndEpoch is the revocation hook's final flush, so it must commit past the
// thresholds that would otherwise hold the offset back.
func TestEndEpoch_FlushesBeforeDroppingState(t *testing.T) {
	var committed atomic.Int64
	committed.Store(-1)

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(_ context.Context, _ uint64, offsets map[int]int64) error {
			for _, offset := range offsets {
				committed.Store(offset + 1)
			}
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
		m.MarkDone(0, offset)
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
		commit: func(context.Context, uint64, map[int]int64) error {
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
	m.MarkDone(0, 10)

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

func TestForceFlush_CommitsDoneOffsets(t *testing.T) {
	var committed sync.Map

	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(_ context.Context, _ uint64, offsets map[int]int64) error {
			for partition, offset := range offsets {
				committed.Store(partition, offset)
			}
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

	m.MarkDone(0, 0)
	m.MarkDone(0, 1)
	m.MarkDone(1, 0)
	m.MarkDone(1, 1)

	_ = m.flush(context.Background(), true)

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
		commit: func(context.Context, uint64, map[int]int64) error {
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

	m.MarkDone(0, 0)
	m.MarkDone(0, 1)
	m.MarkDone(0, 2)
	_ = drainKick(context.Background(), m)

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
		commit: func(_ context.Context, _ uint64, offsets map[int]int64) error {
			for _, offset := range offsets {
				lastCommitted = offset
			}
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

	m.MarkDone(0, 0)
	m.MarkDone(0, 1)
	_ = drainKick(context.Background(), m)
	if lastCommitted != 0 {
		t.Errorf("expected no commit yet, got lastCommitted=%d", lastCommitted)
	}

	m.MarkDone(0, 2)
	_ = drainKick(context.Background(), m)
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
		commit: func(_ context.Context, _ uint64, offsets map[int]int64) error {
			for _, offset := range offsets {
				committed.Store(offset)
			}
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

	// Mirrors the consumer: one fetch loop tracks in order, handlers finish in any
	// order.
	var wg sync.WaitGroup
	for i := range int64(N) {
		m.Track(0, i)
		wg.Add(1)
		go func(offset int64) {
			defer wg.Done()
			m.MarkDone(0, offset)
		}(i)
	}
	wg.Wait()

	_ = m.flush(context.Background(), true)

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

func TestForceFlush_NoPendingCommit(t *testing.T) {
	commitCount := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(context.Context, uint64, map[int]int64) error {
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

	// Nothing marked done, so the watermark stays at lastCommitOffset.
	_ = m.flush(context.Background(), true)

	if commitCount != 0 {
		t.Errorf("expected no commit, got %d", commitCount)
	}
}

func TestMarkDone_UnknownOffsetIsNoop(t *testing.T) {
	commitCount := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(context.Context, uint64, map[int]int64) error {
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

	m.MarkDone(0, 999)

	if commitCount != 0 {
		t.Errorf("expected no commit for unknown offset, got %d", commitCount)
	}
}

// A failed commit must leave the watermark ahead of lastCommit, or the next flush
// would have nothing to retry.
func TestForceFlush_RetriesAfterCommitFailure(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(context.Context, uint64, map[int]int64) error {
			return fmt.Errorf("broker down")
		},
		logger:         slog.Default(),
		minCommitCount: 100, // keep MarkDone from kicking so the forced flush drives it
		maxInterval:    1 * time.Hour,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     1 * time.Millisecond,
	})

	assignSentinel(m, 1, 0)
	m.Track(0, 0)
	m.MarkDone(0, 0)

	_ = m.flush(context.Background(), true)

	if got, _ := lastCommitOf(m, 0); got != -1 {
		t.Fatalf("expected lastCommit unchanged at -1 after failure, got %d", got)
	}

	m.commitFunc = func(context.Context, uint64, map[int]int64) error { return nil }
	if err := m.flush(context.Background(), true); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got, _ := lastCommitOf(m, 0); got != 0 {
		t.Errorf("expected the retry to commit 0, got %d", got)
	}
}

func TestForceFlush_SuccessCleansInFlight(t *testing.T) {
	m := newOffsetManager(offsetManagerConfig{
		topic:          "test-topic",
		commit:         func(context.Context, uint64, map[int]int64) error { return nil },
		logger:         slog.Default(),
		minCommitCount: 100, // MarkDone does not kick; the forced flush drives it
		maxInterval:    1 * time.Hour,
		forceInterval:  5 * time.Second,
		maxRetries:     1,
		retryDelay:     1 * time.Millisecond,
	})

	assignSentinel(m, 1, 0)
	m.Track(0, 0)
	m.Track(0, 1)
	m.MarkDone(0, 0)
	m.MarkDone(0, 1)

	_ = m.flush(context.Background(), true)

	m.mu.RLock()
	s := m.partitions[0]
	m.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastCommitOffset != 1 {
		t.Errorf("expected lastCommit=1, got %d", s.lastCommitOffset)
	}
	if len(s.inFlight) != 0 {
		t.Errorf("expected inFlight cleared after successful commit, got %d entries", len(s.inFlight))
	}
}

func TestCommitWithRetries_ContextCanceledDuringRetryDelay(t *testing.T) {
	attempts := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(context.Context, uint64, map[int]int64) error {
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
	m.MarkDone(0, 0)
	_ = drainKick(context.Background(), m) // first commit attempt happens here

	// Advance the watermark again so the forced flush has something to retry.
	m.Track(0, 1)
	m.MarkDone(0, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = m.flush(ctx, true)

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
		commit: func(context.Context, uint64, map[int]int64) error {
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
		commit: func(context.Context, uint64, map[int]int64) error {
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

	m.MarkDone(0, 7)
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
		commit: func(context.Context, uint64, map[int]int64) error {
			attempts++
			return nil
		},
		logger: slog.Default(),
		// High enough that MarkDone does not kick, leaving the commit to the forced flush.
		minCommitCount: 1000,
		maxInterval:    time.Hour,
		maxRetries:     3,
		retryDelay:     time.Millisecond,
	})

	assignSentinel(m, 1, 0)
	m.Track(0, 0)
	m.MarkDone(0, 0)
	if attempts != 0 {
		t.Fatalf("MarkDone committed %d times below the threshold, want 0", attempts)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = m.flush(ctx, true)

	if attempts != 0 {
		t.Fatalf("commitFunc called %d times with an expired context, want 0", attempts)
	}
}

// A partition assigned but never seeded has no watermark to flush; committing from its
// -1 placeholder would rewind the group to the start of the log.
func TestForceFlush_SkipsUninitializedPartition(t *testing.T) {
	commits := 0
	m := newOffsetManager(offsetManagerConfig{
		topic: "test-topic",
		commit: func(context.Context, uint64, map[int]int64) error {
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
	_ = m.flush(context.Background(), true)

	if commits != 0 {
		t.Fatalf("committed %d times for an unseeded partition, want 0", commits)
	}
}

// Compacted topics and transaction markers leave offsets that are never delivered;
// the watermark must cross them rather than wait for watermark+1 forever.
func TestMarkDone_WatermarkCrossesOffsetGaps(t *testing.T) {
	m := newTestOffsetManager(t)
	m.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 0, Offset: 100}})

	// 100..104 were compacted away, and 107 was a transaction marker.
	for _, offset := range []int64{105, 106, 108} {
		m.Track(0, offset)
	}
	m.MarkDone(0, 108)
	m.MarkDone(0, 105)
	m.MarkDone(0, 106)

	if err := m.flush(context.Background(), true); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got, _ := lastCommitOf(m, 0); got != 108 {
		t.Errorf("expected lastCommit=108 across the gaps, got %d", got)
	}
}

// An offset tracked out of order would break the sorted invariant MarkDone's binary
// search depends on, so it is dropped and left for redelivery.
func TestTrack_IgnoresOutOfOrderOffset(t *testing.T) {
	m := newTestOffsetManager(t)
	assignSentinel(m, 1, 0)

	m.Track(0, 10)
	m.Track(0, 12)
	m.Track(0, 11)
	m.Track(0, 12)

	m.mu.RLock()
	s := m.partitions[0]
	m.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	want := []inFlightOffset{{offset: 10}, {offset: 12}}
	if !slices.Equal(s.inFlight, want) {
		t.Errorf("inFlight = %v, want %v", s.inFlight, want)
	}
}
