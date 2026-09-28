package turnstile

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type commitFn func(ctx context.Context, epoch uint64, offsets map[int]int64) error

type partitionState struct {
	mu               sync.Mutex
	initialized      bool
	lastCommitOffset int64
	watermarkOffset  int64
	lastCommitTime   time.Time
	inFlight         map[int64]bool
}

type offsetManagerConfig struct {
	topic          string
	commit         commitFn
	logger         *slog.Logger
	minCommitCount int64
	maxInterval    time.Duration
	forceInterval  time.Duration
	maxRetries     int
	retryDelay     time.Duration
}

type offsetManager struct {
	topic          string
	commitFunc     commitFn
	logger         *slog.Logger
	minCommitCount int64
	maxInterval    time.Duration
	forceInterval  time.Duration
	maxRetries     int
	retryDelay     time.Duration
	kick           chan struct{}
	commitMu       sync.Mutex
	mu             sync.RWMutex
	epoch          uint64
	partitions     map[int]*partitionState
}

func newOffsetManager(config offsetManagerConfig) *offsetManager {
	return &offsetManager{
		topic:          config.topic,
		commitFunc:     config.commit,
		logger:         config.logger,
		minCommitCount: config.minCommitCount,
		maxInterval:    config.maxInterval,
		forceInterval:  config.forceInterval,
		maxRetries:     config.maxRetries,
		retryDelay:     config.retryDelay,
		kick:           make(chan struct{}, 1),
		partitions:     make(map[int]*partitionState),
	}
}

// BeginEpoch installs the partition set for a new consumer group generation.
func (m *offsetManager) BeginEpoch(epoch uint64, assignments []kafka.PartitionAssignment) {
	partitions := make(map[int]*partitionState, len(assignments))
	now := time.Now()

	for _, pa := range assignments {
		s := &partitionState{
			lastCommitOffset: -1,
			watermarkOffset:  -1,
			inFlight:         make(map[int64]bool),
		}

		// A negative pa.Offset is a FirstOffset/LastOffset sentinel meaning the group has no
		// committed offset, so the partition is left for Track to seed.
		if pa.Offset >= 0 {
			// Kafka stores the next offset to read; the watermark stores the last message
			// processed, so seeding it means stepping back one.
			s.lastCommitOffset = pa.Offset - 1
			s.watermarkOffset = s.lastCommitOffset
			s.lastCommitTime = now
			s.initialized = true
			m.logger.Info("Assigned partition with committed offset",
				"topic", m.topic, "partition", pa.ID, "committed_offset", pa.Offset, "epoch", epoch)
		} else {
			m.logger.Info("Assigned partition with no committed offset, seeding from first message",
				"topic", m.topic, "partition", pa.ID, "start_offset", pa.Offset, "epoch", epoch)
		}

		partitions[pa.ID] = s
	}

	m.mu.Lock()
	m.epoch = epoch
	m.partitions = partitions
	m.mu.Unlock()
}

// EndEpoch flushes every owned partition one last time, then drops all state.
//
// Called from the revoke hook once the generation's done channel has closed.
// Committing after that point is sanctioned: Generation.CommitOffsets writes
// straight to the coordinator connection without consulting done, and the commit
// carries the generation ID, so a broker that has moved on rejects it.
//
// Best-effort by design — in-flight handlers are not waited on, and their work is
// reprocessed by the partition's new owner.
func (m *offsetManager) EndEpoch(epoch uint64) {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()

	m.mu.Lock()
	if m.epoch != epoch {
		m.mu.Unlock()
		return
	}
	partitions := m.partitions
	m.partitions = make(map[int]*partitionState)
	m.mu.Unlock()

	if len(partitions) == 0 {
		return
	}

	// The deadline reuses the retry budget; the floor keeps a zero-retry configuration
	// from producing a zero deadline.
	budget := max(time.Duration(m.maxRetries)*m.retryDelay, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	_ = m.commitLocked(ctx, epoch, partitions, true)
}

// Track records that a message is in flight, seeding the watermark on first sight
// when the assignment carried no committed offset.
//
// A partition absent from the map was never assigned or has been revoked; tracking
// it would resurrect the state BeginEpoch just pruned. Dropping it loses nothing,
// since an untracked offset is never committed and so is redelivered.
func (m *offsetManager) Track(partition int, offset int64) {
	m.mu.RLock()
	s, ok := m.partitions[partition]
	m.mu.RUnlock()
	if !ok {
		m.logger.Debug("Ignoring message for unassigned partition",
			"topic", m.topic, "partition", partition, "offset", offset)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		m.logger.Info("Seeding partition from first message",
			"topic", m.topic, "partition", partition, "first_offset", offset)
		s.lastCommitOffset = offset - 1
		s.watermarkOffset = offset - 1
		s.lastCommitTime = time.Now()
		s.initialized = true
	}

	s.inFlight[offset] = false
}

func (m *offsetManager) MarkDone(partition int, offset int64) {
	m.mu.RLock()
	s, ok := m.partitions[partition]
	m.mu.RUnlock()
	if !ok {
		return
	}

	s.mu.Lock()
	if !s.initialized {
		s.mu.Unlock()
		return
	}
	if _, ok := s.inFlight[offset]; !ok {
		s.mu.Unlock()
		return
	}
	s.inFlight[offset] = true
	s.advanceLocked()
	due := s.dueLocked(m.minCommitCount, m.maxInterval, false)
	s.mu.Unlock()

	if due {
		select {
		case m.kick <- struct{}{}:
		default:
		}
	}
}

func (s *partitionState) dueLocked(minCount int64, maxInterval time.Duration, force bool) bool {
	if !s.initialized {
		return false
	}
	pending := s.watermarkOffset - s.lastCommitOffset
	if pending <= 0 {
		return false
	}
	return force || pending >= minCount || time.Since(s.lastCommitTime) >= maxInterval
}

func (s *partitionState) advanceLocked() {
	for {
		done, ok := s.inFlight[s.watermarkOffset+1]
		if !ok || !done {
			return
		}
		s.watermarkOffset++
	}
}

func (m *offsetManager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.forceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.kick:
			_ = m.flush(ctx, false)
		case <-ticker.C:
			_ = m.flush(ctx, true)
		}
	}
}

func (m *offsetManager) flush(ctx context.Context, force bool) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()

	m.mu.RLock()
	epoch, partitions := m.epoch, m.partitions
	m.mu.RUnlock()

	return m.commitLocked(ctx, epoch, partitions, force)
}

func (m *offsetManager) commitLocked(ctx context.Context, epoch uint64, partitions map[int]*partitionState, force bool) error {
	offsets := make(map[int]int64)
	for partition, s := range partitions {
		s.mu.Lock()
		if s.dueLocked(m.minCommitCount, m.maxInterval, force) {
			offsets[partition] = s.watermarkOffset
		}
		s.mu.Unlock()
	}
	if len(offsets) == 0 {
		return nil
	}

	if err := m.commitWithRetry(ctx, epoch, offsets); err != nil {
		return err
	}

	now := time.Now()
	for partition, offset := range offsets {
		s := partitions[partition]
		s.mu.Lock()
		s.lastCommitOffset = offset
		s.lastCommitTime = now
		for o := range s.inFlight {
			if o <= offset {
				delete(s.inFlight, o)
			}
		}
		s.mu.Unlock()
	}
	m.logger.Info("Committed offsets", "topic", m.topic, "offsets", offsets)
	return nil
}

func (m *offsetManager) commitWithRetry(ctx context.Context, epoch uint64, offsets map[int]int64) error {
	var commitErr error
	for retry := 0; retry < m.maxRetries; retry++ {
		// commitFunc's network call is not context-aware, so an expired budget must be
		// caught here or each attempt still pays a full socket timeout.
		if err := ctx.Err(); err != nil {
			m.logger.Error("Aborting offset commit", "topic", m.topic, "offsets", offsets, "err", err)
			return err
		}

		commitErr = m.commitFunc(ctx, epoch, offsets)
		if commitErr == nil {
			return nil
		}

		staleGeneration := errors.Is(commitErr, ErrStaleGeneration) ||
			errors.Is(commitErr, kafka.ErrGenerationEnded) ||
			errors.Is(commitErr, kafka.ErrGroupClosed)
		if staleGeneration {
			m.logger.Warn("Abandoning offset commit: generation has ended",
				"topic", m.topic, "offsets", offsets, "err", commitErr)
			return commitErr
		}

		m.logger.Error("Failed to commit offsets", "topic", m.topic, "offsets", offsets, "err", commitErr, "retry", retry+1, "maxRetries", m.maxRetries)
		select {
		case <-ctx.Done():
			m.logger.Error("Aborting offset commit", "topic", m.topic, "offsets", offsets, "err", ctx.Err())
			return ctx.Err()
		case <-time.After(m.retryDelay):
		}
	}

	return commitErr
}
