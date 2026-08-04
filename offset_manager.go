package turnstile

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type commitFn func(ctx context.Context, epoch uint64, partition int, offset int64) error

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
	maxRetries     int
	retryDelay     time.Duration
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
		maxRetries:     config.maxRetries,
		retryDelay:     config.retryDelay,
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

	for partition, s := range partitions {
		s.mu.Lock()
		m.flushLocked(ctx, epoch, partition, s)
		s.mu.Unlock()
	}
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

func (m *offsetManager) MarkDone(ctx context.Context, partition int, offset int64) error {
	m.mu.RLock()
	s, ok := m.partitions[partition]
	epoch := m.epoch
	m.mu.RUnlock()
	if !ok {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return nil
	}
	if _, ok := s.inFlight[offset]; !ok {
		return nil
	}
	s.inFlight[offset] = true

	s.advanceLocked()

	if s.watermarkOffset <= s.lastCommitOffset {
		return nil
	}
	if s.watermarkOffset-s.lastCommitOffset < m.minCommitCount && time.Since(s.lastCommitTime) < m.maxInterval {
		return nil
	}

	return m.commitLocked(ctx, epoch, partition, s)
}

// advanceLocked walks the watermark over the contiguous run of done offsets. Requires
// s.mu.
func (s *partitionState) advanceLocked() {
	for {
		done, ok := s.inFlight[s.watermarkOffset+1]
		if !ok || !done {
			return
		}
		s.watermarkOffset++
	}
}

// commitLocked requires s.mu.
func (m *offsetManager) commitLocked(ctx context.Context, epoch uint64, partition int, s *partitionState) error {
	var commitErr error
	for retry := 0; retry < m.maxRetries; retry++ {
		// commitFunc's network call is not context-aware, so an expired budget must be
		// caught here or each partition still pays a full socket timeout.
		if err := ctx.Err(); err != nil {
			m.logger.Error("Aborting offset commit", "offset", s.watermarkOffset, "topic", m.topic, "partition", partition, "err", err)
			return err
		}

		commitErr = m.commitFunc(ctx, epoch, partition, s.watermarkOffset)
		if commitErr == nil {
			s.lastCommitOffset = s.watermarkOffset
			s.lastCommitTime = time.Now()
			m.logger.Info("Committed offset", "offset", s.lastCommitOffset, "topic", m.topic, "partition", partition)

			for o := range s.inFlight {
				if o <= s.lastCommitOffset {
					delete(s.inFlight, o)
				}
			}
			return nil
		}

		staleGeneration := errors.Is(commitErr, ErrStaleGeneration) ||
			errors.Is(commitErr, kafka.ErrGenerationEnded) ||
			errors.Is(commitErr, kafka.ErrGroupClosed)
		if staleGeneration {
			m.logger.Warn("Abandoning offset commit: generation has ended",
				"offset", s.watermarkOffset, "topic", m.topic, "partition", partition, "err", commitErr)
			return commitErr
		}

		m.logger.Error("Failed to commit offset", "offset", s.watermarkOffset, "topic", m.topic, "partition", partition, "err", commitErr, "retry", retry+1, "maxRetries", m.maxRetries)
		select {
		case <-ctx.Done():
			m.logger.Error("Aborting offset commit", "offset", s.watermarkOffset, "topic", m.topic, "partition", partition, "err", ctx.Err())
			return ctx.Err()
		case <-time.After(m.retryDelay):
		}
	}

	return commitErr
}

// ForceCommit commits every partition's contiguous watermark, ignoring the
// minCommitCount and maxInterval thresholds.
func (m *offsetManager) ForceCommit(ctx context.Context) {
	m.mu.RLock()
	epoch := m.epoch
	partitions := make(map[int]*partitionState, len(m.partitions))
	maps.Copy(partitions, m.partitions)
	m.mu.RUnlock()

	for partition, s := range partitions {
		s.mu.Lock()
		m.flushLocked(ctx, epoch, partition, s)
		s.mu.Unlock()
	}
}

// flushLocked requires s.mu.
func (m *offsetManager) flushLocked(ctx context.Context, epoch uint64, partition int, s *partitionState) {
	if !s.initialized {
		return
	}

	s.advanceLocked()

	if s.watermarkOffset <= s.lastCommitOffset {
		return
	}

	_ = m.commitLocked(ctx, epoch, partition, s)
}
