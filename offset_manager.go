package turnstile

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type commitFn func(ctx context.Context, message kafka.Message) error

// hasCommitted is false when no offset has ever been committed, which is
// distinct from a committed offset of 0 and triggers seed-from-first-message.
type fetchOffsetFn func(ctx context.Context, partition int) (offset int64, hasCommitted bool, err error)

type partitionState struct {
	mu             sync.Mutex
	initialized    bool
	lastCommit     int64
	canCommit      int64
	lastCommitTime time.Time
	inFlight       map[int64]bool
}

type offsetManager struct {
	topic           string
	commitFunc      commitFn
	fetchOffsetFunc fetchOffsetFn
	logger          *slog.Logger
	minCommitCount  int64
	maxInterval     time.Duration
	maxRetries      int
	retryDelay      time.Duration
	mu              sync.RWMutex
	partitions      map[int]*partitionState
}

type offsetManagerConfig struct {
	topic           string
	commitFunc      commitFn
	fetchOffsetFunc fetchOffsetFn
	logger          *slog.Logger
	minCommitCount  int64
	maxInterval     time.Duration
	forceInterval   time.Duration
	maxRetries      int
	retryDelay      time.Duration
}

func newOffsetManager(config offsetManagerConfig) *offsetManager {
	return &offsetManager{
		topic:           config.topic,
		commitFunc:      config.commitFunc,
		fetchOffsetFunc: config.fetchOffsetFunc,
		logger:          config.logger,
		minCommitCount:  config.minCommitCount,
		maxInterval:     config.maxInterval,
		maxRetries:      config.maxRetries,
		retryDelay:      config.retryDelay,
		partitions:      make(map[int]*partitionState),
	}
}

func (m *offsetManager) Track(partition int, offset int64) {
	m.mu.RLock()
	s, ok := m.partitions[partition]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		if s, ok = m.partitions[partition]; !ok {
			s = &partitionState{
				lastCommit: -1,
				canCommit:  -1,
				inFlight:   make(map[int64]bool),
			}
			m.partitions[partition] = s
		}
		m.mu.Unlock()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		m.logger.Info("New partition detected", "partition", partition)

		committed := int64(-1)
		hasCommitted := false
		if m.fetchOffsetFunc != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			fetched, has, err := m.fetchOffsetFunc(ctx, partition)
			cancel()
			if err != nil {
				m.logger.Warn("Failed to fetch committed offset, using default", "partition", partition, "err", err)
			} else if has {
				committed = fetched
				hasCommitted = true
			}
		}

		if hasCommitted {
			m.logger.Info("Initializing partition with committed offset", "partition", partition, "committed_offset", committed)
			// committed is the next offset to read (Kafka convention), so the last
			// message actually processed is committed-1; watermarks track message
			// offsets, not the resume point.
			s.lastCommit = committed - 1
			s.canCommit = committed - 1
		} else {
			m.logger.Info("Initializing partition with no committed offset, seeding from first message", "partition", partition, "first_offset", offset)
			s.lastCommit = offset - 1
			s.canCommit = offset - 1
		}
		s.lastCommitTime = time.Now()
		s.initialized = true
	}

	s.inFlight[offset] = false
}

func (m *offsetManager) MarkDone(ctx context.Context, partition int, offset int64) error {
	m.mu.RLock()
	s, ok := m.partitions[partition]
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

	for {
		done, ok := s.inFlight[s.canCommit+1]
		if !ok || !done {
			break
		}
		s.canCommit++
	}

	if s.canCommit <= s.lastCommit {
		return nil
	}
	if s.canCommit-s.lastCommit < m.minCommitCount && time.Since(s.lastCommitTime) < m.maxInterval {
		return nil
	}

	kafkaMsg := kafka.Message{
		Topic:     m.topic,
		Partition: partition,
		Offset:    s.canCommit,
	}
	var commitErr error
	for retry := 0; retry < m.maxRetries; retry++ {
		commitErr = m.commitFunc(context.Background(), kafkaMsg)
		if commitErr == nil {
			s.lastCommit = s.canCommit
			s.lastCommitTime = time.Now()
			m.logger.Info("Committed offset", "offset", s.canCommit, "topic", m.topic, "partition", partition)
			break
		}

		m.logger.Error("Failed to commit offset", "offset", s.canCommit, "topic", m.topic, "partition", partition, "err", commitErr, "retry", retry+1, "maxRetries", m.maxRetries)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.retryDelay):
		}
	}
	if commitErr != nil {
		return commitErr
	}

	for o := range s.inFlight {
		if o <= s.lastCommit {
			delete(s.inFlight, o)
		}
	}

	return nil
}

func (m *offsetManager) ForceCommit(ctx context.Context) {
	m.mu.RLock()
	states := make([]struct {
		partition int
		state     *partitionState
	}, 0, len(m.partitions))
	for p, s := range m.partitions {
		states = append(states, struct {
			partition int
			state     *partitionState
		}{p, s})
	}
	m.mu.RUnlock()

	for _, ps := range states {
		s := ps.state
		s.mu.Lock()
		if !s.initialized {
			s.mu.Unlock()
			continue
		}

		for {
			done, ok := s.inFlight[s.canCommit+1]
			if !ok || !done {
				break
			}
			s.canCommit++
		}

		if s.canCommit > s.lastCommit {
			kafkaMsg := kafka.Message{
				Topic:     m.topic,
				Partition: ps.partition,
				Offset:    s.canCommit,
			}
			var commitErr error
			committed := false
		retryLoop:
			for retry := 0; retry < m.maxRetries; retry++ {
				commitErr = m.commitFunc(context.Background(), kafkaMsg)
				if commitErr == nil {
					s.lastCommit = s.canCommit
					s.lastCommitTime = time.Now()
					m.logger.Info("Committed offset", "offset", s.canCommit, "topic", m.topic, "partition", ps.partition)
					committed = true
					break
				}

				m.logger.Error("Failed to commit offset", "offset", s.canCommit, "topic", m.topic, "partition", ps.partition, "err", commitErr, "retry", retry+1, "maxRetries", m.maxRetries)
				select {
				case <-ctx.Done():
					m.logger.Error("Aborting offset commit", "offset", s.canCommit, "topic", m.topic, "partition", ps.partition, "err", ctx.Err())
					break retryLoop
				case <-time.After(m.retryDelay):
				}
			}
			if committed {
				for o := range s.inFlight {
					if o <= s.lastCommit {
						delete(s.inFlight, o)
					}
				}
			}
		}

		s.mu.Unlock()
	}
}
