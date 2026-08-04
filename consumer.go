package turnstile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type consumerGroup interface {
	Next(ctx context.Context) (*kafka.Generation, error)
	Close() error
}

type partitionReader interface {
	SetOffset(offset int64) error
	FetchMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

type Consumer struct {
	config        Config
	logger        *slog.Logger
	cg            consumerGroup
	newReader     func(context.Context, kafka.PartitionAssignment) partitionReader
	offsetManager *offsetManager
	backpressure  *backpressureController
	keySequencer  *keySequencer
	ctx           context.Context
	cancel        context.CancelFunc
	procCtx       context.Context
	procCancel    context.CancelFunc
	seqCtx        context.Context
	seqCancel     context.CancelFunc
	wg            sync.WaitGroup
	genMu         sync.Mutex
	curGen        *kafka.Generation
	curEpoch      uint64
	runningMutex  sync.Mutex
	running       bool
}

func NewConsumer(config Config) (*Consumer, error) {
	config.applyDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	procCtx, procCancel := context.WithCancel(context.Background())
	seqCtx, seqCancel := context.WithCancel(procCtx)

	c := &Consumer{
		config:     config,
		logger:     config.Logger,
		ctx:        ctx,
		cancel:     cancel,
		procCtx:    procCtx,
		procCancel: procCancel,
		seqCtx:     seqCtx,
		seqCancel:  seqCancel,
	}

	c.backpressure = newBackpressureController(config.MaxInFlight)

	if !config.UnOrdered {
		c.keySequencer = newKeySequencer()
	}

	cg, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      config.GroupID,
		Brokers: config.Brokers,
		Topics:  []string{config.Topic},
		GroupBalancers: []kafka.GroupBalancer{
			kafka.RangeGroupBalancer{},
		},
		HeartbeatInterval:      config.HeartbeatInterval,
		SessionTimeout:         config.SessionTimeout,
		RebalanceTimeout:       config.RebalanceTimeout,
		PartitionWatchInterval: config.PartitionWatchInterval,
		WatchPartitionChanges:  config.WatchPartitionChanges,
		// Must be explicit: ConsumerGroupConfig defaults to FirstOffset, turnstile to
		// LastOffset, so omitting this flips new groups to "earliest".
		StartOffset: config.AutoOffsetReset,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}
	c.cg = cg

	c.newReader = func(fetchCtx context.Context, pa kafka.PartitionAssignment) partitionReader {
		return kafka.NewReader(kafka.ReaderConfig{
			Brokers:   config.Brokers,
			Topic:     config.Topic,
			Partition: pa.ID,
			MinBytes:  config.MinBytes,
			MaxBytes:  config.MaxBytes,
			MaxWait:   config.MaxWait,
			// Per-partition readers each get their own queue, so the default would multiply
			// prefetch buffering by the assignment count.
			QueueCapacity:   1,
			ErrorLogger:     readerErrorLogger(fetchCtx, config.Logger, config.Topic, pa.ID),
			ReadLagInterval: -1,
		})
	}

	commit := func(ctx context.Context, epoch uint64, partition int, offset int64) error {
		c.genMu.Lock()
		gen, curEpoch := c.curGen, c.curEpoch
		c.genMu.Unlock()
		if gen == nil || curEpoch != epoch {
			return ErrStaleGeneration
		}
		// Kafka stores the next offset to read; the watermark stores the last message
		// processed, so committing means stepping forward one.
		return gen.CommitOffsets(map[string]map[int]int64{
			c.config.Topic: {partition: offset + 1},
		})
	}
	c.offsetManager = newOffsetManager(offsetManagerConfig{
		topic:          config.Topic,
		commit:         commit,
		logger:         config.Logger,
		minCommitCount: config.MinOffsetCommitCount,
		maxInterval:    config.MaxCommitInterval,
		forceInterval:  config.ForceCommitInterval,
		maxRetries:     config.MaxCommitRetries,
		retryDelay:     config.CommitRetryDelay,
	})

	return c, nil
}

func (c *Consumer) Start() error {
	c.runningMutex.Lock()
	if c.running {
		c.runningMutex.Unlock()
		return fmt.Errorf("consumer already running")
	}
	c.running = true
	c.runningMutex.Unlock()

	c.logger.Info("Starting Kafka consumer", "topic", c.config.Topic)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runGenerations()
	}()

	if c.keySequencer != nil {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.consumeFromKeySequencer()
		}()
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.forceCommitLoop()
	}()

	return nil
}

func (c *Consumer) runGenerations() {
	for {
		gen, err := c.cg.Next(c.ctx)
		if err != nil {
			if errors.Is(err, kafka.ErrGroupClosed) || c.ctx.Err() != nil {
				return
			}
			// cg.run applies JoinGroupBackoff internally, so this cannot spin.
			c.logger.Error("Failed to join consumer group", "err", err)
			continue
		}

		c.genMu.Lock()
		c.curEpoch++
		c.curGen = gen
		epoch := c.curEpoch
		c.genMu.Unlock()

		assignments := gen.Assignments[c.config.Topic]

		c.logger.Info("Joined consumer group generation",
			"generation", gen.ID, "member", gen.MemberID,
			"topic", c.config.Topic, "partitions", len(assignments))

		c.offsetManager.BeginEpoch(epoch, assignments)

		for _, pa := range assignments {
			c.wg.Add(1)
			gen.Start(func(ctx context.Context) {
				func() {
					defer c.wg.Done()
					c.fetchPartition(ctx, pa)
				}()

				<-ctx.Done()
			})
		}

		// Revocation hook
		gen.Start(func(ctx context.Context) {
			<-ctx.Done()
			c.logger.Info("Consumer group generation ended, flushing offsets",
				"generation", gen.ID, "topic", c.config.Topic)
			c.offsetManager.EndEpoch(epoch)
		})
	}
}

func (c *Consumer) fetchPartition(ctx context.Context, pa kafka.PartitionAssignment) {
	fetchCtx, cancelFetch := context.WithCancel(ctx)
	stopShutdownHook := context.AfterFunc(c.ctx, cancelFetch)

	r := c.newReader(fetchCtx, pa)
	// One defer to keep the teardown ordered: the reader's own cancellation errors must
	// land after fetchCtx is done, or they log at error level.
	defer func() {
		stopShutdownHook()
		cancelFetch()
		if err := r.Close(); err != nil {
			c.logger.Error("Failed to close partition reader", "err", err,
				"topic", c.config.Topic, "partition", pa.ID)
		}
	}()

	if err := r.SetOffset(pa.Offset); err != nil {
		c.logger.Error("Failed to set partition start offset", "err", err,
			"topic", c.config.Topic, "partition", pa.ID, "offset", pa.Offset)
		return
	}

	var fetchBackoff time.Duration
	const maxFetchBackoff = 5 * time.Second

	for fetchCtx.Err() == nil {
		if err := c.backpressure.Acquire(fetchCtx); err != nil {
			return
		}

		msg, err := r.FetchMessage(fetchCtx)
		if err != nil {
			c.backpressure.Release()
			if fetchCtx.Err() != nil {
				return
			}
			c.logger.Error("Failed to fetch message", "err", err,
				"topic", c.config.Topic, "partition", pa.ID)
			fetchBackoff = backoffWithJitter(fetchBackoff, maxFetchBackoff)
			select {
			case <-fetchCtx.Done():
				return
			case <-time.After(fetchBackoff):
			}
			continue
		}
		fetchBackoff = 0

		c.offsetManager.Track(msg.Partition, msg.Offset)

		key := c.config.Handler.GetKey(msg.Key, msg.Value)

		if c.keySequencer != nil {
			if acquired := c.keySequencer.submit(msg, key); !acquired {
				c.backpressure.Release()
				continue
			}
		}

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.processMessage(msg, key)
		}()
	}
}

func (c *Consumer) processMessage(msg kafka.Message, key string) {
	var handlerErr error
	abandoned := false

	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Panic in processMessage", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "panic", r)
		}

		if c.keySequencer != nil {
			c.keySequencer.release(key)
		}

		c.backpressure.Release()

		if abandoned {
			return
		}

		if err := c.offsetManager.MarkDone(context.Background(), msg.Partition, msg.Offset); err != nil {
			c.logger.Error("Failed to mark offset done", "err", err)
		}

		if handlerErr != nil {
			if c.config.DeadLetterPersister != nil {
				if persistErr := c.config.DeadLetterPersister.Save(context.Background(), msg, handlerErr, key); persistErr != nil {
					c.logger.Error("Failed to persist dead-letter message", "err", persistErr)
				}
			}
		}
	}()

	for attempt := 0; attempt <= c.config.RetryCount; attempt++ {
		if attempt > 0 {
			select {
			case <-c.ctx.Done():
				c.logger.Warn("Context canceled during retry, leaving offset uncommitted for redelivery", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
				abandoned = true
				return
			case <-time.After(c.config.RetryDelay):
			}
		}

		handlerErr = c.config.Handler.HandleMessage(c.procCtx, msg)
		if handlerErr == nil {
			break
		}
		c.logger.Error("Failed to process message", "attempt", attempt+1, "maxAttempts", c.config.RetryCount+1, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "err", handlerErr)
	}
}

func (c *Consumer) consumeFromKeySequencer() {
	for {
		msg, key, ok := c.keySequencer.dequeue(c.seqCtx)
		if !ok {
			return
		}
		if err := c.backpressure.Acquire(c.procCtx); err != nil {
			return
		}
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.processMessage(msg, key)
		}()
	}
}

func (c *Consumer) forceCommitLoop() {
	ticker := time.NewTicker(c.config.ForceCommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.offsetManager.ForceCommit(c.ctx)
		}
	}
}

func (c *Consumer) Stop() error {
	c.runningMutex.Lock()
	if !c.running {
		c.runningMutex.Unlock()
		return nil
	}
	c.running = false
	c.runningMutex.Unlock()

	c.logger.Info("Stopping Kafka consumer...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), c.config.ShutdownTimeout)
	defer shutdownCancel()

	c.cancel()

	if c.keySequencer != nil {
		if err := c.keySequencer.drain(shutdownCtx); err != nil {
			c.logger.Warn("Key sequencer drain timed out", "err", err)
		}
		c.seqCancel()
	}

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("All consumer goroutines stopped")
	case <-shutdownCtx.Done():
		c.logger.Warn("Shutdown timeout reached, force canceling in-flight message processing")
		c.procCancel()
		<-done
		c.logger.Info("All consumer goroutines stopped after forced cancel")
	}

	c.procCancel()

	// Closing the group ends the generation, which runs the revocation hook's final
	// commit. It must come after the drain above so in-flight handlers' MarkDone
	// commits land under the still-live generation and the flush picks them up.
	var closeErr error
	if err := c.cg.Close(); err != nil {
		c.logger.Error("Failed to close consumer group", "err", err)
		closeErr = err
	}

	c.logger.Info("Kafka consumer stopped")

	return closeErr
}

func readerErrorLogger(fetchCtx context.Context, logger *slog.Logger, topic string, partition int) kafka.Logger {
	return kafka.LoggerFunc(func(format string, args ...any) {
		level := slog.LevelError
		if fetchCtx.Err() != nil {
			level = slog.LevelDebug
		}
		logger.Log(context.Background(), level, "Kafka reader error",
			"topic", topic, "partition", partition, "err", fmt.Sprintf(format, args...))
	})
}

func backoffWithJitter(current, max time.Duration) time.Duration {
	if current == 0 {
		current = 250 * time.Millisecond
	} else {
		current *= 2
	}
	if current > max {
		current = max
	}
	jitter := time.Duration(rand.Int64N(int64(current) / 2))
	return current + jitter
}
