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

// consumerGroup is the part of *kafka.ConsumerGroup the consumer drives. The
// indirection is what lets tests exercise the lifecycle without a broker.
type consumerGroup interface {
	Next(ctx context.Context) (*kafka.Generation, error)
	Close() error
}

// partitionReader is the part of *kafka.Reader a fetch loop drives, isolated for the
// same reason as consumerGroup.
type partitionReader interface {
	SetOffset(offset int64) error
	FetchMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

type Consumer struct {
	config        Config
	logger        *slog.Logger
	cg            consumerGroup
	newReader     func(kafka.PartitionAssignment) partitionReader
	offsetManager *offsetManager
	backpressure  *backpressureController
	keySequencer  *keySequencer
	ctx           context.Context
	cancel        context.CancelFunc
	procCtx       context.Context
	procCancel    context.CancelFunc
	seqDone       chan struct{}
	seqDoneOnce   sync.Once
	wg            sync.WaitGroup
	genMu         sync.Mutex
	curGen        *kafka.Generation
	curEpoch      uint64
	running       bool
	runningMutex  sync.Mutex
}

func NewConsumer(config Config) (*Consumer, error) {
	config.applyDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	procCtx, procCancel := context.WithCancel(context.Background())

	c := &Consumer{
		config:     config,
		logger:     config.Logger,
		ctx:        ctx,
		cancel:     cancel,
		procCtx:    procCtx,
		procCancel: procCancel,
		seqDone:    make(chan struct{}),
	}
	c.newReader = c.newPartitionReader

	c.backpressure = newBackpressureController(config.MaxInFlight)

	if !config.UnOrdered {
		c.keySequencer = newKeySequencer(config.MaxQueuedPerKey, config.OverflowPolicy)
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

	commit := func(ctx context.Context, epoch uint64, partition int, offset int64) error {
		gen, ok := c.generationFor(epoch)
		if !ok {
			return ErrStaleGeneration
		}
		return gen.CommitOffsets(map[string]map[int]int64{
			c.config.Topic: {partition: nextOffsetToRead(offset)},
		})
	}

	c.offsetManager = newOffsetManager(offsetManagerConfig{
		topic:          config.Topic,
		commitFunc:     commit,
		logger:         config.Logger,
		minCommitCount: config.MinOffsetCommitCount,
		maxInterval:    config.MaxCommitInterval,
		forceInterval:  config.ForceCommitInterval,
		maxRetries:     config.MaxCommitRetries,
		retryDelay:     config.CommitRetryDelay,
	})

	return c, nil
}

func (c *Consumer) beginGeneration(gen *kafka.Generation) uint64 {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	c.curEpoch++
	c.curGen = gen
	return c.curEpoch
}

func (c *Consumer) generationFor(epoch uint64) (*kafka.Generation, bool) {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	if c.curGen == nil || c.curEpoch != epoch {
		return nil, false
	}
	return c.curGen, true
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
	go c.runGenerations()

	if c.keySequencer != nil {
		c.wg.Add(1)
		go c.consumeFromKeySequencer()
	}

	c.wg.Add(1)
	go c.forceCommitLoop()

	return nil
}

// runGenerations is the top-level consume loop; each iteration is one generation.
func (c *Consumer) runGenerations() {
	defer c.wg.Done()

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

		epoch := c.beginGeneration(gen)
		assignments := gen.Assignments[c.config.Topic]

		c.logger.Info("Joined consumer group generation",
			"generation", gen.ID, "member", gen.MemberID,
			"topic", c.config.Topic, "partitions", len(assignments))

		c.offsetManager.BeginEpoch(epoch, assignments)

		for _, pa := range assignments {
			// Tracked on c.wg because the fetch loop spawns processMessage goroutines. An
			// Add that races a Wait which already observed zero is both a data race and a
			// handler still running after Stop reported everything drained.
			c.wg.Add(1)
			gen.Start(func(ctx context.Context) { c.runFetcher(ctx, pa) })
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

// runFetcher consumes one assigned partition, then holds the generation open for the
// rest of its life.
func (c *Consumer) runFetcher(genCtx context.Context, pa kafka.PartitionAssignment) {
	func() {
		// c.wg must be released before parking below, since Stop waits on it and the park
		// only ends once Stop closes the group.
		defer c.wg.Done()
		c.fetchPartition(genCtx, pa)
	}()

	// Park rather than return: Generation.Start ends the generation as soon as any
	// started function exits, so returning on Stop() would trigger the revocation
	// hook's final commit before in-flight handlers had drained. gen.close closes done
	// before waiting for routines to join, so this unblocks rather than deadlocking.
	<-genCtx.Done()
}

func (c *Consumer) newPartitionReader(pa kafka.PartitionAssignment) partitionReader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   c.config.Brokers,
		Topic:     c.config.Topic,
		Partition: pa.ID,
		MinBytes:  c.config.MinBytes,
		MaxBytes:  c.config.MaxBytes,
		MaxWait:   c.config.MaxWait,
		// Per-partition readers each get their own queue, so the default would multiply
		// prefetch buffering by the assignment count. MaxInFlight is the real bound.
		QueueCapacity: 1,
	})
}

// fetchPartition pumps one partition until genCtx ends or Stop halts fetching.
func (c *Consumer) fetchPartition(genCtx context.Context, pa kafka.PartitionAssignment) {
	r := c.newReader(pa)
	defer func() {
		if err := r.Close(); err != nil {
			c.logger.Error("Failed to close partition reader", "err", err,
				"topic", c.config.Topic, "partition", pa.ID)
		}
	}()

	// SetOffset accepts both absolute offsets and the FirstOffset/LastOffset sentinels.
	if err := r.SetOffset(pa.Offset); err != nil {
		c.logger.Error("Failed to set partition start offset", "err", err,
			"topic", c.config.Topic, "partition", pa.ID, "offset", pa.Offset)
		return
	}

	// Stop() must halt fetches without ending the generation — see runFetcher's park.
	fetchCtx, cancelFetch := context.WithCancel(genCtx)
	defer cancelFetch()
	defer context.AfterFunc(c.ctx, cancelFetch)()

	c.fetchLoop(fetchCtx, r, pa.ID)
}

// fetchLoop pumps messages for a single partition until fetchCtx is canceled,
// either by the generation ending or by Stop().
func (c *Consumer) fetchLoop(fetchCtx context.Context, r partitionReader, partition int) {
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
				"topic", c.config.Topic, "partition", partition)
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
			acquired, evicted, err := c.keySequencer.submit(fetchCtx, msg, key)
			if err != nil {
				c.backpressure.Release()
				return
			}
			if evicted != nil {
				c.handleOverflowDrop(*evicted, key)
			}
			if !acquired {
				c.backpressure.Release()
				continue
			}
		}

		c.wg.Add(1)
		go c.processMessage(msg, key)
	}
}

// handleOverflowDrop accounts for a message the key sequencer discarded.
func (c *Consumer) handleOverflowDrop(msg kafka.Message, key string) {
	c.logger.Warn("Key queue at capacity: dropping message",
		"policy", c.config.OverflowPolicy, "key", key,
		"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)

	if c.config.DeadLetterPersister != nil {
		if err := c.config.DeadLetterPersister.Save(context.Background(), msg, ErrKeyQueueOverflow, key); err != nil {
			c.logger.Error("Failed to persist dropped message", "err", err,
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
		}
	}

	if err := c.offsetManager.MarkDone(context.Background(), msg.Partition, msg.Offset); err != nil {
		c.logger.Error("Failed to mark dropped offset done", "err", err,
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
	}
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

func (c *Consumer) consumeFromKeySequencer() {
	defer c.wg.Done()

	for {
		select {
		case <-c.procCtx.Done():
			return
		case <-c.seqDone:
			return
		case <-c.keySequencer.readyChan():
			for {
				msg, key, ok := c.keySequencer.dequeue()
				if !ok {
					break
				}
				if err := c.backpressure.Acquire(c.procCtx); err != nil {
					return
				}
				c.wg.Add(1)
				go c.processMessage(msg, key)
			}
		}
	}
}

func (c *Consumer) processMessage(msg kafka.Message, key string) {
	defer c.wg.Done()

	var handlerErr error
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Panic in processMessage", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "panic", r)
		}

		if err := c.offsetManager.MarkDone(context.Background(), msg.Partition, msg.Offset); err != nil {
			c.logger.Error("Failed to mark offset done", "err", err)
		}

		if c.keySequencer != nil {
			c.keySequencer.release(key)
		}

		c.backpressure.Release()

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
				c.logger.Warn("Context canceled during retry", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
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

func (c *Consumer) forceCommitLoop() {
	defer c.wg.Done()

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
		c.seqDoneOnce.Do(func() { close(c.seqDone) })
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
