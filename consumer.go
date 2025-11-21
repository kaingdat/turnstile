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

type Consumer struct {
	config        Config
	logger        *slog.Logger
	reader        *kafka.Reader
	kafkaClient   *kafka.Client
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

	c.backpressure = newBackpressureController(config.MaxInFlight)

	if !config.UnOrdered {
		c.keySequencer = newKeySequencer(config.MaxQueuedPerKey, config.OverflowPolicy)
	}

	c.kafkaClient = &kafka.Client{
		Addr: kafka.TCP(config.Brokers...),
		Transport: &kafka.Transport{
			ClientID: config.GroupID,
		},
	}

	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     config.Brokers,
		Topic:       config.Topic,
		GroupID:     config.GroupID,
		MinBytes:    config.MinBytes,
		MaxBytes:    config.MaxBytes,
		MaxWait:     config.MaxWait,
		StartOffset: config.AutoOffsetReset,
		GroupBalancers: []kafka.GroupBalancer{
			kafka.RangeGroupBalancer{},
		},
	})

	commit := func(ctx context.Context, message kafka.Message) error {
		return c.reader.CommitMessages(ctx, message)
	}

	fetchOffset := func(ctx context.Context, partition int) (int64, bool, error) {
		request := &kafka.OffsetFetchRequest{
			GroupID: config.GroupID,
			Topics: map[string][]int{
				config.Topic: {partition},
			},
		}

		response, err := c.kafkaClient.OffsetFetch(ctx, request)
		if err != nil {
			return 0, false, fmt.Errorf("failed to fetch offset: %w", err)
		}

		if topicOffsets, ok := response.Topics[config.Topic]; ok {
			for _, partitionOffset := range topicOffsets {
				if partitionOffset.Partition == partition {
					// hasCommitted distinguishes "never committed" (returned as -1 by Kafka) from a
					// genuine commit at offset 0, since the two require different seed behavior.
					if partitionOffset.CommittedOffset == -1 {
						return 0, false, nil
					}
					return partitionOffset.CommittedOffset, true, nil
				}
			}
		}

		return 0, false, nil
	}

	c.offsetManager = newOffsetManager(offsetManagerConfig{
		topic:           config.Topic,
		commitFunc:      commit,
		fetchOffsetFunc: fetchOffset,
		logger:          config.Logger,
		minCommitCount:  config.MinOffsetCommitCount,
		maxInterval:     config.MaxCommitInterval,
		forceInterval:   config.ForceCommitInterval,
		maxRetries:      config.MaxCommitRetries,
		retryDelay:      config.CommitRetryDelay,
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
	go c.consumeFromKafka()

	if c.keySequencer != nil {
		c.wg.Add(1)
		go c.consumeFromKeySequencer()
	}

	c.wg.Add(1)
	go c.forceCommitLoop()

	return nil
}

func (c *Consumer) consumeFromKafka() {
	defer c.wg.Done()

	var fetchBackoff time.Duration
	const maxFetchBackoff = 5 * time.Second

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if err := c.backpressure.Acquire(c.ctx); err != nil {
			return
		}

		msg, err := c.reader.FetchMessage(c.ctx)
		if err != nil {
			c.backpressure.Release()
			if errors.Is(err, context.Canceled) {
				return
			}
			c.logger.Error("Failed to fetch message", "err", err)
			fetchBackoff = backoffWithJitter(fetchBackoff, maxFetchBackoff)
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(fetchBackoff):
			}
			continue
		}
		fetchBackoff = 0

		c.offsetManager.Track(msg.Partition, msg.Offset)

		key := c.config.Handler.GetKey(msg.Key, msg.Value)
		if c.keySequencer != nil {
			// Under OverflowBlock this blocks until the key's queue has room; the
			// backpressure slot stays held meanwhile, which is the point.
			acquired, evicted, err := c.keySequencer.submit(c.ctx, msg, key)
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

// handleOverflowDrop accounts for a message the key sequencer discarded because its
// key's queue was full. The message is never handed to the handler, so nothing else
// will ever mark it done — and because the offset watermark only advances over a
// contiguous run of done offsets, skipping this would freeze commits for the partition
// permanently. Dead-letter first, then mark done, so the offset is never committed
// ahead of the record being persisted.
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
			// Stop() drained the queue; nothing more will be dispatched.
			return
		case <-c.keySequencer.readyChan():
			// Drain until dequeue is empty, otherwise pending messages would sit idle until the next release signals.
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

		// procCtx so in-flight handlers can finish during graceful shutdown after the fetch loop has been canceled.
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
		// The queue is empty (or the drain timed out); release the sequencer pump so
		// wg.Wait() below is gated only on in-flight handlers, not on this goroutine.
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

	c.offsetManager.ForceCommit(context.Background())

	var closeErr error
	if err := c.reader.Close(); err != nil {
		c.logger.Error("Failed to close reader", "err", err)
		closeErr = err
	}

	c.logger.Info("Kafka consumer stopped")

	return closeErr
}
