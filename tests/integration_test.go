//go:build integration
// +build integration

package tests

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datzero9/turnstile"
	"github.com/segmentio/kafka-go"
)

type testMessageHandler struct {
	mu               sync.Mutex
	processedMsgs    []kafka.Message
	processingDelay  time.Duration
	errorOnOffset    map[int64]error
	errorOnAttempt   map[int64]int // offset -> number of attempts to fail before succeeding
	attemptCounts    map[int64]int
	panicOnOffset    map[int64]bool
	keyExtractor     func([]byte, []byte) string
	processedCount   atomic.Int64
	errorCount       atomic.Int64
	panicCount       atomic.Int64
	onProcessMessage func(kafka.Message)
}

func newTestMessageHandler() *testMessageHandler {
	return &testMessageHandler{
		processedMsgs:  make([]kafka.Message, 0),
		errorOnOffset:  make(map[int64]error),
		errorOnAttempt: make(map[int64]int),
		attemptCounts:  make(map[int64]int),
		panicOnOffset:  make(map[int64]bool),
		keyExtractor: func(key []byte, value []byte) string {
			return string(key)
		},
	}
}

func (h *testMessageHandler) HandleMessage(ctx context.Context, message kafka.Message) error {
	h.mu.Lock()
	delay := h.processingDelay
	h.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	h.mu.Lock()
	h.processedMsgs = append(h.processedMsgs, message)
	shouldPanic := h.panicOnOffset[message.Offset]
	errOnMsg, hasErrOnMsg := h.errorOnOffset[message.Offset]
	failAttempts, hasFailAttempts := h.errorOnAttempt[message.Offset]
	currentAttempt := h.attemptCounts[message.Offset]
	h.attemptCounts[message.Offset] = currentAttempt + 1
	hook := h.onProcessMessage
	h.mu.Unlock()

	h.processedCount.Add(1)

	if shouldPanic {
		h.panicCount.Add(1)
		panic(fmt.Sprintf("panic on offset %d", message.Offset))
	}

	if hasErrOnMsg {
		h.errorCount.Add(1)
		return errOnMsg
	}

	if hasFailAttempts && currentAttempt < failAttempts {
		h.errorCount.Add(1)
		return fmt.Errorf("transient error attempt %d", currentAttempt)
	}

	if hook != nil {
		hook(message)
	}

	return nil
}

func (h *testMessageHandler) GetKey(key []byte, value []byte) string {
	h.mu.Lock()
	extractor := h.keyExtractor
	h.mu.Unlock()
	return extractor(key, value)
}

func (h *testMessageHandler) GetProcessedCount() int64 {
	return h.processedCount.Load()
}

func (h *testMessageHandler) GetErrorCount() int64 {
	return h.errorCount.Load()
}

func (h *testMessageHandler) SetProcessingDelay(delay time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.processingDelay = delay
}

func (h *testMessageHandler) SetErrorOnMessage(offset int64, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errorOnOffset[offset] = err
}

func (h *testMessageHandler) SetFailFirstNAttempts(offset int64, n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errorOnAttempt[offset] = n
}

func (h *testMessageHandler) SetPanicOnMessage(offset int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.panicOnOffset[offset] = true
}

func (h *testMessageHandler) SetKeyExtractor(extractor func([]byte, []byte) string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.keyExtractor = extractor
}

func (h *testMessageHandler) SetOnProcessMessage(hook func(kafka.Message)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onProcessMessage = hook
}

type testDeadLetterPersister struct {
	mu            sync.Mutex
	savedMessages []turnstile.DeadLetterMessage
	failNextSave  atomic.Int64
}

func newTestDeadLetterPersister() *testDeadLetterPersister {
	return &testDeadLetterPersister{
		savedMessages: make([]turnstile.DeadLetterMessage, 0),
	}
}

func (p *testDeadLetterPersister) Save(ctx context.Context, message kafka.Message, err error, key string) error {
	if p.failNextSave.Load() > 0 {
		p.failNextSave.Add(-1)
		return errors.New("persister failure")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	pm := turnstile.DeadLetterMessage{
		ID:        fmt.Sprintf("msg-%d", len(p.savedMessages)),
		Message:   message,
		Error:     err.Error(),
		Key:       key,
		CreatedAt: time.Now().Unix(),
	}
	p.savedMessages = append(p.savedMessages, pm)
	return nil
}

func (p *testDeadLetterPersister) GetSavedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.savedMessages)
}

func (p *testDeadLetterPersister) SetFailNextSave(n int) {
	p.failNextSave.Store(int64(n))
}

func createTestMessages(topic string, partition int, count int, keyPrefix, valuePrefix string) []kafka.Message {
	messages := make([]kafka.Message, count)
	for i := range count {
		messages[i] = kafka.Message{
			Topic:     topic,
			Partition: partition,
			Offset:    int64(i),
			Key:       fmt.Appendf(nil, "%s-%d", keyPrefix, i),
			Value:     fmt.Appendf(nil, "%s-%d", valuePrefix, i),
			Time:      time.Now(),
		}
	}
	return messages
}

func waitForCondition(timeout time.Duration, checkInterval time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(checkInterval)
	}
	return false
}

const (
	testBroker     = "localhost:9092"
	testGroupID    = "turnstile-integration-test"
	testTopic      = "turnstile-test-topic"
	testTimeout    = 30 * time.Second
	messageTimeout = 10 * time.Second
	// testMaxWait bounds how long an in-flight fetch pins reader.Close during Stop.
	testMaxWait = 250 * time.Millisecond
)

// uniqueName returns a fresh name for a per-test topic or group ID, using nanosecond
// resolution so two tests running in the same second do not collide.
func uniqueName(prefix, label string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, label, time.Now().UnixNano())
}

// writeMessages writes value-only copies of the given messages to partition 0 of topic.
// It skips the test if Kafka is unavailable.
func writeMessages(t *testing.T, topic string, messages []kafka.Message) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	conn, err := kafka.DialLeader(ctx, "tcp", testBroker, topic, 0)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	payload := make([]kafka.Message, len(messages))
	for i, m := range messages {
		payload[i] = kafka.Message{Key: m.Key, Value: m.Value}
	}
	if _, err := conn.WriteMessages(payload...); err != nil {
		t.Fatalf("Failed to write messages to %s: %v", topic, err)
	}
}

// createTopic explicitly creates a topic with the given partition count, skipping the
// test if Kafka is unavailable. Needed because auto-create defaults to a single partition,
// which makes multi-consumer assignment impossible.
func createTopic(t *testing.T, topic string, partitions int) {
	t.Helper()
	conn, err := kafka.Dial("tcp", testBroker)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("Failed to discover controller: %v", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("Failed to dial controller: %v", err)
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("Failed to create topic %s: %v", topic, err)
	}

	// Wait for partition leaders to be elected — without this, an immediate
	// produce can hit "Not Leader For Partition" while metadata propagates.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for p := 0; p < partitions; p++ {
			leaderConn, err := kafka.DialLeader(context.Background(), "tcp", testBroker, topic, p)
			if err != nil {
				ready = false
				break
			}
			leaderConn.Close()
		}
		if ready {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Topic %s partition leaders not ready", topic)
}

func TestBasicConsumption(t *testing.T) {
	topic := uniqueName(testTopic, "basic")
	groupID := uniqueName(testGroupID, "basic")
	messages := createTestMessages(topic, 0, 10, "key", "value")
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         groupID,
		Topic:           topic,
		Handler:         handler,
		MaxInFlight:     100,
		AutoOffsetReset: kafka.FirstOffset,
		MaxWait:         testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(messages))
	}) {
		t.Fatalf("Expected %d messages processed, got %d", len(messages), handler.GetProcessedCount())
	}
}

func TestSequentialOffsetCommit(t *testing.T) {
	topic := uniqueName(testTopic, "sequential")
	groupID := uniqueName(testGroupID, "sequential")
	messages := createTestMessages(topic, 0, 20, "key", "value")
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	handler.SetErrorOnMessage(5, errors.New("test error"))
	handler.SetProcessingDelay(10 * time.Millisecond)
	persister := newTestDeadLetterPersister()

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:              []string{testBroker},
		GroupID:              groupID,
		Topic:                topic,
		Handler:              handler,
		MaxInFlight:          50,
		MinOffsetCommitCount: 5,
		MaxCommitInterval:    500 * time.Millisecond,
		AutoOffsetReset:      kafka.FirstOffset,
		MaxWait:              testMaxWait,
		DeadLetterPersister:  persister,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(messages))
	}) {
		t.Fatalf("Expected %d messages processed, got %d", len(messages), handler.GetProcessedCount())
	}

	if got := persister.GetSavedCount(); got != 1 {
		t.Errorf("Expected 1 dead-lettered message (the injected failure), got %d", got)
	}
}

func TestWorkerPoolMode(t *testing.T) {
	topic := uniqueName(testTopic, "pool")
	groupID := uniqueName(testGroupID, "pool")
	messages := createTestMessages(topic, 0, 50, "key", "value")
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	handler.SetProcessingDelay(50 * time.Millisecond)

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         groupID,
		Topic:           topic,
		Handler:         handler,
		MaxInFlight:     100,
		AutoOffsetReset: kafka.FirstOffset,
		MaxWait:         testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(messages))
	}) {
		t.Fatalf("Expected %d messages processed, got %d", len(messages), handler.GetProcessedCount())
	}

	t.Logf("Worker pool processed %d messages", handler.GetProcessedCount())
}

func TestBackpressureManagement(t *testing.T) {
	topic := uniqueName(testTopic, "backpressure")
	groupID := uniqueName(testGroupID, "backpressure")
	messages := createTestMessages(topic, 0, 200, "key", "value")
	writeMessages(t, topic, messages)

	maxInFlight := 20

	var inFlight atomic.Int64
	var peak atomic.Int64

	handler := newTestMessageHandler()
	handler.SetProcessingDelay(50 * time.Millisecond)
	handler.SetOnProcessMessage(func(kafka.Message) {
		cur := inFlight.Add(1)
		for {
			p := peak.Load()
			if cur <= p || peak.CompareAndSwap(p, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
	})

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         groupID,
		Topic:           topic,
		Handler:         handler,
		MaxInFlight:     maxInFlight,
		AutoOffsetReset: kafka.FirstOffset,
		MaxWait:         testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(30*time.Second, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(messages))
	}) {
		t.Fatalf("Expected %d messages processed, got %d", len(messages), handler.GetProcessedCount())
	}

	if p := peak.Load(); p > int64(maxInFlight) {
		t.Errorf("Backpressure violated: peak concurrency %d exceeded MaxInFlight %d", p, maxInFlight)
	}
	t.Logf("Peak concurrent in-flight: %d (limit %d)", peak.Load(), maxInFlight)
}

func TestKeyBasedSequencing(t *testing.T) {
	topic := uniqueName(testTopic, "dedup")
	groupID := uniqueName(testGroupID, "dedup")
	messages := []kafka.Message{
		{Key: []byte("key1"), Value: []byte("msg1")},
		{Key: []byte("key1"), Value: []byte("msg2")},
		{Key: []byte("key2"), Value: []byte("msg3")},
		{Key: []byte("key1"), Value: []byte("msg4")},
		{Key: []byte("key2"), Value: []byte("msg5")},
	}
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()

	var keyMutex sync.Mutex
	processingKeys := make(map[string]bool)
	var concurrentAccess atomic.Bool

	handler.SetOnProcessMessage(func(msg kafka.Message) {
		key := string(msg.Key)
		keyMutex.Lock()
		if processingKeys[key] {
			concurrentAccess.Store(true)
		}
		processingKeys[key] = true
		keyMutex.Unlock()

		time.Sleep(50 * time.Millisecond)

		keyMutex.Lock()
		delete(processingKeys, key)
		keyMutex.Unlock()
	})

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         groupID,
		Topic:           topic,
		Handler:         handler,
		MaxInFlight:     10,
		UnOrdered:       false,
		AutoOffsetReset: kafka.FirstOffset,
		MaxWait:         testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(messages))
	}) {
		t.Fatalf("Expected %d messages processed, got %d", len(messages), handler.GetProcessedCount())
	}

	if concurrentAccess.Load() {
		t.Error("Key sequencing failed: same key processed concurrently")
	}
}

func TestUnOrderedMode(t *testing.T) {
	topic := uniqueName(testTopic, "unordered")
	groupID := uniqueName(testGroupID, "unordered")

	const n = 10
	messages := make([]kafka.Message, n)
	for i := range messages {
		messages[i] = kafka.Message{Key: []byte("same-key"), Value: fmt.Appendf(nil, "v-%d", i)}
	}
	writeMessages(t, topic, messages)

	var inFlight atomic.Int64
	var peak atomic.Int64
	handler := newTestMessageHandler()
	handler.SetOnProcessMessage(func(kafka.Message) {
		cur := inFlight.Add(1)
		for {
			p := peak.Load()
			if cur <= p || peak.CompareAndSwap(p, cur) {
				break
			}
		}
		time.Sleep(80 * time.Millisecond)
		inFlight.Add(-1)
	})

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         groupID,
		Topic:           topic,
		Handler:         handler,
		MaxInFlight:     n,
		UnOrdered:       true,
		AutoOffsetReset: kafka.FirstOffset,
		MaxWait:         testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(n)
	}) {
		t.Fatalf("Expected %d messages processed, got %d", n, handler.GetProcessedCount())
	}

	if peak.Load() < 2 {
		t.Errorf("UnOrdered mode should allow same-key concurrency, peak was %d", peak.Load())
	}
}

func TestGracefulShutdown(t *testing.T) {
	topic := uniqueName(testTopic, "shutdown")
	groupID := uniqueName(testGroupID, "shutdown")

	const total = 30
	const maxInFlight = 5
	messages := createTestMessages(topic, 0, total, "key", "value")
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	handler.SetProcessingDelay(300 * time.Millisecond)

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         groupID,
		Topic:           topic,
		Handler:         handler,
		MaxInFlight:     maxInFlight,
		ShutdownTimeout: 10 * time.Second,
		AutoOffsetReset: kafka.FirstOffset,
		MaxWait:         testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()

	// Wait until at least one message is in-flight but the run is not yet complete.
	if !waitForCondition(2*time.Second, 20*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= 1
	}) {
		consumer.Stop()
		t.Fatal("No progress before shutdown — handler never ran")
	}
	processedBefore := handler.GetProcessedCount()
	if processedBefore >= int64(total) {
		consumer.Stop()
		t.Skipf("Consumer finished entire batch (%d) before we could shut down; test is meaningless here", total)
	}

	if err := consumer.Stop(); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
	processedAfter := handler.GetProcessedCount()
	t.Logf("Processed before shutdown: %d, after shutdown: %d", processedBefore, processedAfter)

	// Stop() must drain in-flight handlers (procCtx outlives fetch ctx). Strictly the
	// count should be >= processedBefore + 1 (the in-flight ones complete).
	if processedAfter < processedBefore {
		t.Errorf("Processed count regressed: before=%d after=%d", processedBefore, processedAfter)
	}
	if processedAfter == 0 {
		t.Error("Expected at least the in-flight messages to complete during shutdown")
	}
}

func TestFailedMessagePersistence(t *testing.T) {
	topic := uniqueName(testTopic, "persist")
	groupID := uniqueName(testGroupID, "persist")
	messages := createTestMessages(topic, 0, 10, "key", "value")
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	testErr := errors.New("processing failed")
	handler.SetErrorOnMessage(2, testErr)
	handler.SetErrorOnMessage(5, testErr)
	handler.SetErrorOnMessage(7, testErr)

	persister := newTestDeadLetterPersister()

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:             []string{testBroker},
		GroupID:             groupID,
		Topic:               topic,
		Handler:             handler,
		MaxInFlight:         1,
		DeadLetterPersister: persister,
		AutoOffsetReset:     kafka.FirstOffset,
		MaxWait:             testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(messages))
	}) {
		t.Fatalf("Expected %d messages processed, got %d", len(messages), handler.GetProcessedCount())
	}

	if got := persister.GetSavedCount(); got != 3 {
		t.Errorf("Expected 3 dead-lettered messages, got %d", got)
	}
	if got := handler.GetErrorCount(); got != 3 {
		t.Errorf("Expected 3 handler errors, got %d", got)
	}
}

func TestRetryThenSucceed(t *testing.T) {
	topic := uniqueName(testTopic, "retry")
	groupID := uniqueName(testGroupID, "retry")
	messages := createTestMessages(topic, 0, 5, "key", "value")
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	handler.SetFailFirstNAttempts(2, 2) // message index 2 fails twice then succeeds

	persister := newTestDeadLetterPersister()

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:             []string{testBroker},
		GroupID:             groupID,
		Topic:               topic,
		Handler:             handler,
		MaxInFlight:         1,
		RetryCount:          3,
		RetryDelay:          50 * time.Millisecond,
		DeadLetterPersister: persister,
		AutoOffsetReset:     kafka.FirstOffset,
		MaxWait:             testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(messages)+2) // 2 retries on index 2
	}) {
		t.Fatalf("Expected at least %d handler invocations, got %d", len(messages)+2, handler.GetProcessedCount())
	}

	if got := persister.GetSavedCount(); got != 0 {
		t.Errorf("Expected no dead-letters (retry should have succeeded), got %d", got)
	}
	if got := handler.GetErrorCount(); got != 2 {
		t.Errorf("Expected 2 transient errors before success, got %d", got)
	}
}

func TestRetryExhaustionDeadLetters(t *testing.T) {
	topic := uniqueName(testTopic, "retry-exhaust")
	groupID := uniqueName(testGroupID, "retry-exhaust")
	messages := createTestMessages(topic, 0, 3, "key", "value")
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	handler.SetErrorOnMessage(0, errors.New("permanent failure"))
	handler.SetErrorOnMessage(1, errors.New("permanent failure"))
	handler.SetErrorOnMessage(2, errors.New("permanent failure"))

	persister := newTestDeadLetterPersister()

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:             []string{testBroker},
		GroupID:             groupID,
		Topic:               topic,
		Handler:             handler,
		MaxInFlight:         1,
		RetryCount:          2,
		RetryDelay:          20 * time.Millisecond,
		DeadLetterPersister: persister,
		AutoOffsetReset:     kafka.FirstOffset,
		MaxWait:             testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	// 3 messages * (1 initial + 2 retries) = 9 handler invocations
	if !waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return persister.GetSavedCount() >= 3
	}) {
		t.Fatalf("Expected 3 dead-letters, got %d", persister.GetSavedCount())
	}

	if got := handler.GetProcessedCount(); got != 9 {
		t.Errorf("Expected 9 handler invocations (3 msgs * 3 attempts), got %d", got)
	}
}

func TestPanicRecovery(t *testing.T) {
	topic := uniqueName(testTopic, "panic")
	groupID := uniqueName(testGroupID, "panic")
	messages := createTestMessages(topic, 0, 5, "key", "value")
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	handler.SetPanicOnMessage(2)

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         groupID,
		Topic:           topic,
		Handler:         handler,
		MaxInFlight:     1,
		AutoOffsetReset: kafka.FirstOffset,
		MaxWait:         testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(messages))
	}) {
		t.Fatalf("Expected %d invocations, got %d (panic recovery may have failed)",
			len(messages), handler.GetProcessedCount())
	}

	if handler.panicCount.Load() != 1 {
		t.Errorf("Expected 1 panic, got %d", handler.panicCount.Load())
	}
}

func TestDeadLetterPersisterFailure(t *testing.T) {
	topic := uniqueName(testTopic, "dlq-fail")
	groupID := uniqueName(testGroupID, "dlq-fail")
	messages := createTestMessages(topic, 0, 3, "key", "value")
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	handler.SetErrorOnMessage(1, errors.New("processing failed"))

	persister := newTestDeadLetterPersister()
	persister.SetFailNextSave(1) // first persistence attempt fails; consumer should not crash

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:             []string{testBroker},
		GroupID:             groupID,
		Topic:               topic,
		Handler:             handler,
		MaxInFlight:         1,
		DeadLetterPersister: persister,
		AutoOffsetReset:     kafka.FirstOffset,
		MaxWait:             testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(messages))
	}) {
		t.Fatalf("Consumer stalled after persister error; processed %d", handler.GetProcessedCount())
	}

	if persister.GetSavedCount() != 0 {
		t.Errorf("Expected zero successful saves (we failed the only save), got %d", persister.GetSavedCount())
	}
}

func TestEmptyKeySkipsSequencer(t *testing.T) {
	topic := uniqueName(testTopic, "emptykey")
	groupID := uniqueName(testGroupID, "emptykey")

	const n = 8
	messages := make([]kafka.Message, n)
	for i := range messages {
		// Same kafka key, but the extractor will return "" — bypassing the sequencer.
		messages[i] = kafka.Message{Key: []byte("shared"), Value: fmt.Appendf(nil, "v-%d", i)}
	}
	writeMessages(t, topic, messages)

	var inFlight atomic.Int64
	var peak atomic.Int64
	handler := newTestMessageHandler()
	handler.SetKeyExtractor(func([]byte, []byte) string { return "" })
	handler.SetOnProcessMessage(func(kafka.Message) {
		cur := inFlight.Add(1)
		for {
			p := peak.Load()
			if cur <= p || peak.CompareAndSwap(p, cur) {
				break
			}
		}
		time.Sleep(60 * time.Millisecond)
		inFlight.Add(-1)
	})

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         groupID,
		Topic:           topic,
		Handler:         handler,
		MaxInFlight:     n,
		AutoOffsetReset: kafka.FirstOffset,
		MaxWait:         testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(n)
	}) {
		t.Fatalf("Expected %d messages processed, got %d", n, handler.GetProcessedCount())
	}

	if peak.Load() < 2 {
		t.Errorf("Empty-key messages should run concurrently, peak was %d", peak.Load())
	}
}

func TestConcurrentProcessing(t *testing.T) {
	topic := uniqueName(testTopic, "concurrent")
	groupID := uniqueName(testGroupID, "concurrent")
	messages := createTestMessages(topic, 0, 1000, "key", "value")
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	handler.SetProcessingDelay(2 * time.Millisecond)

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         groupID,
		Topic:           topic,
		Handler:         handler,
		MaxInFlight:     500,
		AutoOffsetReset: kafka.FirstOffset,
		MaxWait:         testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	start := time.Now()
	consumer.Start()
	defer consumer.Stop()

	if !waitForCondition(30*time.Second, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(messages))
	}) {
		t.Fatalf("Expected %d messages processed, got %d", len(messages), handler.GetProcessedCount())
	}

	elapsed := time.Since(start)
	t.Logf("Processed %d messages in %v (%.2f msgs/sec)",
		handler.GetProcessedCount(), elapsed, float64(handler.GetProcessedCount())/elapsed.Seconds())
}

func TestPartitionRebalanceWithMultipleConsumers(t *testing.T) {
	topic := uniqueName(testTopic, "rebalance")
	groupID := uniqueName(testGroupID, "rebalance-group")

	// Multi-partition topic is required for a group to distribute work across two consumers.
	createTopic(t, topic, 4)

	messages := createTestMessages(topic, 0, 50, "key", "value")
	writeMessages(t, topic, messages)

	handler1 := newTestMessageHandler()
	consumer1, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:              []string{testBroker},
		GroupID:              groupID,
		Topic:                topic,
		Handler:              handler1,
		MaxInFlight:          100,
		AutoOffsetReset:      kafka.FirstOffset,
		MaxWait:              testMaxWait,
		MinOffsetCommitCount: 5,
		MaxCommitInterval:    1 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer1: %v", err)
	}
	consumer1.Start()
	defer consumer1.Stop()

	// Wait until consumer1 owns partitions and is making progress, rather than
	// sleeping a fixed interval and hoping the group has settled.
	if !waitForCondition(15*time.Second, 50*time.Millisecond, func() bool {
		return handler1.GetProcessedCount() > 0
	}) {
		t.Fatal("Consumer1 never processed anything before the second consumer joined")
	}
	t.Logf("Consumer1 processed before rebalance: %d", handler1.GetProcessedCount())

	handler2 := newTestMessageHandler()
	consumer2, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:              []string{testBroker},
		GroupID:              groupID,
		Topic:                topic,
		Handler:              handler2,
		MaxInFlight:          100,
		AutoOffsetReset:      kafka.FirstOffset,
		MaxWait:              testMaxWait,
		MinOffsetCommitCount: 5,
		MaxCommitInterval:    1 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer2: %v", err)
	}
	consumer2.Start()
	defer consumer2.Stop()

	// Produce more messages after the second consumer joins to give it work.
	moreMessages := createTestMessages(topic, 0, 50, "key2", "value2")
	writeMessages(t, topic, moreMessages)

	// Both batches should land across the two consumers; poll for that instead of
	// sleeping out the worst-case rebalance time.
	wantTotal := int64(len(messages) + len(moreMessages))
	waitForCondition(20*time.Second, 100*time.Millisecond, func() bool {
		return handler1.GetProcessedCount()+handler2.GetProcessedCount() >= wantTotal
	})

	total := handler1.GetProcessedCount() + handler2.GetProcessedCount()
	t.Logf("After rebalance — Consumer1: %d, Consumer2: %d, Total: %d",
		handler1.GetProcessedCount(), handler2.GetProcessedCount(), total)

	if total == 0 {
		t.Fatal("No messages processed by either consumer")
	}
}

func TestConsumerRestartRebalance(t *testing.T) {
	topic := uniqueName(testTopic, "restart")
	groupID := uniqueName(testGroupID, "restart-group")

	firstBatch := createTestMessages(topic, 0, 20, "key", "value")
	writeMessages(t, topic, firstBatch)

	handler := newTestMessageHandler()

	createConsumer := func() *turnstile.Consumer {
		c, err := turnstile.NewConsumer(turnstile.Config{
			Brokers:              []string{testBroker},
			GroupID:              groupID,
			Topic:                topic,
			Handler:              handler,
			MaxInFlight:          100,
			AutoOffsetReset:      kafka.FirstOffset,
			MaxWait:              testMaxWait,
			MinOffsetCommitCount: 5,
			MaxCommitInterval:    500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Failed to create consumer: %v", err)
		}
		return c
	}

	consumer1 := createConsumer()
	consumer1.Start()

	if !waitForCondition(10*time.Second, 200*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= 20
	}) {
		t.Fatalf("First consumer did not process first batch in time (got %d)", handler.GetProcessedCount())
	}

	// No sleep needed: Stop() runs a final ForceCommit, so the first consumer's
	// progress is durable by the time consumer2 joins.
	processedAfterFirst := handler.GetProcessedCount()
	t.Logf("First consumer processed %d messages", processedAfterFirst)

	consumer1.Stop()

	secondBatch := createTestMessages(topic, 0, 20, "key2", "value2")
	writeMessages(t, topic, secondBatch)

	consumer2 := createConsumer()
	consumer2.Start()
	defer consumer2.Stop()

	waitForCondition(10*time.Second, 200*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= 40
	})

	final := handler.GetProcessedCount()
	t.Logf("After restart total processed: %d (expected ~40)", final)
	if final < 35 {
		t.Errorf("Expected at least 35 messages processed across restart, got %d", final)
	}
}

// TestConfigValidate exercises Validate's error branches. Pure unit test —
// no Kafka required even under the integration tag.
func TestConfigValidate(t *testing.T) {
	handler := newTestMessageHandler()
	base := turnstile.Config{
		Brokers: []string{testBroker},
		GroupID: "g",
		Topic:   "t",
		Handler: handler,
	}

	cases := []struct {
		name    string
		mutate  func(*turnstile.Config)
		wantErr error
	}{
		{"no brokers", func(c *turnstile.Config) { c.Brokers = nil }, turnstile.ErrNoBrokers},
		{"no group ID", func(c *turnstile.Config) { c.GroupID = "" }, turnstile.ErrNoGroupID},
		{"no topic", func(c *turnstile.Config) { c.Topic = "" }, turnstile.ErrNoTopic},
		{"no handler", func(c *turnstile.Config) { c.Handler = nil }, turnstile.ErrNoHandler},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			_, err := turnstile.NewConsumer(cfg)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("expected %v, got %v", tc.wantErr, err)
			}
		})
	}

	t.Run("valid", func(t *testing.T) {
		cfg := base
		c, err := turnstile.NewConsumer(cfg)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		// Don't Start — we just want NewConsumer to succeed so we exercise the
		// happy path of Validate. Close the reader to avoid leaks via Stop().
		_ = c.Stop()
	})
}

// TestMaxQueuedPerKeyOverflow drives the keySequencer past its per-key queue
// capacity to exercise the "drop oldest" eviction path (key_sequencer.submit /
// droppedCount), which the other tests don't reach.
func TestMaxQueuedPerKeyOverflow(t *testing.T) {
	topic := uniqueName(testTopic, "overflow")
	groupID := uniqueName(testGroupID, "overflow")

	const total = 20
	messages := make([]kafka.Message, total)
	for i := range messages {
		// All messages share the same key, so only one can process at a time and
		// the rest queue up — driving past MaxQueuedPerKey.
		messages[i] = kafka.Message{Key: []byte("hot"), Value: fmt.Appendf(nil, "v-%d", i)}
	}
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	// Slow handler keeps the in-flight slot busy so the rest queue behind it.
	handler.SetProcessingDelay(300 * time.Millisecond)

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         groupID,
		Topic:           topic,
		Handler:         handler,
		MaxInFlight:     total,
		MaxQueuedPerKey: 2, // anything beyond 2 queued for "hot" gets evicted
		// The default is OverflowBlock, which never drops; opt in explicitly so this
		// test still exercises the eviction path rather than applying backpressure.
		OverflowPolicy:  turnstile.OverflowDropOldest,
		AutoOffsetReset: kafka.FirstOffset,
		MaxWait:         testMaxWait,
		ShutdownTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	// We can't reasonably wait for all messages — many will be dropped. Wait long
	// enough for the fetch loop to ingest everything and the eviction path to fire.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		// Heuristic: once handler has run >=3 times, dropping must have occurred for
		// any queue of 2 capacity with 20 messages on one key.
		if handler.GetProcessedCount() >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	processed := handler.GetProcessedCount()
	t.Logf("Processed %d / %d (rest dropped by queue cap)", processed, total)
	if processed == 0 {
		t.Fatal("Handler never ran — overflow path not exercised")
	}
	if processed >= int64(total) {
		t.Errorf("Expected drops for shared-key flood, but all %d ran", total)
	}
}

// TestStopHonorsSingleShutdownBudget pins the contract that ShutdownTimeout is a
// total budget for Stop(), not a per-phase one. Draining the key sequencer and
// waiting on in-flight handlers previously took ShutdownTimeout each, so a stuck
// consumer took twice the configured budget to shut down.
func TestStopHonorsSingleShutdownBudget(t *testing.T) {
	topic := uniqueName(testTopic, "shutdown-budget")
	groupID := uniqueName(testGroupID, "shutdown-budget")

	const n = 10
	const shutdownTimeout = 2 * time.Second

	messages := make([]kafka.Message, n)
	for i := range messages {
		// One hot key: the first message occupies it and the other nine sit in the
		// sequencer queue, so the drain phase cannot complete before the deadline.
		messages[i] = kafka.Message{Key: []byte("hot"), Value: fmt.Appendf(nil, "v-%d", i)}
	}
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	// Far longer than the shutdown budget, but ctx-aware, so the forced cancel is
	// what ends it — this measures Stop()'s own bookkeeping, not a stuck handler.
	handler.SetProcessingDelay(30 * time.Second)

	var fetched atomic.Int64
	handler.SetKeyExtractor(func([]byte, []byte) string {
		fetched.Add(1)
		return "hot"
	})

	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         groupID,
		Topic:           topic,
		Handler:         handler,
		MaxInFlight:     n,
		ShutdownTimeout: shutdownTimeout,
		AutoOffsetReset: kafka.FirstOffset,
		MaxWait:         testMaxWait,
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()

	// GetKey runs on the fetch path, so this confirms every message has been
	// submitted to the sequencer and the queue is genuinely backed up.
	if !waitForCondition(messageTimeout, 10*time.Millisecond, func() bool {
		return fetched.Load() >= int64(n)
	}) {
		consumer.Stop()
		t.Fatalf("Only %d/%d messages reached the sequencer", fetched.Load(), n)
	}

	start := time.Now()
	if err := consumer.Stop(); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
	elapsed := time.Since(start)
	t.Logf("Stop took %v with ShutdownTimeout=%v", elapsed, shutdownTimeout)

	// Allowance covers the final ForceCommit and reader.Close (bounded by MaxWait),
	// while still landing well under the 2x budget the old code spent.
	if limit := shutdownTimeout + 1500*time.Millisecond; elapsed > limit {
		t.Errorf("Stop took %v, want <= %v (ShutdownTimeout applied more than once?)", elapsed, limit)
	}
}
