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

	"github.com/kaingdat/turnstile"
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

func (h *testMessageHandler) GetProcessedMessages() []kafka.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]kafka.Message(nil), h.processedMsgs...)
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
	// testMaxWait bounds how long an in-flight fetch pins a partition reader's close.
	testMaxWait = 250 * time.Millisecond
)

// uniqueName uses nanosecond resolution so tests running in the same second do not
// collide.
func uniqueName(prefix, label string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, label, time.Now().UnixNano())
}

// writeMessages writes value-only copies to partition 0, skipping if Kafka is down.
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

// createTopic is needed because auto-create defaults to one partition, which makes
// multi-consumer assignment impossible.
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

	// Without this an immediate produce can hit "Not Leader For Partition" while
	// metadata propagates.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for p := range partitions {
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

	// Stop() must drain in-flight handlers, since procCtx outlives the fetch context.
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

	// Poll for progress rather than sleeping and hoping the group has settled.
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

	// Poll instead of sleeping out the worst-case rebalance time.
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

	// No sleep needed: Stop() flushes on leaving the group, so the first consumer's
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

// startPartitionProducer writes one message per partition on a ticker until stopped,
// returning the round count — so each partition holds exactly that many messages at
// offsets 0..n-1.
//
// The trickle is what makes a rebalance test meaningful: a batch small enough to write
// quickly is also small enough for the first consumer to drain before a second one
// finishes joining, leaving nothing for the rebalance to move.
func startPartitionProducer(t *testing.T, topic string, partitions int, interval time.Duration) (stop func() int) {
	t.Helper()

	conns := make([]*kafka.Conn, partitions)
	for p := range partitions {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		conn, err := kafka.DialLeader(ctx, "tcp", testBroker, topic, p)
		cancel()
		if err != nil {
			for _, c := range conns[:p] {
				c.Close()
			}
			t.Skipf("Kafka not available: %v", err)
		}
		conns[p] = conn
	}

	type result struct {
		rounds int
		err    error
	}

	done := make(chan struct{})
	finished := make(chan result, 1)

	go func() {
		defer func() {
			for _, c := range conns {
				c.Close()
			}
		}()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		rounds := 0
		for {
			select {
			case <-done:
				finished <- result{rounds: rounds}
				return
			case <-ticker.C:
				for p, conn := range conns {
					msg := kafka.Message{
						Key:   fmt.Appendf(nil, "p%d-key%d", p, rounds),
						Value: fmt.Appendf(nil, "p%d-value%d", p, rounds),
					}
					if _, err := conn.WriteMessages(msg); err != nil {
						finished <- result{rounds: rounds, err: fmt.Errorf("partition %d round %d: %w", p, rounds, err)}
						return
					}
				}
				rounds++
			}
		}
	}()

	var once sync.Once
	return func() int {
		once.Do(func() { close(done) })
		r := <-finished
		if r.err != nil {
			t.Errorf("Producer failed: %v", r.err)
		}
		return r.rounds
	}
}

// fetchCommittedOffsets returns -1 for partitions with no commit, and nil on a
// transient error — it is polled from a goroutine that cannot call t.Fatal.
func fetchCommittedOffsets(client *kafka.Client, groupID, topic string, partitions int) map[int]int64 {
	ids := make([]int, partitions)
	for i := range ids {
		ids[i] = i
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: groupID,
		Topics:  map[string][]int{topic: ids},
	})
	if err != nil {
		return nil
	}

	offsets := make(map[int]int64, partitions)
	for _, po := range resp.Topics[topic] {
		offsets[po.Partition] = po.CommittedOffset
	}
	return offsets
}

// The shape matters: consumer2 takes half of consumer1's partitions, advances them,
// then leaves so consumer1 gets them back. Revoked, advanced elsewhere, reassigned to
// the original owner is what breaks a consumer that never resets partition state.
//
// Two assertions the other rebalance tests do not make:
//
//   - No committed offset ever decreases. A commit racing a rebalance used to be
//     applied under whatever generation came next.
//
//   - Every partition's commits reach the high-water mark. This is what a "did the
//     offset go backwards?" check cannot see: a reacquired partition keeps its old
//     watermark, new messages arrive above it, and the contiguous walk waits forever
//     on a gap that never fills.
func TestRebalanceDoesNotRewindOffsets(t *testing.T) {
	const partitions = 4

	topic := uniqueName(testTopic, "no-rewind")
	groupID := uniqueName(testGroupID, "no-rewind-group")

	createTopic(t, topic, partitions)
	stopProducing := startPartitionProducer(t, topic, partitions, 20*time.Millisecond)

	newConsumer := func(handler turnstile.MessageHandler) *turnstile.Consumer {
		c, err := turnstile.NewConsumer(turnstile.Config{
			Brokers:              []string{testBroker},
			GroupID:              groupID,
			Topic:                topic,
			Handler:              handler,
			MaxInFlight:          50,
			AutoOffsetReset:      kafka.FirstOffset,
			MaxWait:              testMaxWait,
			MinOffsetCommitCount: 5,
			MaxCommitInterval:    250 * time.Millisecond,
			ForceCommitInterval:  250 * time.Millisecond,
			// 6s is the broker's default group.min.session.timeout.ms floor; the fast
			// heartbeat keeps the rebalance quick.
			SessionTimeout:    6 * time.Second,
			RebalanceTimeout:  6 * time.Second,
			HeartbeatInterval: 500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Failed to create consumer: %v", err)
		}
		return c
	}

	client := &kafka.Client{Addr: kafka.TCP(testBroker)}

	var (
		offsetMu sync.Mutex
		maxSeen  = make(map[int]int64)
		rewinds  []string
	)

	// observe records a committed-offset sample and flags any decrease.
	observe := func(offsets map[int]int64) {
		offsetMu.Lock()
		defer offsetMu.Unlock()
		for p, o := range offsets {
			if o < 0 {
				continue // never committed yet
			}
			if prev, ok := maxSeen[p]; ok && o < prev {
				rewinds = append(rewinds,
					fmt.Sprintf("partition %d rewound from %d to %d", p, prev, o))
			}
			if o > maxSeen[p] {
				maxSeen[p] = o
			}
		}
	}

	stopPolling := make(chan struct{})
	var pollWG sync.WaitGroup
	pollWG.Add(1)
	go func() {
		defer pollWG.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPolling:
				return
			case <-ticker.C:
				if offsets := fetchCommittedOffsets(client, groupID, topic, partitions); offsets != nil {
					observe(offsets)
				}
			}
		}
	}()

	// Declared ahead of the helper below so it can close over both members.
	var handler1Ref, handler2Ref *testMessageHandler

	// A set, not a count: turnstile is at-least-once and a revoked partition's in-flight
	// work is deliberately not drained, so messages may be handled twice.
	processedOffsets := func() map[int]map[int64]bool {
		seen := make(map[int]map[int64]bool, partitions)
		for _, h := range []*testMessageHandler{handler1Ref, handler2Ref} {
			if h == nil {
				continue
			}
			for _, msg := range h.GetProcessedMessages() {
				if seen[msg.Partition] == nil {
					seen[msg.Partition] = make(map[int64]bool)
				}
				seen[msg.Partition][msg.Offset] = true
			}
		}
		return seen
	}

	handler1Ref = newTestMessageHandler()
	consumer1 := newConsumer(handler1Ref)
	if err := consumer1.Start(); err != nil {
		t.Fatalf("Failed to start consumer1: %v", err)
	}
	stopped1 := false
	defer func() {
		if !stopped1 {
			consumer1.Stop()
		}
	}()

	if !waitForCondition(30*time.Second, 50*time.Millisecond, func() bool {
		return handler1Ref.GetProcessedCount() > 0
	}) {
		t.Fatal("Consumer1 never processed anything before the rebalance")
	}
	t.Logf("Consumer1 processed %d before the rebalance", handler1Ref.GetProcessedCount())

	// A second member forces the rebalance under test.
	handler2Ref = newTestMessageHandler()
	consumer2 := newConsumer(handler2Ref)
	if err := consumer2.Start(); err != nil {
		t.Fatalf("Failed to start consumer2: %v", err)
	}
	stopped2 := false
	defer func() {
		if !stopped2 {
			consumer2.Stop()
		}
	}()

	// Proof that a rebalance actually moved partitions; without it the test passes
	// trivially whenever consumer1 drains the topic before consumer2 joins. The
	// threshold, rather than one message, also pushes the group offsets well past
	// where consumer1 stopped.
	const consumer2Progress = 40
	if !waitForCondition(45*time.Second, 100*time.Millisecond, func() bool {
		return handler2Ref.GetProcessedCount() >= consumer2Progress
	}) {
		t.Fatalf("Consumer2 processed only %d messages — the rebalance never moved meaningful work to it",
			handler2Ref.GetProcessedCount())
	}
	t.Logf("Rebalance moved work to consumer2 (consumer1: %d, consumer2: %d)",
		handler1Ref.GetProcessedCount(), handler2Ref.GetProcessedCount())

	// Consumer1 reacquires partitions it owned in an earlier generation, and must not
	// resume from the watermark it had then.
	consumer1Before := handler1Ref.GetProcessedCount()
	stopped2 = true
	if err := consumer2.Stop(); err != nil {
		t.Errorf("consumer2.Stop: %v", err)
	}

	// On a stale watermark the reacquired partitions would sit below a gap that never
	// fills, and their commits would never move again.
	if !waitForCondition(45*time.Second, 100*time.Millisecond, func() bool {
		return handler1Ref.GetProcessedCount() >= consumer1Before+consumer2Progress
	}) {
		t.Fatalf("Consumer1 processed only %d more messages after reacquiring partitions",
			handler1Ref.GetProcessedCount()-consumer1Before)
	}

	// Freeze the corpus at offsets 0..produced-1 per partition.
	produced := stopProducing()
	if produced == 0 {
		t.Fatal("Producer wrote nothing")
	}
	t.Logf("Produced %d messages per partition across %d partitions", produced, partitions)

	wantCommitted := int64(produced)
	coveredAll := waitForCondition(60*time.Second, 200*time.Millisecond, func() bool {
		seen := processedOffsets()
		for p := range partitions {
			for o := range wantCommitted {
				if !seen[p][o] {
					return false
				}
			}
		}
		return true
	})

	// Stop the last member so its final flush lands, then take a last sample.
	stopped1 = true
	if err := consumer1.Stop(); err != nil {
		t.Errorf("consumer1.Stop: %v", err)
	}

	close(stopPolling)
	pollWG.Wait()

	final := fetchCommittedOffsets(client, groupID, topic, partitions)
	if final == nil {
		t.Fatal("Failed to read final committed offsets")
	}
	observe(final)

	offsetMu.Lock()
	observedRewinds := append([]string(nil), rewinds...)
	offsetMu.Unlock()

	for _, r := range observedRewinds {
		t.Errorf("Committed offset went backwards across the rebalance: %s", r)
	}

	if !coveredAll {
		seen := processedOffsets()
		for p := range partitions {
			var missing []int64
			for o := range wantCommitted {
				if !seen[p][o] {
					missing = append(missing, o)
				}
			}
			if len(missing) > 0 {
				t.Errorf("Partition %d: %d of %d offsets never processed (first few: %v)",
					p, len(missing), produced, missing[:min(len(missing), 10)])
			}
		}
	}

	// The committed offset is the next offset to read, so a fully consumed partition
	// sits at the message count.
	for p := range partitions {
		if final[p] != wantCommitted {
			t.Errorf("Partition %d committed at %d, want %d — commits stalled or never caught up",
				p, final[p], wantCommitted)
		}
	}
}

// Pure unit test — no Kafka required even under the integration tag.
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
		// Never started, so Stop() is only here to release the consumer group.
		_ = c.Stop()
	})
}

// Drives the keySequencer past its per-key capacity to reach the drop-oldest eviction
// path, which no other test exercises.
func TestMaxQueuedPerKeyOverflow(t *testing.T) {
	topic := uniqueName(testTopic, "overflow")
	groupID := uniqueName(testGroupID, "overflow")

	const total = 20
	messages := make([]kafka.Message, total)
	for i := range messages {
		// One shared key, so only one message processes at a time and the rest queue.
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
		// The default OverflowBlock never drops, so opt in to reach the eviction path.
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

	// Most messages are dropped, so wait on the eviction path firing rather than on a
	// total count.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		// With capacity 2 and 20 messages on one key, 3 handler runs implies drops.
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

// ShutdownTimeout is a total budget for Stop(), not a per-phase one. Draining the
// sequencer and waiting on handlers previously took ShutdownTimeout each.
func TestStopHonorsSingleShutdownBudget(t *testing.T) {
	topic := uniqueName(testTopic, "shutdown-budget")
	groupID := uniqueName(testGroupID, "shutdown-budget")

	const n = 10
	const shutdownTimeout = 2 * time.Second

	messages := make([]kafka.Message, n)
	for i := range messages {
		// One hot key, so nine messages sit queued and the drain cannot finish in time.
		messages[i] = kafka.Message{Key: []byte("hot"), Value: fmt.Appendf(nil, "v-%d", i)}
	}
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	// Far longer than the budget but ctx-aware, so the forced cancel is what ends it:
	// this measures Stop()'s own bookkeeping, not a stuck handler.
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

	// GetKey runs on the fetch path, so this confirms the queue is genuinely backed up.
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

	// The allowance covers leaving the group and its final flush, while still landing
	// well under the 2x budget the old code spent.
	if limit := shutdownTimeout + 1500*time.Millisecond; elapsed > limit {
		t.Errorf("Stop took %v, want <= %v (ShutdownTimeout applied more than once?)", elapsed, limit)
	}
}
