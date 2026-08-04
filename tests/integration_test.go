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
	errorOnAttempt   map[int64]int
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
	testBroker           = "localhost:9092"
	testGroupID          = "turnstile-integration-test"
	testTopic            = "turnstile-test-topic"
	testTimeout          = 30 * time.Second
	messageTimeout       = 10 * time.Second
	testMaxWait          = 250 * time.Millisecond
	producerWriteTimeout = 5 * time.Second
	producerWriteTries   = 5
	producerRetryDelay   = 200 * time.Millisecond
)

func uniqueName(prefix, label string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, label, time.Now().UnixNano())
}

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

	// Immediate produce can hit "Not Leader For Partition" while metadata propagates.
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
	const handlerDelay = 300 * time.Millisecond
	messages := createTestMessages(topic, 0, total, "key", "value")
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
	handler.SetProcessingDelay(handlerDelay)

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

	time.Sleep(3 * handlerDelay)
	if late := handler.GetProcessedCount(); late != processedAfter {
		t.Errorf("Handlers still running after Stop returned: %d at Stop, %d later", processedAfter, late)
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
	handler.SetFailFirstNAttempts(2, 2)

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
	persister.SetFailNextSave(1)

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

	closeConns := func() {
		for _, c := range conns {
			c.Close()
		}
	}

	writeRound := func(round int) error {
		for p, conn := range conns {
			msg := kafka.Message{
				Key:   fmt.Appendf(nil, "p%d-key%d", p, round),
				Value: fmt.Appendf(nil, "p%d-value%d", p, round),
			}
			var err error
			for try := range producerWriteTries {
				if try > 0 {
					time.Sleep(producerRetryDelay)
				}
				if err = conn.SetDeadline(time.Now().Add(producerWriteTimeout)); err != nil {
					continue
				}
				if _, err = conn.WriteMessages(msg); err == nil {
					break
				}
			}
			if err != nil {
				return fmt.Errorf("partition %d round %d: %w", p, round, err)
			}
		}
		return nil
	}

	if err := writeRound(0); err != nil {
		closeConns()
		t.Fatalf("Producer failed to seed %s: %v", topic, err)
	}

	type result struct {
		rounds int
		err    error
	}

	done := make(chan struct{})
	finished := make(chan result, 1)

	go func() {
		defer closeConns()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		rounds := 1
		for {
			select {
			case <-done:
				finished <- result{rounds: rounds}
				return
			case <-ticker.C:
				if err := writeRound(rounds); err != nil {
					finished <- result{rounds: rounds, err: err}
					return
				}
				rounds++
			}
		}
	}()

	var (
		once sync.Once
		res  result
	)
	stopFn := func() int {
		once.Do(func() {
			close(done)
			res = <-finished
			if res.err != nil {
				t.Errorf("Producer failed: %v", res.err)
			}
		})
		return res.rounds
	}
	t.Cleanup(func() { stopFn() })

	return stopFn
}

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
	// Registered as cleanup too, so a t.Fatal below does not leave the poller hammering
	// the broker for the rest of the suite.
	stopPoll := sync.OnceFunc(func() {
		close(stopPolling)
		pollWG.Wait()
	})
	t.Cleanup(stopPoll)
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

	var handler1Ref, handler2Ref *testMessageHandler

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
		t.Fatal("Consumer1 never processed anything before the rebalance, though the producer seeded every partition before it started")
	}
	t.Logf("Consumer1 processed %d before the rebalance", handler1Ref.GetProcessedCount())

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

	const consumer2Progress = 40
	if !waitForCondition(45*time.Second, 100*time.Millisecond, func() bool {
		return handler2Ref.GetProcessedCount() >= consumer2Progress
	}) {
		t.Fatalf("Consumer2 processed only %d messages — the rebalance never moved meaningful work to it",
			handler2Ref.GetProcessedCount())
	}
	t.Logf("Rebalance moved work to consumer2 (consumer1: %d, consumer2: %d)",
		handler1Ref.GetProcessedCount(), handler2Ref.GetProcessedCount())

	consumer1Before := handler1Ref.GetProcessedCount()
	stopped2 = true
	if err := consumer2.Stop(); err != nil {
		t.Errorf("consumer2.Stop: %v", err)
	}

	if !waitForCondition(45*time.Second, 100*time.Millisecond, func() bool {
		return handler1Ref.GetProcessedCount() >= consumer1Before+consumer2Progress
	}) {
		t.Fatalf("Consumer1 processed only %d more messages after reacquiring partitions",
			handler1Ref.GetProcessedCount()-consumer1Before)
	}

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

	stopped1 = true
	if err := consumer1.Stop(); err != nil {
		t.Errorf("consumer1.Stop: %v", err)
	}

	stopPoll()

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

	for p := range partitions {
		if final[p] != wantCommitted {
			t.Errorf("Partition %d committed at %d, want %d — commits stalled or never caught up",
				p, final[p], wantCommitted)
		}
	}
}

func TestStopHonorsSingleShutdownBudget(t *testing.T) {
	topic := uniqueName(testTopic, "shutdown-budget")
	groupID := uniqueName(testGroupID, "shutdown-budget")

	const n = 10
	const shutdownTimeout = 2 * time.Second

	messages := make([]kafka.Message, n)
	for i := range messages {
		messages[i] = kafka.Message{Key: []byte("hot"), Value: fmt.Appendf(nil, "v-%d", i)}
	}
	writeMessages(t, topic, messages)

	handler := newTestMessageHandler()
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

	if limit := shutdownTimeout + 1500*time.Millisecond; elapsed > limit {
		t.Errorf("Stop took %v, want <= %v (ShutdownTimeout applied more than once?)", elapsed, limit)
	}
}
