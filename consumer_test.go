package turnstile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type testMessageHandler struct {
	mu               sync.Mutex
	processedMsgs    []kafka.Message
	processingDelay  time.Duration
	errorOnMessage   map[int]error
	errorOnKey       map[string]error
	panicOnMessage   map[int]bool
	keyExtractor     func([]byte, []byte) string
	processedCount   atomic.Int64
	errorCount       atomic.Int64
	onProcessMessage func(kafka.Message)
}

func newTestMessageHandler() *testMessageHandler {
	return &testMessageHandler{
		processedMsgs:  make([]kafka.Message, 0),
		errorOnMessage: make(map[int]error),
		errorOnKey:     make(map[string]error),
		panicOnMessage: make(map[int]bool),
		keyExtractor: func(key []byte, value []byte) string {
			return string(key)
		},
	}
}

func (h *testMessageHandler) HandleMessage(ctx context.Context, message kafka.Message) error {
	if h.processingDelay > 0 {
		select {
		case <-time.After(h.processingDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	h.mu.Lock()
	msgIndex := len(h.processedMsgs)
	h.processedMsgs = append(h.processedMsgs, message)
	h.mu.Unlock()

	h.processedCount.Add(1)

	if h.panicOnMessage[msgIndex] {
		panic(fmt.Sprintf("panic on message %d", msgIndex))
	}

	if err, exists := h.errorOnMessage[msgIndex]; exists {
		h.errorCount.Add(1)
		return err
	}

	if err, exists := h.errorOnKey[string(message.Key)]; exists {
		h.errorCount.Add(1)
		return err
	}

	if h.onProcessMessage != nil {
		h.onProcessMessage(message)
	}

	return nil
}

func (h *testMessageHandler) GetKey(key []byte, value []byte) string {
	return h.keyExtractor(key, value)
}

func (h *testMessageHandler) GetProcessedMessages() []kafka.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]kafka.Message, len(h.processedMsgs))
	copy(result, h.processedMsgs)
	return result
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

func (h *testMessageHandler) SetErrorOnMessage(index int, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errorOnMessage[index] = err
}

func (h *testMessageHandler) SetErrorOnKey(key string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errorOnKey[key] = err
}

func (h *testMessageHandler) SetPanicOnMessage(index int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.panicOnMessage[index] = true
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

func (h *testMessageHandler) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.processedMsgs = make([]kafka.Message, 0)
	h.errorOnMessage = make(map[int]error)
	h.errorOnKey = make(map[string]error)
	h.panicOnMessage = make(map[int]bool)
	h.processedCount.Store(0)
	h.errorCount.Store(0)
}

type testDeadLetterPersister struct {
	mu            sync.Mutex
	savedMessages []DeadLetterMessage
	saveCallback  func(context.Context, kafka.Message, error, string) error
}

func newTestDeadLetterPersister() *testDeadLetterPersister {
	return &testDeadLetterPersister{
		savedMessages: make([]DeadLetterMessage, 0),
	}
}

func (p *testDeadLetterPersister) Save(ctx context.Context, message kafka.Message, err error, key string) error {
	if p.saveCallback != nil {
		return p.saveCallback(ctx, message, err, key)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	pm := DeadLetterMessage{
		ID:        fmt.Sprintf("msg-%d", len(p.savedMessages)),
		Message:   message,
		Error:     err.Error(),
		Key:       key,
		CreatedAt: time.Now().Unix(),
	}
	p.savedMessages = append(p.savedMessages, pm)
	return nil
}

func (p *testDeadLetterPersister) GetSavedMessages() []DeadLetterMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]DeadLetterMessage, len(p.savedMessages))
	copy(result, p.savedMessages)
	return result
}

func (p *testDeadLetterPersister) GetSavedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.savedMessages)
}

func (p *testDeadLetterPersister) SetSaveCallback(callback func(context.Context, kafka.Message, error, string) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.saveCallback = callback
}

func (p *testDeadLetterPersister) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.savedMessages = make([]DeadLetterMessage, 0)
}

func createTestMessage(topic string, partition int, offset int64, key, value string) kafka.Message {
	return kafka.Message{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       []byte(key),
		Value:     []byte(value),
		Time:      time.Now(),
	}
}

func createTestMessages(topic string, partition int, count int, keyPrefix, valuePrefix string) []kafka.Message {
	messages := make([]kafka.Message, count)
	for i := range count {
		messages[i] = createTestMessage(
			topic,
			partition,
			int64(i),
			fmt.Sprintf("%s-%d", keyPrefix, i),
			fmt.Sprintf("%s-%d", valuePrefix, i),
		)
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

// fakeConsumerGroup stands in for *kafka.ConsumerGroup so the lifecycle can be driven
// without a broker.
type fakeConsumerGroup struct {
	next     func(ctx context.Context) (*kafka.Generation, error)
	closeErr error

	mu     sync.Mutex
	closes int
}

func (f *fakeConsumerGroup) Next(ctx context.Context) (*kafka.Generation, error) {
	if f.next != nil {
		return f.next(ctx)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeConsumerGroup) Close() error {
	f.mu.Lock()
	f.closes++
	f.mu.Unlock()
	return f.closeErr
}

func (f *fakeConsumerGroup) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes
}

// fakePartitionReader stands in for *kafka.Reader.
type fakePartitionReader struct {
	setOffsetErr error
	closeErr     error
	fetch        func(ctx context.Context) (kafka.Message, error)

	mu         sync.Mutex
	setOffsets []int64
	fetches    int
	closes     int
}

func (f *fakePartitionReader) SetOffset(offset int64) error {
	f.mu.Lock()
	f.setOffsets = append(f.setOffsets, offset)
	f.mu.Unlock()
	return f.setOffsetErr
}

func (f *fakePartitionReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	f.mu.Lock()
	f.fetches++
	f.mu.Unlock()
	if f.fetch != nil {
		return f.fetch(ctx)
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (f *fakePartitionReader) Close() error {
	f.mu.Lock()
	f.closes++
	f.mu.Unlock()
	return f.closeErr
}

func (f *fakePartitionReader) fetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetches
}

func (f *fakePartitionReader) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, nil))
}

// newFakeConsumer builds a real Consumer through NewConsumer, then swaps the broker-
// backed consumer group for a fake. The unroutable broker keeps the real group's run
// loop from reaching a Kafka that may be listening on the usual port.
func newFakeConsumer(t *testing.T, config Config) (*Consumer, *fakeConsumerGroup) {
	t.Helper()

	if config.Brokers == nil {
		config.Brokers = []string{"127.0.0.1:1"}
	}
	if config.GroupID == "" {
		config.GroupID = "test-group"
	}
	if config.Topic == "" {
		config.Topic = "t"
	}
	if config.Handler == nil {
		config.Handler = newTestMessageHandler()
	}
	if config.Logger == nil {
		config.Logger = testLogger(t)
	}

	c, err := NewConsumer(config)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	if err := c.cg.Close(); err != nil {
		t.Fatalf("closing the real consumer group: %v", err)
	}
	cg := &fakeConsumerGroup{}
	c.cg = cg

	t.Cleanup(func() {
		c.cancel()
		c.procCancel()
	})

	return c, cg
}

// newFailingCommitConsumer returns a consumer whose every commit fails, so the error
// branches around MarkDone become reachable.
func newFailingCommitConsumer(t *testing.T, config Config) (*Consumer, error) {
	t.Helper()

	config.MinOffsetCommitCount = 1
	config.MaxCommitRetries = 1
	config.CommitRetryDelay = time.Millisecond

	c, _ := newFakeConsumer(t, config)

	commitErr := errors.New("commit boom")
	c.offsetManager.commitFunc = func(context.Context, uint64, int, int64) error {
		return commitErr
	}
	c.offsetManager.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 0, Offset: kafka.FirstOffset}})

	return c, commitErr
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError error
	}{
		{
			name: "valid config",
			config: Config{
				Brokers: []string{"localhost:9092"},
				GroupID: "test-group",
				Topic:   "test-topic",
				Handler: newTestMessageHandler(),
			},
			expectError: nil,
		},
		{
			name: "missing brokers",
			config: Config{
				GroupID: "test-group",
				Topic:   "test-topic",
				Handler: newTestMessageHandler(),
			},
			expectError: ErrNoBrokers,
		},
		{
			name: "missing group ID",
			config: Config{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-topic",
				Handler: newTestMessageHandler(),
			},
			expectError: ErrNoGroupID,
		},
		{
			name: "missing topics",
			config: Config{
				Brokers: []string{"localhost:9092"},
				GroupID: "test-group",
				Handler: newTestMessageHandler(),
			},
			expectError: ErrNoTopic,
		},
		{
			name: "missing handler",
			config: Config{
				Brokers: []string{"localhost:9092"},
				GroupID: "test-group",
				Topic:   "test-topic",
			},
			expectError: ErrNoHandler,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if !errors.Is(err, tt.expectError) {
				t.Errorf("Expected error %v, got %v", tt.expectError, err)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	config := Config{
		Brokers: []string{"localhost:9092"},
		GroupID: "test-group",
		Topic:   "test-topic",
		Handler: newTestMessageHandler(),
	}

	config.applyDefaults()

	if config.MaxInFlight != 1000 {
		t.Errorf("Expected default MaxInFlight to be 1000, got %d", config.MaxInFlight)
	}

	if config.MinOffsetCommitCount != 5 {
		t.Errorf("Expected default MinOffsetCommitCount to be 5, got %d", config.MinOffsetCommitCount)
	}

	if config.MaxCommitInterval != 5*time.Second {
		t.Errorf("Expected default MaxCommitInterval to be 5s, got %v", config.MaxCommitInterval)
	}

	if config.ForceCommitInterval != 5*time.Second {
		t.Errorf("Expected default ForceCommitInterval to be 5s, got %v", config.ForceCommitInterval)
	}

	if config.ShutdownTimeout != 30*time.Second {
		t.Errorf("Expected default ShutdownTimeout to be 30s, got %v", config.ShutdownTimeout)
	}

	if config.MaxBytes != 10e6 {
		t.Errorf("Expected default MaxBytes to be 10e6, got %d", config.MaxBytes)
	}

	if config.RetryCount != 0 {
		t.Errorf("Expected default RetryCount to be 0, got %d", config.RetryCount)
	}

	if config.RetryDelay != 500*time.Millisecond {
		t.Errorf("Expected default RetryDelay to be 500ms, got %v", config.RetryDelay)
	}

	// These moved out of kafka.Reader, so the defaults must match what it applied.
	if config.SessionTimeout != 30*time.Second {
		t.Errorf("Expected default SessionTimeout to be 30s, got %v", config.SessionTimeout)
	}

	if config.RebalanceTimeout != 30*time.Second {
		t.Errorf("Expected default RebalanceTimeout to be 30s, got %v", config.RebalanceTimeout)
	}

	if config.HeartbeatInterval != 3*time.Second {
		t.Errorf("Expected default HeartbeatInterval to be 3s, got %v", config.HeartbeatInterval)
	}

	if config.PartitionWatchInterval != 5*time.Second {
		t.Errorf("Expected default PartitionWatchInterval to be 5s, got %v", config.PartitionWatchInterval)
	}

	if config.WatchPartitionChanges {
		t.Error("Expected WatchPartitionChanges to default to false")
	}

	// kafka.ConsumerGroupConfig defaults StartOffset to FirstOffset, so a dropped
	// mapping would silently flip new groups from "latest" to "earliest".
	if config.AutoOffsetReset != kafka.LastOffset {
		t.Errorf("Expected default AutoOffsetReset to be LastOffset, got %d", config.AutoOffsetReset)
	}
}

func TestTestMessageHandler(t *testing.T) {
	handler := newTestMessageHandler()

	msg := createTestMessage("test-topic", 0, 1, "key1", "value1")
	err := handler.HandleMessage(context.Background(), msg)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if handler.GetProcessedCount() != 1 {
		t.Errorf("Expected processed count to be 1, got %d", handler.GetProcessedCount())
	}

	handler.SetErrorOnMessage(1, errors.New("test error"))
	msg2 := createTestMessage("test-topic", 0, 2, "key2", "value2")
	err = handler.HandleMessage(context.Background(), msg2)
	if err == nil {
		t.Error("Expected error, got nil")
	}

	if handler.GetErrorCount() != 1 {
		t.Errorf("Expected error count to be 1, got %d", handler.GetErrorCount())
	}

	handler.SetErrorOnKey("key3", errors.New("key error"))
	msg3 := createTestMessage("test-topic", 0, 3, "key3", "value3")
	err = handler.HandleMessage(context.Background(), msg3)
	if err == nil {
		t.Error("Expected error, got nil")
	}

	handler.SetProcessingDelay(100 * time.Millisecond)
	start := time.Now()
	msg4 := createTestMessage("test-topic", 0, 4, "key4", "value4")
	_ = handler.HandleMessage(context.Background(), msg4)
	elapsed := time.Since(start)

	if elapsed < 100*time.Millisecond {
		t.Errorf("Expected processing delay of at least 100ms, got %v", elapsed)
	}

	handler.Reset()
	if handler.GetProcessedCount() != 0 {
		t.Errorf("Expected processed count to be 0 after reset, got %d", handler.GetProcessedCount())
	}
}

func TestTestDeadLetterPersister(t *testing.T) {
	persister := newTestDeadLetterPersister()
	ctx := context.Background()

	msg := createTestMessage("test-topic", 0, 1, "key1", "value1")
	err := persister.Save(ctx, msg, errors.New("test error"), "key1")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if persister.GetSavedCount() != 1 {
		t.Errorf("Expected saved count to be 1, got %d", persister.GetSavedCount())
	}

	saved := persister.GetSavedMessages()
	if len(saved) != 1 {
		t.Errorf("Expected 1 message, got %d", len(saved))
	}

	_ = persister.Save(ctx, msg, errors.New("test error"), "key1")
	persister.Reset()
	if persister.GetSavedCount() != 0 {
		t.Errorf("Expected saved count to be 0 after reset, got %d", persister.GetSavedCount())
	}
}

func TestCreateTestMessageHelper(t *testing.T) {
	msg := createTestMessage("test-topic", 0, 1, "key1", "value1")

	if msg.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got %s", msg.Topic)
	}

	if msg.Partition != 0 {
		t.Errorf("Expected partition 0, got %d", msg.Partition)
	}

	if msg.Offset != 1 {
		t.Errorf("Expected offset 1, got %d", msg.Offset)
	}

	if string(msg.Key) != "key1" {
		t.Errorf("Expected key 'key1', got %s", string(msg.Key))
	}

	if string(msg.Value) != "value1" {
		t.Errorf("Expected value 'value1', got %s", string(msg.Value))
	}
}

func TestCreateTestMessagesHelper(t *testing.T) {
	messages := createTestMessages("test-topic", 0, 10, "key", "value")

	if len(messages) != 10 {
		t.Errorf("Expected 10 messages, got %d", len(messages))
	}

	for i, msg := range messages {
		expectedKey := fmt.Sprintf("key-%d", i)
		expectedValue := fmt.Sprintf("value-%d", i)

		if string(msg.Key) != expectedKey {
			t.Errorf("Expected key '%s', got %s", expectedKey, string(msg.Key))
		}

		if string(msg.Value) != expectedValue {
			t.Errorf("Expected value '%s', got %s", expectedValue, string(msg.Value))
		}

		if msg.Offset != int64(i) {
			t.Errorf("Expected offset %d, got %d", i, msg.Offset)
		}
	}
}

func TestWaitForConditionHelper(t *testing.T) {
	result := waitForCondition(1*time.Second, 10*time.Millisecond, func() bool {
		return true
	})

	if !result {
		t.Error("Expected condition to succeed immediately")
	}

	counter := 0
	result = waitForCondition(1*time.Second, 10*time.Millisecond, func() bool {
		counter++
		return counter >= 5
	})

	if !result {
		t.Error("Expected condition to succeed eventually")
	}

	result = waitForCondition(100*time.Millisecond, 10*time.Millisecond, func() bool {
		return false
	})

	if result {
		t.Error("Expected condition to timeout")
	}
}

func TestHandlerPanicRecovery(t *testing.T) {
	handler := newTestMessageHandler()
	handler.SetPanicOnMessage(0)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic to occur")
		}
	}()

	msg := createTestMessage("test-topic", 0, 1, "key1", "value1")
	_ = handler.HandleMessage(context.Background(), msg)
}

func TestContextCancellation(t *testing.T) {
	handler := newTestMessageHandler()
	handler.SetProcessingDelay(5 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg := createTestMessage("test-topic", 0, 1, "key1", "value1")
	err := handler.HandleMessage(ctx, msg)

	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
}

func TestKeyExtractor(t *testing.T) {
	handler := newTestMessageHandler()

	key := handler.GetKey([]byte("key1"), []byte("value1"))
	if key != "key1" {
		t.Errorf("Expected key 'key1', got %s", key)
	}

	handler.SetKeyExtractor(func(key []byte, value []byte) string {
		return string(value)
	})

	key = handler.GetKey([]byte("key1"), []byte("value1"))
	if key != "value1" {
		t.Errorf("Expected key 'value1', got %s", key)
	}

	handler.SetKeyExtractor(func(key []byte, value []byte) string {
		return ""
	})

	key = handler.GetKey([]byte("key1"), []byte("value1"))
	if key != "" {
		t.Errorf("Expected empty key, got %s", key)
	}
}

func TestMessageProcessingHook(t *testing.T) {
	handler := newTestMessageHandler()

	hookCalled := false
	var hookedMessage kafka.Message

	handler.SetOnProcessMessage(func(msg kafka.Message) {
		hookCalled = true
		hookedMessage = msg
	})

	msg := createTestMessage("test-topic", 0, 1, "key1", "value1")
	err := handler.HandleMessage(context.Background(), msg)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !hookCalled {
		t.Error("Expected hook to be called")
	}

	if string(hookedMessage.Key) != "key1" {
		t.Errorf("Expected hooked message key 'key1', got %s", string(hookedMessage.Key))
	}
}

func TestDeadLetterPersisterCallbacks(t *testing.T) {
	persister := newTestDeadLetterPersister()

	saveCalled := false

	persister.SetSaveCallback(func(ctx context.Context, msg kafka.Message, err error, key string) error {
		saveCalled = true
		return nil
	})

	ctx := context.Background()
	msg := createTestMessage("test-topic", 0, 1, "key1", "value1")

	_ = persister.Save(ctx, msg, errors.New("test error"), "key1")
	if !saveCalled {
		t.Error("Expected save callback to be called")
	}
}

func TestNewConsumer_RejectsInvalidKafkaConfig(t *testing.T) {
	// Negative durations survive applyDefaults (it only fills zero values) and
	// turnstile's own Validate, so kafka's own validation is the one that must fire.
	_, err := NewConsumer(Config{
		Brokers:        []string{"127.0.0.1:1"},
		GroupID:        "g",
		Topic:          "t",
		Handler:        newTestMessageHandler(),
		Logger:         testLogger(t),
		SessionTimeout: -1,
	})
	if err == nil {
		t.Fatal("expected NewConsumer to reject a negative SessionTimeout")
	}
	if !strings.Contains(err.Error(), "failed to create consumer group") {
		t.Fatalf("expected the consumer group creation error to be wrapped, got %v", err)
	}
}

func TestNewConsumer_RejectsInvalidConfig(t *testing.T) {
	_, err := NewConsumer(Config{
		GroupID: "g",
		Topic:   "t",
		Handler: newTestMessageHandler(),
		Logger:  testLogger(t),
	})
	if !errors.Is(err, ErrNoBrokers) {
		t.Fatalf("expected ErrNoBrokers, got %v", err)
	}
}

// The commit path resolves an epoch against the live generation; without one, every
// commit must be abandoned rather than sent under whatever generation is current.
func TestCommitFunc_StaleGeneration(t *testing.T) {
	c, _ := newFakeConsumer(t, Config{})

	err := c.offsetManager.commitFunc(context.Background(), 1, 0, 5)
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected ErrStaleGeneration with no live generation, got %v", err)
	}

	// A generation exists, but under a different epoch.
	gen := &kafka.Generation{ID: 7}
	epoch := c.beginGeneration(gen)
	if got, ok := c.generationFor(epoch); !ok || got != gen {
		t.Fatalf("generationFor(%d) = (%v, %v), want the generation just begun", epoch, got, ok)
	}
	if err := c.offsetManager.commitFunc(context.Background(), epoch+1, 0, 5); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected ErrStaleGeneration for a superseded epoch, got %v", err)
	}
}

func TestStart_RejectsSecondCall(t *testing.T) {
	c, _ := newFakeConsumer(t, Config{})

	if err := c.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := c.Start(); err == nil {
		t.Fatal("expected the second Start to fail")
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Stop is idempotent, and a second call must not close the group again.
	if err := c.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

// A failure to join is transient — the loop must keep trying rather than fall out and
// leave the consumer silently idle. cg.run applies JoinGroupBackoff internally, so the
// retry cannot spin here.
func TestRunGenerations_RetriesAfterJoinFailure(t *testing.T) {
	c, cg := newFakeConsumer(t, Config{})

	var calls sync.WaitGroup
	calls.Add(2)

	var attempts int
	var mu sync.Mutex
	cg.next = func(ctx context.Context) (*kafka.Generation, error) {
		mu.Lock()
		attempts++
		first := attempts == 1
		mu.Unlock()

		calls.Done()
		if first {
			return nil, errors.New("join boom")
		}
		<-ctx.Done()
		return nil, kafka.ErrGroupClosed
	}

	if err := c.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	joined := make(chan struct{})
	go func() {
		calls.Wait()
		close(joined)
	}()

	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		mu.Lock()
		got := attempts
		mu.Unlock()
		t.Fatalf("Next called %d times after a join failure, want the loop to retry", got)
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStop_ReturnsConsumerGroupCloseError(t *testing.T) {
	c, cg := newFakeConsumer(t, Config{})
	closeErr := errors.New("close boom")
	cg.closeErr = closeErr

	if err := c.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := c.Stop(); !errors.Is(err, closeErr) {
		t.Fatalf("expected Stop to surface the group close error, got %v", err)
	}
	if got := cg.closeCount(); got != 1 {
		t.Fatalf("consumer group closed %d times, want 1", got)
	}
}

// A reader that cannot be positioned must not be polled, and its Close error is
// reported rather than swallowed silently.
func TestFetchPartition_SetOffsetFailureSkipsFetching(t *testing.T) {
	c, _ := newFakeConsumer(t, Config{})

	r := &fakePartitionReader{
		setOffsetErr: errors.New("set offset boom"),
		closeErr:     errors.New("close boom"),
	}
	c.newReader = func(kafka.PartitionAssignment) partitionReader { return r }

	c.fetchPartition(context.Background(), kafka.PartitionAssignment{ID: 3, Offset: kafka.FirstOffset})

	if got := r.fetchCount(); got != 0 {
		t.Fatalf("fetched %d times after SetOffset failed, want 0", got)
	}
	if got := r.closeCount(); got != 1 {
		t.Fatalf("reader closed %d times, want 1", got)
	}
}

func TestFetchLoop_StopsWhenBackpressureAcquireIsCanceled(t *testing.T) {
	c, _ := newFakeConsumer(t, Config{MaxInFlight: 1})

	// Saturate the controller so the loop's Acquire has to wait for the context.
	if err := c.backpressure.Acquire(context.Background()); err != nil {
		t.Fatalf("priming Acquire: %v", err)
	}

	r := &fakePartitionReader{
		fetch: func(context.Context) (kafka.Message, error) {
			t.Error("FetchMessage called even though backpressure was never acquired")
			return kafka.Message{}, errors.New("unreachable")
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	defer cancel()

	c.fetchLoop(ctx, r, 0)

	if got := r.fetchCount(); got != 0 {
		t.Fatalf("fetched %d times, want 0", got)
	}
}

// A failing broker must be retried with a growing backoff, and the wait must abort as
// soon as the fetch context ends.
func TestFetchLoop_BacksOffOnFetchErrorThenExitsOnCancel(t *testing.T) {
	c, _ := newFakeConsumer(t, Config{})

	r := &fakePartitionReader{
		fetch: func(context.Context) (kafka.Message, error) {
			return kafka.Message{}, errors.New("fetch boom")
		},
	}

	// The first backoff is 250-375ms, the second 500-750ms, so cancelling at 600ms
	// lands inside the second wait no matter how the jitter falls.
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(600*time.Millisecond, cancel)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.fetchLoop(ctx, r, 0)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fetchLoop did not return after its context was canceled")
	}

	if got := r.fetchCount(); got < 2 {
		t.Fatalf("fetched %d times, want at least 2 (one per backoff round)", got)
	}
}

// Under OverflowBlock a full key queue parks the fetch loop; cancelling the fetch
// context has to unwind it without leaking the backpressure slot.
func TestFetchLoop_ExitsWhenBlockedSubmitIsCanceled(t *testing.T) {
	c, _ := newFakeConsumer(t, Config{
		MaxInFlight:     4,
		MaxQueuedPerKey: 1,
		OverflowPolicy:  OverflowBlock,
	})

	// One message holds the key, a second fills its single queue slot, so the loop's
	// submit has nowhere to put a third.
	if acquired, _, err := c.keySequencer.submit(context.Background(), createTestMessage("t", 0, 0, "k", "v"), "k"); err != nil || !acquired {
		t.Fatalf("submit(0) = (%v, %v), want the key acquired", acquired, err)
	}
	if acquired, _, err := c.keySequencer.submit(context.Background(), createTestMessage("t", 0, 1, "k", "v"), "k"); err != nil || acquired {
		t.Fatalf("submit(1) = (%v, %v), want the message queued", acquired, err)
	}

	r := &fakePartitionReader{
		fetch: func(context.Context) (kafka.Message, error) {
			return createTestMessage("t", 0, 2, "k", "v"), nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.fetchLoop(ctx, r, 0)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fetchLoop stayed blocked on submit after its context was canceled")
	}

	// The slot taken for the message that never made it into the sequencer must have
	// been handed back, or in-flight capacity leaks one message per canceled submit.
	c.backpressure.cond.L.Lock()
	inFlight := c.backpressure.current
	c.backpressure.cond.L.Unlock()
	if inFlight != 0 {
		t.Fatalf("backpressure has %d slots held after the canceled submit, want 0", inFlight)
	}
}

func TestBackoffWithJitter(t *testing.T) {
	const maxBackoff = 5 * time.Second

	first := backoffWithJitter(0, maxBackoff)
	if first < 250*time.Millisecond || first >= 375*time.Millisecond {
		t.Fatalf("first backoff = %v, want [250ms, 375ms)", first)
	}

	second := backoffWithJitter(250*time.Millisecond, maxBackoff)
	if second < 500*time.Millisecond || second >= 750*time.Millisecond {
		t.Fatalf("doubled backoff = %v, want [500ms, 750ms)", second)
	}

	capped := backoffWithJitter(maxBackoff, maxBackoff)
	if capped < maxBackoff || capped >= maxBackoff+maxBackoff/2 {
		t.Fatalf("capped backoff = %v, want [%v, %v)", capped, maxBackoff, maxBackoff+maxBackoff/2)
	}
}

func TestConsumeFromKeySequencer_StopsOnProcessingContextCancel(t *testing.T) {
	c, _ := newFakeConsumer(t, Config{})
	c.procCancel()

	done := make(chan struct{})
	c.wg.Add(1)
	go func() {
		defer close(done)
		c.consumeFromKeySequencer()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumeFromKeySequencer ignored its canceled processing context")
	}
}

// Draining the sequencer still has to respect MaxInFlight, and a cancel while waiting
// for a slot must end the loop rather than dispatch the message anyway.
func TestConsumeFromKeySequencer_StopsWhenBackpressureAcquireIsCanceled(t *testing.T) {
	handler := newTestMessageHandler()
	c, _ := newFakeConsumer(t, Config{MaxInFlight: 1, Handler: handler})

	if err := c.backpressure.Acquire(context.Background()); err != nil {
		t.Fatalf("priming Acquire: %v", err)
	}

	// Leave exactly one dequeueable message behind: acquire the key, queue behind it,
	// then release so the queued message becomes pending.
	if acquired, _, err := c.keySequencer.submit(context.Background(), createTestMessage("t", 0, 0, "k", "v"), "k"); err != nil || !acquired {
		t.Fatalf("submit(0) = (%v, %v), want the key acquired", acquired, err)
	}
	if acquired, _, err := c.keySequencer.submit(context.Background(), createTestMessage("t", 0, 1, "k", "v"), "k"); err != nil || acquired {
		t.Fatalf("submit(1) = (%v, %v), want the message queued", acquired, err)
	}
	c.keySequencer.release("k")

	done := make(chan struct{})
	c.wg.Add(1)
	go func() {
		defer close(done)
		c.consumeFromKeySequencer()
	}()

	time.AfterFunc(100*time.Millisecond, c.procCancel)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumeFromKeySequencer stayed blocked on backpressure after cancel")
	}

	if got := handler.GetProcessedCount(); got != 0 {
		t.Fatalf("handler ran %d times despite never getting a slot, want 0", got)
	}
}

func TestConsumeFromKeySequencer_StopsOnDrainSignal(t *testing.T) {
	c, _ := newFakeConsumer(t, Config{})

	done := make(chan struct{})
	c.wg.Add(1)
	go func() {
		defer close(done)
		c.consumeFromKeySequencer()
	}()

	c.seqDoneOnce.Do(func() { close(c.seqDone) })

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumeFromKeySequencer ignored the drain signal")
	}
}

func TestProcessMessage_LogsMarkDoneFailure(t *testing.T) {
	handler := newTestMessageHandler()
	c, _ := newFailingCommitConsumer(t, Config{UnOrdered: true, Handler: handler})

	msg := createTestMessage("t", 0, 0, "k", "v")
	c.offsetManager.Track(msg.Partition, msg.Offset)
	if err := c.backpressure.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	c.wg.Add(1)
	c.processMessage(msg, "k")

	// A failed commit must not stop the message from being processed or the in-flight
	// slot from coming back.
	if got := handler.GetProcessedCount(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
	c.backpressure.cond.L.Lock()
	inFlight := c.backpressure.current
	c.backpressure.cond.L.Unlock()
	if inFlight != 0 {
		t.Fatalf("backpressure has %d slots held, want 0", inFlight)
	}
}

// Retries must stop the moment Stop cancels the consumer, rather than sitting through
// the remaining RetryDelay for every attempt left.
func TestProcessMessage_AbandonsRetriesOnCancel(t *testing.T) {
	handler := newTestMessageHandler()
	handler.SetErrorOnKey("k", errors.New("handler boom"))

	c, _ := newFakeConsumer(t, Config{
		UnOrdered:  true,
		Handler:    handler,
		RetryCount: 3,
		RetryDelay: time.Hour,
	})
	c.cancel()

	if err := c.backpressure.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	start := time.Now()
	c.wg.Add(1)
	c.processMessage(createTestMessage("t", 0, 0, "k", "v"), "k")

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("processMessage waited %v before noticing the cancel", elapsed)
	}
	if got := handler.GetProcessedCount(); got != 1 {
		t.Fatalf("handler ran %d times, want 1 — retries should have been abandoned", got)
	}
}

// Both bookkeeping steps for a dropped message can fail independently; neither may
// stop the other from running.
func TestHandleOverflowDrop_SurvivesPersistAndMarkDoneFailures(t *testing.T) {
	dlq := newTestDeadLetterPersister()
	dlq.SetSaveCallback(func(context.Context, kafka.Message, error, string) error {
		return errors.New("persist boom")
	})

	c, _ := newFailingCommitConsumer(t, Config{
		UnOrdered:           true,
		OverflowPolicy:      OverflowDropNewest,
		DeadLetterPersister: dlq,
	})

	msg := createTestMessage("t", 0, 0, "k", "v")
	c.offsetManager.Track(msg.Partition, msg.Offset)
	c.handleOverflowDrop(msg, "k")

	// MarkDone still had to advance the watermark even though the commit failed.
	c.offsetManager.mu.RLock()
	s := c.offsetManager.partitions[0]
	c.offsetManager.mu.RUnlock()
	s.mu.Lock()
	canCommit := s.canCommit
	s.mu.Unlock()
	if canCommit != 0 {
		t.Fatalf("canCommit = %d after the drop, want 0", canCommit)
	}
}

func TestNewPartitionReader_UsesConfiguredFetchBounds(t *testing.T) {
	c, _ := newFakeConsumer(t, Config{
		MinBytes: 1,
		MaxBytes: 2048,
		MaxWait:  250 * time.Millisecond,
	})

	r := c.newPartitionReader(kafka.PartitionAssignment{ID: 2, Offset: kafka.FirstOffset})
	reader, ok := r.(*kafka.Reader)
	if !ok {
		t.Fatalf("newPartitionReader returned %T, want *kafka.Reader", r)
	}
	defer reader.Close()

	cfg := reader.Config()
	if cfg.Partition != 2 {
		t.Errorf("Partition = %d, want 2", cfg.Partition)
	}
	if cfg.MinBytes != 1 || cfg.MaxBytes != 2048 {
		t.Errorf("byte bounds = [%d, %d], want [1, 2048]", cfg.MinBytes, cfg.MaxBytes)
	}
	if cfg.MaxWait != 250*time.Millisecond {
		t.Errorf("MaxWait = %v, want 250ms", cfg.MaxWait)
	}
	// Per-partition readers each buffer independently, so the queue must stay at 1 or
	// prefetching multiplies MaxInFlight by the assignment count.
	if cfg.QueueCapacity != 1 {
		t.Errorf("QueueCapacity = %d, want 1", cfg.QueueCapacity)
	}
}
