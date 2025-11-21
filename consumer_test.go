package turnstile

import (
	"context"
	"errors"
	"fmt"
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
