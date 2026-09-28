package turnstile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type testMessageHandler struct {
	mu             sync.Mutex
	errorOnKey     map[string]error
	processedCount atomic.Int64
	errorCount     atomic.Int64
}

func newTestMessageHandler() *testMessageHandler {
	return &testMessageHandler{
		errorOnKey: make(map[string]error),
	}
}

func (h *testMessageHandler) HandleMessage(ctx context.Context, message kafka.Message) error {
	h.processedCount.Add(1)

	h.mu.Lock()
	err, failing := h.errorOnKey[string(message.Key)]
	h.mu.Unlock()

	if failing {
		h.errorCount.Add(1)
		return err
	}
	return nil
}

func (h *testMessageHandler) GetKey(key []byte, value []byte) string {
	return string(key)
}

func (h *testMessageHandler) GetProcessedCount() int64 {
	return h.processedCount.Load()
}

func (h *testMessageHandler) SetErrorOnKey(key string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errorOnKey[key] = err
}

type testDeadLetterPersister struct {
	mu            sync.Mutex
	savedMessages []DeadLetterMessage
}

func newTestDeadLetterPersister() *testDeadLetterPersister {
	return &testDeadLetterPersister{
		savedMessages: make([]DeadLetterMessage, 0),
	}
}

func (p *testDeadLetterPersister) Save(ctx context.Context, message kafka.Message, err error, key string) error {
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

func (p *testDeadLetterPersister) GetSavedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.savedMessages)
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

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) { return len(p), nil }

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

	err := c.offsetManager.commitFunc(context.Background(), 1, map[int]int64{0: 5})
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected ErrStaleGeneration with no live generation, got %v", err)
	}

	// A generation exists, but under a different epoch.
	gen := &kafka.Generation{ID: 7}
	c.genMu.Lock()
	c.curEpoch++
	c.curGen = gen
	epoch := c.curEpoch
	c.genMu.Unlock()
	if err := c.offsetManager.commitFunc(context.Background(), epoch+1, map[int]int64{0: 5}); !errors.Is(err, ErrStaleGeneration) {
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
	c.newReader = func(context.Context, kafka.PartitionAssignment) partitionReader { return r }

	c.fetchPartition(context.Background(), kafka.PartitionAssignment{ID: 3, Offset: kafka.FirstOffset})

	if got := r.fetchCount(); got != 0 {
		t.Fatalf("fetched %d times after SetOffset failed, want 0", got)
	}
	if got := r.closeCount(); got != 1 {
		t.Fatalf("reader closed %d times, want 1", got)
	}
}

func TestFetchPartition_StopsWhenBackpressureAcquireIsCanceled(t *testing.T) {
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
	c.newReader = func(context.Context, kafka.PartitionAssignment) partitionReader { return r }

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	defer cancel()

	c.fetchPartition(ctx, kafka.PartitionAssignment{ID: 0})

	if got := r.fetchCount(); got != 0 {
		t.Fatalf("fetched %d times, want 0", got)
	}
}

// A failing broker must be retried with a growing backoff, and the wait must abort as
// soon as the fetch context ends.
func TestFetchPartition_BacksOffOnFetchErrorThenExitsOnCancel(t *testing.T) {
	c, _ := newFakeConsumer(t, Config{})

	r := &fakePartitionReader{
		fetch: func(context.Context) (kafka.Message, error) {
			return kafka.Message{}, errors.New("fetch boom")
		},
	}
	c.newReader = func(context.Context, kafka.PartitionAssignment) partitionReader { return r }

	// The first backoff is 250-375ms, the second 500-750ms, so cancelling at 600ms
	// lands inside the second wait no matter how the jitter falls.
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(600*time.Millisecond, cancel)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.fetchPartition(ctx, kafka.PartitionAssignment{ID: 0})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fetchPartition did not return after its context was canceled")
	}

	if got := r.fetchCount(); got < 2 {
		t.Fatalf("fetched %d times, want at least 2 (one per backoff round)", got)
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
	if acquired := c.keySequencer.submit(createTestMessage("t", 0, 0, "k", "v"), "k"); !acquired {
		t.Fatalf("submit(0) = %v, want the key acquired", acquired)
	}
	if acquired := c.keySequencer.submit(createTestMessage("t", 0, 1, "k", "v"), "k"); acquired {
		t.Fatalf("submit(1) = %v, want the message queued", acquired)
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

	c.seqCancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumeFromKeySequencer ignored the drain signal")
	}
}

// Retries must stop the moment Stop cancels the consumer, rather than sitting through
// the remaining RetryDelay for every attempt left.
// A message whose retries are cut short by shutdown never got its full retry budget,
// so it must be left uncommitted for redelivery — not marked done or dead-lettered as
// if the handler had conclusively given up on it.
func TestProcessMessage_AbandonsRetriesOnCancel(t *testing.T) {
	handler := newTestMessageHandler()
	handler.SetErrorOnKey("k", errors.New("handler boom"))
	dlq := newTestDeadLetterPersister()

	c, _ := newFakeConsumer(t, Config{
		UnOrdered:           true,
		Handler:             handler,
		RetryCount:          3,
		RetryDelay:          time.Hour,
		DeadLetterPersister: dlq,
	})
	c.offsetManager.BeginEpoch(1, []kafka.PartitionAssignment{{ID: 0, Offset: kafka.FirstOffset}})

	msg := createTestMessage("t", 0, 0, "k", "v")
	c.offsetManager.Track(msg.Partition, msg.Offset)
	c.cancel()

	if err := c.backpressure.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	start := time.Now()
	c.wg.Add(1)
	c.processMessage(msg, "k")

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("processMessage waited %v before noticing the cancel", elapsed)
	}
	if got := handler.GetProcessedCount(); got != 1 {
		t.Fatalf("handler ran %d times, want 1 — retries should have been abandoned", got)
	}

	if got := dlq.GetSavedCount(); got != 0 {
		t.Fatalf("dead-lettered %d messages, want 0 — an abandoned message isn't a conclusive failure", got)
	}

	c.offsetManager.mu.RLock()
	s := c.offsetManager.partitions[0]
	c.offsetManager.mu.RUnlock()
	s.mu.Lock()
	inFlight := slices.Clone(s.inFlight)
	s.mu.Unlock()
	if want := []inFlightOffset{{offset: 0}}; !slices.Equal(inFlight, want) {
		t.Fatalf("inFlight = %v, want %v — abandoned offset must stay uncommitted", inFlight, want)
	}
}

func TestNewReader_UsesConfiguredFetchBounds(t *testing.T) {
	c, _ := newFakeConsumer(t, Config{
		MinBytes: 1,
		MaxBytes: 2048,
		MaxWait:  250 * time.Millisecond,
	})

	r := c.newReader(context.Background(), kafka.PartitionAssignment{ID: 2, Offset: kafka.FirstOffset})
	reader, ok := r.(*kafka.Reader)
	if !ok {
		t.Fatalf("newReader returned %T, want *kafka.Reader", r)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("closing reader: %v", err)
		}
	}()

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
	if cfg.ErrorLogger == nil {
		t.Error("ErrorLogger is nil — a reader's internal retries would fail silently")
	}
	if cfg.ReadLagInterval >= 0 {
		t.Errorf("ReadLagInterval = %v, want negative to disable the lag poller", cfg.ReadLagInterval)
	}
}

func TestReaderErrorLogger_DowngradesOnceFetchContextIsDone(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithCancel(context.Background())
	l := readerErrorLogger(ctx, logger, "t", 7)

	l.Printf("failed to dial %s", "broker")
	if got := buf.String(); !strings.Contains(got, "level=ERROR") || !strings.Contains(got, "partition=7") {
		t.Errorf("live fetch context logged %q, want an ERROR carrying the partition", got)
	}

	buf.Reset()
	cancel()
	l.Printf("failed to dial %s", "broker")
	if got := buf.String(); !strings.Contains(got, "level=DEBUG") {
		t.Errorf("canceled fetch context logged %q, want DEBUG — teardown is not a fault", got)
	}
}
