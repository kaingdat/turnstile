package turnstile

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// newDropTestConsumer builds the minimal Consumer that handleOverflowDrop needs —
// a logger, a config, and a real offsetManager — without a Kafka reader.
func newDropTestConsumer(t *testing.T, dlq DeadLetterPersister) (*Consumer, func() int64) {
	t.Helper()

	var (
		mu        sync.Mutex
		committed = int64(-1)
	)
	commit := func(_ context.Context, msg kafka.Message) error {
		mu.Lock()
		defer mu.Unlock()
		if msg.Offset > committed {
			committed = msg.Offset
		}
		return nil
	}

	c := &Consumer{
		config: Config{
			Topic:               "t",
			OverflowPolicy:      OverflowDropOldest,
			DeadLetterPersister: dlq,
		},
		logger: slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelError})),
		offsetManager: newOffsetManager(offsetManagerConfig{
			topic:          "t",
			commitFunc:     commit,
			logger:         slog.Default(),
			minCommitCount: 1,
			maxInterval:    time.Millisecond,
			maxRetries:     1,
			retryDelay:     time.Millisecond,
		}),
	}

	return c, func() int64 {
		mu.Lock()
		defer mu.Unlock()
		return committed
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestOverflowDrop_DoesNotStallCommits is the regression test for the bug where a
// message evicted by the key sequencer never reached MarkDone. Because the offset
// watermark only advances over a contiguous run of done offsets, a single evicted
// offset used to freeze commits for the whole partition permanently and leak the
// inFlight entry. Offsets 1 and 2 are dropped here; the watermark must still reach 3.
func TestOverflowDrop_DoesNotStallCommits(t *testing.T) {
	dlq := newTestDeadLetterPersister()
	c, highestCommitted := newDropTestConsumer(t, dlq)

	// Every fetched message is tracked, whether or not it survives the sequencer.
	for offset := int64(0); offset <= 3; offset++ {
		c.offsetManager.Track(0, offset)
	}

	// Offsets 0 and 3 were processed; 1 and 2 were evicted on overflow.
	if err := c.offsetManager.MarkDone(context.Background(), 0, 0); err != nil {
		t.Fatalf("MarkDone(0): %v", err)
	}
	c.handleOverflowDrop(createTestMessage("t", 0, 1, "k", "v1"), "k")
	c.handleOverflowDrop(createTestMessage("t", 0, 2, "k", "v2"), "k")
	if err := c.offsetManager.MarkDone(context.Background(), 0, 3); err != nil {
		t.Fatalf("MarkDone(3): %v", err)
	}

	if got := highestCommitted(); got != 3 {
		t.Fatalf("highest committed offset = %d, want 3 — dropped offsets stalled the watermark", got)
	}

	// The inFlight map must drain too; leaving entries behind is an unbounded leak.
	c.offsetManager.mu.RLock()
	s := c.offsetManager.partitions[0]
	c.offsetManager.mu.RUnlock()
	s.mu.Lock()
	remaining := len(s.inFlight)
	s.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("inFlight has %d leftover entries after all work drained, want 0", remaining)
	}
}

// TestOverflowDrop_DeadLetters verifies dropped messages leave a record rather than
// vanishing silently, and that they are reported with ErrKeyQueueOverflow so callers
// can tell library-side drops apart from handler failures.
func TestOverflowDrop_DeadLetters(t *testing.T) {
	dlq := newTestDeadLetterPersister()
	c, _ := newDropTestConsumer(t, dlq)

	c.offsetManager.Track(0, 0)
	c.handleOverflowDrop(createTestMessage("t", 0, 0, "k", "v0"), "k")

	saved := dlq.GetSavedMessages()
	if len(saved) != 1 {
		t.Fatalf("expected 1 dead-lettered message, got %d", len(saved))
	}
	if saved[0].Message.Offset != 0 {
		t.Fatalf("dead-lettered the wrong message: offset %d", saved[0].Message.Offset)
	}
	if saved[0].Error != ErrKeyQueueOverflow.Error() {
		t.Fatalf("expected %q, got %q", ErrKeyQueueOverflow, saved[0].Error)
	}
	if saved[0].Key != "k" {
		t.Fatalf("expected key %q, got %q", "k", saved[0].Key)
	}
}

// TestOverflowDrop_NoPersisterStillAdvancesCommits makes sure the watermark fix does
// not depend on a DeadLetterPersister being configured.
func TestOverflowDrop_NoPersisterStillAdvancesCommits(t *testing.T) {
	c, highestCommitted := newDropTestConsumer(t, nil)
	c.config.DeadLetterPersister = nil

	c.offsetManager.Track(0, 0)
	c.offsetManager.Track(0, 1)
	c.handleOverflowDrop(createTestMessage("t", 0, 0, "k", "v0"), "k")
	if err := c.offsetManager.MarkDone(context.Background(), 0, 1); err != nil {
		t.Fatalf("MarkDone(1): %v", err)
	}

	if got := highestCommitted(); got != 1 {
		t.Fatalf("highest committed offset = %d, want 1", got)
	}
}

func TestConfig_Validate_OverflowPolicy(t *testing.T) {
	base := func() Config {
		return Config{
			Brokers: []string{"localhost:9092"},
			GroupID: "g",
			Topic:   "t",
			Handler: newTestMessageHandler(),
		}
	}

	t.Run("default is block", func(t *testing.T) {
		cfg := base()
		if cfg.OverflowPolicy != OverflowBlock {
			t.Fatalf("zero value should be OverflowBlock, got %v", cfg.OverflowPolicy)
		}
		cfg.applyDefaults()
		if cfg.OverflowPolicy != OverflowBlock {
			t.Fatalf("applyDefaults changed the policy to %v", cfg.OverflowPolicy)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected default config to validate, got %v", err)
		}
	})

	for _, p := range []OverflowPolicy{OverflowBlock, OverflowDropOldest, OverflowDropNewest} {
		cfg := base()
		cfg.OverflowPolicy = p
		if err := cfg.Validate(); err != nil {
			t.Fatalf("policy %v should be valid, got %v", p, err)
		}
		if p.String() == "unknown" {
			t.Fatalf("policy %d has no String() name", p)
		}
	}

	for _, p := range []OverflowPolicy{-1, 3} {
		cfg := base()
		cfg.OverflowPolicy = p
		err := cfg.Validate()
		if !errors.Is(err, ErrInvalidOverflowPolicy) {
			t.Fatalf("policy %d: expected ErrInvalidOverflowPolicy, got %v", p, err)
		}
	}
}
