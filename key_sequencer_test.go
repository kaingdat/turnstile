package turnstile

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// mustSubmit submits a message and fails the test if submit returns an error.
func mustSubmit(tb testing.TB, ks *keySequencer, msg kafka.Message, key string) bool {
	tb.Helper()
	acquired, _, err := ks.submit(context.Background(), msg, key)
	if err != nil {
		tb.Fatalf("submit(%q) returned an unexpected error: %v", key, err)
	}
	return acquired
}

func TestKeySequencer_PerKeyQueues_BasicFlow(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	if acquired, _, _ := ks.submit(context.Background(), createTestMessage("t", 0, 0, "a", "hold"), "a"); !acquired {
		t.Fatal("expected to acquire key 'a'")
	}

	if acquired, _, _ := ks.submit(context.Background(), createTestMessage("t", 0, 1, "a", "v1"), "a"); acquired {
		t.Fatal("expected message for 'a' to be queued, not acquired")
	}
	if acquired, _, _ := ks.submit(context.Background(), createTestMessage("t", 0, 2, "b", "v2"), "b"); !acquired {
		t.Fatal("expected to acquire key 'b'")
	}

	if ks.queuedMessageCount() != 1 {
		t.Fatalf("expected queue size 1, got %d", ks.queuedMessageCount())
	}

	ks.release("b")
	ks.release("a")

	msg, key, ok := ks.dequeue()
	if !ok {
		t.Fatal("expected to dequeue a message for key 'a'")
	}
	if key != "a" || msg.Offset != 1 {
		t.Fatalf("expected key='a' offset=1, got key='%s' offset=%d", key, msg.Offset)
	}

	ks.release("a")
}

func TestKeySequencer_ReadyChan_SignaledOnSubmit(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	mustSubmit(t, ks, createTestMessage("t", 0, 0, "k1", "hold"), "k1")
	mustSubmit(t, ks, createTestMessage("t", 0, 1, "k1", "v"), "k1")

	select {
	case <-ks.readyChan():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected readyChan to be signaled after queuing")
	}
}

func TestKeySequencer_ReadyChan_SignaledOnRelease(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	mustSubmit(t, ks, createTestMessage("t", 0, 0, "k1", "hold"), "k1")
	mustSubmit(t, ks, createTestMessage("t", 0, 1, "k1", "v"), "k1")

	// Drain the signal from submit.
	select {
	case <-ks.readyChan():
	default:
	}

	ks.release("k1")

	select {
	case <-ks.readyChan():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected readyChan to be signaled after release")
	}
}

func TestKeySequencer_PerKeyDepthLimit(t *testing.T) {
	ks := newKeySequencer(3, OverflowDropOldest)

	mustSubmit(t, ks, createTestMessage("t", 0, 0, "hot", "hold"), "hot")

	for i := 1; i <= 5; i++ {
		mustSubmit(t, ks, createTestMessage("t", 0, int64(i), "hot", "v"), "hot")
	}

	if ks.queuedMessageCount() != 3 {
		t.Fatalf("expected queue size 3 (depth limit), got %d", ks.queuedMessageCount())
	}

	// Oldest messages should be dropped — first dequeued should be offset 3.
	ks.release("hot")

	msg, _, ok := ks.dequeue()
	if !ok {
		t.Fatal("expected to dequeue")
	}
	if msg.Offset != 3 {
		t.Fatalf("expected offset 3 (oldest dropped), got %d", msg.Offset)
	}

	ks.release("hot")
}

func TestKeySequencer_EmptyKey_Bypass(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	if acquired, _, _ := ks.submit(context.Background(), createTestMessage("t", 0, 1, "", "v"), ""); !acquired {
		t.Fatal("empty key should always be acquired")
	}

	if ks.queuedMessageCount() != 0 {
		t.Fatalf("expected queue size 0, got %d", ks.queuedMessageCount())
	}
}

func TestKeySequencer_MaxQueuedPerKey_Zero_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for maxQueuedPerKey=0")
		}
	}()
	newKeySequencer(0, OverflowDropOldest)
}

func TestKeySequencer_MaxQueuedPerKey_One_Valid(t *testing.T) {
	ks := newKeySequencer(1, OverflowDropOldest)

	mustSubmit(t, ks, createTestMessage("t", 0, 0, "k", "hold"), "k")

	// Second submit should queue, dropping nothing (queue is empty before push).
	_, evicted, err := ks.submit(context.Background(), createTestMessage("t", 0, 1, "k", "v1"), "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if evicted != nil {
		t.Fatalf("expected no drop when queue goes from 0 to 1, got offset %d", evicted.Offset)
	}

	// Third submit should drop the oldest (offset=1) to stay at depth=1.
	_, evicted, err = ks.submit(context.Background(), createTestMessage("t", 0, 2, "k", "v2"), "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if evicted == nil {
		t.Fatal("expected drop when queue exceeds depth=1")
	}
	if evicted.Offset != 1 {
		t.Fatalf("expected the oldest queued message (offset 1) back, got offset %d", evicted.Offset)
	}

	if ks.droppedCount() != 1 {
		t.Fatalf("expected droppedCount=1, got %d", ks.droppedCount())
	}

	ks.release("k")
	msg, _, ok := ks.dequeue()
	if !ok || msg.Offset != 2 {
		t.Fatalf("expected offset 2 (surviving message), got %d ok=%v", msg.Offset, ok)
	}
}

func BenchmarkDequeue_PerKeyQueues(b *testing.B) {
	ks := newKeySequencer(1000, OverflowDropOldest)

	const numKeys = 50
	const msgsPerKey = 20

	for i := range numKeys {
		mustSubmit(b, ks, createTestMessage("t", 0, 0, keyName(i), "hold"), keyName(i))
	}

	for i := range numKeys {
		for j := range msgsPerKey {
			mustSubmit(b, ks, createTestMessage("t", 0, int64(j), keyName(i), "v"), keyName(i))
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keyName(i % numKeys)
		ks.release(key)

		_, _, ok := ks.dequeue()
		if ok {
			ks.release(key)
		}

		mustSubmit(b, ks, createTestMessage("t", 0, int64(i), key, "hold"), key)
		mustSubmit(b, ks, createTestMessage("t", 0, int64(i), key, "v"), key)
	}
}

func BenchmarkEnqueue_PerKeyQueues(b *testing.B) {
	ks := newKeySequencer(1000, OverflowDropOldest)

	for i := range 50 {
		mustSubmit(b, ks, createTestMessage("t", 0, 0, keyName(i), "hold"), keyName(i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keyName(i % 50)
		mustSubmit(b, ks, createTestMessage("t", 0, int64(i), key, "v"), key)
	}
}

func keyName(i int) string {
	return "key-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26))
}

// A key with queued messages stays locked after release, so a new message cannot jump
// the queue.
func TestKeySequencer_Release_KeepsKeyLocked(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	mustSubmit(t, ks, createTestMessage("t", 0, 0, "K", "hold"), "K")
	mustSubmit(t, ks, createTestMessage("t", 0, 1, "K", "v1"), "K")
	mustSubmit(t, ks, createTestMessage("t", 0, 2, "K", "v2"), "K")

	ks.release("K")

	if acquired, _, _ := ks.submit(context.Background(), createTestMessage("t", 0, 3, "K", "v3"), "K"); acquired {
		t.Fatal("submit should return false — key must stay locked while queue has messages")
	}

	msg, key, ok := ks.dequeue()
	if !ok {
		t.Fatal("expected to dequeue a message")
	}
	if key != "K" || msg.Offset != 1 {
		t.Fatalf("expected key='K' offset=1, got key='%s' offset=%d — FIFO ordering violated", key, msg.Offset)
	}

	ks.release("K")
	msg, _, ok = ks.dequeue()
	if !ok || msg.Offset != 2 {
		t.Fatalf("expected offset 2, got %d, ok=%v", msg.Offset, ok)
	}

	ks.release("K")
	msg, _, ok = ks.dequeue()
	if !ok || msg.Offset != 3 {
		t.Fatalf("expected offset 3, got %d, ok=%v", msg.Offset, ok)
	}

	ks.release("K")

	if acquired, _, _ := ks.submit(context.Background(), createTestMessage("t", 0, 4, "K", "new"), "K"); !acquired {
		t.Fatal("submit should succeed after all queued messages are drained")
	}
	ks.release("K")
}

// Stresses the release/submit boundary, where a lost ordering guarantee would show up
// as non-monotonic offsets within a key.
func TestKeySequencer_Release_FIFO_Under_Contention(t *testing.T) {
	ks := newKeySequencer(1000, OverflowDropOldest)

	const workers = 8
	const messagesPerWorker = 100

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Each worker submits its own key's offsets in order, so dequeues must match.
	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			key := keyName(id)
			for j := range messagesPerWorker {
				msg := createTestMessage("t", 0, int64(j), key, "v")
				if acquired, _, _ := ks.submit(context.Background(), msg, key); acquired {
					ks.release(key)
				}
			}
		}(w)
	}

	close(start)
	wg.Wait()

	// Drain remaining queued messages and collect per-key order.
	var (
		dequeuedMu      sync.Mutex
		dequeuedOffsets = make(map[string][]int64)
	)

	emptyTicks := 0
	for emptyTicks < 100 {
		msg, key, ok := ks.dequeue()
		if !ok {
			if ks.queuedMessageCount() == 0 {
				emptyTicks++
				runtime.Gosched()
				continue
			}
			runtime.Gosched()
			continue
		}
		emptyTicks = 0
		dequeuedMu.Lock()
		dequeuedOffsets[key] = append(dequeuedOffsets[key], msg.Offset)
		dequeuedMu.Unlock()
		ks.release(key)
	}

	// Assert FIFO: dequeued offsets for each key must be monotonically increasing.
	for key, offsets := range dequeuedOffsets {
		for i := 1; i < len(offsets); i++ {
			if offsets[i] <= offsets[i-1] {
				t.Errorf("key %q: offset[%d]=%d not > offset[%d]=%d — FIFO violated",
					key, i, offsets[i], i-1, offsets[i-1])
			}
		}
	}

	t.Logf("drained %d messages across %d keys", func() int {
		n := 0
		for _, v := range dequeuedOffsets {
			n += len(v)
		}
		return n
	}(), len(dequeuedOffsets))
}

func TestKeySequencer_Release_EmptyQueue_UnlocksKey(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	mustSubmit(t, ks, createTestMessage("t", 0, 0, "K", "hold"), "K")
	ks.release("K")

	if acquired, _, _ := ks.submit(context.Background(), createTestMessage("t", 0, 1, "K", "new"), "K"); !acquired {
		t.Fatal("submit should succeed after release with empty queue")
	}
	ks.release("K")
}

func TestKeySequencer_Release_EmptyKey_Noop(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	// Should be a no-op — no panic, no signal, no state change.
	ks.release("")

	if ks.queuedMessageCount() != 0 {
		t.Fatalf("expected queue size 0, got %d", ks.queuedMessageCount())
	}
	select {
	case <-ks.readyChan():
		t.Fatal("release(\"\") should not signal readyChan")
	default:
	}
}

// Drives dequeue's defensive branch for a pending key with no queue entries. The
// public API should never produce this, but the branch must release the handling slot
// rather than block forever.
func TestKeySequencer_Dequeue_PendingKeyWithEmptyQueue(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	// Construct the inconsistent state directly: pending + handled, nothing queued.
	ks.mu.Lock()
	ks.handlingKeys["ghost"] = struct{}{}
	ks.pendingKeys["ghost"] = struct{}{}
	ks.mu.Unlock()

	if _, _, ok := ks.dequeue(); ok {
		t.Fatal("expected dequeue to return false for pending key with empty queue")
	}

	ks.mu.Lock()
	_, stillHandling := ks.handlingKeys["ghost"]
	ks.mu.Unlock()
	if stillHandling {
		t.Fatal("expected ghost key to be removed from handlingKeys after dequeue")
	}
}

// Drives dequeue's fallback scan: a queued message for a key in neither handlingKeys
// nor pendingKeys. Unreachable via the public API, so the state is built directly.
func TestKeySequencer_Dequeue_FallbackScan(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	ks.mu.Lock()
	q := list.New()
	q.PushBack(messageWithKey{msg: createTestMessage("t", 0, 7, "orphan", "v"), key: "orphan"})
	ks.keyQueues["orphan"] = q
	ks.totalQueuedMessages = 1
	ks.mu.Unlock()

	msg, key, ok := ks.dequeue()
	if !ok {
		t.Fatal("expected fallback scan to dispatch the orphaned queued message")
	}
	if key != "orphan" || msg.Offset != 7 {
		t.Fatalf("expected key='orphan' offset=7, got key=%q offset=%d", key, msg.Offset)
	}

	ks.mu.Lock()
	_, handled := ks.handlingKeys["orphan"]
	_, queueExists := ks.keyQueues["orphan"]
	ks.mu.Unlock()
	if !handled {
		t.Fatal("expected fallback to mark key as handled")
	}
	if queueExists {
		t.Fatal("expected empty queue to be removed from keyQueues")
	}
}

func TestKeySequencer_Drain_EmptyQueueReturnsImmediately(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)
	ctx := context.Background()
	if err := ks.drain(ctx); err != nil {
		t.Fatalf("expected nil on empty queue, got %v", err)
	}
}

func TestKeySequencer_Drain_WaitsForDequeue(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	mustSubmit(t, ks, createTestMessage("t", 0, 0, "k", "hold"), "k")
	mustSubmit(t, ks, createTestMessage("t", 0, 1, "k", "v"), "k")

	// Drain in background; release the lock after a short pause so the queued
	// message can be dequeued.
	done := make(chan error, 1)
	go func() {
		done <- ks.drain(context.Background())
	}()

	// Let the goroutine start polling.
	time.Sleep(25 * time.Millisecond)

	// Dequeue the pending message so queue becomes empty.
	ks.release("k")
	_, _, ok := ks.dequeue()
	if !ok {
		t.Fatal("expected to dequeue")
	}
	ks.release("k")

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain returned error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("drain did not return after queue became empty")
	}
}

func TestKeySequencer_Drain_ContextCanceled(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	// Queue a message that is never dequeued.
	mustSubmit(t, ks, createTestMessage("t", 0, 0, "k", "hold"), "k")
	mustSubmit(t, ks, createTestMessage("t", 0, 1, "k", "v"), "k")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ks.drain(ctx)
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("drain did not return after context cancel")
	}
}

// pendingKeys entries must not accumulate across dequeues.
func TestKeySequencer_PendingKeys_ClearedOnDequeue(t *testing.T) {
	ks := newKeySequencer(10, OverflowDropOldest)

	mustSubmit(t, ks, createTestMessage("t", 0, 0, "A", "hold"), "A")
	mustSubmit(t, ks, createTestMessage("t", 0, 0, "B", "hold"), "B")
	mustSubmit(t, ks, createTestMessage("t", 0, 1, "A", "v"), "A")
	mustSubmit(t, ks, createTestMessage("t", 0, 2, "B", "v"), "B")

	ks.release("A")
	ks.release("B")

	msg, key, ok := ks.dequeue()
	if !ok {
		t.Fatal("expected to dequeue")
	}

	msg2, key2, ok2 := ks.dequeue()
	if !ok2 {
		t.Fatal("expected to dequeue second message")
	}

	offsets := map[int64]bool{msg.Offset: true, msg2.Offset: true}
	if !offsets[1] || !offsets[2] {
		t.Fatalf("expected offsets 1 and 2, got %d and %d", msg.Offset, msg2.Offset)
	}
	_ = key
	_ = key2

	ks.release("A")
	ks.release("B")

	if acquired, _, _ := ks.submit(context.Background(), createTestMessage("t", 0, 3, "A", "new"), "A"); !acquired {
		t.Fatal("key A should be unlocked")
	}
	ks.release("A")

	if acquired, _, _ := ks.submit(context.Background(), createTestMessage("t", 0, 3, "B", "new"), "B"); !acquired {
		t.Fatal("key B should be unlocked")
	}
	ks.release("B")
}

func TestKeySequencer_OverflowDropNewest_KeepsQueuedOrder(t *testing.T) {
	ks := newKeySequencer(2, OverflowDropNewest)

	mustSubmit(t, ks, createTestMessage("t", 0, 0, "k", "hold"), "k")
	mustSubmit(t, ks, createTestMessage("t", 0, 1, "k", "v1"), "k")
	mustSubmit(t, ks, createTestMessage("t", 0, 2, "k", "v2"), "k")

	// Queue is full, so the incoming message is the one discarded.
	_, evicted, err := ks.submit(context.Background(), createTestMessage("t", 0, 3, "k", "v3"), "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if evicted == nil {
		t.Fatal("expected the incoming message back as evicted")
	}
	if evicted.Offset != 3 {
		t.Fatalf("expected the incoming message (offset 3), got offset %d", evicted.Offset)
	}
	if ks.droppedCount() != 1 {
		t.Fatalf("expected droppedCount=1, got %d", ks.droppedCount())
	}

	// The already-queued messages keep their order and are untouched.
	if ks.queuedMessageCount() != 2 {
		t.Fatalf("expected queue size 2, got %d", ks.queuedMessageCount())
	}
	for _, want := range []int64{1, 2} {
		ks.release("k")
		msg, _, ok := ks.dequeue()
		if !ok || msg.Offset != want {
			t.Fatalf("expected offset %d, got %d ok=%v", want, msg.Offset, ok)
		}
	}
}

func TestKeySequencer_OverflowBlock_WaitsForRoom(t *testing.T) {
	ks := newKeySequencer(1, OverflowBlock)

	mustSubmit(t, ks, createTestMessage("t", 0, 0, "k", "hold"), "k")
	mustSubmit(t, ks, createTestMessage("t", 0, 1, "k", "v1"), "k")

	// The key queue is now full, so this submit must block rather than drop.
	done := make(chan error, 1)
	go func() {
		_, evicted, err := ks.submit(context.Background(), createTestMessage("t", 0, 2, "k", "v2"), "k")
		if evicted != nil {
			done <- fmt.Errorf("OverflowBlock must never evict, got offset %d", evicted.Offset)
			return
		}
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("submit returned early (err=%v) — it should have blocked on the full queue", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Freeing a slot must unblock it.
	ks.release("k")
	if _, _, ok := ks.dequeue(); !ok {
		t.Fatal("expected to dequeue the queued message")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("submit failed after room freed up: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("submit stayed blocked after a queue slot was freed")
	}

	if ks.droppedCount() != 0 {
		t.Fatalf("expected droppedCount=0 under OverflowBlock, got %d", ks.droppedCount())
	}
}

func TestKeySequencer_OverflowBlock_ContextCanceled(t *testing.T) {
	ks := newKeySequencer(1, OverflowBlock)

	mustSubmit(t, ks, createTestMessage("t", 0, 0, "k", "hold"), "k")
	mustSubmit(t, ks, createTestMessage("t", 0, 1, "k", "v1"), "k")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := ks.submit(ctx, createTestMessage("t", 0, 2, "k", "v2"), "k")
		done <- err
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked submit did not return after context cancel")
	}

	// The message must not have been queued behind the caller's back.
	if ks.queuedMessageCount() != 1 {
		t.Fatalf("expected queue size 1, got %d", ks.queuedMessageCount())
	}
}

// The fallback scan exists for liveness, so it must tolerate the queue bookkeeping it
// is there to backstop — including a key whose queue was emptied but not deleted.
func TestDequeue_FallbackSkipsEmptyQueues(t *testing.T) {
	ks := newKeySequencer(2, OverflowBlock)

	ks.keyQueues["a"] = list.New()
	ks.keyQueues["b"] = list.New()

	if _, _, ok := ks.dequeue(); ok {
		t.Fatal("dequeue returned a message from empty queues")
	}

	q := list.New()
	q.PushBack(messageWithKey{msg: createTestMessage("t", 0, 5, "c", "v"), key: "c"})
	ks.keyQueues["c"] = q
	ks.totalQueuedMessages = 1

	msg, key, ok := ks.dequeue()
	if !ok {
		t.Fatal("dequeue skipped a non-empty queue behind the empty ones")
	}
	if key != "c" || msg.Offset != 5 {
		t.Fatalf("dequeued (%q, offset %d), want (\"c\", offset 5)", key, msg.Offset)
	}
}
