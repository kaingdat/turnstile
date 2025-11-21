package turnstile

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

type messageWithKey struct {
	msg kafka.Message
	key string
}

// keySequencer prevents concurrent processing of messages with the same key.
type keySequencer struct {
	maxQueuedPerKey     int
	overflowPolicy      OverflowPolicy
	dropped             atomic.Int64
	mu                  sync.Mutex
	handlingKeys        map[string]struct{}
	keyQueues           map[string]*list.List
	pendingKeys         map[string]struct{}
	totalQueuedMessages int
	ready               chan struct{}
	// space is closed and replaced whenever a queued message leaves a key queue,
	// waking submitters blocked on a full queue under OverflowBlock.
	space chan struct{}
}

func newKeySequencer(maxQueuedPerKey int, overflowPolicy OverflowPolicy) *keySequencer {
	if maxQueuedPerKey < 1 {
		panic("turnstile: maxQueuedPerKey must be >= 1")
	}
	return &keySequencer{
		maxQueuedPerKey: maxQueuedPerKey,
		overflowPolicy:  overflowPolicy,
		handlingKeys:    make(map[string]struct{}),
		keyQueues:       make(map[string]*list.List),
		ready:           make(chan struct{}, 1),
		pendingKeys:     make(map[string]struct{}),
		space:           make(chan struct{}),
	}
}

// submit attempts to acquire the key for immediate processing:
//
//   - (true, nil, nil): key acquired; caller must process then call release.
//   - (false, nil, nil): key busy; message has been queued.
//   - (false, evicted, nil): the key's queue was full and the configured
//     OverflowPolicy discarded a message. The caller takes ownership of *evicted:
//     it will never be processed, so the caller must dead-letter it and mark its
//     offset done, otherwise the partition watermark stalls on the gap forever.
//   - (false, nil, err): OverflowBlock only — ctx was canceled while waiting for
//     room in the key's queue.
//
// Under OverflowBlock (the default) submit blocks rather than discarding anything.
// It holds no lock while blocked, and the sequencer is drained by a separate
// goroutine, so a blocked submitter cannot stall the drain that would unblock it.
func (ks *keySequencer) submit(ctx context.Context, msg kafka.Message, key string) (acquired bool, evicted *kafka.Message, err error) {
	// Empty keys bypass sequencing entirely, with no ordering guarantee.
	if key == "" {
		return true, nil, nil
	}

	for {
		ks.mu.Lock()

		if _, exists := ks.handlingKeys[key]; !exists {
			ks.handlingKeys[key] = struct{}{}
			ks.mu.Unlock()
			return true, nil, nil
		}

		q, ok := ks.keyQueues[key]
		if !ok {
			q = list.New()
			ks.keyQueues[key] = q
		}

		if q.Len() < ks.maxQueuedPerKey {
			q.PushBack(messageWithKey{msg: msg, key: key})
			ks.totalQueuedMessages++
			ks.mu.Unlock()
			ks.signal()
			return false, nil, nil
		}

		switch ks.overflowPolicy {
		case OverflowDropOldest:
			front := q.Front()
			oldest := front.Value.(messageWithKey).msg
			q.Remove(front)
			// The freed slot is refilled immediately, so no space signal is owed.
			q.PushBack(messageWithKey{msg: msg, key: key})
			ks.dropped.Add(1)
			ks.mu.Unlock()
			ks.signal()
			return false, &oldest, nil

		case OverflowDropNewest:
			// Queue state is untouched: the messages already accepted for this key
			// keep their order, and nothing needs signaling.
			ks.dropped.Add(1)
			ks.mu.Unlock()
			return false, &msg, nil
		}

		// OverflowBlock: snapshot the current space channel before releasing the
		// lock, then retry from the top — the key itself may have been released
		// by the time we wake.
		space := ks.space
		ks.mu.Unlock()

		select {
		case <-ctx.Done():
			return false, nil, ctx.Err()
		case <-space:
		}
	}
}

// releaseSpaceLocked wakes submitters blocked on a full key queue. Callers must hold ks.mu.
func (ks *keySequencer) releaseSpaceLocked() {
	close(ks.space)
	ks.space = make(chan struct{})
}

func (ks *keySequencer) release(key string) {
	if key == "" {
		return
	}

	ks.mu.Lock()

	if q, ok := ks.keyQueues[key]; ok && q.Len() > 0 {
		ks.pendingKeys[key] = struct{}{}
		ks.mu.Unlock()
		ks.signal()
		return
	}

	delete(ks.handlingKeys, key)

	ks.mu.Unlock()

	ks.signal()
}

// dequeue returns the next message whose key is not currently being processed,
// pending keys take priority to preserve FIFO order
func (ks *keySequencer) dequeue() (kafka.Message, string, bool) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	for key := range ks.pendingKeys {
		delete(ks.pendingKeys, key)

		q, ok := ks.keyQueues[key]
		if !ok || q.Len() == 0 {
			delete(ks.handlingKeys, key)
			continue
		}

		front := q.Front()
		mwk := front.Value.(messageWithKey)
		q.Remove(front)
		ks.totalQueuedMessages--
		if q.Len() == 0 {
			delete(ks.keyQueues, key)
		}
		ks.releaseSpaceLocked()
		return mwk.msg, mwk.key, true
	}

	// Fallback scan: pendingKeys should cover every release, but this guarantees
	// liveness if a queued message is ever left without a corresponding pending entry.
	for _, q := range ks.keyQueues {
		if q.Len() == 0 {
			continue
		}
		front := q.Front()
		mwk := front.Value.(messageWithKey)
		if _, exists := ks.handlingKeys[mwk.key]; !exists {
			ks.handlingKeys[mwk.key] = struct{}{}
			q.Remove(front)
			ks.totalQueuedMessages--
			if q.Len() == 0 {
				delete(ks.keyQueues, mwk.key)
			}
			ks.releaseSpaceLocked()
			return mwk.msg, mwk.key, true
		}
	}

	return kafka.Message{}, "", false
}

func (ks *keySequencer) readyChan() <-chan struct{} {
	return ks.ready
}

func (ks *keySequencer) signal() {
	select {
	case ks.ready <- struct{}{}:
	default:
	}
}

func (ks *keySequencer) drain(ctx context.Context) error {
	for {
		if ks.queuedMessageCount() == 0 {
			return nil
		}
		ks.signal()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (ks *keySequencer) queuedMessageCount() int {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	return ks.totalQueuedMessages
}

func (ks *keySequencer) droppedCount() int64 {
	return ks.dropped.Load()
}
