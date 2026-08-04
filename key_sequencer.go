package turnstile

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type messageWithKey struct {
	msg kafka.Message
	key string
}

// keySequencer prevents concurrent processing of messages with the same key. Each key's
// backlog is an unbounded queue: a busy key just keeps growing until it is released.
type keySequencer struct {
	mu                  sync.Mutex
	handlingKeys        map[string]struct{}
	keyQueues           map[string]*list.List
	pendingKeys         map[string]struct{}
	totalQueuedMessages int
	ready               chan struct{}
}

func newKeySequencer() *keySequencer {
	return &keySequencer{
		handlingKeys: make(map[string]struct{}),
		keyQueues:    make(map[string]*list.List),
		ready:        make(chan struct{}, 1),
		pendingKeys:  make(map[string]struct{}),
	}
}

// submit attempts to acquire the key for immediate processing. Returns true if the
// caller must process the message and call release; false if it was queued behind an
// in-flight message for the same key.
func (ks *keySequencer) submit(msg kafka.Message, key string) (acquired bool) {
	if key == "" {
		return true
	}

	ks.mu.Lock()
	defer ks.mu.Unlock()

	if _, exists := ks.handlingKeys[key]; !exists {
		ks.handlingKeys[key] = struct{}{}
		return true
	}

	q, ok := ks.keyQueues[key]
	if !ok {
		q = list.New()
		ks.keyQueues[key] = q
	}
	q.PushBack(messageWithKey{msg: msg, key: key})
	ks.totalQueuedMessages++
	ks.signal()
	return false
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

// dequeue blocks until a message becomes available or ctx is done.
func (ks *keySequencer) dequeue(ctx context.Context) (kafka.Message, string, bool) {
	for {
		if msg, key, ok := ks.tryDequeue(); ok {
			return msg, key, true
		}
		select {
		case <-ctx.Done():
			return kafka.Message{}, "", false
		case <-ks.ready:
		}
	}
}

// tryDequeue returns the next message whose key is not currently being
// processed, or ok=false if nothing is available right now. Pending keys
// take priority, which is what preserves FIFO order.
func (ks *keySequencer) tryDequeue() (kafka.Message, string, bool) {
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
		return mwk.msg, mwk.key, true
	}

	// Fallback scan, pendingKeys should cover every release; this guarantees liveness
	// if a queued message is ever left without a pending entry.
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
			return mwk.msg, mwk.key, true
		}
	}

	return kafka.Message{}, "", false
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
