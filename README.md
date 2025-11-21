# Turnstile

[![CI](https://github.com/datzero9/turnstile/actions/workflows/test.yml/badge.svg)](https://github.com/datzero9/turnstile/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/datzero9/turnstile/branch/main/graph/badge.svg)](https://codecov.io/gh/datzero9/turnstile)

Turnstile is a high-throughput Kafka consumer built on [`segmentio/kafka-go`](https://github.com/segmentio/kafka-go) that unlocks concurrency _within_ a partition while preserving per-key ordering. Kafka's native ordering guarantee is per-partition, which caps consumer parallelism at the partition count, Turnstile lifts that cap by running different keys in parallel while keeping messages that share a key strictly ordered.

## Installation

```bash
go get github.com/datzero9/turnstile
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/datzero9/turnstile"
)

type OrderHandler struct{}

func (h *OrderHandler) HandleMessage(ctx context.Context, msg turnstile.Message) error {
	fmt.Printf("Processing order: %s\n", string(msg.Value))
	return nil
}

func (h *OrderHandler) GetKey(key []byte, value []byte) string {
	// Messages with the same key are processed in order.
	// Messages with different keys are processed concurrently.
	return string(key)
}

func main() {
	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:     []string{"localhost:9092"},
		GroupID:     "order-service",
		Topic:       "orders",
		Handler:     &OrderHandler{},
		MaxInFlight: 500,
		RetryCount:  3,
	})
	if err != nil {
		log.Fatal(err)
	}

	consumer.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan

	consumer.Stop()
}
```

## Configuration

| Field                  | Default    | Description                                             |
| ---------------------- | ---------- | ------------------------------------------------------- |
| `Brokers`              | _required_ | Kafka broker addresses                                  |
| `GroupID`              | _required_ | Consumer group ID                                       |
| `Topic`                | _required_ | Topic to consume from                                   |
| `Handler`              | _required_ | Your `MessageHandler` implementation                    |
| `MaxInFlight`          | 1000       | Max concurrent messages being processed                 |
| `UnOrdered`            | false      | Set `true` to disable key ordering (max throughput)     |
| `MaxQueuedPerKey`      | 100        | Max queued messages per key before `OverflowPolicy` applies |
| `OverflowPolicy`       | `OverflowBlock` | What to do when a key's queue is full — see below  |
| `RetryCount`           | 0          | Retry attempts for failed `HandleMessage` calls         |
| `RetryDelay`           | 500ms      | Delay between retries                                   |
| `DeadLetterPersister`  | nil        | Optional handler for messages that exhaust all retries  |
| `AutoOffsetReset`      | LastOffset | Where to start if no committed offset exists            |
| `MinOffsetCommitCount` | 5          | Min messages before triggering an offset commit         |
| `MaxCommitInterval`    | 5s         | Max time between commits (when new offsets are ready)   |
| `ForceCommitInterval`  | 5s         | Interval for force-committing highest contiguous offset |
| `MaxCommitRetries`     | 50         | Max retries for offset commits                          |
| `CommitRetryDelay`     | 100ms      | Delay between commit retries                            |
| `MinBytes`             | 0          | Min bytes per Kafka fetch                               |
| `MaxBytes`             | 10MB       | Max bytes per Kafka fetch                               |
| `ShutdownTimeout`      | 30s        | Max wait for in-flight messages during shutdown         |

### Overflow policy

Messages for the same key are processed one at a time, so a slow key builds a queue.
`MaxQueuedPerKey` caps that queue, and `OverflowPolicy` decides what happens when the
cap is reached:

| Policy               | Behavior                                                                     |
| -------------------- | ---------------------------------------------------------------------------- |
| `OverflowBlock`      | **Default.** Pause fetching until the key's queue drains. Nothing is discarded, but a single hot key can slow consumption of the partitions it lives on. |
| `OverflowDropOldest` | Discard the oldest queued message for the key to make room for the new one.  |
| `OverflowDropNewest` | Discard the incoming message, leaving the already-queued messages in order.  |

The drop policies trade data loss for throughput. When one discards a message,
Turnstile hands it to your `DeadLetterPersister` (if configured) with
`ErrKeyQueueOverflow` — distinct from a handler error, so you can tell library-side
drops apart from processing failures — and marks its offset done so commits keep
advancing past it.

```go
cfg := turnstile.Config{
    // ...
    MaxQueuedPerKey:     500,
    OverflowPolicy:      turnstile.OverflowDropOldest,
    DeadLetterPersister: myPersister, // strongly recommended with a drop policy
}
```

## Interfaces

### MessageHandler

```go
type MessageHandler interface {
    HandleMessage(ctx context.Context, message Message) error
    GetKey(key []byte, value []byte) string
}
```

- **`HandleMessage`** — process a single message. Return an error to trigger retries.
- **`GetKey`** — extract a key for ordering. Messages with the same key are never processed concurrently. Return `""` to skip ordering for a specific message.

### DeadLetterPersister

```go
type DeadLetterPersister interface {
    Save(ctx context.Context, message Message, err error, key string) error
}
```

Implement this to capture messages that fail after all retry attempts. Store them in a database, file, or another Kafka topic for later inspection and replay.

## License

MIT
