# Turnstile

[![CI](https://github.com/kaingdat/turnstile/actions/workflows/test.yml/badge.svg)](https://github.com/kaingdat/turnstile/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/kaingdat/turnstile/branch/main/graph/badge.svg)](https://codecov.io/gh/kaingdat/turnstile)
[![Go Reference](https://pkg.go.dev/badge/github.com/kaingdat/turnstile.svg)](https://pkg.go.dev/github.com/kaingdat/turnstile)

Turnstile is a Kafka consumer library for Go, built on [`segmentio/kafka-go`](https://github.com/segmentio/kafka-go), that processes messages from a partition concurrently while keeping messages that share a key in order.

Kafka orders messages per partition, so a consumer that must preserve order is limited to one worker per partition. Turnstile serializes only messages that share an ordering key you choose, and runs everything else in parallel. A topic with 6 partitions can keep hundreds of messages in flight, and per-key order still holds.

Requires Go 1.23 or later.

## Installation

```bash
go get github.com/kaingdat/turnstile
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kaingdat/turnstile"
)

type OrderHandler struct{}

func (h *OrderHandler) HandleMessage(ctx context.Context, msg turnstile.Message) error {
	fmt.Printf("processing order: %s\n", msg.Value)
	return nil
}

// Messages sharing a key are processed one at a time, in offset order.
// Messages with different keys are processed concurrently. Return "" to opt
// a message out of ordering.
func (h *OrderHandler) GetKey(key, value []byte) string {
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

	if err := consumer.Start(); err != nil {
		log.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	if err := consumer.Stop(); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
```

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `Brokers` | `[]string` | required | Kafka broker addresses |
| `GroupID` | `string` | required | Consumer group ID |
| `Topic` | `string` | required | Topic to consume from |
| `Handler` | `MessageHandler` | required | Your handler implementation |
| `MaxInFlight` | `int` | `1000` | Messages processed concurrently, across all assigned partitions |
| `UnOrdered` | `bool` | `false` | Disable key ordering |
| `MaxQueuedPerKey` | `int` | `100` | Queue cap per key before `OverflowPolicy` applies |
| `OverflowPolicy` | `OverflowPolicy` | `OverflowBlock` | Action when a key's queue is full |
| `RetryCount` | `int` | `0` | Retry attempts after a failed `HandleMessage` |
| `RetryDelay` | `time.Duration` | `500ms` | Delay between retries |
| `DeadLetterPersister` | `DeadLetterPersister` | `nil` | Sink for undeliverable messages |
| `AutoOffsetReset` | `int64` | `kafka.LastOffset` | Where to start when the group has no committed offset. Use `kafka.FirstOffset` to start from the beginning |
| `ShutdownTimeout` | `time.Duration` | `30s` | Budget for `Stop` to drain in-flight handlers |
| `Logger` | `*slog.Logger` | `slog.Default()` | Structured logger |

### Offset commits

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `MinOffsetCommitCount` | `int64` | `5` | Messages completed before a commit is considered |
| `MaxCommitInterval` | `time.Duration` | `5s` | Upper bound between commits |
| `ForceCommitInterval` | `time.Duration` | `5s` | How often progress is committed regardless of the thresholds above |
| `MaxCommitRetries` | `int` | `50` | Attempts before a commit is abandoned |
| `CommitRetryDelay` | `time.Duration` | `100ms` | Delay between commit attempts |

### Fetching

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `MinBytes` | `int` | `0` | Minimum bytes per fetch |
| `MaxBytes` | `int` | `10MB` | Maximum bytes per fetch |
| `MaxWait` | `time.Duration` | `10s` | How long a fetch waits for `MinBytes` to accumulate |

### Group membership

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `SessionTimeout` | `time.Duration` | `30s` | Time without a heartbeat before the group evicts this member |
| `RebalanceTimeout` | `time.Duration` | `30s` | Time the coordinator waits for members to rejoin |
| `HeartbeatInterval` | `time.Duration` | `3s` | Heartbeat frequency. Keep it well below `SessionTimeout` |
| `WatchPartitionChanges` | `bool` | `false` | Rebalance when the topic's partition count changes |
| `PartitionWatchInterval` | `time.Duration` | `5s` | Poll interval for `WatchPartitionChanges` |

## Development

```bash
./scripts/build.sh build       # compile
./scripts/build.sh pre-commit  # fmt, vet, modernize, unit tests
./scripts/test_unit.sh         # unit tests with -race
./scripts/test_integration.sh  # integration tests, needs Docker
./scripts/test.sh              # both suites, with a merged coverage report
```

Issues and pull requests are welcome. Run `./scripts/build.sh pre-commit` before opening one, and add tests for behavior changes.

## License

[MIT](LICENSE)
