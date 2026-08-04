package turnstile

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Message is the message type handlers receive.
type Message = kafka.Message

// MessageHandler processes messages consumed from Kafka.
type MessageHandler interface {
	// HandleMessage processes a single message. Returning an error triggers retries
	// per RetryCount, then dead-lettering.
	HandleMessage(ctx context.Context, message Message) error

	// GetKey extracts the ordering key. Messages sharing a key are never processed
	// concurrently. Return "" to skip sequencing for this message.
	GetKey(key []byte, value []byte) string
}

// DeadLetterPersister stores messages that failed after all retry attempts.
type DeadLetterPersister interface {
	Save(ctx context.Context, message Message, err error, key string) error
}

// DeadLetterMessage represents a dead-lettered message stored for inspection.
type DeadLetterMessage struct {
	ID        string
	Message   Message
	Error     string
	Key       string
	CreatedAt int64
}
