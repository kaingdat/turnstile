package turnstile

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Message = kafka.Message

// MessageHandler defines the interface for processing Kafka messages.
// Users must implement this interface to handle messages consumed from Kafka.
type MessageHandler interface {
	// HandleMessage processes a single Kafka message.
	// Return an error if processing fails; the library will handle retries based on configuration.
	HandleMessage(ctx context.Context, message Message) error

	// GetKey extracts a unique key from the message for ordered processing.
	// Messages with the same key will not be processed concurrently.
	// Return empty string to disable key-based sequencing for this message.
	GetKey(key []byte, value []byte) string
}

// DeadLetterPersister defines the interface for persisting messages that failed
// after all retry attempts. Users can implement this to store dead-lettered
// messages in their preferred storage (database, file, etc.) for inspection.
type DeadLetterPersister interface {
	// Save stores a dead-lettered message with its error information.
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
