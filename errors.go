package turnstile

import "errors"

var (
	ErrNoBrokers             = errors.New("turnstile: no brokers specified")
	ErrNoGroupID             = errors.New("turnstile: no group ID specified")
	ErrNoTopic               = errors.New("turnstile: no topic specified")
	ErrNoHandler             = errors.New("turnstile: no message handler specified")
	ErrInvalidOverflowPolicy = errors.New("turnstile: invalid overflow policy")
	ErrKeyQueueOverflow      = errors.New("turnstile: message dropped: key queue at capacity")

	// ErrStaleGeneration means the group moved past the generation an offset was
	// tracked under. The commit is dropped without touching the network, since the
	// broker would reject it anyway; commit retries treat it as terminal.
	ErrStaleGeneration = errors.New("turnstile: consumer group generation is stale")
)
