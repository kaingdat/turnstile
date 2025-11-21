package turnstile

import "errors"

var (
	ErrNoBrokers             = errors.New("turnstile: no brokers specified")
	ErrNoGroupID             = errors.New("turnstile: no group ID specified")
	ErrNoTopic               = errors.New("turnstile: no topic specified")
	ErrNoHandler             = errors.New("turnstile: no message handler specified")
	ErrInvalidOverflowPolicy = errors.New("turnstile: invalid overflow policy")
	ErrKeyQueueOverflow      = errors.New("turnstile: message dropped: key queue at capacity")
)
