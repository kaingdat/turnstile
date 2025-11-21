package turnstile

// OverflowPolicy determines what the key sequencer does when a key's queue is
// already at MaxQueuedPerKey and another message arrives for that key.
type OverflowPolicy int

const (
	OverflowBlock OverflowPolicy = iota
	OverflowDropOldest
	OverflowDropNewest
)

func (p OverflowPolicy) String() string {
	switch p {
	case OverflowBlock:
		return "block"
	case OverflowDropOldest:
		return "drop-oldest"
	case OverflowDropNewest:
		return "drop-newest"
	default:
		return "unknown"
	}
}

func (p OverflowPolicy) valid() bool {
	switch p {
	case OverflowBlock, OverflowDropOldest, OverflowDropNewest:
		return true
	default:
		return false
	}
}
