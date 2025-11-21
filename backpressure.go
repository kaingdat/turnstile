package turnstile

import (
	"context"
	"sync"
)

type backpressureController struct {
	maxInFlight int
	current     int
	cond        *sync.Cond
}

func newBackpressureController(maxInFlight int) *backpressureController {
	if maxInFlight <= 0 {
		panic("backpressure: maxInFlight must be > 0")
	}
	return &backpressureController{
		maxInFlight: maxInFlight,
		cond:        sync.NewCond(&sync.Mutex{}),
	}
}

func (c *backpressureController) Acquire(ctx context.Context) error {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	if c.current < c.maxInFlight {
		c.current++
		return nil
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	stop := context.AfterFunc(ctx, func() {
		c.cond.L.Lock()
		defer c.cond.L.Unlock()
		c.cond.Broadcast()
	})
	defer stop()

	for c.current >= c.maxInFlight {
		c.cond.Wait()
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	c.current++
	return nil
}

func (c *backpressureController) Release() {
	c.cond.L.Lock()
	if c.current <= 0 {
		c.cond.L.Unlock()
		panic("backpressure: Release called without matching Acquire")
	}
	c.current--
	c.cond.L.Unlock()
	c.cond.Broadcast()
}
