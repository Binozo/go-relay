package relay

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"time"
)

type subList[T any] struct {
	subs []*ChanSubscription[T]
}

type ChanBus[T any] struct {
	ctx                      context.Context
	cancel                   context.CancelCauseFunc
	subscriptionBackpressure int
	slowPolicy               SlowSubscriberPolicy
	slowTimeout              time.Duration
	sem                      chan struct{} // nil => unlimited concurrent broadcasts
	shards                   *Shards[T]
	cleanupRunning           atomic.Bool
	broadcastCallback        func(duration time.Duration, alive, dropped int)
	closing                  atomic.Bool
	inFlight                 atomic.Int32
}

// NewChanBus creates a new event bus with the provided options.
func NewChanBus[T any](options ...ChanBusOption) *ChanBus[T] {
	cfg := &busConfig{
		ctx:        context.Background(),
		poolSize:   runtime.GOMAXPROCS(0),
		shardCount: -1,
		slowPolicy: SlowSubscriberPolicyBlock,
	}

	for _, opt := range options {
		opt(cfg)
	}

	shardsCount := cfg.shardCount
	if shardsCount <= 0 {
		shardsCount = runtime.GOMAXPROCS(0)
	}

	ctx, cancel := context.WithCancelCause(cfg.ctx)

	bus := &ChanBus[T]{
		ctx:                      ctx,
		cancel:                   cancel,
		subscriptionBackpressure: cfg.subscriptionBackpressure,
		slowPolicy:               cfg.slowPolicy,
		slowTimeout:              cfg.slowTimeout,
		broadcastCallback:        cfg.broadcastCallback,
		shards:                   NewShards[T](shardsCount),
	}

	if cfg.poolSize > 0 {
		bus.sem = make(chan struct{}, cfg.poolSize)
	}

	return bus
}

func (c *ChanBus[T]) Subscribe(options ...ChanSubscriptionOption) (Subscription[T], error) {
	return c.SubscribeContext(context.Background(), options...)
}

// SubscribeContext creates a new subscription attached to ctx.
func (c *ChanBus[T]) SubscribeContext(ctx context.Context, options ...ChanSubscriptionOption) (Subscription[T], error) {
	if c.closing.Load() || c.ctx.Err() != nil {
		return nil, ErrBusClosed
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	sub := NewChanSubscription[T](
		append(
			[]ChanSubscriptionOption{
				SubWithContext(ctx),
				SubWithBackpressure(c.subscriptionBackpressure),
			},
			options...,
		)...,
	)

	userCallback := sub.unsubscribeCallback
	sub.unsubscribeCallback = func() {
		if userCallback != nil {
			userCallback()
		}

		c.shards.UnsubscribeAt(sub.shardIdx, sub)
	}

	idx := c.shards.Subscribe(sub)
	sub.shardIdx = idx

	return sub, nil
}

// Len returns the total number of subscribers currently connected to the bus.
func (c *ChanBus[T]) Len() int {
	return c.shards.Len()
}

// Stats returns aggregate statistics across all active subscriptions.
func (c *ChanBus[T]) Stats() AggregateStats {
	var total AggregateStats
	for _, sub := range c.shards.All() {
		if sub.ctx.Err() != nil {
			continue
		}

		total.Subscribers++
		total.EventsReceived += sub.received.Load()
		total.EventsDropped += sub.dropped.Load()
	}
	return total
}

// Broadcast sends the event to all active subscribers. It blocks until the
// semaphore has room (if poolSize is configured), then returns immediately;
// use BroadcastResult.Wait() to block until the fan-out is complete.
func (c *ChanBus[T]) Broadcast(event T) BroadcastResult {
	return c.tryBroadcast(event, true)
}

// TryBroadcast attempts to broadcast immediately. It returns the result and
// true on success, or false if the broadcast semaphore is saturated.
func (c *ChanBus[T]) TryBroadcast(event T) (BroadcastResult, bool) {
	res := c.tryBroadcast(event, false)
	if errors.Is(res.err, ErrBroadcastFull) {
		return res, false
	}

	return res, true
}

func (c *ChanBus[T]) tryBroadcast(event T, wait bool) BroadcastResult {
	if c.closing.Load() || c.ctx.Err() != nil {
		return resultDoneClosed
	}

	snaps := c.shards.Snapshot()
	var total int
	for _, snap := range snaps {
		total += len(snap.subs)
	}

	if total == 0 {
		return resultDoneNil
	}

	if c.sem != nil {
		if wait {
			select {
			case c.sem <- struct{}{}:
			case <-c.ctx.Done():
				return resultDoneClosed
			}
		} else {
			select {
			case c.sem <- struct{}{}:
			case <-c.ctx.Done():
				return resultDoneClosed
			default:
				return resultDoneFull
			}
		}
	}

	// Re-check closing after sem acquisition to close the race with
	// CloseGracefully starting while we were blocked on the semaphore.
	if c.closing.Load() {
		if c.sem != nil {
			<-c.sem
		}
		return resultDoneClosed
	}

	c.inFlight.Add(1)
	done := make(chan error, 1)
	go c.broadcastSequential(snaps, event, done)
	return BroadcastResult{
		done: done,
	}
}

func (c *ChanBus[T]) broadcastSequential(snaps []*subList[T], event T, done chan error) {
	defer c.inFlight.Add(-1)

	if c.sem != nil {
		defer func() {
			<-c.sem
		}()
	}

	start := time.Now()
	var dropped int
	var timer *time.Timer
	if c.slowPolicy == SlowSubscriberPolicyTimeout && c.slowTimeout > 0 {
		timer = time.NewTimer(c.slowTimeout)
		defer timer.Stop()
	}

	var needCleanup bool
	aliveCount := 0
	for _, snap := range snaps {
		for _, sub := range snap.subs {
			if sub.ctx.Err() != nil {
				needCleanup = true
				dropped++
				continue
			}

			// aliveCount only counts non-dead subscriptions.
			// The callback's `dropped` includes dead subs skipped above,
			// while Stats().EventsDropped only sums alive subscriptions
			// whose event was dropped by the policy.
			aliveCount++
			sent := c.sendToSubscriber(sub, event, timer)
			if !sent {
				dropped++
				sub.recordDropped()
			}
		}
	}

	if c.ctx.Err() != nil {
		done <- ErrBusClosed
	}
	close(done)

	if c.broadcastCallback != nil {
		c.broadcastCallback(time.Since(start), aliveCount, dropped)
	}

	if needCleanup {
		go c.eagerCleanup()
	}
}

// sendToSubscriber returns true if the event was delivered (or would have been
// delivered under the Block policy), false if it was dropped.
func (c *ChanBus[T]) sendToSubscriber(sub *ChanSubscription[T], event T, timer *time.Timer) bool {
	switch c.slowPolicy {
	case SlowSubscriberPolicyDrop:
		select {
		case <-c.ctx.Done():
			return false
		case <-sub.ctx.Done():
			return false
		case sub.dataChan <- event:
			sub.received.Add(1)
			return true
		default:
			return false
		}

	case SlowSubscriberPolicyTimeout:
		if c.slowTimeout <= 0 {
			select {
			case <-c.ctx.Done():
				return false
			case <-sub.ctx.Done():
				return false
			case sub.dataChan <- event:
				sub.received.Add(1)
				return true
			}
		}

		resetTimer(timer, c.slowTimeout)

		select {
		case <-c.ctx.Done():
			return false
		case <-sub.ctx.Done():
			return false
		case sub.dataChan <- event:
			sub.received.Add(1)
			return true
		case <-timer.C:
			return false
		}

	default: // Block
		select {
		case <-c.ctx.Done():
			return false
		case <-sub.ctx.Done():
			return false
		case sub.dataChan <- event:
			sub.received.Add(1)
			return true
		}
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}

	t.Reset(d)
}

func (c *ChanBus[T]) eagerCleanup() {
	if !c.cleanupRunning.CompareAndSwap(false, true) {
		return
	}

	defer c.cleanupRunning.Store(false)
	c.shards.Cleanup()
}

func (c *ChanBus[T]) Close() error {
	c.closing.Store(true)
	c.cancel(ErrBusClosed)
	return nil
}

// CloseGracefully initiates a graceful shutdown. It rejects new subscriptions
// and broadcasts, waits for all in-flight broadcasts to complete (or until ctx
// is cancelled), and then closes the bus. If ctx expires before all broadcasts
// finish, the bus is force-closed and ctx.Err() is returned.
func (c *ChanBus[T]) CloseGracefully(ctx context.Context) error {
	c.closing.Store(true)

	for {
		if c.inFlight.Load() == 0 {
			// Yield so any goroutine that passed the closing check but
			// has not yet incremented inFlight gets a chance to finish.
			runtime.Gosched()
			if c.inFlight.Load() == 0 {
				break
			}
		}

		select {
		case <-ctx.Done():
			c.cancel(ErrBusClosed)
			return ctx.Err()
		default:
		}

		time.Sleep(100 * time.Microsecond)
	}

	c.cancel(ErrBusClosed)
	return nil
}
