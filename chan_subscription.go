package relay

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type ChanSubscription[T any] struct {
	dataChan            chan T
	ctx                 context.Context
	cancel              context.CancelCauseFunc
	unsubscribeCallback func()
	closeOnce           sync.Once
	shardIdx            int
	received            atomic.Uint64
	dropped             atomic.Uint64
}

func NewChanSubscription[T any](options ...ChanSubscriptionOption) *ChanSubscription[T] {
	cfg := &chanSubscriptionConfig{
		ctx:          context.Background(),
		backpressure: 0,
	}

	for _, opt := range options {
		opt(cfg)
	}

	ctx, cancel := context.WithCancelCause(cfg.ctx)

	return &ChanSubscription[T]{
		dataChan:            make(chan T, cfg.backpressure),
		ctx:                 ctx,
		cancel:              cancel,
		unsubscribeCallback: cfg.unsubscribeCallback,
	}
}

func (c *ChanSubscription[T]) Listen(ctx context.Context) (T, error) {
	var empty T
	select {
	case <-c.ctx.Done():
		return empty, context.Cause(c.ctx)
	case <-ctx.Done():
		return empty, ctx.Err()
	case data := <-c.dataChan:
		return data, nil
	}
}

func (c *ChanSubscription[T]) Unsubscribe() {
	c.closeOnce.Do(func() {
		if c.unsubscribeCallback != nil {
			c.unsubscribeCallback()
		}

		c.cancel(ErrUnsubscribed)
		c.drain()
	})
}

// drain consumes and discards buffered events so the channel can be GC'd.
func (c *ChanSubscription[T]) drain() {
	for {
		select {
		case <-c.dataChan:
		default:
			return
		}
	}
}

func (c *ChanSubscription[T]) Stats() SubscriptionStats {
	return SubscriptionStats{
		EventsReceived: c.received.Load(),
		EventsDropped:  c.dropped.Load(),
	}
}

func (c *ChanSubscription[T]) String() string {
	stats := c.Stats()
	return fmt.Sprintf("relay.Subscription[received:%d dropped:%d]", stats.EventsReceived, stats.EventsDropped)
}

func (c *ChanSubscription[T]) recordDropped() {
	c.dropped.Add(1)
}

func (c *ChanSubscription[T]) Close() error {
	c.Unsubscribe()
	return nil
}
