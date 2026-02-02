package relay

import (
	"context"
	"sync"

	"github.com/alitto/pond/v2"
)

type ChanBus[T any] struct {
	ctx                       context.Context
	cancel                    context.CancelFunc
	broadcastPool             pond.Pool
	broadcastPipeline         chan T
	subscriptionBackpressure  int
	subscriptions             []*ChanSubscription[T]
	subscriptionsLock         sync.RWMutex
	internalSubscriptions     []*ChanSubscription[internalEvent]
	internalSubscriptionsLock sync.RWMutex
}

func NewChanBus[T any](options ...ChanBusOption) *ChanBus[T] {
	cfg := &busConfig{
		ctx:                   context.Background(),
		poolSize:              10,
		broadcastBackpressure: 0, // Unbuffered
	}

	for _, option := range options {
		option(cfg)
	}

	ctx, cancel := context.WithCancel(cfg.ctx)

	pool := pond.NewPool(cfg.poolSize,
		pond.WithContext(ctx),
		pond.WithNonBlocking(true),
		pond.WithoutPanicRecovery())

	bus := &ChanBus[T]{
		ctx:                      ctx,
		cancel:                   cancel,
		broadcastPool:            pool,
		broadcastPipeline:        make(chan T, cfg.broadcastBackpressure),
		subscriptionBackpressure: cfg.subscriptionBackpressure,
		subscriptions:            make([]*ChanSubscription[T], 0),
		internalSubscriptions:    make([]*ChanSubscription[internalEvent], 0),
	}

	pool.SubmitErr(bus.eventWorker)

	return bus
}

func (c *ChanBus[T]) eventWorker() error {
	for c.ctx.Err() == nil {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case event := <-c.broadcastPipeline:
			result := c.broadcast(event)

			if err := result.WaitContext(c.ctx); err != nil {
				return err
			}
		}
	}

	return c.ctx.Err()
}

func (c *ChanBus[T]) Subscribe() Subscription[T] {
	var subscription *ChanSubscription[T]
	subscription = NewChanSubscription[T](c.subscriptionBackpressure, func() {
		// Unsubscribe
		subscription.Close()

		c.subscriptionsLock.Lock()
		defer c.subscriptionsLock.Unlock()

		newSubscriptions := make([]*ChanSubscription[T], 0, len(c.subscriptions))
		for _, curSubscription := range c.subscriptions {
			if curSubscription.id != subscription.id {
				newSubscriptions = append(newSubscriptions, curSubscription)
			}
		}

		c.subscriptions = newSubscriptions

		c.broadcastInternalEvent(&internalEventUnsubscribed{
			baseInternalEvent: baseInternalEvent{
				internalEventType: internalEventTypeUnsubscribed,
			},
			TotalSubscriptions: len(c.subscriptions),
		})
	})

	c.subscriptionsLock.Lock()
	defer c.subscriptionsLock.Unlock()
	c.subscriptions = append(c.subscriptions, subscription)

	c.broadcastInternalEvent(&internalEventSubscribed{
		baseInternalEvent: baseInternalEvent{
			internalEventType: internalEventTypeSubscribed,
		},
		TotalSubscriptions: len(c.subscriptions),
	})

	return subscription
}

// Broadcast sends the event to all subscribers.
func (c *ChanBus[T]) Broadcast(event T) BroadcastResult {
	select {
	case <-c.ctx.Done():
		return BroadcastResult{}
	case c.broadcastPipeline <- event:
		return BroadcastResult{} // TODO
	}
}

// broadcast sends the event to all subscribers.
func (c *ChanBus[T]) broadcast(event T) BroadcastResult {
	c.subscriptionsLock.RLock()
	subs := c.subscriptions
	c.subscriptionsLock.RUnlock()

	group := c.broadcastPool.NewGroup()

	for _, subscription := range subs {
		if subscription.ctx.Err() != nil {
			continue // Marked for removal
		}

		group.SubmitErr(func() error {
			select {
			case <-c.ctx.Done():
				return c.ctx.Err()
			case <-subscription.ctx.Done():
				return subscription.ctx.Err()
			case subscription.dataChan <- event:
				break
			}

			return nil
		})
	}

	return BroadcastResult{
		group: group,
	}
}

func (c *ChanBus[T]) broadcastInternalEvent(event internalEvent) {
	c.internalSubscriptionsLock.RLock()
	defer c.internalSubscriptionsLock.RUnlock()

	group := c.broadcastPool.NewGroup()

	for _, subscription := range c.internalSubscriptions {
		if subscription.ctx.Err() != nil {
			continue
		}

		group.SubmitErr(func() error {
			select {
			case <-c.ctx.Done():
				return c.ctx.Err()
			case <-subscription.ctx.Done():
				return subscription.ctx.Err()
			case subscription.dataChan <- event:
				break
			}

			return nil
		})
	}
}

func (c *ChanBus[T]) subscribeToInternalEvents() Subscription[internalEvent] {
	c.internalSubscriptionsLock.Lock()
	defer c.internalSubscriptionsLock.Unlock()

	var sub *ChanSubscription[internalEvent]
	sub = NewChanSubscription[internalEvent](0, func() {
		c.internalSubscriptionsLock.Lock()
		defer c.internalSubscriptionsLock.Unlock()

		newInternalSubscriptions := make([]*ChanSubscription[internalEvent], 0, len(c.internalSubscriptions)-1)
		for _, curInternalSubscription := range c.internalSubscriptions {
			if curInternalSubscription.id != sub.id {
				newInternalSubscriptions = append(newInternalSubscriptions, curInternalSubscription)
			}
		}

		c.internalSubscriptions = newInternalSubscriptions
	})

	c.internalSubscriptions = append(c.internalSubscriptions, sub)
	return sub
}

func (c *ChanBus[T]) Close() error {
	c.cancel()
	return nil
}
