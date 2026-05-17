// Package relay provides a high-throughput, type-safe pub/sub event bus with
// sharded subscriber storage, configurable backpressure, and graceful shutdown.
//
// Create a bus
// with NewChanBus, subscribe with Subscribe or SubscribeContext, and broadcast
// events with Broadcast or TryBroadcast.
package relay

import "context"

// Bus[T] is a type-safe event broadcaster. Implementations must be safe for
// concurrent use by multiple goroutines.
type Bus[T any] interface {
	// Broadcast sends an event to all active subscribers. It returns
	// immediately; use BroadcastResult.Wait() to block until delivery is
	// complete. If poolSize > 0, Broadcast blocks until the semaphore has room.
	Broadcast(event T) BroadcastResult

	// TryBroadcast attempts to broadcast without blocking. It returns (result,
	// true) on success, or (result, false) if the semaphore is saturated.
	// The result's Wait() will return ErrBroadcastFull.
	TryBroadcast(event T) (BroadcastResult, bool)

	// Subscribe creates a new subscription with the bus defaults. Additional
	// ChanSubscriptionOption values may override those defaults.
	Subscribe(options ...ChanSubscriptionOption) (Subscription[T], error)

	// SubscribeContext creates a subscription tied to the given context. When
	// ctx is cancelled, the subscription is automatically unsubscribed.
	SubscribeContext(ctx context.Context, options ...ChanSubscriptionOption) (Subscription[T], error)

	// Len returns the total number of subscribers currently connected to the bus.
	Len() int

	// Stats returns aggregate delivery statistics across all active subscriptions.
	Stats() AggregateStats

	// Close force-closes the bus immediately. In-flight broadcasts are not
	// waited for.
	Close() error

	// CloseGracefully rejects new operations, waits for in-flight broadcasts
	// to finish (or until ctx is cancelled), and then closes the bus.
	CloseGracefully(ctx context.Context) error
}
