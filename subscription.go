package relay

import "context"

// SubscriptionStats holds per-subscription delivery metrics.
type SubscriptionStats struct {
	EventsReceived uint64
	EventsDropped  uint64
}

// AggregateStats holds bus-wide delivery metrics across all active subscriptions.
type AggregateStats struct {
	Subscribers    int
	EventsReceived uint64
	EventsDropped  uint64
}

// Subscription[T] represents a single consumer attached to a Bus.
type Subscription[T any] interface {
	Listen(ctx context.Context) (T, error)
	Unsubscribe()
	Stats() SubscriptionStats
	String() string
	Close() error
}
