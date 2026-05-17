package relay

import (
	"context"
	"time"
)

// SlowSubscriberPolicy controls how the bus behaves when a subscriber's
// channel is full or the subscriber is otherwise slow to consume events.
type SlowSubscriberPolicy int

const (
	// SlowSubscriberPolicyBlock waits until the subscriber has room in its
	// buffer or until the subscriber/bus context is cancelled.
	SlowSubscriberPolicyBlock SlowSubscriberPolicy = iota
	// SlowSubscriberPolicyDrop skips the subscriber immediately if its buffer
	// is full. No event is lost for other subscribers.
	SlowSubscriberPolicyDrop
	// SlowSubscriberPolicyTimeout attempts delivery for up to the configured
	// duration (see WithSlowSubscriberTimeout) before skipping the subscriber.
	SlowSubscriberPolicyTimeout
)

type busConfig struct {
	ctx                      context.Context
	poolSize                 int
	shardCount               int
	subscriptionBackpressure int
	slowPolicy               SlowSubscriberPolicy
	slowTimeout              time.Duration
	broadcastCallback        func(duration time.Duration, alive, dropped int)
}

// ChanBusOption configures a ChanBus during construction.
type ChanBusOption func(cfg *busConfig)

// WithContext sets the parent context for the bus. When this context is
// cancelled, all operations (Subscribe, Broadcast) will fail with
// ErrBusClosed.
func WithContext(ctx context.Context) ChanBusOption {
	return func(cfg *busConfig) {
		cfg.ctx = ctx
	}
}

// WithPoolSize caps the number of concurrent broadcast goroutines. A value of
// 0 means unlimited; values > 0 create a semaphore of that size. If the
// semaphore is full, Broadcast blocks until room is available, while
// TryBroadcast returns ErrBroadcastFull. Defaults to runtime.GOMAXPROCS(0).
func WithPoolSize(poolSize int) ChanBusOption {
	return func(cfg *busConfig) {
		cfg.poolSize = poolSize
	}
}

// WithSubscriptionBackpressure sets the default buffer size for every
// subscriber created by this bus. Individual subscriptions may override this
// with SubWithBackpressure. A value of 0 creates an unbuffered channel.
func WithSubscriptionBackpressure(backpressure int) ChanBusOption {
	return func(cfg *busConfig) {
		cfg.subscriptionBackpressure = backpressure
	}
}

// WithSlowSubscriberPolicy selects how the bus treats a subscriber whose
// buffer is full. Block waits, Drop skips immediately, Timeout gives up
// after WithSlowSubscriberTimeout. Defaults to Block.
func WithSlowSubscriberPolicy(policy SlowSubscriberPolicy) ChanBusOption {
	return func(cfg *busConfig) {
		cfg.slowPolicy = policy
	}
}

// WithSlowSubscriberTimeout sets the per-subscriber timeout used by the
// Timeout policy. Has no effect on Block or Drop policies.
func WithSlowSubscriberTimeout(timeout time.Duration) ChanBusOption {
	return func(cfg *busConfig) {
		cfg.slowTimeout = timeout
	}
}

// WithBroadcastCallback attaches a callback that is invoked after every
// broadcast completes. It receives the broadcast duration, the number of
// active (alive) subscribers, and how many events were dropped.
func WithBroadcastCallback(cb func(duration time.Duration, alive, dropped int)) ChanBusOption {
	return func(cfg *busConfig) {
		cfg.broadcastCallback = cb
	}
}

// WithShardCount sets the number of subscriber shards. Higher values reduce
// subscribe/unsubscribe contention but increase the cost of snapshots.
// Defaults to runtime.GOMAXPROCS(0).
func WithShardCount(shardCount int) ChanBusOption {
	return func(cfg *busConfig) {
		cfg.shardCount = shardCount
	}
}
