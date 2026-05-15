package relay

import "context"

type chanSubscriptionConfig struct {
	ctx                 context.Context
	backpressure        int
	unsubscribeCallback func()
}

// ChanSubscriptionOption configures an individual subscription during creation.
type ChanSubscriptionOption func(cfg *chanSubscriptionConfig)

// SubWithContext sets the parent context for the subscription. When this context
// is cancelled, Listen returns the context error.
func SubWithContext(ctx context.Context) ChanSubscriptionOption {
	return func(cfg *chanSubscriptionConfig) {
		cfg.ctx = ctx
	}
}

// SubWithBackpressure sets the size of the subscription's internal buffer.
// A value of 0 creates an unbuffered channel.
func SubWithBackpressure(backpressure int) ChanSubscriptionOption {
	return func(cfg *chanSubscriptionConfig) {
		cfg.backpressure = backpressure
	}
}

// SubWithUnsubscribeCallback registers a function that is called when the
// subscription is unsubscribed. The bus also installs its own internal
// callback to remove the subscription from the shard; both are executed.
func SubWithUnsubscribeCallback(unsubscribeCallback func()) ChanSubscriptionOption {
	return func(cfg *chanSubscriptionConfig) {
		cfg.unsubscribeCallback = unsubscribeCallback
	}
}
