package relay

import "context"

type chanSubscriptionConfig struct {
	ctx                 context.Context
	backpressure        int
	unsubscribeCallback func()
}

type ChanSubscriptionOption func(cfg *chanSubscriptionConfig)

func SubWithContext(ctx context.Context) ChanSubscriptionOption {
	return func(cfg *chanSubscriptionConfig) {
		cfg.ctx = ctx
	}
}

func SubWithBackpressure(backpressure int) ChanSubscriptionOption {
	return func(cfg *chanSubscriptionConfig) {
		cfg.backpressure = backpressure
	}
}

func SubWithUnsubscribeCallback(unsubscribeCallback func()) ChanSubscriptionOption {
	return func(cfg *chanSubscriptionConfig) {
		cfg.unsubscribeCallback = unsubscribeCallback
	}
}
