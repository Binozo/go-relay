package relay

import "context"

type busConfig struct {
	ctx                      context.Context
	poolSize                 int
	broadcastBackpressure    int
	subscriptionBackpressure int
}

type ChanBusOption func(cfg *busConfig)

func WithContext(ctx context.Context) ChanBusOption {
	return func(cfg *busConfig) {
		cfg.ctx = ctx
	}
}

func WithPoolSize(poolSize int) ChanBusOption {
	return func(cfg *busConfig) {
		cfg.poolSize = poolSize
	}
}

func WithBroadcastBackpressure(backpressure int) ChanBusOption {
	return func(cfg *busConfig) {
		cfg.broadcastBackpressure = backpressure
	}
}
