package relay

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

type ChanSubscription[T any] struct {
	id                  string
	dataChan            chan T
	ctx                 context.Context
	cancel              context.CancelCauseFunc
	unsubscribeCallback func()
	closed              bool
}

func NewChanSubscription[T any](backpressure int, unsubscribeCallback func()) *ChanSubscription[T] {
	ctx, cancel := context.WithCancelCause(context.Background())

	return &ChanSubscription[T]{
		id:                  uuid.New().String(),
		dataChan:            make(chan T, backpressure),
		unsubscribeCallback: unsubscribeCallback,
		ctx:                 ctx,
		cancel:              cancel,
	}
}

func (c *ChanSubscription[T]) Listen(ctx context.Context) (T, error) {
	select {
	case <-c.ctx.Done():
		var empty T
		return empty, c.ctx.Err()
	case <-ctx.Done():
		var empty T
		return empty, ctx.Err()
	case data, ok := <-c.dataChan:
		if !ok {
			var empty T
			return empty, c.ctx.Err()
		}
		return data, nil
	}
}

func (c *ChanSubscription[T]) Pump(data T, ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.dataChan <- data:
		return nil
	}
}

func (c *ChanSubscription[T]) CancelError(err error) {
	c.cancel(err)
}

func (c *ChanSubscription[T]) Unsubscribe() {
	c.unsubscribeCallback()
}

func (c *ChanSubscription[T]) Close() error {
	if c.closed {
		return nil
	}

	c.closed = true
	c.cancel(fmt.Errorf("subscription closed"))
	close(c.dataChan)
	return nil
}
