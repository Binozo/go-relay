package relay

import "context"

type Subscription[T any] interface {
	Listen(ctx context.Context) (T, error)
	Unsubscribe()
	Pump(data T, ctx context.Context) error
}
