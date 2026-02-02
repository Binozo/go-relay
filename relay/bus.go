package relay

type Bus[T any] interface {
	Broadcast(event T) BroadcastResult
	Subscribe() Subscription[T]
	Close() error
}
