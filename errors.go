package relay

import "errors"

var (
	ErrUnsubscribed  = errors.New("subscription closed")
	ErrBusClosed     = errors.New("bus closed")
	ErrBroadcastFull = errors.New("broadcast semaphore full")
)
