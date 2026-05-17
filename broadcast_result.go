package relay

import "context"

var (
	resultDoneNil    = BroadcastResult{err: nil}
	resultDoneClosed = BroadcastResult{err: ErrBusClosed}
	resultDoneFull   = BroadcastResult{err: ErrBroadcastFull}
)

// BroadcastResult allows waiting for a broadcast to finish.
type BroadcastResult struct {
	done chan error
	err  error // pre-computed error when done is nil
}

// Wait blocks until the broadcast completes and returns an error if the bus
// was closed before or during the broadcast (ErrBusClosed) or if the broadcast
// was rejected due to a full semaphore (ErrBroadcastFull).
func (b BroadcastResult) Wait() error {
	if b.done == nil {
		return b.err
	}

	err, _ := <-b.done
	return err
}

// WaitContext blocks until the broadcast completes or ctx is cancelled.
// It may return ErrBusClosed or ErrBroadcastFull.
func (b BroadcastResult) WaitContext(ctx context.Context) error {
	if b.done == nil {
		return b.err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-b.done:
		return err
	}
}
