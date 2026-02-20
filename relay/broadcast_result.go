package relay

import (
	"context"

	"github.com/alitto/pond/v2"
)

type BroadcastResult struct {
	group pond.TaskGroup
}

func (b *BroadcastResult) Wait() error {
	if b.group == nil {
		return nil
	}

	defer b.group.Stop() // Cancel context

	return b.group.Wait()
}

func (b *BroadcastResult) WaitContext(ctx context.Context) error {
	if b.group == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.group.Done():
		return b.Wait()
	}
}
