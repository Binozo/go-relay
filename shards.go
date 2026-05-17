package relay

import "sync/atomic"

type shard[T any] struct {
	subs atomic.Pointer[subList[T]]
}

func newShard[T any]() *shard[T] {
	s := &shard[T]{}
	s.subs.Store(&subList[T]{})

	return s
}

// Shards holds subscriber lists partitioned across K shards.
type Shards[T any] struct {
	shards []*shard[T]
	next   atomic.Uint64
}

func NewShards[T any](count int) *Shards[T] {
	if count <= 0 {
		count = 1
	}

	s := &Shards[T]{
		shards: make([]*shard[T], count),
	}

	for i := range s.shards {
		s.shards[i] = newShard[T]()
	}

	return s
}

// Subscribe places sub into the next shard by round-robin. On success it
// returns the shard index that was chosen.
func (s *Shards[T]) Subscribe(sub *ChanSubscription[T]) int {
	idx := int(s.next.Load() % uint64(len(s.shards)))
	sh := s.shards[idx]

	for {
		old := sh.subs.Load()
		newSubs := make([]*ChanSubscription[T], len(old.subs)+1)
		copy(newSubs, old.subs)
		newSubs[len(old.subs)] = sub

		if sh.subs.CompareAndSwap(old, &subList[T]{subs: newSubs}) {
			break
		}
	}

	s.next.Add(1)

	return idx
}

// UnsubscribeAt removes sub from shard idx only.
func (s *Shards[T]) UnsubscribeAt(idx int, sub *ChanSubscription[T]) {
	if idx < 0 || idx >= len(s.shards) {
		return
	}

	sh := s.shards[idx]
	for {
		old := sh.subs.Load()
		found := false

		for _, cur := range old.subs {
			if cur == sub {
				found = true
				break
			}
		}

		if !found {
			return
		}

		newSubs := make([]*ChanSubscription[T], 0, len(old.subs)-1)
		for _, cur := range old.subs {
			if cur != sub {
				newSubs = append(newSubs, cur)
			}
		}

		if sh.subs.CompareAndSwap(old, &subList[T]{subs: newSubs}) {
			return
		}
	}
}

// Unsubscribe scans all shards when the shard
// index is unknown. Prefer UnsubscribeAt.
func (s *Shards[T]) Unsubscribe(sub *ChanSubscription[T]) {
	for idx := range s.shards {
		s.UnsubscribeAt(idx, sub)
	}
}

// Snapshot returns a copy of all current shard lists.
func (s *Shards[T]) Snapshot() []*subList[T] {
	result := make([]*subList[T], len(s.shards))
	for i, sh := range s.shards {
		result[i] = sh.subs.Load()
	}

	return result
}

// Len returns the total number of subscriptions across all shards.
func (s *Shards[T]) Len() int {
	total := 0
	for _, sh := range s.shards {
		total += len(sh.subs.Load().subs)
	}

	return total
}

// Cleanup removes dead subscriptions from every shard.
func (s *Shards[T]) Cleanup() {
	for _, sh := range s.shards {
		for {
			old := sh.subs.Load()
			alive := make([]*ChanSubscription[T], 0, len(old.subs))
			for _, sub := range old.subs {
				if sub.ctx.Err() == nil {
					alive = append(alive, sub)
				}
			}

			if len(alive) == len(old.subs) {
				break
			}

			if sh.subs.CompareAndSwap(old, &subList[T]{subs: alive}) {
				break
			}
		}
	}
}

// All returns every subscription in every shard.
func (s *Shards[T]) All() []*ChanSubscription[T] {
	var total int
	for _, sh := range s.shards {
		total += len(sh.subs.Load().subs)
	}

	result := make([]*ChanSubscription[T], 0, total)
	for _, sh := range s.shards {
		result = append(result, sh.subs.Load().subs...)
	}

	return result
}
