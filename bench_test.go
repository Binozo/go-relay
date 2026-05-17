package relay

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// BenchmarkBroadcastWithListeners measures the cost of Broadcast+Wait with a
// fixed number of active consumers. The background listener goroutines
// create a new context.WithTimeout on every Listen(); those allocations are
// counted by the benchmark but are not overhead from Relay's broadcast path.
func BenchmarkBroadcastWithListeners(b *testing.B) {
	for _, subs := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("subs-%d", subs), func(b *testing.B) {
			bus := NewChanBus[int](WithSubscriptionBackpressure(1))
			defer bus.Close()

			subscriptions := make([]Subscription[int], subs)
			for i := range subscriptions {
				s, err := bus.Subscribe()
				if err != nil {
					b.Fatalf("subscribe: %v", err)
				}
				subscriptions[i] = s
				go func(s Subscription[int]) {
					for {
						ctx, cancel := context.WithTimeout(context.Background(), time.Second)
						_, err := s.Listen(ctx)
						cancel()
						if err != nil {
							return
						}
					}
				}(subscriptions[i])
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result := bus.Broadcast(i)
				_ = result.Wait()
			}
		})
	}
}

// BenchmarkBroadcastPolicy measures Broadcast under each slow-subscriber policy.
func BenchmarkBroadcastPolicy(b *testing.B) {
	policies := []struct {
		name   string
		policy SlowSubscriberPolicy
		to     time.Duration
	}{
		{"Block", SlowSubscriberPolicyBlock, 0},
		{"Drop", SlowSubscriberPolicyDrop, 0},
		{"Timeout-1ms", SlowSubscriberPolicyTimeout, time.Millisecond},
	}

	for _, p := range policies {
		b.Run(p.name, func(b *testing.B) {
			bus := NewChanBus[int](
				WithSubscriptionBackpressure(10),
				WithSlowSubscriberPolicy(p.policy),
				WithSlowSubscriberTimeout(p.to),
			)
			defer bus.Close()

			// 100 fast subscribers; benchmark measures policy overhead, not blocking
			subs := make([]Subscription[int], 100)
			for i := range subs {
				s, _ := bus.Subscribe()
				subs[i] = s
				go func(s Subscription[int]) {
					for {
						_, err := s.Listen(context.Background())
						if err != nil {
							return
						}
					}
				}(s)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result := bus.Broadcast(i)
				_ = result.Wait()
			}
		})
	}
}

// BenchmarkChurn measures subscribe → broadcast → unsubscribe throughput.
func BenchmarkChurn(b *testing.B) {
	for _, shards := range []int{1, 4, runtime.GOMAXPROCS(0)} {
		b.Run(fmt.Sprintf("shards-%d", shards), func(b *testing.B) {
			bus := NewChanBus[int](
				WithShardCount(shards),
				WithSubscriptionBackpressure(1),
				WithSlowSubscriberPolicy(SlowSubscriberPolicyDrop),
			)
			defer bus.Close()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sub, _ := bus.Subscribe()
				bus.Broadcast(i)
				sub.Unsubscribe()
			}
		})
	}
}

// BenchmarkTryBroadcast measures non-blocking broadcast attempts.
func BenchmarkTryBroadcast(b *testing.B) {
	bus := NewChanBus[int](
		WithPoolSize(1),
		WithSubscriptionBackpressure(100),
		WithSlowSubscriberPolicy(SlowSubscriberPolicyDrop),
	)
	defer bus.Close()

	// Fast consumer keeps the bus unblocked
	sub, _ := bus.Subscribe()
	defer sub.Unsubscribe()
	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := sub.Listen(ctx)
			cancel()
			if err != nil {
				return
			}
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = bus.TryBroadcast(i)
	}
}

// BenchmarkStats measures aggregate stats retrieval overhead.
func BenchmarkStats(b *testing.B) {
	for _, subs := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("subs-%d", subs), func(b *testing.B) {
			bus := NewChanBus[int](WithShardCount(runtime.GOMAXPROCS(0)))
			defer bus.Close()

			allSubs := make([]Subscription[int], 0, subs)
			for i := 0; i < subs; i++ {
				s, _ := bus.Subscribe()
				allSubs = append(allSubs, s)
			}

			defer func() {
				for _, s := range allSubs {
					s.Unsubscribe()
				}
			}()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = bus.Stats()
			}
		})
	}
}
