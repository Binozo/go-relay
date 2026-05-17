package relay

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestBasicSubscribeBroadcast(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(1))
	defer bus.Close()

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	result := bus.Broadcast("hello")
	if err := result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	msg, err := sub.Listen(ctx)
	if err != nil {
		t.Fatalf("unexpected listen error: %v", err)
	}

	if msg != "hello" {
		t.Fatalf("expected hello, got %s", msg)
	}
}

func TestMultipleSubscribers(t *testing.T) {
	bus := NewChanBus[int](WithSubscriptionBackpressure(1))
	defer bus.Close()

	subs := make([]Subscription[int], 5)
	for i := range subs {
		s, err := bus.Subscribe()
		if err != nil {
			t.Fatalf("unexpected subscribe error: %v", err)
		}

		subs[i] = s
	}

	result := bus.Broadcast(42)
	if err := result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	for i, sub := range subs {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		msg, err := sub.Listen(ctx)
		cancel()
		if err != nil {
			t.Fatalf("sub %d listen error: %v", i, err)
		}

		if msg != 42 {
			t.Fatalf("sub %d expected 42, got %d", i, msg)
		}

		sub.Unsubscribe()
	}
}

func TestUnsubscribe(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(1))
	defer bus.Close()

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	sub.Unsubscribe()

	result := bus.Broadcast("world")
	if err := result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = sub.Listen(ctx)
	if err == nil {
		t.Fatal("expected error after unsubscribe, got nil")
	}
}

func TestBusClosePreventsBroadcast(t *testing.T) {
	bus := NewChanBus[string]()
	bus.Close()

	result := bus.Broadcast("nope")
	if err := result.Wait(); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("expected ErrBusClosed, got %v", err)
	}
}

func TestBusClosePreventsSubscribe(t *testing.T) {
	bus := NewChanBus[string]()
	bus.Close()

	_, err := bus.Subscribe()
	if !errors.Is(err, ErrBusClosed) {
		t.Fatalf("expected ErrBusClosed, got %v", err)
	}
}

func TestSubscribeContext(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(1))
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sub, err := bus.SubscribeContext(ctx)
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	cancel()

	result := bus.Broadcast("after-cancel")
	if err := result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	// The subscription's parent context was cancelled, so it should not receive
	listenCtx, listenCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer listenCancel()
	_, err = sub.Listen(listenCtx)
	if err == nil {
		t.Fatal("expected error after parent context cancelled, got nil")
	}
}

func TestSubscribeContextCancelled(t *testing.T) {
	bus := NewChanBus[string]()
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := bus.SubscribeContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestWaitContext(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(1))
	defer bus.Close()

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	result := bus.Broadcast("msg")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := result.WaitContext(ctx); err != nil {
		t.Fatalf("unexpected wait context error: %v", err)
	}

	msg, err := sub.Listen(ctx)
	if err != nil {
		t.Fatalf("unexpected listen error: %v", err)
	}

	if msg != "msg" {
		t.Fatalf("expected msg, got %s", msg)
	}
}

func TestSlowSubscriberDrop(t *testing.T) {
	bus := NewChanBus[string](
		WithSubscriptionBackpressure(1),
		WithSlowSubscriberPolicy(SlowSubscriberPolicyDrop),
	)
	defer bus.Close()

	// Subscriber that never reads (buffer fills, then drops)
	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	// Should not block forever
	result := bus.Broadcast("drop")
	if err := result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	// Ensure fast subscribers still work
	sub2, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub2.Unsubscribe()

	received := make(chan string, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		msg, err := sub2.Listen(ctx)
		if err == nil {
			received <- msg
		}
	}()

	result = bus.Broadcast("keep")
	if err := result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	select {
	case msg := <-received:
		if msg != "keep" {
			t.Fatalf("expected keep, got %s", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sub2 to receive")
	}
}

func TestSlowSubscriberTimeout(t *testing.T) {
	bus := NewChanBus[string](
		WithSubscriptionBackpressure(0),
		WithSlowSubscriberPolicy(SlowSubscriberPolicyTimeout),
		WithSlowSubscriberTimeout(50*time.Millisecond),
	)
	defer bus.Close()

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	start := time.Now()
	result := bus.Broadcast("timeout")
	if err := result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("broadcast took too long: %v", elapsed)
	}
}

func TestSubscriptionCloseRemovesFromBus(t *testing.T) {
	bus := NewChanBus[int]()
	defer bus.Close()

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	sub.Close()

	// Broadcast to trigger eager cleanup
	_ = bus.Broadcast(1)

	// Wait a bit for the cleanup goroutine to run
	time.Sleep(50 * time.Millisecond)

	// Verify the subscription no longer receives events
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = sub.Listen(ctx)
	if err == nil {
		t.Fatal("expected error after close, got nil")
	}
}

func TestBroadcastCallback(t *testing.T) {
	var callbackMu sync.Mutex
	var durations []time.Duration
	var subscriberCounts []int
	var droppedCounts []int

	bus := NewChanBus[string](
		WithSubscriptionBackpressure(1),
		WithSlowSubscriberPolicy(SlowSubscriberPolicyDrop),
		WithBroadcastCallback(func(d time.Duration, alive, dropped int) {
			callbackMu.Lock()
			defer callbackMu.Unlock()
			durations = append(durations, d)
			subscriberCounts = append(subscriberCounts, alive)
			droppedCounts = append(droppedCounts, dropped)
		}),
	)
	defer bus.Close()

	// One subscriber that will consume
	fastSub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer fastSub.Unsubscribe()
	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := fastSub.Listen(ctx)
			cancel()
			if err != nil {
				return
			}
		}
	}()

	// One subscriber that will drop (buffer fills)
	slowSub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer slowSub.Unsubscribe()

	// Give fastSub goroutine time to reach Listen
	time.Sleep(50 * time.Millisecond)

	// Fill slowSub's buffer deterministically
	_ = bus.Broadcast("fill1").Wait()
	_ = bus.Broadcast("fill2").Wait()

	result := bus.Broadcast("test")
	if err := result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	callbackMu.Lock()
	defer callbackMu.Unlock()

	if len(durations) == 0 {
		t.Fatal("expected callback to be called")
	}
	lastIdx := len(durations) - 1
	if subscriberCounts[lastIdx] != 2 {
		t.Fatalf("expected 2 subscribers in callback, got %d", subscriberCounts[lastIdx])
	}

	// With backpressure=1 and rapid back-to-back broadcasts, fastSub may
	// occasionally drop an event if its Listen goroutine is between calls.
	// We accept 1 or 2 dropped events as valid.
	if droppedCounts[lastIdx] < 1 || droppedCounts[lastIdx] > 2 {
		t.Fatalf("expected 1 or 2 dropped in callback, got %d", droppedCounts[lastIdx])
	}
}

func TestRace(t *testing.T) {
	bus := NewChanBus[int](WithPoolSize(4), WithSubscriptionBackpressure(1))
	defer bus.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = bus.Broadcast(i)
		}
	}()

	subs := make([]Subscription[int], 10)
	for i := range subs {
		s, err := bus.Subscribe()
		if err != nil {
			t.Fatalf("unexpected subscribe error: %v", err)
		}
		subs[i] = s
		go func(s Subscription[int]) {
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				_, err := s.Listen(ctx)
				cancel()
				if err != nil {
					return
				}
			}
		}(subs[i])
	}

	for i := 0; i < 100; i++ {
		idx := i % len(subs)
		if i%7 == 0 {
			subs[idx].Unsubscribe()
			s, err := bus.Subscribe()
			if err != nil {
				t.Fatalf("unexpected subscribe error: %v", err)
			}
			subs[idx] = s
			go func(s Subscription[int]) {
				for {
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
					_, err := s.Listen(ctx)
					cancel()
					if err != nil {
						return
					}
				}
			}(subs[idx])
		}
	}

	wg.Wait()
	for _, sub := range subs {
		sub.Unsubscribe()
	}
}

func TestLen(t *testing.T) {
	bus := NewChanBus[string]()
	defer bus.Close()

	if bus.Len() != 0 {
		t.Fatalf("expected Len=0, got %d", bus.Len())
	}

	s1, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	if bus.Len() != 1 {
		t.Fatalf("expected Len=1, got %d", bus.Len())
	}

	s2, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	if bus.Len() != 2 {
		t.Fatalf("expected Len=2, got %d", bus.Len())
	}

	s1.Unsubscribe()
	// Unsubscribe is async (CAS-loop), but eventually consistent
	time.Sleep(20 * time.Millisecond)
	if bus.Len() != 1 {
		t.Fatalf("expected Len=1 after unsubscribe, got %d", bus.Len())
	}

	s2.Unsubscribe()
	time.Sleep(20 * time.Millisecond)
	if bus.Len() != 0 {
		t.Fatalf("expected Len=0 after all unsubscribe, got %d", bus.Len())
	}
}

func TestShardedBroadcast(t *testing.T) {
	bus := NewChanBus[int](WithShardCount(4), WithSubscriptionBackpressure(1))
	defer bus.Close()

	subs := make([]Subscription[int], 20)
	for i := range subs {
		s, err := bus.Subscribe()
		if err != nil {
			t.Fatalf("unexpected subscribe error: %v", err)
		}
		subs[i] = s
	}

	result := bus.Broadcast(99)
	if err := result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	for i, sub := range subs {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		msg, err := sub.Listen(ctx)
		cancel()
		if err != nil {
			t.Fatalf("sub %d listen error: %v", i, err)
		}
		if msg != 99 {
			t.Fatalf("sub %d expected 99, got %d", i, msg)
		}
		sub.Unsubscribe()
	}
}

func TestShardedUnsubscribe(t *testing.T) {
	bus := NewChanBus[int](WithShardCount(8))
	defer bus.Close()

	subs := make([]Subscription[int], 100)
	for i := range subs {
		s, err := bus.Subscribe()
		if err != nil {
			t.Fatalf("unexpected subscribe error: %v", err)
		}
		subs[i] = s
	}

	if bus.Len() != 100 {
		t.Fatalf("expected Len=100, got %d", bus.Len())
	}

	for _, sub := range subs {
		sub.Unsubscribe()
	}

	// Broadcast to trigger cleanup
	_ = bus.Broadcast(1)

	// Wait for cleanup goroutines
	time.Sleep(100 * time.Millisecond)

	if bus.Len() != 0 {
		t.Fatalf("expected Len=0 after unsubscribe, got %d", bus.Len())
	}
}

func TestShardedRace(t *testing.T) {
	bus := NewChanBus[int](
		WithShardCount(runtime.GOMAXPROCS(0)*2),
		WithPoolSize(4),
		WithSubscriptionBackpressure(1),
		WithSlowSubscriberPolicy(SlowSubscriberPolicyDrop),
	)
	defer bus.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			_ = bus.Broadcast(i)
		}
	}()

	subs := make([]Subscription[int], 20)
	for i := range subs {
		s, err := bus.Subscribe()
		if err != nil {
			t.Fatalf("unexpected subscribe error: %v", err)
		}
		subs[i] = s
		go func(s Subscription[int]) {
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				_, err := s.Listen(ctx)
				cancel()
				if err != nil {
					return
				}
			}
		}(subs[i])
	}

	for i := 0; i < 500; i++ {
		idx := i % len(subs)
		if i%5 == 0 {
			subs[idx].Unsubscribe()
			s, err := bus.Subscribe()
			if err != nil {
				t.Fatalf("unexpected subscribe error: %v", err)
			}
			subs[idx] = s
			go func(s Subscription[int]) {
				for {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
					_, err := s.Listen(ctx)
					cancel()
					if err != nil {
						return
					}
				}
			}(subs[idx])
		}
	}

	wg.Wait()
	for _, sub := range subs {
		sub.Unsubscribe()
	}
}

func TestCloseGracefullyImmediate(t *testing.T) {
	bus := NewChanBus[string]()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := bus.CloseGracefully(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After graceful close, new operations are rejected
	_, err := bus.Subscribe()
	if !errors.Is(err, ErrBusClosed) {
		t.Fatalf("expected ErrBusClosed, got %v", err)
	}
}

func TestCloseGracefullyWaitsForBroadcast(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(1))

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	// Start a broadcast
	result := bus.Broadcast("hello")

	// Graceful close should wait for it
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := bus.CloseGracefully(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The broadcast should have completed successfully
	if err := result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	msg, err := sub.Listen(context.Background())
	if err != nil {
		t.Fatalf("unexpected listen error: %v", err)
	}

	if msg != "hello" {
		t.Fatalf("expected hello, got %s", msg)
	}
}

func TestCloseGracefullyTimeout(t *testing.T) {
	bus := NewChanBus[string](
		WithSlowSubscriberPolicy(SlowSubscriberPolicyBlock),
	)

	// Subscriber that never reads; broadcast will block
	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	// Start a blocking broadcast in the background
	go bus.Broadcast("blocked")

	// Give the broadcast time to start
	time.Sleep(20 * time.Millisecond)

	// Graceful close with short timeout should fail
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := bus.CloseGracefully(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestCloseGracefullyRejectsNewBroadcasts(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(1))

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	// Complete a broadcast first
	result := bus.Broadcast("hello")
	if err := result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	// Gracefully close synchronously
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := bus.CloseGracefully(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// New broadcasts should be rejected
	result = bus.Broadcast("late")
	if err := result.Wait(); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("expected ErrBusClosed, got %v", err)
	}
}

func TestCloseGracefullyRejectsNewSubscribes(t *testing.T) {
	bus := NewChanBus[string]()

	// Gracefully close synchronously
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := bus.CloseGracefully(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// New subscribes should be rejected
	_, err := bus.Subscribe()
	if !errors.Is(err, ErrBusClosed) {
		t.Fatalf("expected ErrBusClosed, got %v", err)
	}
}

func TestShardIndexOptimization(t *testing.T) {
	bus := NewChanBus[string](WithShardCount(8))
	defer bus.Close()

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}

	if bus.Len() != 1 {
		t.Fatalf("expected Len=1, got %d", bus.Len())
	}

	sub.Unsubscribe()
	time.Sleep(20 * time.Millisecond)

	if bus.Len() != 0 {
		t.Fatalf("expected Len=0 after unsubscribe, got %d", bus.Len())
	}
}

func TestAggregateStats(t *testing.T) {
	bus := NewChanBus[string](
		WithSubscriptionBackpressure(10),
		WithSlowSubscriberPolicy(SlowSubscriberPolicyDrop),
	)
	defer bus.Close()

	// Fast subscriber that we read from manually to guarantee delivery count
	fastSub, err := bus.Subscribe(SubWithBackpressure(10))
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer fastSub.Unsubscribe()

	// Slow subscriber with backpressure=0 (drops everything immediately)
	slowSub, err := bus.Subscribe(SubWithBackpressure(0))
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer slowSub.Unsubscribe()

	// Wait for subscriptions to settle in shards
	time.Sleep(5 * time.Millisecond)

	_ = bus.Broadcast("a").Wait()
	_ = bus.Broadcast("b").Wait()
	_ = bus.Broadcast("c").Wait()

	// Consume the 3 events from fastSub
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := fastSub.Listen(ctx)
		cancel()
		if err != nil {
			t.Fatalf("fastSub listen error on event %d: %v", i, err)
		}
	}

	agg := bus.Stats()
	if agg.Subscribers != 2 {
		t.Fatalf("expected 2 subscribers, got %d", agg.Subscribers)
	}
	if agg.EventsReceived != 3 {
		t.Fatalf("expected EventsReceived=3, got %d", agg.EventsReceived)
	}
	if agg.EventsDropped != 3 {
		t.Fatalf("expected EventsDropped=3, got %d", agg.EventsDropped)
	}
}

func TestSubscriptionString(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(1))
	defer bus.Close()

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
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

	// Before any events
	str := sub.String()
	if str != "relay.Subscription[received:0 dropped:0]" {
		t.Fatalf("unexpected initial string: %s", str)
	}

	_ = bus.Broadcast("hello").Wait()
	_ = bus.Broadcast("world").Wait()

	// After 2 events
	str = sub.String()
	if str != "relay.Subscription[received:2 dropped:0]" {
		t.Fatalf("unexpected string after events: %s", str)
	}
}

func TestTryBroadcast(t *testing.T) {
	bus := NewChanBus[string](
		WithPoolSize(1),
		WithSubscriptionBackpressure(1),
		WithSlowSubscriberPolicy(SlowSubscriberPolicyTimeout),
		WithSlowSubscriberTimeout(50*time.Millisecond),
	)
	defer bus.Close()

	// Create a subscriber that never reads
	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	// Pre-fill buffer
	_ = bus.Broadcast("prefill").Wait()

	// Start a blocking broadcast in background
	go bus.Broadcast("first")
	time.Sleep(5 * time.Millisecond)

	// TryBroadcast should return false (rejected) without error
	result, ok := bus.TryBroadcast("second")
	if ok {
		t.Fatal("expected TryBroadcast to return false")
	}

	if !errors.Is(result.Wait(), ErrBroadcastFull) {
		t.Fatalf("expected ErrBroadcastFull, got %v", result.Wait())
	}
}

func TestTryBroadcastSuccess(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(1))
	defer bus.Close()

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
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

	result, ok := bus.TryBroadcast("hello")
	if !ok {
		t.Fatal("expected TryBroadcast to return true")
	}

	if err = result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
}

func TestWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bus := NewChanBus[string](WithContext(ctx))

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	cancel()

	// Give the cancellation a moment to propagate
	time.Sleep(10 * time.Millisecond)

	if _, err = bus.Subscribe(); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("expected ErrBusClosed, got %v", err)
	}

	result := bus.Broadcast("nope")
	if err = result.Wait(); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("expected ErrBusClosed, got %v", err)
	}
}

func TestSubWithUnsubscribeCallback(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(1))
	defer bus.Close()

	var called bool
	sub, err := bus.Subscribe(SubWithUnsubscribeCallback(func() {
		called = true
	}))
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}

	sub.Unsubscribe()
	if !called {
		t.Fatal("expected unsubscribe callback to be called")
	}
}

func TestWaitContextCancelled(t *testing.T) {
	bus := NewChanBus[string](
		WithSubscriptionBackpressure(0),
		WithSlowSubscriberPolicy(SlowSubscriberPolicyBlock),
	)
	defer bus.Close()

	// Subscriber that never reads; broadcast will block
	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	result := bus.Broadcast("blocked")

	// WaitContext with a very short timeout should fail
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err = result.WaitContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	// Unblock the broadcast so the goroutine can exit
	sub.Unsubscribe()
}

func TestShardsUnsubscribe(t *testing.T) {
	shards := NewShards[int](4)
	subs := make([]*ChanSubscription[int], 10)
	for i := range subs {
		subs[i] = NewChanSubscription[int](SubWithBackpressure(1))
		shards.Subscribe(subs[i])
	}

	if shards.Len() != 10 {
		t.Fatalf("expected Len=10, got %d", shards.Len())
	}

	// Remove a sub using the scan-all Unsubscribe helper
	shards.Unsubscribe(subs[0])
	if shards.Len() != 9 {
		t.Fatalf("expected Len=9, got %d", shards.Len())
	}

	// Removing the same sub again should be a no-op
	shards.Unsubscribe(subs[0])
	if shards.Len() != 9 {
		t.Fatalf("expected Len=9 after duplicate unsubscribe, got %d", shards.Len())
	}

	// Remove remaining subs
	for i := 1; i < len(subs); i++ {
		shards.Unsubscribe(subs[i])
	}
	if shards.Len() != 0 {
		t.Fatalf("expected Len=0, got %d", shards.Len())
	}
}

func TestListenSubscriptionContextCancelled(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(1))
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sub, err := bus.SubscribeContext(ctx)
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}

	cancel()

	listenCtx, listenCancel := context.WithTimeout(context.Background(), time.Second)
	defer listenCancel()

	_, err = sub.Listen(listenCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestTryBroadcastBusClosed(t *testing.T) {
	bus := NewChanBus[string]()
	bus.Close()

	result, ok := bus.TryBroadcast("nope")
	if !ok {
		// TryBroadcast returns false only for backpressure (ErrBroadcastFull).
		// A closed bus returns true with an ErrBusClosed result.
		t.Fatal("expected TryBroadcast to return true")
	}

	if !errors.Is(result.Wait(), ErrBusClosed) {
		t.Fatalf("expected ErrBusClosed, got %v", result.Wait())
	}
}

func TestSubscribeWithBackpressureOverride(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(10))
	defer bus.Close()

	// Override with 0 backpressure (unbuffered)
	sub, err := bus.Subscribe(SubWithBackpressure(0))
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	// Because the channel is unbuffered and nobody is reading, Broadcast
	// with Drop policy should skip this subscriber immediately.
	bus2 := NewChanBus[string](
		WithSubscriptionBackpressure(10),
		WithSlowSubscriberPolicy(SlowSubscriberPolicyDrop),
	)
	defer bus2.Close()

	sub2, err := bus2.Subscribe(SubWithBackpressure(0))
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer sub2.Unsubscribe()

	result := bus2.Broadcast("test")
	if err = result.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	// sub2 should have dropped the event
	stats := sub2.Stats()
	if stats.EventsDropped != 1 {
		t.Fatalf("expected 1 dropped event, got %d", stats.EventsDropped)
	}
}

func TestUnsubscribeReturnsErrUnsubscribed(t *testing.T) {
	bus := NewChanBus[string](WithSubscriptionBackpressure(1))
	defer bus.Close()

	sub, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}

	sub.Unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = sub.Listen(ctx)
	if !errors.Is(err, ErrUnsubscribed) {
		t.Fatalf("expected ErrUnsubscribed, got %v", err)
	}
}
