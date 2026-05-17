# Relay

<p align="center">
  <a href="https://pkg.go.dev/github.com/binozo/go-relay/relay"><img src="https://pkg.go.dev/badge/github.com/binozo/go-relay/relay.svg" alt="Go Reference"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-%5E1.22-00ADD8?logo=go" alt="Go Version"></a>
</p>

<p align="center">
  <strong>Zero-dependency, type-safe, context-aware event bus for Go.</strong>
</p>

Relay provides a simple `Bus` pattern for one-to-many communication using generics and Go channels. Built on the standard library with no external dependencies.

## Features

- **Zero external dependencies.** Only the Go standard library.
- **Fire-and-forget or wait.** `Broadcast()` returns a `BroadcastResult`; ignore it for async delivery or call `Wait()` to block until all subscribers have been handled.
- **Configurable slow-subscriber policy.** Block (default), drop, or apply a timeout per subscriber.
- **Per-subscription backpressure.** Control the size of each subscriber's event buffer.
- **Context-aware.** Subscriptions respect cancellation, and the bus can be shut down cleanly with `Close()`.
- **Observability hooks.** Attach a callback to monitor broadcast latency and drop rates.
- **Lock-free broadcasts.** Subscriber snapshots use `atomic.Pointer` for zero contention on the hot path.
- **Sharded subscriber storage.** Subscribe/unsubscribe scale with `GOMAXPROCS` shards.
- **Graceful shutdown.** `CloseGracefully` waits for in-flight broadcasts.

## Installation

```bash
go get -u github.com/binozo/go-relay
```

Requires Go 1.22 or later.

## Quick Start

```go
package main

import (
	"context"
	"fmt"

	"github.com/binozo/go-relay/relay"
)

func main() {
	bus := relay.NewChanBus[string]()
	defer bus.Close()

	sub, err := bus.Subscribe()
	if err != nil {
		panic(err)
	}
	defer sub.Unsubscribe()

	go func() {
		for {
			msg, err := sub.Listen(context.TODO())
			if err != nil {
				return
			}
			fmt.Println("Received:", msg)
		}
	}()

	result := bus.Broadcast("Hello, Relay!")
	_ = result.Wait() // optional: wait until the event has been delivered
}
```

## Configuration

```go
bus := relay.NewChanBus[string](
    relay.WithPoolSize(4),               // limit concurrent broadcast goroutines; defaults to GOMAXPROCS
    relay.WithShardCount(8),              // subscriber list shards; defaults to GOMAXPROCS
    relay.WithSubscriptionBackpressure(10), // each subscriber gets a buffered channel of size 10
    relay.WithSlowSubscriberPolicy(relay.SlowSubscriberPolicyDrop), // drop events for slow readers
    relay.WithSlowSubscriberTimeout(100*time.Millisecond),           // only used with SlowSubscriberPolicyTimeout
    relay.WithBroadcastCallback(func(d time.Duration, alive, dropped int) {
        fmt.Printf("broadcast: %d alive, %d dropped, took %v\n", alive, dropped, d)
    }),
)
```

## Observability & Metrics

```go
// Query live subscriber count
fmt.Println("subscribers:", bus.Len())

// Query aggregate stats across all subscribers
agg := bus.Stats()
fmt.Printf("received=%d dropped=%d subscribers=%d\n", agg.EventsReceived, agg.EventsDropped, agg.Subscribers)

// Per-subscriber stats
stats := sub.Stats()
fmt.Printf("received=%d dropped=%d\n", stats.EventsReceived, stats.EventsDropped)
```

## Backpressure

If `WithPoolSize` is configured, the semaphore caps concurrent broadcasts. Use `TryBroadcast` for non-blocking attempts:

```go
result, ok := bus.TryBroadcast("urgent")
if !ok {
    // bus is saturated -- drop or retry later
}
```

For blocking semantics (wait until the semaphore has room), use `Broadcast`:

```go
result := bus.Broadcast("normal")
if err := result.Wait(); errors.Is(err, relay.ErrBroadcastFull) {
    // only possible if poolSize > 0 and the bus is saturated
}
```

## Error Handling

| Method | Error | Meaning |
|---|---|---|
| `Bus.Subscribe()` | `relay.ErrBusClosed` | Bus has been shut down. |
| `Bus.SubscribeContext(ctx)` | `context.Canceled` / `context.DeadlineExceeded` | Context was cancelled before subscription completed. |
| `Bus.Broadcast()` | - | Returns a `BroadcastResult`; actual error comes from `Wait()`. |
| `BroadcastResult.Wait()` | `relay.ErrBusClosed` | Bus closed during broadcast. |
| `BroadcastResult.Wait()` | `relay.ErrBroadcastFull` | Semaphore saturated (`WithPoolSize` only). |
| `Subscription.Listen(ctx)` | `relay.ErrSubscriptionClosed` | Subscription unsubscribed. |
| `Subscription.Listen(ctx)` | ctx error | Listener context was cancelled. |

## Context-Aware Subscription

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

sub, err := bus.SubscribeContext(ctx)
if err != nil {
    // handle error
}
defer sub.Unsubscribe()

// If ctx expires, Listen will return the context error
msg, err := sub.Listen(context.Background())
```

## Graceful Shutdown

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := bus.CloseGracefully(ctx); err != nil {
    // timeout -- some broadcasts were still in flight
}
```

## API Reference

| Type | Description |
|---|---|
| `ChanBus[T]` | Main event bus for type `T`. |
| `BroadcastResult` | Handle for tracking a broadcast; call `Wait()` to block. |
| `Subscription[T]` | Receives events of type `T` via `Listen(ctx)`. |
| `Stats` | Aggregated subscriber metrics. |
| `SlowSubscriberPolicy` | Block / Drop / Timeout strategy. |

Full API docs: [pkg.go.dev](https://pkg.go.dev/github.com/binozo/go-relay)

## Benchmarks

Run `go test -bench=. -benchmem .` in the project root.

**Environment:** Go 1.22, AMD Ryzen 5 5600X, Linux

### Broadcast Throughput

| Subscribers | ops/sec | ns/op | B/op | allocs/op |
|------------:|--------:|------:|-----:|----------:|
| 1           | ~270k   | 3,709 | 672  | 9         |
| 10          | ~82k    | 14,278| 4,128| 54        |
| 100         | ~18k    | 64,305| 38,708| 504      |
| 1000        | ~2.4k   | 497,939| 384,609| 5,006  |

### Slow-Subscriber Policy (100 subscribers, fast readers)

| Policy | ops/sec | ns/op | B/op | allocs/op |
|--------|--------:|------:|-----:|----------:|
| Block  | ~39k    | 31,228| 288  | **4**     |
| Drop   | ~40k    | 29,698| 288  | **4**     |
| Timeout (1ms) | ~25k | 48,065| 536  | **7**     |

### Churn (subscribe -> broadcast -> unsubscribe)

| Shards | ops/sec | ns/op | B/op | allocs/op |
|--------|--------:|------:|-----:|----------:|
| 1      | ~521k   | 2,031 | 707  | 16        |
| 4      | ~567k   | 2,039 | 732  | 16        |
| 12     | ~465k   | 2,150 | 795  | 16        |

### Utilities

| Operation | ops/sec | ns/op | B/op | allocs/op |
|-----------|--------:|------:|-----:|----------:|
| `TryBroadcast` | ~5.8M | 194 | 97 | 1 |
| `Stats` (10 subs) | ~6.1M | 193 | 80 | 1 |
| `Stats` (100 subs) | ~1.2M | 1,015 | 896 | 1 |
| `Stats` (1000 subs) | ~125k | 9,828 | 8,194 | 1 |

### Notes on Benchmarks

- Broadcast cost **scales linearly** with subscriber count by design: one sequential goroutine iterates the snapshot. The broadcast hot path itself allocates 3-5 times regardless of subscriber count. Higher numbers in `BenchmarkBroadcast` come from the benchmark's background listener goroutines, which create a `context.WithTimeout` on every `Listen()` call. This is not overhead from Relay.
- `TryBroadcast` is sub-microsecond.
- Churn throughput is ~500k/sec regardless of shard count because the dominant cost is goroutine creation, not shard lookup.

## Design

Relay is intentionally simple:

1. **Snapshots, not locks.** On every broadcast, the bus takes an atomic snapshot of the current subscriber list. Readers never block writers and vice versa.
2. **Sharded maps.** Subscribers are distributed across `GOMAXPROCS` shards to reduce contention during subscribe/unsubscribe.
3. **Channels everywhere.** Events flow through Go channels for familiarity and composability with standard patterns.

## License

[Apache 2.0](LICENSE)
