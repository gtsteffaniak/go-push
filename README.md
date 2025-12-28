# go-push

A flexible Go library for regulating data flow with support for throttling, debouncing, rate limiting, and queuing. Built with generics for type-safe usage.

## Features

- **Throttling**: Emit the latest value at most once per interval
- **Debouncing**: Wait for a quiet period before emitting
- **Rate Limiting**: Allow up to N items per time period
- **Queuing**: Buffer items and emit them in order
- **Type-Safe**: Uses Go generics for compile-time type safety
- **Thread-Safe**: Safe to use from multiple goroutines
- **Non-Blocking**: Push operations won't block your application

## Installation

```bash
go get github.com/gtsteffaniak/go-push
```

## Quick Start

```go
package main

import (
    "time"
    "github.com/gtsteffaniak/go-push/push"
)

type StockPrice struct {
    Symbol string
    Price  float64
}

func main() {
    // Create a throttled pacer
    config := push.Config{
        Mode:     push.ModeThrottle,
        Interval: 1 * time.Second,
    }
    p := push.New[StockPrice](config)
    defer p.Stop()

    // Push updates
    p.Push(StockPrice{Symbol: "GOOG", Price: 100.50})

    // Receive throttled updates
    for update := range p.Updates() {
        fmt.Printf("Received: %+v\n", update)
    }
}
```

## Modes

### 1. Throttling (`ModeThrottle`)

Throttling emits the latest value at most once per interval. If multiple values arrive during an interval, only the most recent one is emitted.

**Use Case**: UI updates, sensor readings, reducing update frequency

```go
config := push.Config{
    Mode:     push.ModeThrottle,
    Interval: 1 * time.Second,
}
p := push.New[MyType](config)
```

**Example**: If you push 10 values in 1 second, only the last one is emitted after 1 second.

### 2. Debouncing (`ModeDebounce`)

Debouncing waits for a quiet period (no new inputs) before emitting the latest value. Each new input resets the timer.

**Use Case**: Search input, button clicks, form validation

```go
config := push.Config{
    Mode:     push.ModeDebounce,
    Interval: 500 * time.Millisecond,
}
p := push.New[MyType](config)
```

**Example**: If you push values at t=0ms, 100ms, 200ms, the value is emitted 500ms after the last push (at t=700ms).

### 3. Rate Limiting (`ModeRateLimit`)

Rate limiting allows up to `MaxItems` per `Interval`. Excess items are queued and emitted in subsequent intervals.

**Use Case**: API rate limiting, database writes, network requests

```go
config := push.Config{
    Mode:     push.ModeRateLimit,
    Interval: 1 * time.Second,
    MaxItems: 10, // Max 10 items per second
}
p := push.New[MyType](config)
```

**Example**: If you push 25 items in 1 second with `MaxItems: 10`, 10 are emitted immediately, then 10 more after 1 second, then 5 more after 2 seconds.

### 4. Queuing (`ModeQueue`)

Queuing buffers items and emits them one at a time at the specified interval, maintaining order.

**Use Case**: Processing items sequentially, batch operations, ordered processing

```go
config := push.Config{
    Mode:     push.ModeQueue,
    Interval: 1 * time.Second,
    QueueSize: 100, // Buffer size (optional, defaults to 100)
}
p := push.New[MyType](config)
```

**Example**: If you push 5 items quickly, they are emitted one per second in the order they were pushed.

## Configuration

The `Config` struct supports the following fields:

```go
type Config struct {
    Mode      Mode          // Mode: ModeThrottle, ModeDebounce, ModeRateLimit, or ModeQueue
    Interval  time.Duration // Time duration for the mode
    MaxItems  int           // Used for rate limiting (max items per interval)
    QueueSize int           // Buffer size for queue mode (0 = default 100)
}
```

### Default Configuration

Use `DefaultConfig` for a quick setup with throttling:

```go
config := push.DefaultConfig(1 * time.Second)
p := push.New[MyType](config)
```

## API Reference

### `New[T any](config Config) *Pacer[T]`

Creates a new Pacer with the given configuration. The type parameter `T` is the type of data you want to regulate.

### `Push(item T)`

Pushes a new item to the pacer. This is non-blocking and safe to call from multiple goroutines.

### `Updates() <-chan T`

Returns a read-only channel that emits regulated updates. The channel is closed when `Stop()` is called.

### `Stop()`

Stops the pacer, cleans up resources, and closes the output channel. Always call this when done to prevent goroutine leaks.

## Complete Examples

### Example 1: Throttling Stock Prices

```go
package main

import (
    "fmt"
    "time"
    "github.com/gtsteffaniak/go-push/push"
)

type StockPrice struct {
    Symbol string
    Price  float64
}

func main() {
    config := push.Config{
        Mode:     push.ModeThrottle,
        Interval: 1 * time.Second,
    }
    p := push.New[StockPrice](config)
    defer p.Stop()

    // Producer sends updates every 100ms
    go func() {
        for i := 0; i < 10; i++ {
            p.Push(StockPrice{Symbol: "GOOG", Price: 100.0 + float64(i)})
            time.Sleep(100 * time.Millisecond)
        }
    }()

    // Consumer receives throttled updates (once per second)
    for update := range p.Updates() {
        fmt.Printf("Price: $%.2f\n", update.Price)
    }
}
```

### Example 2: Debouncing Search Input

```go
config := push.Config{
    Mode:     push.ModeDebounce,
    Interval: 300 * time.Millisecond,
}
p := push.New[string](config)
defer p.Stop()

// User types quickly
p.Push("g")
p.Push("go")
p.Push("gol")
p.Push("gola")
p.Push("golan")
p.Push("golang")

// Only "golang" is emitted after 300ms of no new input
for query := range p.Updates() {
    performSearch(query)
}
```

### Example 3: Rate Limiting API Calls

```go
config := push.Config{
    Mode:     push.ModeRateLimit,
    Interval: 1 * time.Second,
    MaxItems: 5, // Max 5 API calls per second
}
p := push.New[APIRequest](config)
defer p.Stop()

// Push many requests
for _, req := range requests {
    p.Push(req)
}

// Requests are rate-limited to 5 per second
for req := range p.Updates() {
    makeAPICall(req)
}
```

### Example 4: Queuing Tasks

```go
config := push.Config{
    Mode:     push.ModeQueue,
    Interval: 500 * time.Millisecond,
    QueueSize: 1000,
}
p := push.New[Task](config)
defer p.Stop()

// Add tasks quickly
for _, task := range tasks {
    p.Push(task)
}

// Tasks are processed one every 500ms in order
for task := range p.Updates() {
    processTask(task)
}
```

## Thread Safety

All methods are safe to call from multiple goroutines:
- `Push()` can be called concurrently
- `Updates()` can be read from multiple goroutines (though each value is only delivered once)
- `Stop()` should be called once when done

## Performance Considerations

- **Throttle/Debounce**: O(1) memory, only stores the latest value
- **Rate Limit**: O(N) memory where N is the number of pending items
- **Queue**: O(N) memory where N is the queue size

The library uses buffered channels where appropriate to prevent blocking. If the output channel is full and the receiver isn't reading, items may be dropped (in throttle/debounce) or queued (in rate limit/queue modes).

## License

MIT

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.


