# go-push

**Not a notification library.** go-push is an in-process generic data-flow pacer for Go: throttle, debounce, rate-limit, and queue values between producers and consumers inside a single process.

Use it for UI refresh coalescing, search debounce, API pacing, sensor sampling, and ordered job dispatch — not for APNs, FCM, or Web Push.

## Features

- **Throttling** — emit the latest value at most once per interval (trailing edge)
- **Debouncing** — emit after a quiet period
- **Rate limiting** — up to `MaxItems` per interval, with immediate first-window flush
- **Queuing** — paced FIFO (`Interval > 0`) or unpaced FIFO (`Interval == 0`)
- **Overflow policies** — drop newest, drop oldest, or block producers
- **Errors & stats** — no silent drops unless you choose a drop policy
- **Logging** — inject any `Logger` (including [go-logger](https://github.com/gtsteffaniak/go-logger))
- **Generics** — type-safe `Pacer[T]`
- **Thread-safe** — safe concurrent `Push` and `Stop`

## Installation

```bash
go get github.com/gtsteffaniak/go-push@v1.0.0
```

Requires Go 1.24+.

## Quick start

```go
package main

import (
    "log"
    "time"

    "github.com/gtsteffaniak/go-push/push"
)

func main() {
    p, err := push.New[int](push.Config{
        Mode:     push.ModeThrottle,
        Interval: time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }

    if err := p.Push(42); err != nil {
        log.Fatal(err)
    }

    log.Println(<-p.Updates())
    p.Stop()
}
```

Run the stock example:

```bash
go run ./examples/stock
```

## Configuration

```go
type Config struct {
    Mode        Mode          // Throttle, Debounce, RateLimit, Queue
    Interval    time.Duration // Ticker/debounce window; 0 = unpaced queue
    MaxItems    int           // Rate limit: items per interval (required > 0)
    QueueSize   int           // Input buffer + max pending (queue/rate-limit)
    Overflow    Overflow      // DropNewest (default), DropOldest, Block
    PushTimeout time.Duration // DropNewest wait before ErrOverflow (default 50ms)
    DrainOnStop bool          // Emit remaining queue/rate-limit items on Stop
    Logger      Logger        // Optional; defaults to NopLogger
}
```

### Overflow policies

| Policy | Behavior |
|--------|----------|
| `OverflowDropNewest` | Wait up to `PushTimeout`, then return `ErrOverflow` |
| `OverflowDropOldest` | Remove the oldest buffered item to make room |
| `OverflowBlock` | Block until space is available, context cancelled, or `Stop` |

Blocking queue (jobs you cannot lose):

```go
p, err := push.New[Job](push.Config{
    Mode:      push.ModeQueue,
    Interval:  0,                 // unpaced: emit as fast as consumer reads
    QueueSize: 1000,
    Overflow:  push.OverflowBlock,
})
```

### Logging with go-logger

go-push does not import go-logger. Pass any logger that implements `Debug/Info/Warn/Error`:

```go
import gtlogger "github.com/gtsteffaniak/go-logger/logger"

log, _ := gtlogger.NewLogger(gtlogger.JsonConfig{Levels: "INFO,WARN,ERROR"})
p, err := push.New[int](push.Config{
    Mode:   push.ModeQueue,
    Logger: log.WithGroup("pacer"),
})
```

Or use the stdlib:

```go
p, err := push.New[int](push.Config{
    Logger: push.FromSlog(slog.Default()),
})
```

### Stats

```go
stats := p.Stats()
// stats.Emitted, stats.Dropped, stats.Pending
```

## What's new in v1.0.0

- **Errors instead of silent drops** — `Push` returns `ErrOverflow`, `ErrStopped`, or `ErrTimeout`
- **Config validation** — `New` returns `ErrInvalidConfig` for invalid modes/intervals
- **Overflow policies** — explicit backpressure control
- **Injectable logging** — drops, stops, and overflow warnings
- **Stats API** — observability for emitted/dropped/pending counts
- **Rate-limit fix** — up to `MaxItems` flush immediately, then per-interval windows
- **Stop safety** — `Updates()` never returns `nil` after `Stop`; debounce stop race fixed
- **CI & tests** — race detector, Go/OS matrix, go vet, govulncheck

See [CHANGELOG.md](CHANGELOG.md) for the full list.

## Migrating from v0.1

### 1. Handle `New` errors

```go
// v0.1
p := push.New[int](config)

// v1.0
p, err := push.New[int](config)
if err != nil {
    return err
}
```

### 2. Handle `Push` errors

```go
// v0.1 — silent drop after 50ms
p.Push(item)

// v1.0 — explicit result
if err := p.Push(item); err != nil {
    if errors.Is(err, push.ErrOverflow) {
        // buffer full under OverflowDropNewest
    }
}
```

Use `PushContext` for blocking producers:

```go
ctx, cancel := context.WithTimeout(ctx, time.Second)
defer cancel()
if err := p.PushContext(ctx, item); errors.Is(err, push.ErrTimeout) {
    // timed out waiting for space under OverflowBlock
}
```

### 3. Overflow default behavior

v0.1 silently dropped items when the input channel was full for 50ms. v1.0 defaults to `OverflowDropNewest` with the same 50ms `PushTimeout`, but returns **`ErrOverflow`** and increments **`Stats().Dropped`**. Choose `OverflowBlock` if you need delivery guarantees.

### 4. Demo location

The root `main.go` demo moved to `examples/stock/`.

### 5. Rate limiting

v1.0 emits up to `MaxItems` as soon as items arrive (first window), then refills each `Interval`. This matches the original README intent; v0.1 waited for the first ticker tick.

## Development

```bash
make test        # unit tests
make test-race   # race detector
make lint        # go vet
make gofmt       # gofmt check (CI parity)
make format      # gofmt -w
make examples    # build examples/stock
```

## License

MIT — see [LICENSE](LICENSE).
