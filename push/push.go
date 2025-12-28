package push

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Mode defines the behavior of the Pacer
type Mode int

const (
	// ModeThrottle emits the latest value at most once per interval
	ModeThrottle Mode = iota
	// ModeDebounce waits for a quiet period before emitting
	ModeDebounce
	// ModeRateLimit allows up to MaxItems per Interval
	ModeRateLimit
	// ModeQueue buffers items and emits them in order
	ModeQueue
)

// Config holds configuration for creating a new Pacer
type Config struct {
	// Mode determines the behavior (Throttle, Debounce, RateLimit, or Queue)
	Mode Mode
	// Interval is the time duration for throttling/debouncing/rate limiting
	Interval time.Duration
	// MaxItems is used for rate limiting (max items per interval)
	MaxItems int
	// QueueSize is the buffer size for queue mode (0 = unbuffered)
	QueueSize int
}

// DefaultConfig returns a default configuration with throttling mode
func DefaultConfig(interval time.Duration) Config {
	return Config{
		Mode:      ModeThrottle,
		Interval:  interval,
		MaxItems:  10,
		QueueSize: 0,
	}
}

// Pacer is a generic struct that regulates the flow of data.
// It supports multiple modes: throttling, debouncing, rate limiting, and queuing.
type Pacer[T any] struct {
	input   chan T
	output  chan T
	config  Config
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	queue   []T
	stopped int32 // Atomic flag to track if stopped
}

// New creates a new Pacer with the given configuration.
func New[T any](config Config) *Pacer[T] {
	ctx, cancel := context.WithCancel(context.Background())

	queueSize := config.QueueSize
	if queueSize == 0 && config.Mode == ModeQueue {
		queueSize = 100 // Default queue size
	}

	p := &Pacer[T]{
		input:  make(chan T, queueSize),
		output: make(chan T, queueSize),
		config: config,
		cancel: cancel,
		queue:  make([]T, 0),
	}

	p.wg.Add(1)
	go p.run(ctx)

	return p
}

// Push updates the current state. This is non-blocking and safe to call
// from multiple goroutines.
func (p *Pacer[T]) Push(item T) {
	select {
	case p.input <- item:
	case <-time.After(50 * time.Millisecond):
		// Safety valve: if the input channel is full or the run loop is shutting down,
		// we don't want callers to deadlock.
	}
}

// Updates returns the read-only channel to receive the regulated data.
func (p *Pacer[T]) Updates() <-chan T {
	p.mu.Lock()
	ch := p.output
	p.mu.Unlock()
	return ch
}

// Stop cleans up resources and stops the background goroutine.
// It is safe to call Stop() multiple times.
func (p *Pacer[T]) Stop() {
	// Use atomic to ensure Stop() is idempotent
	if !atomic.CompareAndSwapInt32(&p.stopped, 0, 1) {
		// Already stopped
		return
	}

	p.cancel()
	p.wg.Wait() // Wait for the loop to finish

	// Close output channel safely
	p.mu.Lock()
	if p.output != nil {
		close(p.output)
		p.output = nil
	}
	p.mu.Unlock()
}

// run is the internal loop that handles the mechanics based on the mode.
func (p *Pacer[T]) run(ctx context.Context) {
	defer p.wg.Done()

	switch p.config.Mode {
	case ModeThrottle:
		p.runThrottle(ctx)
	case ModeDebounce:
		p.runDebounce(ctx)
	case ModeRateLimit:
		p.runRateLimit(ctx)
	case ModeQueue:
		p.runQueue(ctx)
	}
}

// runThrottle implements throttling: emits the latest value at most once per interval.
func (p *Pacer[T]) runThrottle(ctx context.Context) {
	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()

	var latest T
	var hasData bool

	for {
		select {
		case <-ctx.Done():
			return

		case val := <-p.input:
			latest = val
			hasData = true

		case <-ticker.C:
			if hasData {
				select {
				case p.output <- latest:
					hasData = false
				case <-ctx.Done():
					return
				default:
					// Receiver is blocked, skip this tick
				}
			}
		}
	}
}

// runDebounce implements debouncing: waits for a quiet period before emitting.
func (p *Pacer[T]) runDebounce(ctx context.Context) {
	var latest T
	var hasData bool
	var debounceTimer *time.Timer
	var timerMu sync.Mutex

	for {
		select {
		case <-ctx.Done():
			timerMu.Lock()
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			timerMu.Unlock()
			return

		case val := <-p.input:
			p.mu.Lock()
			latest = val
			hasData = true
			p.mu.Unlock()

			// Reset the debounce timer
			timerMu.Lock()
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceTimer = time.AfterFunc(p.config.Interval, func() {
				p.mu.Lock()
				if hasData {
					select {
					case p.output <- latest:
						hasData = false
					default:
						// Receiver is blocked
					}
				}
				p.mu.Unlock()
			})
			timerMu.Unlock()
		}
	}
}

// runRateLimit implements rate limiting: allows up to MaxItems per Interval.
func (p *Pacer[T]) runRateLimit(ctx context.Context) {
	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()

	pending := make([]T, 0)

	for {
		select {
		case <-ctx.Done():
			return

		case val := <-p.input:
			p.mu.Lock()
			pending = append(pending, val)
			p.mu.Unlock()

		case <-ticker.C:
			// Reset the rate limit window
			p.mu.Lock()
			toSend := p.config.MaxItems
			if toSend > len(pending) {
				toSend = len(pending)
			}

			for i := 0; i < toSend; i++ {
				select {
				case p.output <- pending[i]:
				case <-ctx.Done():
					p.mu.Unlock()
					return
				default:
					// Receiver is blocked, stop trying
					break
				}
			}

			// Remove sent items
			if toSend > 0 {
				pending = pending[toSend:]
			}
			p.mu.Unlock()
		}
	}
}

// runQueue implements queuing: buffers items and emits them in order.
func (p *Pacer[T]) runQueue(ctx context.Context) {
	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Drain remaining items
			for {
				select {
				case val := <-p.input:
					select {
					case p.output <- val:
					default:
						// Receiver is blocked, drop item
					}
				default:
					return
				}
			}

		case val := <-p.input:
			p.mu.Lock()
			p.queue = append(p.queue, val)
			p.mu.Unlock()

		case <-ticker.C:
			p.mu.Lock()
			if len(p.queue) > 0 {
				item := p.queue[0]
				p.queue = p.queue[1:]

				select {
				case p.output <- item:
				case <-ctx.Done():
					p.mu.Unlock()
					return
				default:
					// Receiver is blocked, put item back at front
					p.queue = append([]T{item}, p.queue...)
				}
			}
			p.mu.Unlock()
		}
	}
}
