package push

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Stats holds cumulative counters for a Pacer.
type Stats struct {
	Emitted uint64
	Dropped uint64
	Pending uint64
}

// Pacer regulates the flow of data between producers and consumers.
type Pacer[T any] struct {
	config Config

	input  chan T
	output chan T
	done   chan struct{}

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	stopped int32

	emitted  atomic.Uint64
	dropped  atomic.Uint64
	pending  atomic.Uint64
	inFlight atomic.Int32
}

// New creates a new Pacer with the given configuration.
func New[T any](config Config) (*Pacer[T], error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	config = config.withDefaults()

	queueSize := config.QueueSize
	if queueSize <= 0 {
		queueSize = 0
	}

	outputSize := 0
	if config.Mode == ModeRateLimit {
		outputSize = config.MaxItems
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Pacer[T]{
		config: config,
		input:  make(chan T, queueSize),
		output: make(chan T, outputSize),
		done:   make(chan struct{}),
		cancel: cancel,
	}

	p.wg.Add(1)
	go p.run(ctx)

	return p, nil
}

// Push enqueues an item using context.Background().
func (p *Pacer[T]) Push(item T) error {
	return p.PushContext(context.Background(), item)
}

// PushContext enqueues an item respecting the configured overflow policy.
func (p *Pacer[T]) PushContext(ctx context.Context, item T) error {
	if atomic.LoadInt32(&p.stopped) != 0 {
		return ErrStopped
	}

	switch p.config.Overflow {
	case OverflowDropOldest:
		return p.pushDropOldest(ctx, item)
	case OverflowBlock:
		return p.pushBlock(ctx, item)
	default:
		return p.pushDropNewest(ctx, item)
	}
}

func (p *Pacer[T]) maxPending() int {
	if p.config.QueueSize > 0 {
		return p.config.QueueSize
	}
	return 0
}

func (p *Pacer[T]) atCapacity() bool {
	cap := p.maxPending()
	if cap <= 0 {
		return false
	}
	switch p.config.Mode {
	case ModeQueue, ModeRateLimit:
		total := int(p.pending.Load()) + int(p.inFlight.Load())
		return total >= cap
	default:
		return false
	}
}

func (p *Pacer[T]) emit(ctx context.Context, item T) bool {
	if p.config.Mode == ModeQueue && p.config.Interval == 0 {
		return p.sendBlocking(ctx, item)
	}
	return p.trySend(ctx, item)
}

func (p *Pacer[T]) acquireInFlight() {
	p.inFlight.Add(1)
}

func (p *Pacer[T]) releaseInFlight() {
	p.inFlight.Add(-1)
}

func (p *Pacer[T]) enqueueInput(ctx context.Context, item T) error {
	p.acquireInFlight()
	select {
	case p.input <- item:
		return nil
	case <-ctx.Done():
		p.releaseInFlight()
		return ctx.Err()
	case <-p.done:
		p.releaseInFlight()
		return ErrStopped
	}
}

func (p *Pacer[T]) pushDropNewest(ctx context.Context, item T) error {
	if !p.atCapacity() {
		return p.enqueueInput(ctx, item)
	}

	timer := time.NewTimer(p.config.PushTimeout)
	defer timer.Stop()

	for {
		if !p.atCapacity() {
			return p.enqueueInput(ctx, item)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.done:
			return ErrStopped
		case <-timer.C:
			p.recordDrop("input buffer full")
			return ErrOverflow
		case <-time.After(time.Millisecond):
		}
	}
}

func (p *Pacer[T]) pushDropOldest(ctx context.Context, item T) error {
	if !p.atCapacity() {
		return p.enqueueInput(ctx, item)
	}
	select {
	case <-p.input:
		p.recordDrop("dropped oldest item")
		p.releaseInFlight()
	default:
	}
	return p.enqueueInput(ctx, item)
}

func (p *Pacer[T]) pushBlock(ctx context.Context, item T) error {
	for {
		if atomic.LoadInt32(&p.stopped) != 0 {
			return ErrStopped
		}
		if !p.atCapacity() {
			err := p.enqueueInput(ctx, item)
			if err == nil || err == ErrStopped {
				return err
			}
			if ctx.Err() == context.DeadlineExceeded {
				return ErrTimeout
			}
			return err
		}
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return ErrTimeout
			}
			return ctx.Err()
		case <-p.done:
			return ErrStopped
		case <-time.After(time.Millisecond):
		}
	}
}

func (p *Pacer[T]) recordDrop(reason string) {
	p.dropped.Add(1)
	p.config.Logger.Warn("item dropped", "reason", reason, "mode", p.config.Mode)
}

// Updates returns the read-only channel that emits regulated items.
// After Stop, the same closed channel is returned (never nil).
func (p *Pacer[T]) Updates() <-chan T {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.output
}

// Stats returns a snapshot of cumulative counters.
func (p *Pacer[T]) Stats() Stats {
	return Stats{
		Emitted: p.emitted.Load(),
		Dropped: p.dropped.Load(),
		Pending: p.pending.Load(),
	}
}

// Stop shuts down the pacer and closes the Updates channel.
func (p *Pacer[T]) Stop() {
	if !atomic.CompareAndSwapInt32(&p.stopped, 0, 1) {
		return
	}

	p.config.Logger.Debug("stopping pacer", "mode", p.config.Mode)
	p.cancel()
	close(p.done)
	p.wg.Wait()

	p.mu.Lock()
	close(p.output)
	p.mu.Unlock()
}

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
		if p.config.Interval == 0 {
			p.runQueueUnpaced(ctx)
		} else {
			p.runQueue(ctx)
		}
	}
}

func (p *Pacer[T]) trySend(ctx context.Context, item T) bool {
	select {
	case p.output <- item:
		p.emitted.Add(1)
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

func (p *Pacer[T]) sendBlocking(ctx context.Context, item T) bool {
	select {
	case p.output <- item:
		p.emitted.Add(1)
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *Pacer[T]) setPending(n int) {
	p.pending.Store(uint64(n))
}
