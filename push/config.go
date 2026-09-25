package push

import "time"

// Mode defines the behavior of the Pacer.
type Mode int

const (
	// ModeThrottle emits the latest value at most once per interval (trailing edge).
	ModeThrottle Mode = iota
	// ModeDebounce waits for a quiet period before emitting the latest value.
	ModeDebounce
	// ModeRateLimit allows up to MaxItems per Interval.
	ModeRateLimit
	// ModeQueue buffers items and emits them in order (one per interval, or as fast
	// as the consumer when Interval is zero).
	ModeQueue
)

// Overflow defines how Push behaves when the input buffer is full.
type Overflow int

const (
	// OverflowDropNewest waits up to PushTimeout for space, then returns ErrOverflow.
	OverflowDropNewest Overflow = iota
	// OverflowDropOldest removes the oldest buffered item to make room for the new one.
	// The evicted item may be one whose Push already returned nil. That removal is
	// counted in Stats.Dropped.
	OverflowDropOldest
	// OverflowBlock waits until space is available, the context is cancelled, or Stop is called.
	OverflowBlock
)

// Config holds configuration for creating a new Pacer.
type Config struct {
	// Mode determines the behavior (Throttle, Debounce, RateLimit, or Queue).
	Mode Mode
	// Interval is the time duration for throttling, debouncing, rate limiting, or
	// paced queue emission. For ModeQueue, zero means emit as fast as the consumer.
	Interval time.Duration
	// MaxItems is used for rate limiting (max items per interval).
	MaxItems int
	// QueueSize is the input channel buffer and maximum pending items (0 = default).
	QueueSize int
	// Overflow controls backpressure when the input buffer is full.
	Overflow Overflow
	// PushTimeout is how long OverflowDropNewest waits before returning ErrOverflow.
	PushTimeout time.Duration
	// DrainOnStop delivers items still buffered by ModeQueue and ModeRateLimit when
	// Stop is called. A consumer must be reading Updates during Stop. Each send
	// waits at most PushTimeout; if that elapses, the rest are dropped, counted
	// in Stats.Dropped, and Stop returns.
	DrainOnStop bool
	// Logger receives diagnostic messages. Nil defaults to NopLogger.
	Logger Logger
}

// DefaultConfig returns a default configuration with throttling mode.
func DefaultConfig(interval time.Duration) Config {
	return Config{
		Mode:     ModeThrottle,
		Interval: interval,
		MaxItems: 10,
	}
}

func (c Config) withDefaults() Config {
	if c.MaxItems <= 0 {
		c.MaxItems = 10
	}
	if c.QueueSize <= 0 && (c.Mode == ModeQueue || c.Mode == ModeRateLimit) {
		c.QueueSize = 100
	}
	if c.PushTimeout <= 0 {
		c.PushTimeout = 50 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = NopLogger()
	}
	return c
}

// Validate checks the configuration and returns ErrInvalidConfig on failure.
func (c Config) Validate() error {
	switch c.Mode {
	case ModeThrottle, ModeDebounce, ModeRateLimit, ModeQueue:
	default:
		return ErrInvalidConfig
	}
	switch c.Mode {
	case ModeQueue:
		if c.Interval < 0 {
			return ErrInvalidConfig
		}
	case ModeRateLimit:
		if c.Interval <= 0 || c.MaxItems <= 0 {
			return ErrInvalidConfig
		}
	default:
		if c.Interval <= 0 {
			return ErrInvalidConfig
		}
	}
	if c.QueueSize < 0 {
		return ErrInvalidConfig
	}
	switch c.Overflow {
	case OverflowDropNewest, OverflowDropOldest, OverflowBlock:
	default:
		return ErrInvalidConfig
	}
	return nil
}
