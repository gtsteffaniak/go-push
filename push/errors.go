package push

import "errors"

var (
	// ErrStopped is returned when Push or PushContext is called after Stop.
	ErrStopped = errors.New("push: pacer stopped")

	// ErrOverflow is returned when the input buffer is full and OverflowDropNewest
	// is configured (after PushTimeout elapses).
	ErrOverflow = errors.New("push: input buffer full")

	// ErrTimeout is returned when PushContext times out waiting for buffer space
	// under OverflowBlock.
	ErrTimeout = errors.New("push: push timed out waiting for buffer space")

	// ErrInvalidConfig is returned when Config fails validation.
	ErrInvalidConfig = errors.New("push: invalid config")
)
