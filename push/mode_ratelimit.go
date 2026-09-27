package push

import (
	"context"
	"time"
)

func (p *Pacer[T]) runRateLimit(ctx context.Context) {
	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()

	pending := make([]T, 0, p.config.MaxItems)
	limit := p.maxPending()

	// tokens is the single emission budget for the current interval. It is
	// refilled only when the ticker fires so that repeated flushes cannot emit
	// more than MaxItems per interval.
	tokens := p.config.MaxItems

	flush := func() {
		for tokens > 0 && len(pending) > 0 {
			if !p.emit(ctx, pending[0]) {
				break
			}
			pending = pending[1:]
			tokens--
		}
		p.setPending(len(pending))
	}

	for {
		select {
		case <-ctx.Done():
			p.collectInput(func(val T) {
				pending = append(pending, val)
				p.setPending(len(pending))
			})
			// ctx is already canceled. Drain on a timeout so Stop cannot
			// block forever when nothing is reading Updates.
			p.drainRetained(pending)
			return

		case val := <-p.input:
			pending = p.retainPending(pending, val, limit)
			flush()

		case <-ticker.C:
			tokens = p.config.MaxItems
			flush()
		}
	}
}
