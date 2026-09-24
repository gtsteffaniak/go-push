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
			if p.config.DrainOnStop {
				// ctx is already canceled here, so use a fresh context to
				// deliver the remaining items instead of discarding them.
				for len(pending) > 0 {
					if !p.sendBlocking(context.Background(), pending[0]) {
						break
					}
					pending = pending[1:]
				}
			}
			p.setPending(0)
			return

		case val := <-p.input:
			p.releaseInFlight()
			if limit > 0 && len(pending) >= limit {
				if p.config.Overflow == OverflowDropOldest {
					pending = pending[1:]
					p.recordDrop("dropped oldest item")
				} else {
					p.recordDrop("rate limit pending full")
					continue
				}
			}
			pending = append(pending, val)
			p.setPending(len(pending))
			flush()

		case <-ticker.C:
			tokens = p.config.MaxItems
			flush()
		}
	}
}
