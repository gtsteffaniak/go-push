package push

import (
	"context"
	"time"
)

func (p *Pacer[T]) runRateLimit(ctx context.Context) {
	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()

	pending := make([]T, 0, p.config.MaxItems)
	cap := p.maxPending()

	flush := func() {
		sent := 0
		limit := p.config.MaxItems
		for sent < limit && len(pending) > 0 {
			if !p.emit(ctx, pending[0]) {
				break
			}
			pending = pending[1:]
			sent++
		}
		p.setPending(len(pending))
	}

	for {
		select {
		case <-ctx.Done():
			if p.config.DrainOnStop {
				for len(pending) > 0 {
					if !p.sendBlocking(ctx, pending[0]) {
						break
					}
					pending = pending[1:]
				}
			}
			p.setPending(0)
			return

		case val := <-p.input:
			p.releaseInFlight()
			if cap > 0 && len(pending) >= cap {
				p.recordDrop("rate limit pending full")
				continue
			}
			pending = append(pending, val)
			p.setPending(len(pending))
			for {
				before := len(pending)
				flush()
				if len(pending) == 0 || len(pending) == before {
					break
				}
			}

		case <-ticker.C:
			flush()
		}
	}
}
