package push

import (
	"context"
	"time"
)

func (p *Pacer[T]) runThrottle(ctx context.Context) {
	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()

	var latest T
	var hasData bool

	for {
		select {
		case <-ctx.Done():
			return

		case val, ok := <-p.input:
			if !ok {
				return
			}
			p.releaseInFlight()
			latest = val
			hasData = true

		case <-ticker.C:
			if hasData {
				if p.trySend(ctx, latest) {
					hasData = false
				}
			}
		}
	}
}

func (p *Pacer[T]) runDebounce(ctx context.Context) {
	var latest T
	var hasData bool
	var debounceTimer *time.Timer
	var debounceC <-chan time.Time

	stopTimer := func() {
		if debounceTimer == nil {
			return
		}
		if !debounceTimer.Stop() {
			select {
			case <-debounceTimer.C:
			default:
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			stopTimer()
			return

		case val, ok := <-p.input:
			if !ok {
				stopTimer()
				return
			}
			p.releaseInFlight()
			latest = val
			hasData = true
			stopTimer()
			debounceTimer = time.NewTimer(p.config.Interval)
			debounceC = debounceTimer.C

		case <-debounceC:
			if hasData {
				if p.trySend(ctx, latest) {
					hasData = false
				} else {
					debounceTimer = time.NewTimer(p.config.Interval)
					debounceC = debounceTimer.C
				}
			}
		}
	}
}
