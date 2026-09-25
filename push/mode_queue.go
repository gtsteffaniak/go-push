package push

import (
	"context"
	"time"
)

func (p *Pacer[T]) runQueue(ctx context.Context) {
	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()

	queue := make([]T, 0)
	limit := p.maxPending()

	emitOne := func() {
		if len(queue) == 0 {
			return
		}
		item := queue[0]
		if p.emit(ctx, item) {
			queue = queue[1:]
			p.setPending(len(queue))
		}
	}

	for {
		select {
		case <-ctx.Done():
			p.collectInput(func(val T) {
				queue = append(queue, val)
				p.setPending(len(queue))
			})
			p.drainRetained(queue)
			return

		case val := <-p.input:
			queue = p.retainPending(queue, val, limit)

		case <-ticker.C:
			emitOne()
		}
	}
}

func (p *Pacer[T]) runQueueUnpaced(ctx context.Context) {
	queue := make([]T, 0)
	limit := p.maxPending()

	tryEmit := func() {
		for len(queue) > 0 {
			if !p.emit(ctx, queue[0]) {
				return
			}
			queue = queue[1:]
			p.setPending(len(queue))
		}
	}

	for {
		select {
		case <-ctx.Done():
			p.collectInput(func(val T) {
				queue = append(queue, val)
				p.setPending(len(queue))
			})
			p.drainRetained(queue)
			return

		case val := <-p.input:
			queue = p.retainPending(queue, val, limit)
			tryEmit()
		}
	}
}
