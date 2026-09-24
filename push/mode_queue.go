package push

import (
	"context"
	"time"
)

func (p *Pacer[T]) runQueue(ctx context.Context) {
	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()

	queue := make([]T, 0)
	cap := p.maxPending()

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
			p.drainQueue(context.Background(), &queue)
			return

		case val := <-p.input:
			p.releaseInFlight()
			if cap > 0 && len(queue) >= cap {
				if p.config.Overflow == OverflowDropOldest {
					queue = queue[1:]
					p.recordDrop("dropped oldest item")
				} else {
					p.recordDrop("queue full")
					continue
				}
			}
			queue = append(queue, val)
			p.setPending(len(queue))

		case <-ticker.C:
			emitOne()
		}
	}
}

func (p *Pacer[T]) runQueueUnpaced(ctx context.Context) {
	queue := make([]T, 0)
	cap := p.maxPending()

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
			p.drainQueue(context.Background(), &queue)
			return

		case val := <-p.input:
			p.releaseInFlight()
			if cap > 0 && len(queue) >= cap {
				if p.config.Overflow == OverflowDropOldest {
					queue = queue[1:]
					p.recordDrop("dropped oldest item")
				} else {
					p.recordDrop("queue full")
					continue
				}
			}
			queue = append(queue, val)
			p.setPending(len(queue))
			tryEmit()
		}
	}
}

func (p *Pacer[T]) drainQueue(ctx context.Context, queue *[]T) {
	if !p.config.DrainOnStop {
		p.setPending(0)
		return
	}
	for len(*queue) > 0 {
		if !p.sendBlocking(ctx, (*queue)[0]) {
			break
		}
		*queue = (*queue)[1:]
	}
	p.setPending(len(*queue))
}
