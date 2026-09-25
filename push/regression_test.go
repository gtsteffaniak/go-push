package push

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDrainOnStopWithoutConsumer verifies Stop returns even when DrainOnStop
// is set and nothing reads Updates. The old path blocked in sendBlocking on
// context.Background.
func TestDrainOnStopWithoutConsumer(t *testing.T) {
	cases := []struct {
		name        string
		cfg         Config
		n           int
		wantDropped uint64
	}{
		{
			name: "queue",
			cfg: Config{
				Mode:        ModeQueue,
				Interval:    time.Hour,
				QueueSize:   8,
				DrainOnStop: true,
				PushTimeout: 20 * time.Millisecond,
			},
			n:           3,
			wantDropped: 3,
		},
		{
			name: "queue-unpaced",
			cfg: Config{
				Mode:        ModeQueue,
				Interval:    0,
				QueueSize:   8,
				DrainOnStop: true,
				PushTimeout: 20 * time.Millisecond,
			},
			n:           3,
			wantDropped: 3,
		},
		{
			name: "ratelimit",
			cfg: Config{
				Mode:        ModeRateLimit,
				Interval:    time.Hour,
				MaxItems:    1,
				QueueSize:   8,
				DrainOnStop: true,
				PushTimeout: 20 * time.Millisecond,
			},
			n: 4,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := mustNew[int](t, tc.cfg)
			for i := 0; i < tc.n; i++ {
				mustPush(t, p, i)
			}
			// Let the run loop accept the items so drain, not the input
			// channel, is what would hang.
			time.Sleep(15 * time.Millisecond)

			done := make(chan struct{})
			go func() {
				p.Stop()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Stop blocked with DrainOnStop and no consumer")
			}
			if tc.wantDropped > 0 && p.Stats().Dropped != tc.wantDropped {
				t.Fatalf("Dropped = %d, want %d", p.Stats().Dropped, tc.wantDropped)
			}
		})
	}
}

// TestPushNilIsNotDropped stresses Push itself. A nil return must still be
// delivered on drain. OverflowDropNewest used to recordDrop that item after
// Push had already succeeded, once the run loop released capacity too early.
func TestPushNilIsNotDropped(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{
			name: "queue/drop-newest",
			cfg: Config{
				Mode:        ModeQueue,
				Interval:    time.Hour,
				QueueSize:   1,
				Overflow:    OverflowDropNewest,
				PushTimeout: 2 * time.Millisecond,
				DrainOnStop: true,
			},
		},
		{
			name: "ratelimit/drop-newest",
			cfg: Config{
				Mode:        ModeRateLimit,
				Interval:    time.Hour,
				MaxItems:    1,
				QueueSize:   1,
				Overflow:    OverflowDropNewest,
				PushTimeout: 2 * time.Millisecond,
				DrainOnStop: true,
			},
		},
		{
			name: "queue/block",
			cfg: Config{
				Mode:        ModeQueue,
				Interval:    time.Hour,
				QueueSize:   1,
				Overflow:    OverflowBlock,
				DrainOnStop: true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for attempt := 0; attempt < 6; attempt++ {
				assertSuccessfulPushesDelivered(t, tc.cfg)
			}
		})
	}
}

// TestAdmitWindowDoesNotDrop hammers the same success path Push uses
// (tryAcquire + enqueue) without Push's retry backoff, so a retain/release
// race is likely to be hit under the race detector.
func TestAdmitWindowDoesNotDrop(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{
			name: "queue/drop-newest",
			cfg: Config{
				Mode:        ModeQueue,
				Interval:    time.Hour,
				QueueSize:   1,
				Overflow:    OverflowDropNewest,
				PushTimeout: time.Second,
				DrainOnStop: true,
			},
		},
		{
			name: "ratelimit/drop-newest",
			cfg: Config{
				Mode:        ModeRateLimit,
				Interval:    time.Hour,
				MaxItems:    1,
				QueueSize:   1,
				Overflow:    OverflowDropNewest,
				PushTimeout: time.Second,
				DrainOnStop: true,
			},
		},
		{
			name: "queue/block",
			cfg: Config{
				Mode:        ModeQueue,
				Interval:    time.Hour,
				QueueSize:   1,
				Overflow:    OverflowBlock,
				PushTimeout: time.Second,
				DrainOnStop: true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for attempt := 0; attempt < 5; attempt++ {
				p := mustNew[int](t, tc.cfg)
				success := hammerAdmit(p, 16, 20*time.Millisecond)
				if success == 0 {
					t.Fatalf("attempt %d: hammer admitted nothing", attempt)
				}
				got := collectAfterStop(p)
				if got != int(success) {
					t.Fatalf("attempt %d: admitted %d, delivered %d (dropped %d)", attempt, success, got, p.Stats().Dropped)
				}
				if p.Stats().Dropped != 0 {
					t.Fatalf("attempt %d: admitted item was dropped (%d)", attempt, p.Stats().Dropped)
				}
			}
		})
	}
}

// TestDropOldestAccountsForSuccessfulPushes checks the explicit eviction
// policy. A nil Push may later be dropped, but only as a counted DropOldest
// eviction: every successful push is either delivered or present in Stats.Dropped.
func TestDropOldestAccountsForSuccessfulPushes(t *testing.T) {
	const queueSize = 2
	p := mustNew[int](t, Config{
		Mode:        ModeQueue,
		Interval:    time.Hour,
		QueueSize:   queueSize,
		Overflow:    OverflowDropOldest,
		DrainOnStop: true,
		PushTimeout: time.Second,
	})

	const producers = 8
	const per = 25
	var success atomic.Int32
	var wg sync.WaitGroup
	errCh := make(chan error, producers)
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < per; j++ {
				if err := p.Push(id*per + j); err != nil {
					errCh <- err
					return
				}
				success.Add(1)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("Push: %v", err)
	}

	want := uint64(success.Load())
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		st := p.Stats()
		if st.Pending <= uint64(queueSize) && st.Pending+st.Dropped == want {
			break
		}
		time.Sleep(time.Millisecond)
	}

	got := collectAfterStop(p)
	dropped := p.Stats().Dropped
	if uint64(got)+dropped != want {
		t.Fatalf("delivered %d + dropped %d != successful pushes %d", got, dropped, want)
	}
	if got == 0 || got > queueSize {
		t.Fatalf("delivered %d items, want between 1 and %d", got, queueSize)
	}
}

func assertSuccessfulPushesDelivered(t *testing.T, cfg Config) {
	t.Helper()
	p := mustNew[int](t, cfg)

	var success atomic.Int32
	var wg sync.WaitGroup
	const producers = 32
	const per = 20
	errCh := make(chan error, 1)
	var errOnce sync.Once
	report := func(err error) {
		errOnce.Do(func() { errCh <- err })
	}
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				err := pushForTest(p, cfg, j)
				if err == nil {
					success.Add(1)
					continue
				}
				if cfg.Overflow == OverflowBlock {
					if !errors.Is(err, ErrTimeout) && !errors.Is(err, ErrStopped) {
						report(err)
					}
					continue
				}
				if !errors.Is(err, ErrOverflow) && !errors.Is(err, ErrStopped) {
					report(err)
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("Push: %v", err)
	default:
	}

	got := collectAfterStop(p)
	if success.Load() == 0 {
		t.Fatal("no Push returned nil")
	}
	if got != int(success.Load()) {
		t.Fatalf("Push returned nil for %d items but delivered %d (dropped %d)", success.Load(), got, p.Stats().Dropped)
	}
}

func pushForTest(p *Pacer[int], cfg Config, v int) error {
	if cfg.Overflow != OverflowBlock {
		return p.Push(v)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	return p.PushContext(ctx, v)
}

func hammerAdmit(p *Pacer[int], hammers int, d time.Duration) int32 {
	var success atomic.Int32
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < hammers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if !p.tryAcquire() {
					runtime.Gosched()
					continue
				}
				select {
				case p.input <- 1:
					success.Add(1)
				case <-stop:
					p.releaseInFlight()
					return
				case <-p.done:
					p.releaseInFlight()
					return
				}
			}
		}()
	}
	time.Sleep(d)
	close(stop)
	wg.Wait()
	return success.Load()
}

func collectAfterStop(p *Pacer[int]) int {
	go func() {
		time.Sleep(10 * time.Millisecond)
		p.Stop()
	}()
	n := 0
	for range p.Updates() {
		n++
	}
	return n
}
