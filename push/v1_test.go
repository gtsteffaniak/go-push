package push

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestNewInvalidConfig(t *testing.T) {
	_, err := New[int](Config{Mode: Mode(99), Interval: time.Millisecond})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("got %v, want ErrInvalidConfig", err)
	}

	_, err = New[int](Config{Mode: ModeThrottle, Interval: 0})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("got %v, want ErrInvalidConfig", err)
	}

	_, err = New[int](Config{Mode: ModeRateLimit, Interval: time.Millisecond, MaxItems: 0})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("got %v, want ErrInvalidConfig", err)
	}

	_, err = New[int](Config{Mode: ModeQueue, Overflow: Overflow(99)})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid overflow: got %v, want ErrInvalidConfig", err)
	}
}

func TestRateLimitDefaultQueueSize(t *testing.T) {
	p := mustNew[int](t, Config{Mode: ModeRateLimit, Interval: time.Millisecond, MaxItems: 1})
	defer p.Stop()

	if got := p.maxPending(); got != 100 {
		t.Fatalf("maxPending = %d, want 100", got)
	}
}

func TestOverflowDropNewest(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:        ModeQueue,
		Interval:    time.Hour,
		QueueSize:   2,
		Overflow:    OverflowDropNewest,
		PushTimeout: 20 * time.Millisecond,
	})
	defer p.Stop()

	if err := p.Push(1); err != nil {
		t.Fatalf("first push: %v", err)
	}
	if err := p.Push(2); err != nil {
		t.Fatalf("second push: %v", err)
	}
	if err := p.Push(3); !errors.Is(err, ErrOverflow) {
		t.Fatalf("third push: got %v, want ErrOverflow", err)
	}
	if p.Stats().Dropped == 0 {
		t.Fatal("expected dropped stat")
	}
}

func TestOverflowDropOldest(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:      ModeQueue,
		Interval:  0,
		QueueSize: 2,
		Overflow:  OverflowDropOldest,
	})
	defer p.Stop()

	for i := 0; i < 5; i++ {
		if err := p.Push(i); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}
}

func TestOverflowBlock(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:      ModeQueue,
		Interval:  time.Hour,
		QueueSize: 2,
		Overflow:  OverflowBlock,
	})
	defer p.Stop()

	if err := p.Push(1); err != nil {
		t.Fatalf("first push: %v", err)
	}
	if err := p.Push(2); err != nil {
		t.Fatalf("second push: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- p.Push(3)
	}()

	select {
	case err := <-done:
		t.Fatalf("expected blocking push, got err=%v", err)
	case <-time.After(30 * time.Millisecond):
	}

	p.Stop()

	select {
	case err := <-done:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("blocked push after stop: got %v, want ErrStopped", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for blocked push after stop")
	}
}

func TestPushContextTimeout(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:      ModeQueue,
		Interval:  time.Hour,
		QueueSize: 2,
		Overflow:  OverflowBlock,
	})
	defer p.Stop()

	if err := p.Push(1); err != nil {
		t.Fatalf("first push: %v", err)
	}
	if err := p.Push(2); err != nil {
		t.Fatalf("second push: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	if err := p.PushContext(ctx, 3); !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout", err)
	}
}

func TestUpdatesAfterStopNotNil(t *testing.T) {
	p := mustNew[int](t, Config{Mode: ModeThrottle, Interval: time.Millisecond})
	ch := p.Updates()
	p.Stop()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel")
		}
	default:
	}

	if p.Updates() != ch {
		t.Fatal("Updates should return same channel after Stop")
	}
}

func TestDebounceStopRace(t *testing.T) {
	for i := 0; i < 20; i++ {
		p := mustNew[int](t, Config{Mode: ModeDebounce, Interval: 5 * time.Millisecond})
		for j := 0; j < 10; j++ {
			_ = p.Push(j)
		}
		p.Stop()
	}
}

func TestRateLimitImmediateFlush(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:     ModeRateLimit,
		Interval: 500 * time.Millisecond,
		MaxItems: 3,
	})
	defer p.Stop()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			<-p.Updates()
		}
	}()
	time.Sleep(5 * time.Millisecond)

	for i := 0; i < 3; i++ {
		mustPush(t, p, i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected 3 immediate emits before ticker window")
	}
}

func TestQueueUnpaced(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:      ModeQueue,
		Interval:  0,
		QueueSize: 10,
		Overflow:  OverflowBlock,
	})
	defer p.Stop()

	for i := 0; i < 5; i++ {
		mustPush(t, p, i)
	}

	received := make([]int, 0, 5)
	timeout := time.After(200 * time.Millisecond)
	for len(received) < 5 {
		select {
		case v := <-p.Updates():
			received = append(received, v)
		case <-timeout:
			t.Fatalf("timeout: got %v", received)
		}
	}
}

func TestStats(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:     ModeThrottle,
		Interval: 10 * time.Millisecond,
	})
	defer p.Stop()

	mustPush(t, p, 1)
	select {
	case <-p.Updates():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout")
	}
	if p.Stats().Emitted != 1 {
		t.Fatalf("Emitted = %d, want 1", p.Stats().Emitted)
	}
}

type recordingLogger struct {
	mu    sync.Mutex
	warns []string
}

func (r *recordingLogger) Debug(string, ...any) {}
func (r *recordingLogger) Info(string, ...any)  {}
func (r *recordingLogger) Warn(msg string, _ ...any) {
	r.mu.Lock()
	r.warns = append(r.warns, msg)
	r.mu.Unlock()
}
func (r *recordingLogger) Error(string, ...any) {}

func TestLoggerOnDrop(t *testing.T) {
	log := &recordingLogger{}
	p := mustNew[int](t, Config{
		Mode:        ModeQueue,
		Interval:    time.Millisecond,
		QueueSize:   1,
		Overflow:    OverflowDropNewest,
		PushTimeout: 10 * time.Millisecond,
		Logger:      log,
	})
	defer p.Stop()

	_ = p.Push(1)
	_ = p.Push(2)
	_ = p.Push(3)

	time.Sleep(20 * time.Millisecond)
	log.mu.Lock()
	n := len(log.warns)
	log.mu.Unlock()
	if n == 0 {
		t.Fatal("expected warn log on drop")
	}
}

func TestFromSlog(t *testing.T) {
	l := FromSlog(slog.Default())
	if l == nil {
		t.Fatal("FromSlog returned nil")
	}
	if FromSlog(nil) == nil {
		t.Fatal("FromSlog(nil) returned nil")
	}
}

func TestWithGroup(t *testing.T) {
	base := &recordingLogger{}
	g := WithGroup(base, "push")
	g.Warn("test")
	base.mu.Lock()
	n := len(base.warns)
	base.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected 1 warn, got %d", n)
	}
}

func TestDrainOnStop(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:        ModeQueue,
		Interval:    500 * time.Millisecond,
		QueueSize:   10,
		DrainOnStop: true,
	})

	for i := 0; i < 3; i++ {
		mustPush(t, p, i)
	}

	go func() {
		time.Sleep(10 * time.Millisecond)
		p.Stop()
	}()

	received := 0
	for range p.Updates() {
		received++
	}
	if received != 3 {
		t.Fatalf("DrainOnStop: got %d items, want 3", received)
	}
}

// TestQueueDrainIncludesInputBuffer verifies that values accepted by Push but
// still buffered in the input channel are drained on Stop, not lost.
func TestQueueDrainIncludesInputBuffer(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:        ModeQueue,
		Interval:    0,
		QueueSize:   4,
		DrainOnStop: true,
	})

	mustPush(t, p, 1)
	mustPush(t, p, 2)
	time.Sleep(20 * time.Millisecond) // let the run loop pick up 1 and block emitting it

	go p.Stop()

	var got []int
	for v := range p.Updates() {
		got = append(got, v)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("shutdown drain: got %v, want [1 2]", got)
	}
}

func TestRateLimitDrainOnStop(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:        ModeRateLimit,
		Interval:    time.Second,
		MaxItems:    2,
		QueueSize:   16,
		DrainOnStop: true,
	})

	for i := 0; i < 5; i++ {
		mustPush(t, p, i)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		p.Stop()
	}()

	received := 0
	for range p.Updates() {
		received++
	}
	if received != 5 {
		t.Fatalf("RateLimit DrainOnStop: got %d items, want 5", received)
	}
}

// TestRateLimitBudgetPerInterval verifies that a fast consumer cannot cause the
// pacer to emit more than MaxItems within a single interval.
func TestRateLimitBudgetPerInterval(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:      ModeRateLimit,
		Interval:  time.Second,
		MaxItems:  2,
		QueueSize: 16,
	})
	defer p.Stop()

	for i := 0; i < 6; i++ {
		mustPush(t, p, i)
	}

	received := 0
	deadline := time.After(200 * time.Millisecond)
collect:
	for {
		select {
		case <-p.Updates():
			received++
		case <-deadline:
			break collect
		}
	}
	if received != 2 {
		t.Fatalf("emitted %d items in one interval, want 2", received)
	}
}

// TestDropOldestPreservesEvictedOnFailedAdmission verifies that a failed
// replacement admission does not discard an item an earlier Push accepted.
func TestDropOldestPreservesEvictedOnFailedAdmission(t *testing.T) {
	for i := 0; i < 10; i++ {
		p := mustNew[int](t, Config{
			Mode:        ModeQueue,
			Interval:    0,
			QueueSize:   2,
			Overflow:    OverflowDropOldest,
			DrainOnStop: true,
		})

		mustPush(t, p, 1)
		mustPush(t, p, 2)
		time.Sleep(10 * time.Millisecond) // run loop retains 1 and blocks emitting it

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := p.PushContext(ctx, 3)

		go p.Stop()

		var got []int
		for v := range p.Updates() {
			got = append(got, v)
		}

		if len(got) != 2 || got[0] != 1 {
			t.Fatalf("iteration %d: got %v, want 2 items starting with 1", i, got)
		}
		if err == nil {
			if got[1] != 3 {
				t.Fatalf("iteration %d: admitted push dropped: got %v, want [1 3]", i, got)
			}
		} else if got[1] != 2 {
			t.Fatalf("iteration %d: failed push discarded an accepted item: got %v, want [1 2]", i, got)
		}
	}
}

// TestOverflowDropOldestEvictsRetained verifies that a full mode-local queue
// evicts its oldest item to admit the newest push instead of dropping the push.
func TestOverflowDropOldestEvictsRetained(t *testing.T) {
	p := mustNew[int](t, Config{
		Mode:        ModeQueue,
		Interval:    time.Hour,
		QueueSize:   2,
		Overflow:    OverflowDropOldest,
		DrainOnStop: true,
	})

	for i := 1; i <= 3; i++ {
		mustPush(t, p, i)
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		p.Stop()
	}()

	var got []int
	for v := range p.Updates() {
		got = append(got, v)
	}
	if len(got) != 2 {
		t.Fatalf("DropOldest: got %v, want 2 retained items", got)
	}
	if got[len(got)-1] != 3 {
		t.Fatalf("DropOldest dropped the newest item: got %v", got)
	}
}

func TestConcurrentPushStopRace(t *testing.T) {
	p := mustNew[int](t, Config{Mode: ModeDebounce, Interval: time.Millisecond})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = p.Push(n*100 + j)
			}
		}(i)
	}
	time.Sleep(5 * time.Millisecond)
	p.Stop()
	wg.Wait()
}
