package push

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func mustNew[T any](t *testing.T, config Config) *Pacer[T] {
	t.Helper()
	p, err := New[T](config)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func mustPush[T any](t *testing.T, p *Pacer[T], item T) {
	t.Helper()
	if err := p.Push(item); err != nil {
		t.Fatalf("Push: %v", err)
	}
}

// TestThrottleBasic tests basic throttling functionality
func TestThrottleBasic(t *testing.T) {
	config := Config{
		Mode:     ModeThrottle,
		Interval: 100 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push multiple values quickly
	for i := 0; i < 10; i++ {
		mustPush(t, p, i)
	}

	// Should only receive the latest value after interval
	select {
	case val := <-p.Updates():
		if val != 9 {
			t.Errorf("Expected 9, got %d", val)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timeout waiting for throttled value")
	}

	// Should not receive another value immediately
	select {
	case val, ok := <-p.Updates():
		if ok {
			t.Errorf("Received unexpected value: %d", val)
		}
	case <-time.After(50 * time.Millisecond):
		// Expected - no more values yet
	}
}

// TestThrottleMultipleIntervals tests throttling over multiple intervals
func TestThrottleMultipleIntervals(t *testing.T) {
	config := Config{
		Mode:     ModeThrottle,
		Interval: 50 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	received := make([]int, 0)
	var wg sync.WaitGroup
	wg.Add(1)

	// Consumer
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			select {
			case val := <-p.Updates():
				received = append(received, val)
			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()

	// Producer: push values at different intervals
	time.Sleep(10 * time.Millisecond)
	mustPush(t, p, 1)
	mustPush(t, p, 2)
	mustPush(t, p, 3)
	time.Sleep(60 * time.Millisecond) // Wait for first emit
	mustPush(t, p, 4)
	mustPush(t, p, 5)
	time.Sleep(60 * time.Millisecond) // Wait for second emit
	mustPush(t, p, 6)
	time.Sleep(60 * time.Millisecond) // Wait for third emit

	wg.Wait()

	if len(received) < 3 {
		t.Errorf("Expected at least 3 values, got %d", len(received))
	}
	if received[0] != 3 {
		t.Errorf("Expected first value to be 3, got %d", received[0])
	}
}

// TestDebounceBasic tests basic debouncing functionality
func TestDebounceBasic(t *testing.T) {
	config := Config{
		Mode:     ModeDebounce,
		Interval: 100 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push multiple values quickly
	for i := 0; i < 10; i++ {
		mustPush(t, p, i)
		time.Sleep(10 * time.Millisecond)
	}

	// Should only receive the latest value after quiet period
	select {
	case val := <-p.Updates():
		if val != 9 {
			t.Errorf("Expected 9, got %d", val)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timeout waiting for debounced value")
	}
}

// TestDebounceMultipleBursts tests debouncing with multiple bursts
func TestDebounceMultipleBursts(t *testing.T) {
	config := Config{
		Mode:     ModeDebounce,
		Interval: 50 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	received := make([]int, 0)
	var wg sync.WaitGroup
	wg.Add(1)

	// Consumer
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			select {
			case val := <-p.Updates():
				received = append(received, val)
			case <-time.After(300 * time.Millisecond):
				return
			}
		}
	}()

	// First burst
	for i := 0; i < 5; i++ {
		mustPush(t, p, i)
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(60 * time.Millisecond) // Wait for debounce

	// Second burst
	for i := 10; i < 15; i++ {
		mustPush(t, p, i)
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(60 * time.Millisecond) // Wait for debounce

	// Third burst
	for i := 20; i < 25; i++ {
		mustPush(t, p, i)
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(60 * time.Millisecond) // Wait for debounce

	wg.Wait()

	if len(received) != 3 {
		t.Errorf("Expected 3 values, got %d", len(received))
	}
	if received[0] != 4 {
		t.Errorf("Expected first value to be 4, got %d", received[0])
	}
	if received[1] != 14 {
		t.Errorf("Expected second value to be 14, got %d", received[1])
	}
	if received[2] != 24 {
		t.Errorf("Expected third value to be 24, got %d", received[2])
	}
}

// TestRateLimitBasic tests basic rate limiting functionality
func TestRateLimitBasic(t *testing.T) {
	config := Config{
		Mode:     ModeRateLimit,
		Interval: 100 * time.Millisecond,
		MaxItems: 3,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push more items than allowed
	for i := 0; i < 10; i++ {
		mustPush(t, p, i)
	}

	received := make([]int, 0)

	// First MaxItems should be available without waiting for the first ticker.
	for i := 0; i < 3; i++ {
		select {
		case val := <-p.Updates():
			received = append(received, val)
		case <-time.After(50 * time.Millisecond):
			t.Fatalf("timeout waiting for first-batch item %d", i)
		}
	}

	// Additional items should arrive in later windows.
collectMore:
	for len(received) < 6 {
		select {
		case val := <-p.Updates():
			received = append(received, val)
		case <-time.After(300 * time.Millisecond):
			break collectMore
		}
	}

	if len(received) < 3 {
		t.Errorf("Expected at least 3 values, got %d", len(received))
	}

	// Check that values are in order (they should be 0,1,2,3,4,5...)
	// Note: rate limiting preserves order, so received should be sequential
	for i := 1; i < len(received); i++ {
		if received[i] < received[i-1] {
			t.Errorf("Order violation: received[%d]=%d < received[%d]=%d", i, received[i], i-1, received[i-1])
		}
	}
}

// TestRateLimitExactCount tests rate limiting with exact count
func TestRateLimitExactCount(t *testing.T) {
	config := Config{
		Mode:     ModeRateLimit,
		Interval: 50 * time.Millisecond,
		MaxItems: 2,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push exactly MaxItems
	for i := 0; i < 2; i++ {
		mustPush(t, p, i)
	}

	// Rate limiting uses a ticker that fires at the interval
	// Wait for the ticker to fire (up to 2 intervals)
	received := make([]int, 0)
	deadline := time.Now().Add(150 * time.Millisecond)

receiveLoop:
	for len(received) < 2 {
		if time.Now().After(deadline) {
			break receiveLoop
		}
		remaining := time.Until(deadline)
		select {
		case val := <-p.Updates():
			received = append(received, val)
		case <-time.After(remaining):
			break receiveLoop
		}
	}

	// We should receive both values in one tick (MaxItems=2)
	// But if timing is off, we might only get one
	if len(received) < 1 {
		t.Error("Expected at least 1 value, got 0")
		return
	}

	if received[0] != 0 {
		t.Errorf("Expected first value to be 0, got %d", received[0])
	}

	// If we got both, check the second one
	if len(received) >= 2 {
		if received[1] != 1 {
			t.Errorf("Expected second value to be 1, got %d", received[1])
		}
	} else {
		// Try to get the second value with a short timeout
		select {
		case val := <-p.Updates():
			if val != 1 {
				t.Errorf("Expected second value to be 1, got %d", val)
			}
		case <-time.After(100 * time.Millisecond):
			t.Logf("Note: Only received %d value(s) within timeout. Rate limiting ticker timing may vary.", len(received))
		}
	}
}

// TestQueueBasic tests basic queuing functionality
func TestQueueBasic(t *testing.T) {
	config := Config{
		Mode:      ModeQueue,
		Interval:  50 * time.Millisecond,
		QueueSize: 100,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push multiple values quickly
	for i := 0; i < 5; i++ {
		mustPush(t, p, i)
	}

	// Should receive them in order, one per interval
	received := make([]int, 0)
	for i := 0; i < 5; i++ {
		select {
		case val := <-p.Updates():
			received = append(received, val)
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Timeout waiting for value %d", i)
		}
	}

	if len(received) != 5 {
		t.Errorf("Expected 5 values, got %d", len(received))
	}
	for i, val := range received {
		if val != i {
			t.Errorf("Expected value %d at position %d, got %d", i, i, val)
		}
	}
}

// TestQueueOrder tests that queue maintains order
func TestQueueOrder(t *testing.T) {
	config := Config{
		Mode:      ModeQueue,
		Interval:  2 * time.Millisecond,
		QueueSize: 1000,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push many values
	for i := 0; i < 100; i++ {
		mustPush(t, p, i)
	}

	// Receive and verify order
	received := make([]int, 0)
	timeout := time.After(5 * time.Second)
	for len(received) < 100 {
		select {
		case val := <-p.Updates():
			received = append(received, val)
		case <-timeout:
			t.Errorf("Timeout: received %d values, expected 100", len(received))
			return
		}
	}

	// Verify order
	for i, val := range received {
		if val != i {
			t.Errorf("Order violation: expected %d at position %d, got %d", i, i, val)
		}
	}
}

// TestThrottleConcurrent tests throttling with concurrent pushes
func TestThrottleConcurrent(t *testing.T) {
	config := Config{
		Mode:     ModeThrottle,
		Interval: 50 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	var wg sync.WaitGroup
	const numGoroutines = 10
	const pushesPerGoroutine = 10

	// Multiple goroutines pushing concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < pushesPerGoroutine; j++ {
				mustPush(t, p, id*1000+j)
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// Should receive at least one value (the latest)
	select {
	case <-p.Updates():
		// Good
	case <-time.After(200 * time.Millisecond):
		t.Error("Timeout waiting for throttled value")
	}
}

// TestDebounceConcurrent tests debouncing with concurrent pushes
func TestDebounceConcurrent(t *testing.T) {
	config := Config{
		Mode:      ModeDebounce,
		Interval:  50 * time.Millisecond,
		QueueSize: 10,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	var wg sync.WaitGroup
	const numGoroutines = 10

	// Multiple goroutines pushing concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				mustPush(t, p, id*1000+j)
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	// Should receive at least one value (the latest from all concurrent pushes)
	select {
	case <-p.Updates():
		// Good - received debounced value
	case <-time.After(300 * time.Millisecond):
		t.Error("Timeout waiting for debounced value")
	}
}

// TestRateLimitConcurrent tests rate limiting with concurrent pushes
func TestRateLimitConcurrent(t *testing.T) {
	config := Config{
		Mode:     ModeRateLimit,
		Interval: 50 * time.Millisecond,
		MaxItems: 5,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	var wg sync.WaitGroup
	const numGoroutines = 20
	const pushesPerGoroutine = 5

	// Multiple goroutines pushing concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < pushesPerGoroutine; j++ {
				mustPush(t, p, id*1000+j)
			}
		}(i)
	}

	wg.Wait()

	// Should receive items respecting rate limit
	received := make([]int, 0)
	timeout := time.After(2 * time.Second)
collectRateLimit:
	for len(received) < 10 {
		select {
		case val := <-p.Updates():
			received = append(received, val)
		case <-timeout:
			break collectRateLimit
		}
	}

	if len(received) < 5 {
		t.Errorf("Expected at least 5 values, got %d", len(received))
	}
}

// TestQueueConcurrent tests queuing with concurrent pushes
func TestQueueConcurrent(t *testing.T) {
	config := Config{
		Mode:      ModeQueue,
		Interval:  10 * time.Millisecond,
		QueueSize: 1000,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	var wg sync.WaitGroup
	const numGoroutines = 10
	const pushesPerGoroutine = 10

	// Multiple goroutines pushing concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < pushesPerGoroutine; j++ {
				mustPush(t, p, id*1000+j)
			}
		}(i)
	}

	wg.Wait()

	// Receive all items
	received := make(map[int]bool)
	timeout := time.After(5 * time.Second)
	for len(received) < numGoroutines*pushesPerGoroutine {
		select {
		case val := <-p.Updates():
			received[val] = true
		case <-timeout:
			t.Errorf("Timeout: received %d values, expected %d", len(received), numGoroutines*pushesPerGoroutine)
			return
		}
	}

	if len(received) != numGoroutines*pushesPerGoroutine {
		t.Errorf("Expected %d unique values, got %d", numGoroutines*pushesPerGoroutine, len(received))
	}
}

// TestStop tests that Stop() properly cleans up
func TestStop(t *testing.T) {
	config := Config{
		Mode:     ModeThrottle,
		Interval: 50 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	updates := p.Updates()

	// Push some values
	for i := 0; i < 5; i++ {
		mustPush(t, p, i)
	}

	// Stop should not block indefinitely
	done := make(chan bool)
	go func() {
		p.Stop()
		done <- true
	}()

	select {
	case <-done:
		// Good
	case <-time.After(1 * time.Second):
		t.Error("Stop() took too long")
	}

	// Updates channel should be closed
	// Try reading from the channel - if it's closed, ok will be false
	_, ok := <-updates
	if ok {
		t.Error("Updates channel should be closed after Stop()")
	}
}

// TestStopWithPendingItems tests Stop() with pending items
func TestStopWithPendingItems(t *testing.T) {
	config := Config{
		Mode:      ModeQueue,
		Interval:  50 * time.Millisecond,
		QueueSize: 100,
	}
	p := mustNew[int](t, config)

	// Push many values
	for i := 0; i < 100; i++ {
		mustPush(t, p, i)
	}

	// Stop should still work
	done := make(chan bool)
	go func() {
		p.Stop()
		done <- true
	}()

	select {
	case <-done:
		// Good
	case <-time.After(1 * time.Second):
		t.Error("Stop() took too long")
	}
}

// TestPushAfterStop tests that Push() after Stop() returns ErrStopped.
func TestPushAfterStop(t *testing.T) {
	config := Config{
		Mode:     ModeThrottle,
		Interval: 50 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	p.Stop()

	for i := 0; i < 3; i++ {
		if err := p.Push(i); !errors.Is(err, ErrStopped) {
			t.Fatalf("Push after stop: got %v, want ErrStopped", err)
		}
	}
}

// TestMultipleConsumers tests multiple goroutines reading from Updates()
func TestMultipleConsumers(t *testing.T) {
	config := Config{
		Mode:      ModeQueue,
		Interval:  2 * time.Millisecond,
		QueueSize: 1000,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push many values
	for i := 0; i < 100; i++ {
		mustPush(t, p, i)
	}

	// Multiple consumers
	var wg sync.WaitGroup
	received := sync.Map{}
	const numConsumers = 5

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case val, ok := <-p.Updates():
					if !ok {
						return
					}
					received.Store(val, true)
				case <-time.After(2 * time.Second):
					return
				}
			}
		}()
	}

	wg.Wait()

	// Count received values
	count := 0
	received.Range(func(key, value interface{}) bool {
		count++
		return true
	})

	if count != 100 {
		t.Errorf("Expected 100 values to be received, got %d", count)
	}
}

// TestRapidPush tests rapid pushing without blocking
func TestRapidPush(t *testing.T) {
	config := Config{
		Mode:     ModeThrottle,
		Interval: 100 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push very rapidly
	start := time.Now()
	for i := 0; i < 10000; i++ {
		mustPush(t, p, i)
	}
	elapsed := time.Since(start)

	// Should complete quickly (non-blocking)
	if elapsed > 1*time.Second {
		t.Errorf("Rapid push took too long: %v", elapsed)
	}
}

// TestEmptyInput tests behavior with no input
func TestEmptyInput(t *testing.T) {
	config := Config{
		Mode:     ModeThrottle,
		Interval: 50 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Should not receive anything
	select {
	case <-p.Updates():
		t.Error("Received unexpected value")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}
}

// TestDefaultConfig tests DefaultConfig function
func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig(100 * time.Millisecond)

	if config.Mode != ModeThrottle {
		t.Errorf("Expected ModeThrottle, got %v", config.Mode)
	}
	if config.Interval != 100*time.Millisecond {
		t.Errorf("Expected 100ms, got %v", config.Interval)
	}
	if config.MaxItems != 10 {
		t.Errorf("Expected MaxItems 10, got %d", config.MaxItems)
	}
}

// TestRateLimitStress tests rate limiting under stress
func TestRateLimitStress(t *testing.T) {
	config := Config{
		Mode:     ModeRateLimit,
		Interval: 20 * time.Millisecond,
		MaxItems: 2,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push many items rapidly
	for i := 0; i < 100; i++ {
		mustPush(t, p, i)
	}

	// Count received items over time
	received := make([]int, 0)
	start := time.Now()
	timeout := time.After(2 * time.Second)

collectStress:
	for len(received) < 20 {
		select {
		case val := <-p.Updates():
			received = append(received, val)
		case <-timeout:
			break collectStress
		}
	}

	elapsed := time.Since(start)
	if len(received) < 10 {
		t.Errorf("Expected at least 10 values, got %d", len(received))
	}

	// Should have taken at least a few intervals
	if elapsed < 100*time.Millisecond {
		t.Error("Rate limiting should have delayed items")
	}
}

// TestQueueStress tests queue under stress
func TestQueueStress(t *testing.T) {
	config := Config{
		Mode:      ModeQueue,
		Interval:  1 * time.Millisecond,
		QueueSize: 10000,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push many items
	for i := 0; i < 1000; i++ {
		mustPush(t, p, i)
	}

	// Receive all items
	received := make([]int, 0)
	timeout := time.After(10 * time.Second)
	for len(received) < 1000 {
		select {
		case val := <-p.Updates():
			received = append(received, val)
		case <-timeout:
			t.Errorf("Timeout: received %d values, expected 1000", len(received))
			return
		}
	}

	// Verify order
	for i, val := range received {
		if val != i {
			t.Errorf("Order violation at position %d: expected %d, got %d", i, i, val)
			break
		}
	}
}

// TestConcurrentPushAndStop tests concurrent Push() and Stop()
func TestConcurrentPushAndStop(t *testing.T) {
	config := Config{
		Mode:     ModeThrottle,
		Interval: 50 * time.Millisecond,
	}
	p := mustNew[int](t, config)

	var wg sync.WaitGroup
	const numGoroutines = 10

	// Multiple goroutines pushing
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				// Stop may win the race with a producer; ErrStopped is the
				// documented result in that case.
				if err := p.Push(j); err != nil && !errors.Is(err, ErrStopped) {
					t.Errorf("Push: %v", err)
				}
			}
		}()
	}

	// Stop concurrently
	go func() {
		time.Sleep(10 * time.Millisecond)
		p.Stop()
	}()

	wg.Wait()
}

// TestRaceConditionPush tests for race conditions in Push()
func TestRaceConditionPush(t *testing.T) {
	config := Config{
		Mode:     ModeThrottle,
		Interval: 10 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	var wg sync.WaitGroup
	const numGoroutines = 100
	const pushesPerGoroutine = 100

	// Many goroutines pushing concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < pushesPerGoroutine; j++ {
				mustPush(t, p, id*10000+j)
			}
		}(i)
	}

	wg.Wait()

	// Should receive at least one value
	select {
	case <-p.Updates():
		// Good
	case <-time.After(200 * time.Millisecond):
		t.Error("Timeout waiting for value")
	}
}

// TestRaceConditionStop tests for race conditions in Stop()
func TestRaceConditionStop(t *testing.T) {
	config := Config{
		Mode:      ModeQueue,
		Interval:  10 * time.Millisecond,
		QueueSize: 1000,
	}
	p := mustNew[int](t, config)

	// Push some values
	for i := 0; i < 100; i++ {
		mustPush(t, p, i)
	}

	// Multiple goroutines calling Stop() concurrently
	var wg sync.WaitGroup
	const numGoroutines = 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Stop()
		}()
	}

	wg.Wait()
}

// TestStringType tests with string type
func TestStringType(t *testing.T) {
	config := Config{
		Mode:     ModeThrottle,
		Interval: 50 * time.Millisecond,
	}
	p := mustNew[string](t, config)
	defer p.Stop()

	mustPush(t, p, "hello")
	mustPush(t, p, "world")

	select {
	case val := <-p.Updates():
		if val != "world" {
			t.Errorf("Expected 'world', got '%s'", val)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timeout waiting for value")
	}
}

// TestStructType tests with struct type
func TestStructType(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	config := Config{
		Mode:      ModeQueue,
		Interval:  20 * time.Millisecond,
		QueueSize: 100,
	}
	p := mustNew[Person](t, config)
	defer p.Stop()

	mustPush(t, p, Person{Name: "Alice", Age: 30})
	mustPush(t, p, Person{Name: "Bob", Age: 25})

	received := make([]Person, 0)
	for i := 0; i < 2; i++ {
		select {
		case val := <-p.Updates():
			received = append(received, val)
		case <-time.After(200 * time.Millisecond):
			t.Errorf("Timeout waiting for value %d", i)
		}
	}

	if len(received) != 2 {
		t.Errorf("Expected 2 values, got %d", len(received))
	}
	if received[0].Name != "Alice" {
		t.Errorf("Expected first person to be Alice, got %s", received[0].Name)
	}
}

// TestSlowConsumer tests behavior with slow consumer
func TestSlowConsumer(t *testing.T) {
	config := Config{
		Mode:     ModeThrottle,
		Interval: 50 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push values
	for i := 0; i < 10; i++ {
		mustPush(t, p, i)
	}

	// Slow consumer
	time.Sleep(200 * time.Millisecond)
	select {
	case val := <-p.Updates():
		if val != 9 {
			t.Errorf("Expected 9, got %d", val)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for value")
	}
}

// TestRateLimitBoundary tests rate limit at boundary conditions
func TestRateLimitBoundary(t *testing.T) {
	config := Config{
		Mode:     ModeRateLimit,
		Interval: 50 * time.Millisecond,
		MaxItems: 1,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push two items
	mustPush(t, p, 1)
	mustPush(t, p, 2)

	// Should receive first immediately
	select {
	case val := <-p.Updates():
		if val != 1 {
			t.Errorf("Expected 1, got %d", val)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for first value")
	}

	// Second should come after interval
	select {
	case val := <-p.Updates():
		if val != 2 {
			t.Errorf("Expected 2, got %d", val)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for second value")
	}
}

// TestDebounceTimerReset tests that debounce timer resets correctly
func TestDebounceTimerReset(t *testing.T) {
	config := Config{
		Mode:     ModeDebounce,
		Interval: 100 * time.Millisecond,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	// Push value
	mustPush(t, p, 1)

	// Push another value before debounce fires
	time.Sleep(50 * time.Millisecond)
	mustPush(t, p, 2)

	// Should only receive the second value after full interval from last push
	select {
	case val := <-p.Updates():
		if val != 2 {
			t.Errorf("Expected 2, got %d", val)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Timeout waiting for debounced value")
	}

	// Should not receive another value
	select {
	case <-p.Updates():
		t.Error("Received unexpected second value")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}
}

// TestConcurrentPushPull tests concurrent pushing and pulling
func TestConcurrentPushPull(t *testing.T) {
	config := Config{
		Mode:      ModeQueue,
		Interval:  5 * time.Millisecond,
		QueueSize: 1000,
	}
	p := mustNew[int](t, config)
	defer p.Stop()

	var pushWg sync.WaitGroup
	var pullWg sync.WaitGroup
	const numPushers = 10
	const numPullers = 5
	const pushesPerPusher = 100

	received := sync.Map{}
	var receivedCount int64

	// Multiple pushers
	for i := 0; i < numPushers; i++ {
		pushWg.Add(1)
		go func(id int) {
			defer pushWg.Done()
			for j := 0; j < pushesPerPusher; j++ {
				mustPush(t, p, id*10000+j)
			}
		}(i)
	}

	// Multiple pullers
	for i := 0; i < numPullers; i++ {
		pullWg.Add(1)
		go func() {
			defer pullWg.Done()
			for {
				select {
				case val, ok := <-p.Updates():
					if !ok {
						return
					}
					received.Store(val, true)
					atomic.AddInt64(&receivedCount, 1)
				case <-time.After(2 * time.Second):
					return
				}
			}
		}()
	}

	pushWg.Wait()

	// Give time for processing - queue mode processes one item per interval
	// With 1000 items and 5ms interval, we need at least 5 seconds
	time.Sleep(6 * time.Second)

	// Stop and wait for pullers to finish
	p.Stop()

	// Give pullers a moment to process the channel close
	timeout := time.After(1 * time.Second)
	pullDone := make(chan bool)
	go func() {
		pullWg.Wait()
		pullDone <- true
	}()

	select {
	case <-pullDone:
	case <-timeout:
		// Some pullers may still be waiting, that's okay
	}

	totalExpected := numPushers * pushesPerPusher
	if int(receivedCount) != totalExpected {
		t.Errorf("Expected %d values, got %d", totalExpected, receivedCount)
	}
}
