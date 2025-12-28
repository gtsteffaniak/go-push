package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/gtsteffaniak/go-push/push"
)

// StockPrice is a sample struct to show generic usage
type StockPrice struct {
	Symbol string
	Price  float64
}

func main() {
	fmt.Println("=== Go-Push Library Examples ===")

	// Example 1: Throttling Mode
	exampleThrottle()

	// Example 2: Debouncing Mode
	// exampleDebounce()

	// Example 3: Rate Limiting Mode
	// exampleRateLimit()

	// Example 4: Queue Mode
	// exampleQueue()
}

// Example 1: Throttling - emits latest value at most once per interval
func exampleThrottle() {
	fmt.Println("--- Example 1: Throttling Mode ---")
	fmt.Println("Producer sends updates every 100ms, but only latest value")
	fmt.Println("is emitted once per second.")
	fmt.Println()

	config := push.Config{
		Mode:     push.ModeThrottle,
		Interval: 1 * time.Second,
	}
	p := push.New[StockPrice](config)
	defer p.Stop()

	// Producer: sends updates every 100ms
	go func() {
		for i := 0; i < 15; i++ {
			newPrice := 100.0 + rand.Float64()*10
			fmt.Printf("-> PRODUCING: $%.2f\n", newPrice)
			p.Push(StockPrice{Symbol: "GOOG", Price: newPrice})
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Consumer: receives throttled updates
	startTime := time.Now()
	count := 0
	for update := range p.Updates() {
		elapsed := time.Since(startTime)
		fmt.Printf("[THROTTLE] Update %d: %s is $%.2f (Time: %v)\n\n",
			count+1, update.Symbol, update.Price, elapsed.Round(time.Millisecond))
		count++
		if count >= 5 {
			break
		}
	}
	fmt.Println("Throttle example finished.")
	fmt.Println()
}

// Example 2: Debouncing - waits for quiet period before emitting
func exampleDebounce() {
	fmt.Println("--- Example 2: Debouncing Mode ---")
	fmt.Println("Waits 500ms after last input before emitting.")
	fmt.Println()

	config := push.Config{
		Mode:     push.ModeDebounce,
		Interval: 500 * time.Millisecond,
	}
	p := push.New[StockPrice](config)
	defer p.Stop()

	// Producer: sends bursts of updates
	go func() {
		for i := 0; i < 3; i++ {
			fmt.Printf("-> Burst %d starting...\n", i+1)
			for j := 0; j < 5; j++ {
				newPrice := 100.0 + rand.Float64()*10
				fmt.Printf("  -> PRODUCING: $%.2f\n", newPrice)
				p.Push(StockPrice{Symbol: "MSFT", Price: newPrice})
				time.Sleep(50 * time.Millisecond)
			}
			fmt.Println("  -> Burst ended, waiting for debounce...")
			fmt.Println()
			time.Sleep(600 * time.Millisecond)
		}
	}()

	// Consumer
	count := 0
	for update := range p.Updates() {
		fmt.Printf("[DEBOUNCE] Emitted: %s is $%.2f\n\n", update.Symbol, update.Price)
		count++
		if count >= 3 {
			break
		}
	}
	fmt.Println("Debounce example finished.")
	fmt.Println()
}

// Example 3: Rate Limiting - allows max N items per interval
func exampleRateLimit() {
	fmt.Println("--- Example 3: Rate Limiting Mode ---")
	fmt.Println("Allows max 3 items per second.")
	fmt.Println()

	config := push.Config{
		Mode:     push.ModeRateLimit,
		Interval: 1 * time.Second,
		MaxItems: 3,
	}
	p := push.New[StockPrice](config)
	defer p.Stop()

	// Producer: sends many updates
	go func() {
		for i := 0; i < 20; i++ {
			newPrice := 100.0 + rand.Float64()*10
			fmt.Printf("-> PRODUCING: $%.2f\n", newPrice)
			p.Push(StockPrice{Symbol: "AAPL", Price: newPrice})
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Consumer
	startTime := time.Now()
	count := 0
	for update := range p.Updates() {
		elapsed := time.Since(startTime)
		fmt.Printf("[RATE LIMIT] Update %d: %s is $%.2f (Time: %v)\n\n",
			count+1, update.Symbol, update.Price, elapsed.Round(time.Millisecond))
		count++
		if count >= 9 {
			break
		}
	}
	fmt.Println("Rate limit example finished.")
	fmt.Println()
}

// Example 4: Queue Mode - buffers and emits items in order
func exampleQueue() {
	fmt.Println("--- Example 4: Queue Mode ---")
	fmt.Println("Buffers items and emits them one per second in order.")
	fmt.Println()

	config := push.Config{
		Mode:      push.ModeQueue,
		Interval:  1 * time.Second,
		QueueSize: 100,
	}
	p := push.New[StockPrice](config)
	defer p.Stop()

	// Producer: sends updates quickly
	go func() {
		for i := 0; i < 10; i++ {
			newPrice := 100.0 + float64(i)
			fmt.Printf("-> PRODUCING: $%.2f (item %d)\n", newPrice, i+1)
			p.Push(StockPrice{Symbol: "TSLA", Price: newPrice})
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Consumer: receives queued items in order
	count := 0
	for update := range p.Updates() {
		fmt.Printf("[QUEUE] Received: %s is $%.2f (item %d)\n\n",
			update.Symbol, update.Price, count+1)
		count++
		if count >= 10 {
			break
		}
	}
	fmt.Println("Queue example finished.")
	fmt.Println()
}
