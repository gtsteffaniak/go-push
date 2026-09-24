package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/gtsteffaniak/go-push/push"
)

type StockPrice struct {
	Symbol string
	Price  float64
}

func main() {
	config := push.Config{
		Mode:     push.ModeThrottle,
		Interval: time.Second,
	}
	p, err := push.New[StockPrice](config)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 15; i++ {
			price := 100.0 + rand.Float64()*10
			fmt.Printf("-> PRODUCING: $%.2f\n", price)
			if err := p.Push(StockPrice{Symbol: "GOOG", Price: price}); err != nil {
				log.Printf("push: %v", err)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	go func() {
		<-done
		p.Stop()
	}()

	for update := range p.Updates() {
		fmt.Printf("[THROTTLE] %s is $%.2f\n", update.Symbol, update.Price)
	}
}
