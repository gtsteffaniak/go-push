package push_test

import (
	"fmt"
	"log"
	"time"

	"github.com/gtsteffaniak/go-push/push"
)

func ExampleNew_throttle() {
	p, err := push.New[int](push.Config{
		Mode:     push.ModeThrottle,
		Interval: 10 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Stop()

	go func() {
		for i := 0; i < 5; i++ {
			_ = p.Push(i)
		}
	}()

	v, ok := <-p.Updates()
	if !ok {
		log.Fatal("updates closed")
	}
	fmt.Println(v)
	// Output: 4
}
