// Package push provides an in-process generic data-flow pacer.
//
// It is not a mobile or web push notification library. Use Pacer to throttle,
// debounce, rate-limit, or queue values between producers and consumers inside
// a single Go process.
//
// Create a pacer with New, push items with Push or PushContext, receive regulated
// output on Updates, and call Stop when finished.
//
// Example:
//
//	p, err := push.New[int](push.Config{
//	    Mode:     push.ModeThrottle,
//	    Interval: time.Second,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer p.Stop()
//
//	p.Push(42)
//	for v := range p.Updates() {
//	    fmt.Println(v)
//	}
package push
