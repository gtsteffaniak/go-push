// Package push provides an in-process generic data-flow pacer.
//
// It is not a mobile or web push notification library. Use Pacer to throttle,
// debounce, rate-limit, or queue values between producers and consumers inside
// a single Go process.
//
// Create a pacer with New, push items with Push or PushContext, receive regulated
// output on Updates, and call Stop when finished. A nil error from Push means the
// item was retained. DrainOnStop needs a consumer reading Updates; each remaining
// send waits at most PushTimeout, and if that elapses Stop drops the rest and returns.
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
//
//	p.Push(42)
//	fmt.Println(<-p.Updates())
//	p.Stop()
package push
