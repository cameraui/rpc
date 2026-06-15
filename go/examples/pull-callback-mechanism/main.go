package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

type MechService struct{}

func (s *MechService) PullSteady(count int, invoker *rpc.CallbackInvoker) (<-chan struct{}, error) {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		for i := range count {
			if !invoker.Active() {
				return
			}
			invoker.Invoke("onItem", i)
			select {
			case ch <- struct{}{}:
			case <-time.After(30 * time.Second):
				return
			}
		}
	}()
	return ch, nil
}

func main() {
	fmt.Println("Pull-Callback Mechanism Test (Go)")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "pull-callback-mech-server",
	})
	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Server connect: %v\n", err)
		os.Exit(1)
	}
	unsubHandler, err := server.RegisterHandler("mech", &MechService{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Register: %v\n", err)
		os.Exit(1)
	}

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "pull-callback-mech-client",
	})
	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Client connect: %v\n", err)
		os.Exit(1)
	}

	proxy := client.CreateProxy("mech")

	const COUNT = 10
	const HANDLER_DELAY = 200 * time.Millisecond

	var mu sync.Mutex
	starts := make([]time.Time, 0, COUNT)
	ends := make([]time.Time, 0, COUNT)

	callbacks := rpc.PullCallbackMap{
		"onItem": func(idx any) {
			mu.Lock()
			starts = append(starts, time.Now())
			mu.Unlock()
			time.Sleep(HANDLER_DELAY)
			mu.Lock()
			ends = append(ends, time.Now())
			n := len(ends)
			mu.Unlock()
			fmt.Printf("  onItem(%v) finished after %dms\n", idx, HANDLER_DELAY/time.Millisecond)
			_ = n
		},
	}

	fmt.Printf("Consuming %d items, each handler blocks %dms.\n", COUNT, HANDLER_DELAY/time.Millisecond)
	fmt.Println("Consumer loop has no delay — only the handler blocks.")
	fmt.Println()

	start := time.Now()

	ch, err := proxy.InvokePullIteratorWithCallback(
		ctx, "pullSteady", callbacks, []string{"onItem"}, COUNT,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "InvokePullIteratorWithCallback: %v\n", err)
		os.Exit(1)
	}

	consumed := 0
	for v := range ch {
		if v.Error != nil {
			fmt.Fprintf(os.Stderr, "Iterator error: %v\n", v.Error)
			break
		}
		consumed++
	}

	elapsed := time.Since(start)
	time.Sleep(50 * time.Millisecond) // let final handler finish

	fmt.Println()
	fmt.Println("Timing")
	fmt.Printf("  Handler invocations:   %d\n", len(starts))
	fmt.Printf("  Total elapsed:         %.1fms\n", float64(elapsed.Microseconds())/1000.0)
	fmt.Printf("  Expected (serialized): %dms\n", COUNT*int(HANDLER_DELAY/time.Millisecond))
	fmt.Printf("  Expected (no BP):      ~%dms (network only)\n", COUNT*2)

	overlaps := 0
	for i := 1; i < len(starts) && i < len(ends); i++ {
		if starts[i].Before(ends[i-1].Add(-5 * time.Millisecond)) {
			overlaps++
		}
	}
	fmt.Printf("  Overlapping handlers:  %d (expected 0)\n", overlaps)

	expectedMs := float64(COUNT * int(HANDLER_DELAY/time.Millisecond))
	elapsedMs := float64(elapsed.Microseconds()) / 1000.0
	within := elapsedMs >= expectedMs*0.9 && elapsedMs <= expectedMs*1.3
	ok := consumed == COUNT && len(starts) == COUNT && within && overlaps == 0 && math.Abs(elapsedMs-expectedMs) < expectedMs*0.3

	fmt.Println()
	if ok {
		fmt.Println("PASS — callback handlers are serialized, backpressure propagates")
	} else {
		fmt.Println("FAIL — handlers did not serialize, or timing does not match")
	}

	_ = unsubHandler()
	_ = client.Disconnect()
	_ = server.Disconnect()
}
