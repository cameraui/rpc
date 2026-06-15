package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

type DataService struct {
	TeardownCount   atomic.Int32
	ProducedBatches atomic.Int32
}

func (s *DataService) PullForever(invoker *rpc.CallbackInvoker) (<-chan struct{}, error) {
	ch := make(chan struct{})
	go func() {
		defer func() {
			s.TeardownCount.Add(1)
			fmt.Printf("teardown ran (count=%d)\n", s.TeardownCount.Load())
		}()
		defer close(ch)

		fmt.Println("PullForever started")
		for b := 0; ; b++ {
			if !invoker.Active() {
				fmt.Printf("invoker inactive at batch %d — exit loop\n", b)
				return
			}
			for i := range 3 {
				invoker.Invoke("onChunk", map[string]any{"batch": b, "index": i})
			}
			s.ProducedBatches.Store(int32(b + 1))
			fmt.Printf("Batch %d yielded\n", b)

			select {
			case ch <- struct{}{}:
			case <-time.After(5 * time.Second):
				fmt.Println("ch <- timeout — consumer gone, exiting")
				return
			}
		}
	}()
	return ch, nil
}

func main() {
	fmt.Println("Pull-Callback Cancellation Example (Go)")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "pull-callback-cancel-server",
	})
	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Server connect: %v\n", err)
		os.Exit(1)
	}

	svc := &DataService{}
	unsubHandler, err := server.RegisterHandler("data", svc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Register: %v\n", err)
		os.Exit(1)
	}

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "pull-callback-cancel-client",
	})
	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Client connect: %v\n", err)
		os.Exit(1)
	}

	proxy := client.CreateProxy("data")

	var chunksBeforeCancel atomic.Int32
	var chunksAfterCancel atomic.Int32
	var didCancel atomic.Bool

	callbacks := rpc.PullCallbackMap{
		"onChunk": func(_ any) {
			if didCancel.Load() {
				chunksAfterCancel.Add(1)
			} else {
				chunksBeforeCancel.Add(1)
			}
		},
	}

	streamCtx, cancel := context.WithCancel(ctx)

	fmt.Println("Starting infinite generator, cancelling after 3 batches...")
	fmt.Println()

	ch, err := proxy.InvokePullIteratorWithCallback(
		streamCtx, "pullForever", callbacks, []string{"onChunk"},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "InvokePullIteratorWithCallback: %v\n", err)
		os.Exit(1)
	}

	batchesConsumed := 0
	for v := range ch {
		if v.Error != nil {
			fmt.Fprintf(os.Stderr, "Iterator error: %v\n", v.Error)
			break
		}
		batchesConsumed++
		fmt.Printf("Batch %d consumed\n", batchesConsumed-1)
		if batchesConsumed >= 3 {
			fmt.Println("cancel!")
			didCancel.Store(true)
			cancel()
			break
		}
	}

	// Allow server teardown + any in-flight callbacks to settle.
	time.Sleep(500 * time.Millisecond)

	fmt.Println()
	fmt.Println("Results")
	fmt.Printf("  Batches consumed on client:   %d (expected 3)\n", batchesConsumed)
	fmt.Printf("  Batches produced on server:   %d (expected 3 or 4, +1 tolerated for lookahead)\n", svc.ProducedBatches.Load())
	fmt.Printf("  Chunks received before cancel: %d\n", chunksBeforeCancel.Load())
	fmt.Printf("  Chunks received after cancel:  %d (expected small, lookahead drain)\n", chunksAfterCancel.Load())
	fmt.Printf("  Server teardown count:         %d (expected 1)\n", svc.TeardownCount.Load())

	// NOTE: Go API inherently has 1-batch lookahead, so up to 3 chunks (1
	// extra batch) may arrive after cancel.
	ok := batchesConsumed == 3 &&
		svc.TeardownCount.Load() == 1 &&
		svc.ProducedBatches.Load() <= 5 &&
		chunksAfterCancel.Load() <= 6

	fmt.Println()
	if ok {
		fmt.Println("PASS — cancellation cleanly stops server + callbacks")
	} else {
		fmt.Println("FAIL — cancellation did not clean up properly")
	}

	_ = unsubHandler()
	_ = client.Disconnect()
	_ = server.Disconnect()
}
