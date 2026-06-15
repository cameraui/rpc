package main

import (
	"context"
	"fmt"
	"os"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

type DataService struct{}

func (s *DataService) PullBatches(batchCount, chunksPerBatch int, invoker *rpc.CallbackInvoker) (<-chan struct{}, error) {
	// Unbuffered: the `ch <- struct{}{}` is the yield statement — it must
	// block until the framework Recv()s on client next() for backpressure
	// to work.
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		fmt.Printf("PullBatches(%d, %d) started\n", batchCount, chunksPerBatch)
		for b := range batchCount {
			if !invoker.Active() {
				return
			}
			for i := range chunksPerBatch {
				invoker.Invoke("onChunk", map[string]any{"batch": b, "index": i})
			}
			invoker.Invoke("onChunk", nil)
			fmt.Printf("Batch %d produced, yielding...\n", b)
			select {
			case ch <- struct{}{}:
			case <-time.After(5 * time.Second):
				return
			}
			fmt.Printf("Resumed after batch %d\n", b)
		}
		fmt.Println("Generator complete")
	}()
	return ch, nil
}

func main() {
	totalStart := time.Now()
	fmt.Println("Pull-Callback Basic Example (Go)")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "pull-callback-basic-server",
	})
	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Server connect: %v\n", err)
		os.Exit(1)
	}

	unsubHandler, err := server.RegisterHandler("data", &DataService{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Register: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Server connected, handler registered")
	fmt.Println()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "pull-callback-basic-client",
	})
	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Client connect: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Client connected")
	fmt.Println()

	proxy := client.CreateProxy("data")

	received := []map[string]any{}
	batchEnds := 0

	callbacks := rpc.PullCallbackMap{
		"onChunk": func(data any) {
			if data == nil {
				batchEnds++
				fmt.Printf("Batch %d end-of-batch sentinel received\n", batchEnds-1)
				return
			}
			if m, ok := data.(map[string]any); ok {
				received = append(received, m)
			}
		},
	}

	const BATCHES = 3
	const CHUNKS = 5

	fmt.Printf("Starting pull iteration for %d batches × %d chunks...\n\n", BATCHES, CHUNKS)
	start := time.Now()

	ch, err := proxy.InvokePullIteratorWithCallback(
		ctx, "pullBatches", callbacks, []string{"onChunk"}, BATCHES, CHUNKS,
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
		fmt.Printf("Batch %d boundary crossed (iteration %d/%d)\n",
			batchesConsumed-1, batchesConsumed, BATCHES)
	}

	elapsed := time.Since(start)
	time.Sleep(50 * time.Millisecond) // allow in-flight callback messages

	fmt.Println()
	fmt.Println("Results")
	fmt.Printf("  Batches consumed:   %d (expected %d)\n", batchesConsumed, BATCHES)
	fmt.Printf("  Chunks received:    %d (expected %d)\n", len(received), BATCHES*CHUNKS)
	fmt.Printf("  End-of-batch marks: %d (expected %d)\n", batchEnds, BATCHES)
	fmt.Printf("  Total elapsed:      %.1fms\n", float64(elapsed.Microseconds())/1000.0)

	ok := batchesConsumed == BATCHES &&
		len(received) == BATCHES*CHUNKS &&
		batchEnds == BATCHES

	if ok {
		for i, r := range received {
			expectedBatch := i / CHUNKS
			expectedIdx := i % CHUNKS
			gotBatch, _ := r["batch"].(int8) // msgpack may decode small ints as int8
			gotIdx, _ := r["index"].(int8)
			if int(gotBatch) != expectedBatch || int(gotIdx) != expectedIdx {
				gb64, _ := toInt(r["batch"])
				gi64, _ := toInt(r["index"])
				if gb64 != int64(expectedBatch) || gi64 != int64(expectedIdx) {
					ok = false
					fmt.Printf("  Order mismatch at i=%d: got batch=%v index=%v\n", i, r["batch"], r["index"])
					break
				}
			}
		}
	}

	fmt.Println()
	if ok {
		fmt.Println("PASS — correctness check")
	} else {
		fmt.Println("FAIL — correctness check")
	}

	_ = unsubHandler()
	_ = client.Disconnect()
	_ = server.Disconnect()

	fmt.Printf("\nTotal test time: %.1fms\n", float64(time.Since(totalStart).Microseconds())/1000.0)
}

// toInt coerces msgpack-decoded numeric values (int8/int16/int32/int64/uint*/float*) to int64.
func toInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		return int64(n), true
	case float32:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}
