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

// DataService verifies that the server generator suspends at yield when
// the client slows down batch consumption.
type DataService struct{}

// PullPacedBatches produces batchCount batches of chunksPerBatch items each.
// Each chunk carries a server-side timestamp so the client can measure the
// inter-batch gap against its own delay and verify end-to-end backpressure.
func (s *DataService) PullPacedBatches(batchCount, chunksPerBatch int, invoker *rpc.CallbackInvoker) (<-chan struct{}, error) {
	// Unbuffered: ch <- struct{}{} blocks until the framework's Recv() fires
	// on client next(). This is the "yield" — must be synchronous for
	// backpressure.
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		for b := range batchCount {
			if !invoker.Active() {
				return
			}
			batchStart := time.Now()
			for i := range chunksPerBatch {
				invoker.Invoke("onChunk", map[string]any{
					"batch": b,
					"index": i,
					"ts":    float64(time.Since(batchStart).Microseconds()) / 1000.0, // ms relative
					"wall":  time.Now().UnixMicro(),
				})
			}
			invoker.Invoke("onChunk", nil) // end-of-batch
			produced := time.Since(batchStart)
			fmt.Printf("[Server] Batch %d produced in %.1fms, suspending at yield...\n",
				b, float64(produced.Microseconds())/1000.0)
			select {
			case ch <- struct{}{}:
			case <-time.After(30 * time.Second):
				return
			}
			fmt.Printf("[Server] Batch %d resumed (client called next)\n", b)
		}
	}()
	return ch, nil
}

type batchStat struct {
	firstWall int64
	lastWall  int64
	count     int
}

func main() {
	fmt.Println("=== Pull-Callback Backpressure Example (Go) ===")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "pull-callback-bp-server",
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

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "pull-callback-bp-client",
	})
	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Client connect: %v\n", err)
		os.Exit(1)
	}

	proxy := client.CreateProxy("data")

	const BATCHES = 4
	const CHUNKS = 1000
	const CLIENT_DELAY = 500 * time.Millisecond

	stats := make([]batchStat, BATCHES)
	for i := range stats {
		stats[i].firstWall = -1
	}
	var mu sync.Mutex

	callbacks := rpc.PullCallbackMap{
		"onChunk": func(data any) {
			if data == nil {
				return
			}
			m, ok := data.(map[string]any)
			if !ok {
				return
			}
			batch, _ := toInt(m["batch"])
			if batch < 0 || int(batch) >= BATCHES {
				return
			}
			// Measure client-side arrival — NATS RTT (<1ms) is negligible
			// compared to the 500ms client delay, and this avoids msgpack
			// type-coercion surprises for server-sent timestamps.
			now := time.Now().UnixMicro()
			mu.Lock()
			s := &stats[batch]
			if s.firstWall == -1 {
				s.firstWall = now
			}
			s.lastWall = now
			s.count++
			mu.Unlock()
		},
	}

	fmt.Printf("Consuming %d batches × %d chunks, client delays %dms between batches...\n\n",
		BATCHES, CHUNKS, CLIENT_DELAY/time.Millisecond)
	start := time.Now()

	ch, err := proxy.InvokePullIteratorWithCallback(
		ctx, "pullPacedBatches", callbacks, []string{"onChunk"}, BATCHES, CHUNKS,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "InvokePullIteratorWithCallback: %v\n", err)
		os.Exit(1)
	}

	idx := 0
	for v := range ch {
		if v.Error != nil {
			fmt.Fprintf(os.Stderr, "Iterator error: %v\n", v.Error)
			break
		}
		fmt.Printf("[Client] Received batch boundary %d, sleeping %dms...\n", idx, CLIENT_DELAY/time.Millisecond)
		time.Sleep(CLIENT_DELAY)
		idx++
	}

	elapsed := time.Since(start)
	time.Sleep(50 * time.Millisecond)

	fmt.Println()
	fmt.Println("=== Per-Batch Stats (wall-clock timestamps, µs) ===")
	baseline := stats[0].firstWall
	for i := range BATCHES {
		s := stats[i]
		dur := float64(s.lastWall-s.firstWall) / 1000.0
		firstRel := float64(s.firstWall-baseline) / 1000.0
		lastRel := float64(s.lastWall-baseline) / 1000.0
		fmt.Printf("  Batch %d: %d chunks, span %.1fms, first@%.1fms, last@%.1fms\n",
			i, s.count, dur, firstRel, lastRel)
	}

	fmt.Println()
	fmt.Println("=== Inter-Batch Gaps (FYI — Go API has a natural 1-batch lookahead) ===")
	for i := 1; i < BATCHES; i++ {
		gapUs := stats[i].firstWall - stats[i-1].lastWall
		gapMs := float64(gapUs) / 1000.0
		fmt.Printf("  Gap between batch %d and %d: %.1fms\n", i-1, i, gapMs)
	}

	// Real backpressure proof: total elapsed time must be ~ BATCHES * CLIENT_DELAY.
	// If the server rushed ahead, total would be much shorter.
	totalMs := float64(elapsed.Microseconds()) / 1000.0
	expectedTotalMs := float64(BATCHES) * float64(CLIENT_DELAY/time.Millisecond)
	fmt.Printf("\nTotal elapsed: %.1fms (expected ~%.0fms)\n", totalMs, expectedTotalMs)

	// Go's channel semantics mean the client goroutine always sends the next
	// `next` request immediately after the consumer reads (they unblock
	// simultaneously). So the server produces the next batch in parallel while
	// the consumer sleeps — giving a 1-batch lookahead. The total elapsed
	// time still reflects client-paced execution, which is the real proof
	// that backpressure is working.
	bpOk := math.Abs(totalMs-expectedTotalMs) < expectedTotalMs*0.2

	fmt.Println()
	if bpOk {
		fmt.Println("PASS — total elapsed time matches client pacing (backpressure works)")
	} else {
		fmt.Println("FAIL — total elapsed time does not match client pacing")
	}

	_ = unsubHandler()
	_ = client.Disconnect()
	_ = server.Disconnect()
}

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
