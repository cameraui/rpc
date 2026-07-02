package main

// Hot-path benchmark: measures the production-critical RPC paths
// (NVR frame delivery, parallel/sequential calls, channel throughput).
// Setup happens outside the measurements; each section runs one
// unmeasured warmup round first.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

const (
	onewayBatches        = 5
	onewayFramesPerBatch = 1000
	parallelCalls        = 500
	sequentialCalls      = 200
	channelMessages      = 2000
)

type FrameService struct{}

// PullFrames routes through pull-callback mode (method name contains "pull").
func (s *FrameService) PullFrames(batches, framesPerBatch, frameSize int, invoker *rpc.CallbackInvoker) (<-chan struct{}, error) {
	// Unbuffered: the `ch <- struct{}{}` is the yield statement.
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		frame := make([]byte, frameSize)
		for i := range frame {
			frame[i] = byte(i % 256)
		}
		for range batches {
			if !invoker.Active() {
				return
			}
			for range framesPerBatch {
				invoker.Invoke("onFrame", frame)
			}
			select {
			case ch <- struct{}{}:
			case <-time.After(30 * time.Second):
				return
			}
		}
	}()
	return ch, nil
}

func waitFor(cond func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for condition")
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// runOneway runs one pull-callback iteration; returns (elapsedMs, bytesReceived).
func runOneway(ctx context.Context, proxy *rpc.Proxy, frameSize, batches, framesPerBatch int) (float64, int64, error) {
	expected := int64(batches * framesPerBatch)
	var frames int64
	var totalBytes int64

	callbacks := rpc.PullCallbackMap{
		"onFrame": func(frame []byte) {
			atomic.AddInt64(&frames, 1)
			atomic.AddInt64(&totalBytes, int64(len(frame)))
		},
	}

	start := time.Now()
	ch, err := proxy.InvokePullIteratorWithCallback(
		ctx, "pullFrames", callbacks, []string{"onFrame"}, batches, framesPerBatch, frameSize,
	)
	if err != nil {
		return 0, 0, fmt.Errorf("invoke pull iterator: %w", err)
	}
	for v := range ch {
		if v.Error != nil {
			return 0, 0, fmt.Errorf("iterator error: %w", v.Error)
		}
	}
	if err := waitFor(func() bool { return atomic.LoadInt64(&frames) >= expected }, 30*time.Second); err != nil {
		return 0, 0, fmt.Errorf("oneway frames: %w (got %d of %d)", err, atomic.LoadInt64(&frames), expected)
	}
	elapsedMs := float64(time.Since(start).Microseconds()) / 1000

	if got := atomic.LoadInt64(&frames); got != expected {
		return 0, 0, fmt.Errorf("oneway frame count mismatch: got %d, expected %d", got, expected)
	}
	if got := atomic.LoadInt64(&totalBytes); got != expected*int64(frameSize) {
		return 0, 0, fmt.Errorf("oneway byte count mismatch: got %d, expected %d", got, expected*int64(frameSize))
	}

	return elapsedMs, atomic.LoadInt64(&totalBytes), nil
}

func run() error {
	fmt.Println("Perf Hotpath Benchmark (Go)")
	fmt.Println()

	ctx := context.Background()

	// --- Setup (not measured) ---
	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Name:    "perf-hotpath-server",
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
	})
	if err := server.Connect(ctx); err != nil {
		return fmt.Errorf("server connect: %w", err)
	}

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Name:    "perf-hotpath-client",
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
	})
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("client connect: %w", err)
	}

	unsubFrames, err := server.RegisterHandler("hotpath-frames", &FrameService{})
	if err != nil {
		return fmt.Errorf("register frames handler: %w", err)
	}

	echoHandlers := map[string]any{
		"echo": func(obj map[string]any) (map[string]any, error) {
			return obj, nil
		},
	}
	unsubEcho, err := server.RegisterHandler("hotpath-rpc", echoHandlers)
	if err != nil {
		return fmt.Errorf("register echo handler: %w", err)
	}
	time.Sleep(50 * time.Millisecond) // Let subscriptions settle

	frameProxy := client.CreateProxy("hotpath-frames")
	echoProxy := client.CreateProxy("hotpath-rpc")

	// --- 1. Oneway callback throughput (NVR frame path) ---
	fmt.Println("1. Oneway Callback Throughput (pull-callback iterator)")

	if _, _, err := runOneway(ctx, frameProxy, 1024, 1, 50); err != nil { // warmup (not measured)
		return fmt.Errorf("oneway 1KB warmup: %w", err)
	}
	oneway1kMs, _, err := runOneway(ctx, frameProxy, 1024, onewayBatches, onewayFramesPerBatch)
	if err != nil {
		return fmt.Errorf("oneway 1KB: %w", err)
	}
	msgsPerSec := float64(onewayBatches*onewayFramesPerBatch) / (oneway1kMs / 1000)
	fmt.Printf("Oneway 1KB: %.2fms (%.0f msg/s)\n", oneway1kMs, msgsPerSec)

	if _, _, err := runOneway(ctx, frameProxy, 100*1024, 1, 50); err != nil { // warmup (not measured)
		return fmt.Errorf("oneway 100KB warmup: %w", err)
	}
	oneway100kMs, oneway100kBytes, err := runOneway(ctx, frameProxy, 100*1024, onewayBatches, onewayFramesPerBatch)
	if err != nil {
		return fmt.Errorf("oneway 100KB: %w", err)
	}
	mbPerSec := float64(oneway100kBytes) / (1024 * 1024) / (oneway100kMs / 1000)
	fmt.Printf("Oneway 100KB: %.2fms (%.1f MB/s)\n", oneway100kMs, mbPerSec)

	// --- 2. Parallel calls ---
	fmt.Println("\n2. Parallel Calls")

	echoCall := func(i int) error {
		result, err := echoProxy.Invoke(ctx, "echo", map[string]any{"seq": i, "camera": "cam-01", "kind": "echo"})
		if err != nil {
			return fmt.Errorf("echo %d: %w", i, err)
		}
		m, ok := result.(map[string]any)
		if !ok {
			return fmt.Errorf("echo %d: unexpected type %T", i, result)
		}
		if got, _ := toInt(m["seq"]); got != int64(i) {
			return fmt.Errorf("echo seq mismatch at %d: got %v", i, m["seq"])
		}
		return nil
	}

	runParallel := func(n int) error {
		var wg sync.WaitGroup
		var callErr error
		var errOnce sync.Once
		for i := range n {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				if err := echoCall(idx); err != nil {
					errOnce.Do(func() { callErr = err })
				}
			}(i)
		}
		wg.Wait()
		return callErr
	}

	if err := runParallel(20); err != nil { // warmup (not measured)
		return fmt.Errorf("parallel warmup: %w", err)
	}

	parallelStart := time.Now()
	if err := runParallel(parallelCalls); err != nil {
		return fmt.Errorf("parallel calls: %w", err)
	}
	parallelMs := float64(time.Since(parallelStart).Microseconds()) / 1000
	fmt.Printf("Parallel %d calls: %.2fms\n", parallelCalls, parallelMs)

	// --- 3. Sequential calls ---
	fmt.Println("\n3. Sequential Calls")

	for i := range 10 { // warmup (not measured)
		if err := echoCall(i); err != nil {
			return fmt.Errorf("sequential warmup: %w", err)
		}
	}

	seqStart := time.Now()
	for i := range sequentialCalls {
		if err := echoCall(i); err != nil {
			return fmt.Errorf("sequential calls: %w", err)
		}
	}
	seqMs := float64(time.Since(seqStart).Microseconds()) / 1000
	usPerCall := seqMs * 1000 / float64(sequentialCalls)
	fmt.Printf("Sequential %d calls: %.2fms (%.1f µs/call)\n", sequentialCalls, seqMs, usPerCall)

	// --- 4. Channel throughput ---
	fmt.Println("\n4. Channel Throughput")

	serverChannel, err := server.Channel("hotpath-channel")
	if err != nil {
		return fmt.Errorf("server channel: %w", err)
	}
	clientChannel, err := client.Channel("hotpath-channel")
	if err != nil {
		return fmt.Errorf("client channel: %w", err)
	}

	var channelReceived int64
	serverChannel.OnMessage(func(data any) {
		atomic.AddInt64(&channelReceived, 1)
	})
	time.Sleep(50 * time.Millisecond) // Let subscription settle

	// Warmup (not measured)
	for i := range 10 {
		if err := clientChannel.Send(map[string]any{"index": i, "warmup": true}); err != nil {
			return fmt.Errorf("channel warmup send %d: %w", i, err)
		}
	}
	if err := waitFor(func() bool { return atomic.LoadInt64(&channelReceived) >= 10 }, 30*time.Second); err != nil {
		return fmt.Errorf("channel warmup: %w", err)
	}

	channelTarget := atomic.LoadInt64(&channelReceived) + channelMessages
	channelStart := time.Now()
	for i := range channelMessages {
		if err := clientChannel.Send(map[string]any{"index": i}); err != nil {
			return fmt.Errorf("channel send %d: %w", i, err)
		}
	}
	if err := waitFor(func() bool { return atomic.LoadInt64(&channelReceived) >= channelTarget }, 30*time.Second); err != nil {
		return fmt.Errorf("channel messages: %w (got %d of %d)", err, atomic.LoadInt64(&channelReceived), channelTarget)
	}
	channelMs := float64(time.Since(channelStart).Microseconds()) / 1000
	usPerMsg := channelMs * 1000 / float64(channelMessages)
	fmt.Printf("Channel %d msgs: %.2fms (%.1f µs/msg)\n", channelMessages, channelMs, usPerMsg)

	_ = serverChannel.Close()
	_ = clientChannel.Close()

	// --- Cleanup ---
	_ = unsubFrames()
	_ = unsubEcho()
	_ = client.Disconnect()
	_ = server.Disconnect()

	fmt.Println("\nAll hotpath benchmarks completed successfully!")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Printf("Benchmark failed: %v\n", err)
		os.Exit(1)
	}
}

// toInt coerces msgpack-decoded numeric values to int64.
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
