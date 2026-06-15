package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

var buffer1MB = makePattern(1024 * 1024)

func makePattern(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i % 256)
	}
	return b
}

type DataService struct{}

func (s *DataService) GenerateNumbers(count int) (<-chan int, error) {
	fmt.Printf("[Server] Starting push-based number generation (%d items)\n", count)
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := range count {
			ch <- i
		}
		fmt.Println("[Server] Push-based generation complete")
	}()
	return ch, nil
}

func (s *DataService) GenerateLargeData(chunks int) (<-chan []byte, error) {
	fmt.Printf("[Server] Starting push-based data generation (%d x 1MB)\n", chunks)
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		for range chunks {
			ch <- buffer1MB
		}
		fmt.Println("[Server] Push-based data generation complete")
	}()
	return ch, nil
}

func (s *DataService) PullNumbers(count int) (<-chan int, error) {
	fmt.Printf("[Server] Starting pull-based number iteration (%d items)\n", count)
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := range count {
			fmt.Printf("[Server] Client pulled number %d\n", i)
			ch <- i
		}
		fmt.Println("[Server] Pull-based iteration complete")
	}()
	return ch, nil
}

func (s *DataService) PullLargeData(chunks int) (<-chan []byte, error) {
	fmt.Printf("[Server] Starting pull-based data iteration (%d x 1MB)\n", chunks)
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		for i := range chunks {
			fmt.Printf("[Server] Client pulled chunk %d\n", i)
			ch <- buffer1MB
		}
		fmt.Println("[Server] Pull-based data iteration complete")
	}()
	return ch, nil
}

func (s *DataService) GenerateSlowData(delayMs int) (<-chan string, error) {
	fmt.Printf("[Server] Starting slow push generation (%dms delay)\n", delayMs)
	ch := make(chan string)
	go func() {
		defer close(ch)
		for i := range 10 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
			ch <- fmt.Sprintf("Push data %d at %s", i, time.Now().Format("15:04:05.000"))
		}
	}()
	return ch, nil
}

func (s *DataService) PullSlowData(delayMs int) (<-chan string, error) {
	fmt.Printf("[Server] Starting slow pull iteration (%dms delay)\n", delayMs)
	ch := make(chan string)
	go func() {
		defer close(ch)
		for i := range 10 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
			ch <- fmt.Sprintf("Pull data %d at %s", i, time.Now().Format("15:04:05.000"))
		}
	}()
	return ch, nil
}

func main() {
	fmt.Println("=== Pull vs Push Generator Comparison ===")
	fmt.Println()
	fmt.Println("This example demonstrates the differences between:")
	fmt.Println("- Push-based generators (method names with \"generate\")")
	fmt.Println("- Pull-based iterators (method names with \"pull\")")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "generator-test-server",
	})

	if err := server.Connect(ctx); err != nil {
		fatal("server connect", err)
	}
	fmt.Println("Server connected")

	unsub, err := server.RegisterHandler("data", &DataService{})
	if err != nil {
		fatal("register handler", err)
	}
	fmt.Println("Service registered")
	fmt.Println()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "generator-test-client",
	})

	if err := client.Connect(ctx); err != nil {
		fatal("client connect", err)
	}
	fmt.Println("Client connected")
	fmt.Println()

	totalStart := time.Now()

	proxy := client.CreateProxy("data")

	testPushVsPull(ctx, proxy)
	testBackpressure(ctx, proxy)
	testMemoryPressure(ctx, proxy)
	testCancellation(ctx, proxy)

	fmt.Println("=== Summary ===")
	fmt.Println()
	fmt.Println("Push-based (generate*):")
	fmt.Println("  + Server sends all data immediately")
	fmt.Println("  + Good for small datasets or fast consumers")
	fmt.Println("  - Can cause memory pressure with slow consumers")
	fmt.Println("  - No natural backpressure")
	fmt.Println()
	fmt.Println("Pull-based (pull*):")
	fmt.Println("  + Client controls the flow")
	fmt.Println("  + Natural backpressure handling")
	fmt.Println("  + Memory efficient")
	fmt.Println("  + Better cancellation behavior")
	fmt.Println("  - Slightly more latency per item")

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nAll tests completed! Total time: %.1fms\n", totalElapsed)

	_ = unsub()
	client.Disconnect()
	server.Disconnect()
}

func testPushVsPull(ctx context.Context, proxy *rpc.Proxy) {
	fmt.Println("=== Test 1: Fast Number Generation ===")
	fmt.Println()

	start := time.Now()
	count := 0
	fmt.Println("[Client] Starting push-based consumption...")
	streamCh, err := proxy.InvokeStream(ctx, "generateNumbers", 1000)
	if err != nil {
		fatal("push generateNumbers", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Printf("[Client] Stream error: %v\n", val.Error)
			break
		}
		count++
		if count%100 == 0 {
			fmt.Printf("[Client] Processing pushed number %v...\n", val.Data)
			time.Sleep(10 * time.Millisecond)
		}
	}
	pushTime := ms(time.Since(start))
	fmt.Printf("[Client] Push-based: Received %d numbers in %.1fms\n\n", count, pushTime)

	start = time.Now()
	count = 0
	fmt.Println("[Client] Starting pull-based consumption...")
	pullCh, err := proxy.InvokePullIterator(ctx, "pullNumbers", 1000)
	if err != nil {
		fatal("pull pullNumbers", err)
	}
	for val := range pullCh {
		if val.Error != nil {
			fmt.Printf("[Client] Iterator error: %v\n", val.Error)
			break
		}
		count++
		if count%100 == 0 {
			fmt.Printf("[Client] Processing pulled number %v...\n", val.Value)
			time.Sleep(10 * time.Millisecond)
		}
	}
	pullTime := ms(time.Since(start))
	fmt.Printf("[Client] Pull-based: Received %d numbers in %.1fms\n\n", count, pullTime)

	fmt.Printf("Performance comparison: Push %.1fms vs Pull %.1fms\n\n", pushTime, pullTime)
}

func testBackpressure(ctx context.Context, proxy *rpc.Proxy) {
	fmt.Println("=== Test 2: Backpressure Handling (Large Data) ===")
	fmt.Println()

	fmt.Println("[Client] Testing push-based with slow consumer...")
	start := time.Now()
	bytesReceived := 0
	chunkCount := 0

	streamCh, err := proxy.InvokeStream(ctx, "generateLargeData", 10)
	if err != nil {
		fatal("push generateLargeData", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Printf("[Client] Stream error: %v\n", val.Error)
			break
		}
		chunk := toBytes(val.Data)
		bytesReceived += len(chunk)
		chunkCount++
		fmt.Printf("[Client] Received push chunk %d, total: %.1fMB\n",
			chunkCount, float64(bytesReceived)/1024/1024)
		time.Sleep(100 * time.Millisecond)
	}
	pushTime := ms(time.Since(start))
	pushThroughput := float64(bytesReceived) / 1024 / 1024 / (pushTime / 1000)
	fmt.Printf("[Client] Push completed: %.1fMB in %.1fms (%.1f MB/s)\n\n",
		float64(bytesReceived)/1024/1024, pushTime, pushThroughput)

	fmt.Println("[Client] Testing pull-based with slow consumer...")
	start = time.Now()
	bytesReceived = 0
	chunkCount = 0

	pullCh, err := proxy.InvokePullIterator(ctx, "pullLargeData", 10)
	if err != nil {
		fatal("pull pullLargeData", err)
	}
	for val := range pullCh {
		if val.Error != nil {
			fmt.Printf("[Client] Iterator error: %v\n", val.Error)
			break
		}
		chunk := toBytes(val.Value)
		bytesReceived += len(chunk)
		chunkCount++
		fmt.Printf("[Client] Received pull chunk %d, total: %.1fMB\n",
			chunkCount, float64(bytesReceived)/1024/1024)
		time.Sleep(100 * time.Millisecond)
	}
	pullTime := ms(time.Since(start))
	pullThroughput := float64(bytesReceived) / 1024 / 1024 / (pullTime / 1000)
	fmt.Printf("[Client] Pull completed: %.1fMB in %.1fms (%.1f MB/s)\n\n",
		float64(bytesReceived)/1024/1024, pullTime, pullThroughput)
}

func testMemoryPressure(ctx context.Context, proxy *rpc.Proxy) {
	fmt.Println("=== Test 3: Memory Pressure Simulation ===")
	fmt.Println()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	initialMemory := float64(m.HeapAlloc) / 1024 / 1024
	fmt.Printf("Initial memory: %.1fMB\n", initialMemory)

	fmt.Println()
	fmt.Println("[Client] Starting push-based with intentional delay...")
	maxMemoryPush := initialMemory

	pushCh, err := proxy.InvokeStream(ctx, "generateLargeData", 20)
	if err != nil {
		fatal("push generateLargeData memory", err)
	}
	time.Sleep(1 * time.Second)

	for val := range pushCh {
		if val.Error != nil {
			break
		}
		runtime.ReadMemStats(&m)
		current := float64(m.HeapAlloc) / 1024 / 1024
		if current > maxMemoryPush {
			maxMemoryPush = current
		}
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Printf("Push-based peak memory: %.1fMB\n", maxMemoryPush)

	fmt.Println()
	fmt.Println("[Client] Starting pull-based with same delay pattern...")
	maxMemoryPull := initialMemory

	pullCh, err := proxy.InvokePullIterator(ctx, "pullLargeData", 20)
	if err != nil {
		fatal("pull pullLargeData memory", err)
	}
	// Delay has no effect - nothing is generated yet
	time.Sleep(1 * time.Second)

	for val := range pullCh {
		if val.Error != nil {
			break
		}
		runtime.ReadMemStats(&m)
		current := float64(m.HeapAlloc) / 1024 / 1024
		if current > maxMemoryPull {
			maxMemoryPull = current
		}
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Printf("Pull-based peak memory: %.1fMB\n", maxMemoryPull)
	fmt.Printf("Memory difference: %.1fMB\n\n", maxMemoryPush-maxMemoryPull)
}

func testCancellation(ctx context.Context, proxy *rpc.Proxy) {
	fmt.Println("=== Test 4: Early Termination ===")
	fmt.Println()

	fmt.Println("[Client] Testing push-based early termination...")
	pushCtx, pushCancel := context.WithCancel(ctx)
	start := time.Now()
	count := 0

	streamCh, err := proxy.InvokeStream(pushCtx, "generateSlowData", 100)
	if err != nil {
		fatal("push generateSlowData", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			break
		}
		fmt.Printf("[Client] Received push: %v\n", val.Data)
		count++
		if count >= 3 {
			fmt.Println("[Client] Breaking from push loop...")
			pushCancel()
			for range streamCh {
			}
			break
		}
	}
	pushCancel()
	pushTime := ms(time.Since(start))
	fmt.Printf("[Client] Push terminated after %d items in %.1fms\n\n", count, pushTime)

	fmt.Println("[Client] Testing pull-based early termination...")
	pullCtx, pullCancel := context.WithCancel(ctx)
	start = time.Now()
	count = 0

	pullCh, err := proxy.InvokePullIterator(pullCtx, "pullSlowData", 100)
	if err != nil {
		fatal("pull pullSlowData", err)
	}
	for val := range pullCh {
		if val.Error != nil {
			break
		}
		fmt.Printf("[Client] Received pull: %v\n", val.Value)
		count++
		if count >= 3 {
			fmt.Println("[Client] Breaking from pull loop...")
			pullCancel()
			for range pullCh {
			}
			break
		}
	}
	pullCancel()
	pullTime := ms(time.Since(start))
	fmt.Printf("[Client] Pull terminated after %d items in %.1fms\n\n", count, pullTime)
}

func toBytes(val any) []byte {
	if b, ok := val.([]byte); ok {
		return b
	}
	data, _ := rpc.Encode(val)
	return data
}

func fatal(label string, err error) {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", label, err)
	os.Exit(1)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
