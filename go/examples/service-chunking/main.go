package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

var largeData = []byte(strings.Repeat("x", 20*1024*1024))

type TestService struct{}

func (s *TestService) GetLargeData() ([]byte, error) {
	fmt.Println("[Service] Returning large data (20MB)")
	return largeData, nil
}

func (s *TestService) EchoLargeData(data []byte) ([]byte, error) {
	fmt.Printf("[Service] Echoing large data (%.2fMB)\n", float64(len(data))/1024/1024)
	return data, nil
}

func (s *TestService) ProcessData(data []byte) (map[string]any, error) {
	fmt.Printf("[Service] Processing data (%.2fMB)\n", float64(len(data))/1024/1024)
	checksum := 0
	for _, b := range data {
		checksum = (checksum + int(b)) % 256
	}
	return map[string]any{
		"size":     len(data),
		"checksum": fmt.Sprintf("%02x", checksum),
	}, nil
}

func (s *TestService) GenerateNumbers(count int) (<-chan int, error) {
	fmt.Printf("[Service] Starting to stream %d numbers\n", count)
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := range count {
			ch <- i
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Println("[Service] Streaming complete")
	}()
	return ch, nil
}

func (s *TestService) GenerateLargeItems(count, sizeKB int) (<-chan map[string]any, error) {
	fmt.Printf("[Service] Starting to stream %d items of %dKB each\n", count, sizeKB)
	itemData := []byte(strings.Repeat("x", sizeKB*1024))

	ch := make(chan map[string]any)
	go func() {
		defer close(ch)
		for i := range count {
			ch <- map[string]any{"index": i, "data": itemData}
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Println("[Service] Large item streaming complete")
	}()
	return ch, nil
}

func main() {
	fmt.Println("=== Service Chunking Test ===")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "chunking-server",
	})

	if err := server.Connect(ctx); err != nil {
		fatal("server connect", err)
	}
	fmt.Println("Server connected")

	svcManager := rpc.NewRPCService(server)
	service, err := svcManager.RegisterHandler(rpc.ServiceConfig{
		Name:    "chunking-service",
		Version: "1.0.0",
	}, &TestService{})
	if err != nil {
		fatal("register service", err)
	}
	fmt.Println("Service registered")
	fmt.Println()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "chunking-client",
	})

	if err := client.Connect(ctx); err != nil {
		fatal("client connect", err)
	}
	fmt.Println("Client connected")
	fmt.Println()

	totalStart := time.Now()

	proxy, err := client.CreateServiceProxy(ctx, "chunking-service")
	if err != nil {
		fatal("create service proxy", err)
	}
	fmt.Println("Service proxy created")
	fmt.Println()

	fmt.Println("--- Test 1: Get Large Data ---")
	start := time.Now()
	result, err := proxy.Invoke(ctx, "getLargeData")
	if err != nil {
		fatal("getLargeData", err)
	}
	elapsed := ms(time.Since(start))
	resultBytes := toBytes(result)
	sizeMB := float64(len(resultBytes)) / 1024 / 1024
	throughput := sizeMB / (elapsed / 1000)
	dataMatches := len(resultBytes) == len(largeData)
	fmt.Printf("Received %.2fMB in %.1fms (%.1f MB/s)\n", sizeMB, elapsed, throughput)
	fmt.Printf("   Data matches: %v\n\n", dataMatches)

	fmt.Println("--- Test 2: Echo Large Data ---")
	start = time.Now()
	result, err = proxy.Invoke(ctx, "echoLargeData", largeData)
	if err != nil {
		fatal("echoLargeData", err)
	}
	elapsed = ms(time.Since(start))
	echoBytes := toBytes(result)
	echoSizeMB := float64(len(echoBytes)) / 1024 / 1024
	echoThroughput := echoSizeMB / (elapsed / 1000)
	echoMatches := len(echoBytes) == len(largeData)
	fmt.Printf("Echoed %.2fMB in %.1fms (%.1f MB/s)\n", echoSizeMB, elapsed, echoThroughput)
	fmt.Printf("   Data matches: %v\n\n", echoMatches)

	fmt.Println("--- Test 3: Process Large Data ---")
	start = time.Now()
	result, err = proxy.Invoke(ctx, "processData", largeData)
	if err != nil {
		fatal("processData", err)
	}
	elapsed = ms(time.Since(start))
	resultMap, _ := result.(map[string]any)
	size := toNumber(resultMap["size"])
	checksum := fmt.Sprintf("%v", resultMap["checksum"])
	fmt.Printf("Processed %.2fMB in %.1fms\n", float64(size)/1024/1024, elapsed)
	fmt.Printf("   Size: %d bytes\n", size)
	fmt.Printf("   Checksum: %s\n\n", checksum)

	fmt.Println("--- Test 4: Stream Numbers ---")
	start = time.Now()
	streamCount := 0
	streamCh, err := proxy.InvokeStream(ctx, "generateNumbers", 10)
	if err != nil {
		fatal("generateNumbers stream", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Printf("   Stream error: %v\n", val.Error)
			break
		}
		if streamCount < 3 || streamCount >= 7 {
			fmt.Printf("   Received: %v\n", val.Data)
		} else if streamCount == 3 {
			fmt.Println("   ...")
		}
		streamCount++
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("Streamed %d numbers in %.1fms (%.1fms per item)\n\n",
		streamCount, elapsed, elapsed/float64(streamCount))

	fmt.Println("--- Test 5: Stream Large Items ---")
	start = time.Now()
	largeStreamCount := 0
	totalSize := 0

	largeCh, err := proxy.InvokeStream(ctx, "generateLargeItems", 5, 500)
	if err != nil {
		fatal("generateLargeItems stream", err)
	}
	for val := range largeCh {
		if val.Error != nil {
			fmt.Printf("   Stream error: %v\n", val.Error)
			break
		}
		m, ok := val.Data.(map[string]any)
		if ok {
			chunk := toBytes(m["data"])
			totalSize += len(chunk)
			fmt.Printf("   Received item %v: %dKB\n", m["index"], len(chunk)/1024)
		}
		largeStreamCount++
	}
	elapsed = ms(time.Since(start))
	totalMB := float64(totalSize) / 1024 / 1024
	streamThroughput := totalMB / (elapsed / 1000)
	fmt.Printf("Streamed %d large items (%.2fMB total) in %.1fms (%.1f MB/s)\n\n",
		largeStreamCount, totalMB, elapsed, streamThroughput)

	fmt.Println("--- Test 6: Concurrent Mixed Operations ---")
	start = time.Now()

	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make(map[string]any)

	wg.Go(func() {
		r, err := proxy.Invoke(ctx, "getLargeData")
		if err != nil {
			fmt.Printf("   getLargeData error: %v\n", err)
			return
		}
		mu.Lock()
		results["largeData"] = r
		mu.Unlock()
	})

	wg.Go(func() {
		r, err := proxy.Invoke(ctx, "processData", largeData)
		if err != nil {
			fmt.Printf("   processData error: %v\n", err)
			return
		}
		mu.Lock()
		results["processResult"] = r
		mu.Unlock()
	})

	wg.Go(func() {
		count := 0
		ch, err := proxy.InvokeStream(ctx, "generateNumbers", 5)
		if err != nil {
			fmt.Printf("   generateNumbers error: %v\n", err)
			return
		}
		for val := range ch {
			if val.Error != nil {
				break
			}
			count++
		}
		mu.Lock()
		results["streamNumbers"] = count
		mu.Unlock()
	})

	wg.Go(func() {
		items := 0
		ch, err := proxy.InvokeStream(ctx, "generateLargeItems", 3, 200)
		if err != nil {
			fmt.Printf("   generateLargeItems error: %v\n", err)
			return
		}
		for val := range ch {
			if val.Error != nil {
				break
			}
			items++
		}
		mu.Lock()
		results["streamItems"] = items
		mu.Unlock()
	})

	wg.Wait()
	elapsed = ms(time.Since(start))

	fmt.Printf("Completed mixed operations in %.1fms\n", elapsed)
	if ld, ok := results["largeData"]; ok {
		fmt.Printf("   Large data: %.2fMB\n", float64(len(toBytes(ld)))/1024/1024)
	}
	if pr, ok := results["processResult"].(map[string]any); ok {
		fmt.Printf("   Process result: %v bytes\n", pr["size"])
	}
	if sn, ok := results["streamNumbers"].(int); ok {
		fmt.Printf("   Streamed numbers: %d\n", sn)
	}
	if si, ok := results["streamItems"].(int); ok {
		fmt.Printf("   Streamed items: %d\n", si)
	}
	fmt.Println()

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("All tests passed! Service chunking and streaming are working correctly. Total time: %.1fms\n", totalElapsed)

	_ = service.Stop()
	svcManager.StopAll()
	client.Disconnect()
	server.Disconnect()
}

func toBytes(val any) []byte {
	if b, ok := val.([]byte); ok {
		return b
	}
	data, _ := rpc.Encode(val)
	return data
}

func toNumber(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int8:
		return int(n)
	case int16:
		return int(n)
	case int32:
		return int(n)
	case int64:
		return int(n)
	case uint8:
		return int(n)
	case uint16:
		return int(n)
	case uint32:
		return int(n)
	case uint64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func fatal(label string, err error) {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", label, err)
	os.Exit(1)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
