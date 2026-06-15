package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

var (
	smallData  = "This is a small response that fits in a single message"
	mediumData = make([]byte, 5*1024*1024)  // 5MB
	largeData  = make([]byte, 50*1024*1024) // 50MB

	chunkBuffers [3][]byte
	echoData     = make([]byte, 4*1024*1024) // 4MB
)

func init() {
	for i := range mediumData {
		mediumData[i] = byte(i % 256)
	}
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	for i := range 3 {
		buf := make([]byte, 5*1024*1024)
		for j := range buf {
			buf[j] = byte((i + j) % 256)
		}
		chunkBuffers[i] = buf
	}
	for i := range echoData {
		echoData[i] = byte((i * 2) % 256)
	}
}

type TestService struct{}

func (s *TestService) GetSmallData() (string, error) {
	return smallData, nil
}

func (s *TestService) GetMediumData() ([]byte, error) {
	fmt.Println("Returning 5MB buffer")
	return mediumData, nil
}

func (s *TestService) GetLargeData() ([]byte, error) {
	fmt.Println("Returning 50MB buffer")
	return largeData, nil
}

func (s *TestService) Echo(data []byte) ([]byte, error) {
	fmt.Printf("Echoing %.2fMB\n", float64(len(data))/1024/1024)
	return data, nil
}

func (s *TestService) GenerateLargeDataStream() (<-chan []byte, error) {
	ch := make(chan []byte)
	fmt.Println("Streaming 3 chunks of 5MB each")
	go func() {
		defer close(ch)
		for i := range 3 {
			fmt.Printf("Yielding chunk %d/3 - size: %d bytes\n", i+1, len(chunkBuffers[i]))
			fmt.Printf("First bytes: %v\n", chunkBuffers[i][:10])
			ch <- chunkBuffers[i]
		}
	}()
	return ch, nil
}

func (s *TestService) Ping() (string, error) {
	return fmt.Sprintf("pong at %s", time.Now().UTC().Format(time.RFC3339Nano)), nil
}

func main() {
	totalStart := time.Now()

	fmt.Println("Automatic Chunking Test")
	fmt.Println()
	fmt.Println("Testing transparent chunking in RPC library")
	fmt.Println("Note: NATS server default max_payload is typically 1MB")

	ctx := context.Background()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Name:    "test-client",
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
	})

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Name:    "test-server",
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
	})

	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Client connect error: %v\n", err)
		os.Exit(1)
	}
	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Server connect error: %v\n", err)
		os.Exit(1)
	}

	unsub1, err := server.RegisterHandler("test", &TestService{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "RegisterHandler error: %v\n", err)
		os.Exit(1)
	}
	unsub2, err := server.RegisterHandler("testSmall", &TestService{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "RegisterHandler error: %v\n", err)
		os.Exit(1)
	}

	proxy := client.CreateProxy("test")

	fmt.Println("\nTest 1: Small Data")
	smallStart := time.Now()
	smallResult, err := invokeString(ctx, proxy, "getSmallData")
	if err != nil {
		fmt.Fprintf(os.Stderr, "getSmallData error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Small data received in %dms: \"%s\"\n", time.Since(smallStart).Milliseconds(), smallResult)

	fmt.Println("\nTest 2: Medium Data (5MB)")
	mediumStart := time.Now()
	mediumResult, err := proxy.Invoke(ctx, "getMediumData")
	if err != nil {
		fmt.Fprintf(os.Stderr, "getMediumData error: %v\n", err)
		os.Exit(1)
	}
	mediumTime := time.Since(mediumStart)
	mediumBytes := toBytes(mediumResult)
	fmt.Printf("Medium data received: %.2fMB in %dms\n", float64(len(mediumBytes))/1024/1024, mediumTime.Milliseconds())
	fmt.Printf("Throughput: %.2f MB/s\n", 5.0/mediumTime.Seconds())

	corrupted := false
	checkLen := min(len(mediumBytes), 1000)
	for i := range checkLen {
		if mediumBytes[i] != byte(i%256) {
			corrupted = true
			break
		}
	}
	if corrupted {
		fmt.Println("Data integrity: CORRUPTED")
	} else {
		fmt.Println("Data integrity: OK")
	}

	fmt.Println("\nTest 3: Large Data (50MB)")
	largeStart := time.Now()
	largeResult, err := proxy.Invoke(ctx, "getLargeData")
	if err != nil {
		fmt.Fprintf(os.Stderr, "getLargeData error: %v\n", err)
		os.Exit(1)
	}
	largeTime := time.Since(largeStart)
	largeBytes := toBytes(largeResult)
	fmt.Printf("Large data received: %.2fMB in %dms\n", float64(len(largeBytes))/1024/1024, largeTime.Milliseconds())
	fmt.Printf("Throughput: %.2f MB/s\n", 50.0/largeTime.Seconds())

	fmt.Println("\nTest 4: Echo Test (4MB round-trip)")
	echoStart := time.Now()
	echoResult, err := proxy.Invoke(ctx, "echo", echoData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "echo error: %v\n", err)
		os.Exit(1)
	}
	echoTime := time.Since(echoStart)
	echoBytes := toBytes(echoResult)
	fmt.Printf("Echo completed in %dms\n", echoTime.Milliseconds())
	fmt.Printf("Round-trip throughput: %.2f MB/s\n", 8.0/echoTime.Seconds())
	if bytes.Equal(echoData, echoBytes) {
		fmt.Println("Data matches")
	} else {
		fmt.Println("Data mismatch")
	}

	fmt.Println("\nTest 5: Streaming Large Chunks")
	streamStart := time.Now()
	streamedBytes := 0
	chunkCount := 0

	streamCh, err := proxy.InvokeStream(ctx, "generateLargeDataStream")
	if err != nil {
		fmt.Fprintf(os.Stderr, "stream error: %v\n", err)
		os.Exit(1)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Fprintf(os.Stderr, "stream value error: %v\n", val.Error)
			break
		}
		chunk := toBytes(val.Data)
		chunkCount++
		streamedBytes += len(chunk)
		fmt.Printf("Received stream chunk %d: %.2fMB\n", chunkCount, float64(len(chunk))/1024/1024)
	}
	streamTime := time.Since(streamStart)
	fmt.Printf("Stream completed: %.2fMB in %dms\n", float64(streamedBytes)/1024/1024, streamTime.Milliseconds())
	fmt.Printf("Stream throughput: %.2f MB/s\n", float64(streamedBytes)/1024/1024/streamTime.Seconds())

	fmt.Println("\nTest 6: Concurrent Operations")
	var wg sync.WaitGroup

	wg.Go(func() {
		res, err := proxy.Invoke(ctx, "getLargeData")
		if err != nil {
			fmt.Printf("Large data error: %v\n", err)
			return
		}
		b := toBytes(res)
		fmt.Printf("Large data received: %.2fMB\n", float64(len(b))/1024/1024)
	})

	for i := range 5 {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			start := time.Now()
			_, err := proxy.Invoke(ctx, "ping")
			if err != nil {
				fmt.Printf("Ping %d error: %v\n", index+1, err)
				return
			}
			fmt.Printf("Ping %d latency: %dms\n", index+1, time.Since(start).Milliseconds())
		}(i)
		time.Sleep(time.Duration(i) * 100 * time.Millisecond)
	}

	wg.Wait()

	fmt.Println("\nTest 7: Extreme Chunking (1KB max payload)")

	clientSmall := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Name:    "test-client-small",
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
	})

	if err := clientSmall.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Small client connect error: %v\n", err)
		os.Exit(1)
	}

	clientSmall.SetMaxPayloadSize(1024)

	proxySmall := clientSmall.CreateProxy("testSmall")

	smallDataResult, err := invokeString(ctx, proxySmall, "getSmallData")
	if err != nil {
		fmt.Fprintf(os.Stderr, "getSmallData (small) error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Small data with 1KB limit: \"%s\"\n", smallDataResult)

	tenKBData := make([]byte, 10*1024)
	for i := range tenKBData {
		tenKBData[i] = 0x42
	}
	echoSmallResult, err := proxySmall.Invoke(ctx, "echo", tenKBData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "echo 10KB error: %v\n", err)
		os.Exit(1)
	}
	echoSmallBytes := toBytes(echoSmallResult)
	fmt.Printf("10KB data with 1KB limit: received %d bytes (~10 chunks)\n", len(echoSmallBytes))

	fiveMBStart := time.Now()
	fiveMBResult, err := proxySmall.Invoke(ctx, "getMediumData")
	if err != nil {
		fmt.Fprintf(os.Stderr, "getMediumData (small) error: %v\n", err)
		os.Exit(1)
	}
	fiveMBBytes := toBytes(fiveMBResult)
	fmt.Printf("5MB data with 1KB limit: received %d bytes in %dms (~5120 chunks)\n",
		len(fiveMBBytes), time.Since(fiveMBStart).Milliseconds())

	clientSmall.Disconnect()
	unsub1()
	unsub2()
	client.Disconnect()
	server.Disconnect()

	fmt.Printf("\nAll tests completed! Total time: %.1fms\n", float64(time.Since(totalStart).Microseconds())/1000)
}

func invokeString(ctx context.Context, proxy *rpc.Proxy, method string) (string, error) {
	raw, err := proxy.Invoke(ctx, method)
	if err != nil {
		return "", err
	}
	if s, ok := raw.(string); ok {
		return s, nil
	}
	if b, ok := raw.([]byte); ok {
		return string(b), nil
	}
	return fmt.Sprintf("%v", raw), nil
}

func toBytes(val any) []byte {
	switch v := val.(type) {
	case []byte:
		return v
	default:
		data, err := rpc.Encode(v)
		if err != nil {
			return nil
		}
		return data
	}
}
