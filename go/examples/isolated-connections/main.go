package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

type TestService struct{}

func (s *TestService) Ping() (string, error) {
	return fmt.Sprintf("pong at %s", time.Now().UTC().Format(time.RFC3339Nano)), nil
}

func (s *TestService) HeavyComputation(seconds int) (int, error) {
	fmt.Printf("[Service] Starting heavy computation for %ds\n", seconds)
	start := time.Now()
	for time.Since(start) < time.Duration(seconds)*time.Second {
		time.Sleep(1 * time.Millisecond)
	}
	fmt.Println("[Service] Heavy computation completed")
	return int(time.Since(start).Milliseconds()), nil
}

func (s *TestService) GenerateDataStream() (<-chan string, error) {
	ch := make(chan string)
	go func() {
		defer close(ch)
		for i := range 100 {
			ch <- fmt.Sprintf("Data chunk %d at %s", i, time.Now().UTC().Format(time.RFC3339Nano))
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return ch, nil
}

func testIsolatedChannels() error {
	fmt.Println("=== Testing Isolated Channels ===")
	fmt.Println()

	ctx := context.Background()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "test-client",
	})

	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("client connect: %w", err)
	}

	regularChannel, err := client.Channel("regular-channel")
	if err != nil {
		return fmt.Errorf("regular channel: %w", err)
	}

	isolatedChannel, err := client.Channel("isolated-channel", true)
	if err != nil {
		return fmt.Errorf("isolated channel: %w", err)
	}

	regularChannel.OnMessage(func(data any) {
		fmt.Printf("[Regular Channel]: %s\n", formatMsg(data))
	})

	isolatedChannel.OnMessage(func(data any) {
		fmt.Printf("[Isolated Channel]: %s\n", formatMsg(data))
	})

	fmt.Println("Sending heavy traffic on isolated channel...")
	isolatedStart := time.Now()
	testBuffer := []byte(strings.Repeat("x", 10000)) // 10KB buffer
	var wg sync.WaitGroup
	for i := range 1000 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = isolatedChannel.Send(map[string]any{
				"type":  "bulk",
				"index": idx,
				"data":  testBuffer,
			})
		}(i)
	}

	fmt.Println("Testing regular channel responsiveness...")
	regularStart := time.Now()
	_ = regularChannel.Send(map[string]any{"type": "ping", "timestamp": time.Now().UnixMilli()})
	regularTime := ms(time.Since(regularStart))
	fmt.Printf("Regular channel responded in %.1fms\n", regularTime)

	wg.Wait()
	isolatedTime := ms(time.Since(isolatedStart))
	throughput := (1000 * 10) / (isolatedTime / 1000) // 10KB * 1000 messages
	fmt.Printf("Heavy traffic completed in %.1fms (%.1f MB/s)\n\n", isolatedTime, throughput/1024)

	regularChannel.Close()
	isolatedChannel.Close()
	client.Disconnect()

	return nil
}

func testIsolatedProxies() error {
	fmt.Println("=== Testing Isolated Proxies ===")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "test-server",
	})

	if err := server.Connect(ctx); err != nil {
		return fmt.Errorf("server connect: %w", err)
	}

	unsub, err := server.RegisterHandler("test", &TestService{})
	if err != nil {
		return fmt.Errorf("register handler: %w", err)
	}

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "test-client",
	})

	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("client connect: %w", err)
	}

	regularProxy := client.CreateProxy("test")

	isolatedProxy, closeIsolated, err := client.CreateIsolatedProxy("test")
	if err != nil {
		return fmt.Errorf("create isolated proxy: %w", err)
	}

	fmt.Println("Starting heavy computation on isolated proxy...")
	heavyStart := time.Now()

	type heavyResult struct {
		value any
		err   error
	}
	heavyCh := make(chan heavyResult, 1)
	go func() {
		val, err := isolatedProxy.Invoke(ctx, "heavyComputation", 3)
		heavyCh <- heavyResult{val, err}
	}()

	fmt.Println("Testing regular proxy responsiveness...")
	var pingTimes []float64
	for i := range 5 {
		pingStart := time.Now()
		result, err := regularProxy.Invoke(ctx, "ping")
		if err != nil {
			return fmt.Errorf("ping %d: %w", i+1, err)
		}
		pingTime := ms(time.Since(pingStart))
		pingTimes = append(pingTimes, pingTime)
		fmt.Printf("Regular proxy ping %d: %.1fms - %v\n", i+1, pingTime, result)
		time.Sleep(500 * time.Millisecond)
	}
	var avgPingTime float64
	for _, t := range pingTimes {
		avgPingTime += t
	}
	avgPingTime /= float64(len(pingTimes))
	fmt.Printf("Average ping time during heavy computation: %.1fms\n", avgPingTime)

	hr := <-heavyCh
	if hr.err != nil {
		return fmt.Errorf("heavy computation: %w", hr.err)
	}
	totalHeavyTime := ms(time.Since(heavyStart))
	fmt.Printf("Heavy computation completed in %vms (total: %.1fms)\n\n", hr.value, totalHeavyTime)

	fmt.Println("Testing streaming on isolated proxy...")
	streamStart := time.Now()
	count := 0
	streamCh, err := isolatedProxy.InvokeStream(ctx, "generateDataStream")
	if err != nil {
		return fmt.Errorf("stream: %w", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			return fmt.Errorf("stream error: %w", val.Error)
		}
		count++
		if count%20 == 0 {
			fmt.Printf("Received %d chunks from isolated stream\n", count)
		}
	}
	streamTime := ms(time.Since(streamStart))
	fmt.Printf("Stream completed: %d total chunks in %.1fms (%.1f chunks/s)\n\n",
		count, streamTime, float64(count)*1000/streamTime)

	unsub()

	_ = closeIsolated()
	client.Disconnect()
	server.Disconnect()

	fmt.Println("Isolated proxies test completed")
	fmt.Println()

	return nil
}

func testMixedWorkload() error {
	fmt.Println("=== Testing Mixed Workload ===")
	fmt.Println()

	ctx := context.Background()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "mixed-client",
	})

	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("client connect: %w", err)
	}

	chatChannel, err := client.PrivateChannelConnect("chat", "partner", true)
	if err != nil {
		return fmt.Errorf("chat channel: %w", err)
	}

	dataChannel, err := client.PrivateChannelConnect("data-transfer", "partner", true)
	if err != nil {
		return fmt.Errorf("data channel: %w", err)
	}

	fmt.Println("Starting mixed workload...")

	var chatRunning atomic.Bool
	chatRunning.Store(true)
	var chatWg sync.WaitGroup
	chatWg.Go(func() {
		for chatRunning.Load() {
			_ = chatChannel.Send(map[string]any{
				"type":      "chat",
				"message":   "Hello from chat",
				"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
			})
			time.Sleep(1 * time.Second)
		}
	})

	dataStart := time.Now()
	dataBuffer := []byte(strings.Repeat("x", 100*1024)) // 100KB buffer
	var dataWg sync.WaitGroup
	for i := range 100 {
		dataWg.Add(1)
		go func(chunk int) {
			defer dataWg.Done()
			_ = dataChannel.Send(map[string]any{
				"type":    "data",
				"chunk":   chunk,
				"payload": dataBuffer,
			})
		}(i)
	}

	var chatMessages atomic.Int64
	var dataChunks atomic.Int64

	chatChannel.OnMessage(func(data any) {
		chatMessages.Add(1)
	})
	dataChannel.OnMessage(func(data any) {
		dataChunks.Add(1)
	})

	dataWg.Wait()
	chatRunning.Store(false)
	chatWg.Wait()
	dataTime := ms(time.Since(dataStart))
	dataThroughput := (100 * 100) / (dataTime / 1000) // 100KB * 100 messages

	fmt.Printf("Chat messages: %d\n", chatMessages.Load())
	fmt.Printf("Data chunks: %d\n", dataChunks.Load())
	fmt.Printf("Data transfer: 10MB in %.1fms (%.1f MB/s)\n", dataTime, dataThroughput/1024)
	fmt.Println("Mixed workload completed")
	fmt.Println()

	chatChannel.Close()
	dataChannel.Close()
	client.Disconnect()

	return nil
}

func main() {
	totalStart := time.Now()

	if err := testIsolatedChannels(); err != nil {
		fmt.Fprintf(os.Stderr, "Test failed: %v\n", err)
		os.Exit(1)
	}

	if err := testIsolatedProxies(); err != nil {
		fmt.Fprintf(os.Stderr, "Test failed: %v\n", err)
		os.Exit(1)
	}

	if err := testMixedWorkload(); err != nil {
		fmt.Fprintf(os.Stderr, "Test failed: %v\n", err)
		os.Exit(1)
	}

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("All isolated connection tests completed! Total time: %.1fms\n", totalElapsed)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func formatMsg(data any) string {
	m, ok := data.(map[string]any)
	if !ok {
		return fmt.Sprintf("%v", data)
	}
	for k, v := range m {
		if b, ok := v.([]byte); ok {
			m[k] = fmt.Sprintf("<%d bytes>", len(b))
		}
	}
	return fmt.Sprintf("%v", m)
}
