package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

var (
	mediumData = make([]byte, 5*1024*1024)  // 5MB
	largeData  = make([]byte, 10*1024*1024) // 10MB
)

func init() {
	for i := range mediumData {
		mediumData[i] = byte(i % 256)
	}
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
}

type TestService struct{}

func (s *TestService) Echo(msg string) (string, error)      { return msg, nil }
func (s *TestService) Add(a, b int) (int, error)            { return a + b, nil }
func (s *TestService) GetLargeData() ([]byte, error)        { return largeData, nil }
func (s *TestService) EchoData(data []byte) ([]byte, error) { return data, nil }

func (s *TestService) GenerateNumbers(count int) (<-chan int, error) {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := range count {
			ch <- i
		}
	}()
	return ch, nil
}

func (s *TestService) ErrorMethod(shouldFail bool) (string, error) {
	if shouldFail {
		return "", rpc.NewRPCException(rpc.ErrCodeInternalError, "Test error")
	}
	return "Success", nil
}

func asInt(v any) int {
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
	case float32:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func sleep(ms int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

type perfOp struct {
	name     string
	duration time.Duration
}

type PerfTimer struct {
	name string
	ops  []perfOp
}

func (t *PerfTimer) start(name string) func() {
	s := time.Now()
	return func() {
		d := time.Since(s)
		t.ops = append(t.ops, perfOp{name, d})
		fmt.Printf("  %s: %.2fms\n", name, float64(d.Microseconds())/1000)
	}
}

func (t *PerfTimer) summary() {
	fmt.Printf("\n%s Performance Summary:\n", t.name)
	fmt.Println(strings.Repeat("=", 50))
	var total time.Duration
	for _, op := range t.ops {
		total += op.duration
	}
	for _, op := range t.ops {
		pct := 0.0
		if total > 0 {
			pct = float64(op.duration) / float64(total) * 100
		}
		pad := max(40-len(op.name), 0)
		fmt.Printf("%s%s %8.2fms (%5.1f%%)\n", op.name, strings.Repeat(".", pad),
			float64(op.duration.Microseconds())/1000, pct)
	}
	fmt.Println(strings.Repeat("=", 50))
	pad := max(40-len("Total Time:"), 0)
	fmt.Printf("Total Time:%s %8.2fms\n\n", strings.Repeat(".", pad),
		float64(total.Microseconds())/1000)
}

func testAllFeatures() error {
	fmt.Println("Comprehensive RPC Performance Test (Go)")
	fmt.Println()

	ctx := context.Background()
	timer := &PerfTimer{name: "All-in-One Test"}

	end := timer.start("Server Client Creation")
	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Name:    "test-server",
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
	})
	if err := server.Connect(ctx); err != nil {
		return fmt.Errorf("server connect: %w", err)
	}
	end()

	end = timer.start("Client Creation")
	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Name:    "test-client",
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
	})
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("client connect: %w", err)
	}
	end()

	fmt.Println("\n1. Native Request/Reply")
	end = timer.start("Request Handler Setup & 100 calls")

	unsub1, err := server.OnRequest("echo.*", func(data []byte) (any, error) {
		var payload map[string]any
		if err := rpc.Decode(data, &payload); err != nil {
			return nil, err
		}
		return map[string]any{
			"echo":      payload["msg"],
			"timestamp": time.Now().UnixMilli(),
		}, nil
	})
	if err != nil {
		return fmt.Errorf("onRequest: %w", err)
	}
	sleep(50)

	for i := range 100 {
		respBytes, err := client.Request(ctx, "echo.test", map[string]any{"msg": fmt.Sprintf("Message %d", i)})
		if err != nil {
			return fmt.Errorf("request %d: %w", i, err)
		}
		var resp map[string]any
		if err := rpc.Decode(respBytes, &resp); err != nil {
			return fmt.Errorf("decode response %d: %w", i, err)
		}
		if resp["echo"] != fmt.Sprintf("Message %d", i) {
			return fmt.Errorf("echo mismatch at %d: got %v", i, resp["echo"])
		}
	}

	unsub1()
	end()

	fmt.Println("\n2. Register Handlers (RPC style)")
	end = timer.start("RPC Handler Setup & 100 calls")

	handlers := map[string]any{
		"echo": func(msg string) (string, error) {
			return "Echo: " + msg, nil
		},
		"add": func(a, b int) (int, error) {
			return a + b, nil
		},
	}

	unsub2, err := server.RegisterHandler("test", handlers)
	if err != nil {
		return fmt.Errorf("registerHandler: %w", err)
	}
	sleep(50)

	testProxy := client.CreateProxy("test")

	for i := range 50 {
		echoResult, err := testProxy.Invoke(ctx, "echo", fmt.Sprintf("Message %d", i))
		if err != nil {
			return fmt.Errorf("echo invoke %d: %w", i, err)
		}
		expected := fmt.Sprintf("Echo: Message %d", i)
		if echoResult != expected {
			return fmt.Errorf("echo result mismatch: got %v, want %v", echoResult, expected)
		}

		addResult, err := testProxy.Invoke(ctx, "add", i, i+1)
		if err != nil {
			return fmt.Errorf("add invoke %d: %w", i, err)
		}
		if asInt(addResult) != 2*i+1 {
			return fmt.Errorf("add result mismatch: got %v, want %d", addResult, 2*i+1)
		}
	}

	_ = unsub2()
	end()

	fmt.Println("\n3. Large Data Transfer (Auto-Chunking)")
	end = timer.start("Large Data Transfer (10MB)")

	largeHandlers := map[string]any{
		"getLarge": func() ([]byte, error) {
			return largeData, nil
		},
		"echoData": func(data []byte) ([]byte, error) {
			return data, nil
		},
	}

	unsub3, err := server.RegisterHandler("data", largeHandlers)
	if err != nil {
		return fmt.Errorf("registerHandler data: %w", err)
	}
	sleep(50)

	dataProxy := client.CreateProxy("data")

	result, err := dataProxy.Invoke(ctx, "getLarge")
	if err != nil {
		return fmt.Errorf("getLarge: %w", err)
	}
	resultBytes, ok := result.([]byte)
	if !ok {
		return fmt.Errorf("getLarge: unexpected type %T", result)
	}
	if len(resultBytes) != len(largeData) {
		return fmt.Errorf("large data size mismatch: got %d, want %d", len(resultBytes), len(largeData))
	}

	echoResult, err := dataProxy.Invoke(ctx, "echoData", mediumData)
	if err != nil {
		return fmt.Errorf("echoData: %w", err)
	}
	echoBytes, ok := echoResult.([]byte)
	if !ok {
		return fmt.Errorf("echoData: unexpected type %T", echoResult)
	}
	if !bytes.Equal(echoBytes, mediumData) {
		return fmt.Errorf("echo data mismatch")
	}

	_ = unsub3()
	end()

	fmt.Println("\n4. Channel Communication")
	end = timer.start("Channel Setup & 1000 messages")

	serverChannel, err := server.Channel("perf-channel")
	if err != nil {
		return fmt.Errorf("server channel: %w", err)
	}
	clientChannel, err := client.Channel("perf-channel")
	if err != nil {
		return fmt.Errorf("client channel: %w", err)
	}

	var messagesReceived int64
	serverChannel.OnMessage(func(data any) {
		atomic.AddInt64(&messagesReceived, 1)
	})
	sleep(50)

	for i := range 1000 {
		if err := clientChannel.Send(map[string]any{"index": i, "data": fmt.Sprintf("Message %d", i)}); err != nil {
			return fmt.Errorf("channel send %d: %w", i, err)
		}
	}

	sleep(200)
	count := atomic.LoadInt64(&messagesReceived)
	if count != 1000 {
		return fmt.Errorf("expected 1000 messages, got %d", count)
	}

	_ = serverChannel.Close()
	_ = clientChannel.Close()
	end()

	fmt.Println("\n5. Private Channel Communication")
	end = timer.start("Private Channel Setup & 100 calls")

	serverPrivate, err := server.PrivateChannelConnect("perf-private", "test-client")
	if err != nil {
		return fmt.Errorf("server private channel: %w", err)
	}

	unsub5, err := serverPrivate.OnRequest(func(data []byte) (any, error) {
		var req map[string]any
		if err := rpc.Decode(data, &req); err != nil {
			return nil, err
		}
		return map[string]any{"processed": asInt(req["value"]) * 2}, nil
	})
	if err != nil {
		return fmt.Errorf("private onRequest: %w", err)
	}
	sleep(50)

	clientPrivate, err := client.PrivateChannelConnect("perf-private", "test-server")
	if err != nil {
		return fmt.Errorf("client private channel: %w", err)
	}

	for i := range 100 {
		respBytes, err := clientPrivate.Request(ctx, map[string]any{"value": i}, 5*time.Second)
		if err != nil {
			return fmt.Errorf("private request %d: %w", i, err)
		}
		var resp map[string]any
		if err := rpc.Decode(respBytes, &resp); err != nil {
			return fmt.Errorf("decode private response %d: %w", i, err)
		}
		if asInt(resp["processed"]) != i*2 {
			return fmt.Errorf("private channel mismatch at %d: got %v, want %d", i, resp["processed"], i*2)
		}
	}

	unsub5()
	_ = serverPrivate.Close()
	_ = clientPrivate.Close()
	end()

	fmt.Println("\n6. Service Creation & Discovery")
	end = timer.start("Service Setup")

	svcMgr := rpc.NewRPCService(server)
	service, err := svcMgr.RegisterHandler(rpc.ServiceConfig{
		Name:        "perf-service",
		Version:     "1.0.0",
		Description: "Performance test service",
	}, &TestService{})
	if err != nil {
		return fmt.Errorf("register service: %w", err)
	}

	sleep(100)

	info := service.Info()
	if info.Name != "perf-service" {
		return fmt.Errorf("service name mismatch: %s", info.Name)
	}

	monitor := rpc.NewServiceMonitor(client)
	infos, err := monitor.Info(ctx, "perf-service")
	if err != nil {
		return fmt.Errorf("discover service: %w", err)
	}
	if len(infos) == 0 {
		return fmt.Errorf("no services discovered")
	}
	if infos[0].Name != "perf-service" {
		return fmt.Errorf("discovered service name mismatch: %s", infos[0].Name)
	}

	end()

	fmt.Println("\n7. Service Proxy Calls")
	end = timer.start("Service Proxy Creation & 50 calls")

	calcProxy, err := client.CreateServiceProxy(ctx, "perf-service")
	if err != nil {
		return fmt.Errorf("create service proxy: %w", err)
	}

	for i := range 50 {
		result, err := calcProxy.Invoke(ctx, "add", i, i+1)
		if err != nil {
			return fmt.Errorf("service add %d: %w", i, err)
		}
		if asInt(result) != 2*i+1 {
			return fmt.Errorf("service add mismatch: got %v, want %d", result, 2*i+1)
		}
	}

	end()

	fmt.Println("\n8. Service Streaming")
	end = timer.start("Stream Processing (100 items)")

	stream, err := calcProxy.InvokeStream(ctx, "generateNumbers", 100)
	if err != nil {
		return fmt.Errorf("invoke stream: %w", err)
	}

	var streamSum int
	for sv := range stream {
		if sv.Error != nil {
			return fmt.Errorf("stream error: %w", sv.Error)
		}
		streamSum += asInt(sv.Data)
	}

	expectedSum := 99 * 100 / 2
	if streamSum != expectedSum {
		return fmt.Errorf("stream sum mismatch: got %d, want %d", streamSum, expectedSum)
	}

	end()

	fmt.Println("\n8b. Large Data via RPC Handler")
	end = timer.start("Get Large Data (10MB) via RPC")

	largeDataHandlers := map[string]any{
		"getLargeData": func() ([]byte, error) {
			return largeData, nil
		},
	}

	unsub8b, err := server.RegisterHandler("largedata", largeDataHandlers)
	if err != nil {
		return fmt.Errorf("register largedata: %w", err)
	}
	sleep(50)

	largeRpcProxy := client.CreateProxy("largedata")
	largeRpcResult, err := largeRpcProxy.Invoke(ctx, "getLargeData")
	if err != nil {
		return fmt.Errorf("getLargeData: %w", err)
	}
	largeRpcBytes, ok := largeRpcResult.([]byte)
	if !ok {
		return fmt.Errorf("getLargeData: unexpected type %T", largeRpcResult)
	}
	if len(largeRpcBytes) != len(largeData) {
		return fmt.Errorf("large data RPC size mismatch: got %d, want %d", len(largeRpcBytes), len(largeData))
	}

	_ = unsub8b()
	end()

	fmt.Println("\n9. Concurrent Operations")
	end = timer.start("Concurrent Requests (500 parallel)")

	unsub9, err := server.OnRequest("echo.concurrent", func(data []byte) (any, error) {
		var payload map[string]any
		if err := rpc.Decode(data, &payload); err != nil {
			return nil, err
		}
		return map[string]any{"echo": payload, "handled": true}, nil
	})
	if err != nil {
		return fmt.Errorf("concurrent onRequest: %w", err)
	}
	sleep(50)

	var wg sync.WaitGroup
	var concurrentCount int64
	var concurrentErr error
	var errOnce sync.Once

	for i := range 500 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := client.Request(ctx, "echo.concurrent", map[string]any{"index": idx})
			if err != nil {
				errOnce.Do(func() { concurrentErr = err })
				return
			}
			atomic.AddInt64(&concurrentCount, 1)
		}(i)
	}
	wg.Wait()

	if concurrentErr != nil {
		return fmt.Errorf("concurrent error: %w", concurrentErr)
	}
	if concurrentCount != 500 {
		return fmt.Errorf("concurrent count mismatch: got %d, want 500", concurrentCount)
	}

	// Keep handler for mixed workload test
	end()

	fmt.Println("\n10. Error Handling")
	end = timer.start("Error Handling Test")

	successResult, err := calcProxy.Invoke(ctx, "echo", "test")
	if err != nil {
		return fmt.Errorf("echo success: %w", err)
	}
	if successResult != "test" {
		return fmt.Errorf("echo success mismatch: got %v", successResult)
	}

	_, err = calcProxy.Invoke(ctx, "errorMethod", true)
	if err == nil {
		return fmt.Errorf("should have raised exception")
	}
	rpcErr, ok := err.(*rpc.RPCException)
	if !ok {
		return fmt.Errorf("unexpected error type: %T: %v", err, err)
	}
	if rpcErr.Code != "500" && rpcErr.Code != rpc.ErrCodeInternalError {
		return fmt.Errorf("unexpected error code: %s", rpcErr.Code)
	}

	end()

	fmt.Println("\n11. Isolated Connection Proxy")
	end = timer.start("Isolated Proxy Test")

	handlersIsolated := map[string]any{
		"echo": func(msg string) (string, error) {
			return "Echo: " + msg, nil
		},
		"add": func(a, b int) (int, error) {
			return a + b, nil
		},
	}

	unsub11, err := server.RegisterHandler("test", handlersIsolated)
	if err != nil {
		return fmt.Errorf("register isolated: %w", err)
	}
	sleep(50)

	isolatedProxy, closeIsolated, err := client.CreateIsolatedProxy("test")
	if err != nil {
		return fmt.Errorf("create isolated proxy: %w", err)
	}

	for i := range 10 {
		result, err := isolatedProxy.Invoke(ctx, "echo", fmt.Sprintf("Isolated %d", i))
		if err != nil {
			return fmt.Errorf("isolated echo %d: %w", i, err)
		}
		expected := fmt.Sprintf("Echo: Isolated %d", i)
		if result != expected {
			return fmt.Errorf("isolated echo mismatch: got %v, want %v", result, expected)
		}
	}

	_ = closeIsolated()
	_ = unsub11()
	end()

	fmt.Printf("\n12. Mixed Workload (Simulating Real Usage)\n")
	end = timer.start("Mixed Operations")

	testChannel, err := client.Channel("mixed-channel")
	if err != nil {
		return fmt.Errorf("mixed channel: %w", err)
	}

	mixedOp := func() error {
		var ops sync.WaitGroup
		var mixedErr error
		var mixedOnce sync.Once

		for range 10 {
			ops.Go(func() {
				_, err := client.Request(ctx, "echo.concurrent", map[string]any{"msg": "quick"})
				if err != nil {
					mixedOnce.Do(func() { mixedErr = err })
				}
			})

			ops.Go(func() {
				_, err := calcProxy.Invoke(ctx, "add", rand.IntN(100), rand.IntN(100))
				if err != nil {
					mixedOnce.Do(func() { mixedErr = err })
				}
			})

			ops.Go(func() {
				err := testChannel.Send(map[string]any{"event": "mixed", "data": "test"})
				if err != nil {
					mixedOnce.Do(func() { mixedErr = err })
				}
			})
		}

		ops.Go(func() {
			_, err := calcProxy.Invoke(ctx, "echo", "test data")
			if err != nil {
				mixedOnce.Do(func() { mixedErr = err })
			}
		})

		ops.Wait()
		return mixedErr
	}

	var rounds sync.WaitGroup
	var roundErr error
	var roundOnce sync.Once

	for range 5 {
		rounds.Go(func() {
			if err := mixedOp(); err != nil {
				roundOnce.Do(func() { roundErr = err })
			}
		})
	}
	rounds.Wait()

	if roundErr != nil {
		return fmt.Errorf("mixed workload: %w", roundErr)
	}

	_ = testChannel.Close()
	end()

	fmt.Println("\nCleanup")
	end = timer.start("Cleanup")

	unsub9()
	_ = service.Stop()
	_ = client.Disconnect()
	_ = server.Disconnect()

	end()

	timer.summary()
	return nil
}

func main() {
	if err := testAllFeatures(); err != nil {
		fmt.Printf("Test failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("All tests completed successfully!")
}
