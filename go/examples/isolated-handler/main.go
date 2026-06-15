package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

func main() {
	fmt.Println("=== Isolated Handler Connection Test ===")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "main-server",
	})

	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "server connect: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Main server connected")

	handlers := map[string]any{
		"echo": func(msg string) (string, error) {
			fmt.Printf("[Handler] Echo received: %s\n", msg)
			return fmt.Sprintf("Echo: %s", msg), nil
		},
		"getStats": func() (map[string]any, error) {
			fmt.Println("[Handler] Stats requested")
			return map[string]any{
				"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
				"connections": 42,
				"uptime":      123.45,
			}, nil
		},
		"slowOperation": func(delay int) (string, error) {
			fmt.Printf("[Handler] Starting slow operation (%dms)\n", delay)
			time.Sleep(time.Duration(delay) * time.Millisecond)
			fmt.Println("[Handler] Slow operation completed")
			return fmt.Sprintf("Completed after %dms", delay), nil
		},
		"generateData": func(count int) (<-chan map[string]any, error) {
			fmt.Printf("[Handler] Streaming %d items\n", count)
			ch := make(chan map[string]any)
			go func() {
				defer close(ch)
				for i := range count {
					ch <- map[string]any{"index": i, "data": fmt.Sprintf("Item %d", i)}
				}
				fmt.Println("[Handler] Stream completed")
			}()
			return ch, nil
		},
	}

	fmt.Println()
	fmt.Println("Registering handlers with isolated connection")
	unsub, err := server.RegisterHandler("isolated-test", handlers, rpc.WithIsolatedConnection())
	if err != nil {
		fmt.Fprintf(os.Stderr, "register handler: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Handlers registered with isolated connection")
	fmt.Println()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "test-client",
	})

	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "client connect: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Client connected")
	fmt.Println()

	proxy := client.CreateProxy("isolated-test")

	totalStart := time.Now()

	start := time.Now()
	echoResult, err := proxy.Invoke(ctx, "echo", "Hello Isolated World!")
	if err != nil {
		fmt.Fprintf(os.Stderr, "echo: %v\n", err)
		os.Exit(1)
	}
	elapsed := ms(time.Since(start))
	fmt.Printf("Result: %v - Time: %.1fms\n", echoResult, elapsed)

	fmt.Println()
	start = time.Now()
	stats, err := proxy.Invoke(ctx, "getStats")
	if err != nil {
		fmt.Fprintf(os.Stderr, "getStats: %v\n", err)
		os.Exit(1)
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("Stats: %s - Time: %.1fms\n", toJSON(stats), elapsed)

	fmt.Println()
	start = time.Now()

	type indexedResult struct {
		index  int
		result any
		err    error
	}

	results := make([]indexedResult, 5)
	var wg sync.WaitGroup

	calls := []struct {
		method string
		args   []any
	}{
		{"slowOperation", []any{100}},
		{"slowOperation", []any{200}},
		{"slowOperation", []any{150}},
		{"echo", []any{"Concurrent test"}},
		{"getStats", nil},
	}

	for i, call := range calls {
		wg.Add(1)
		go func(idx int, method string, args []any) {
			defer wg.Done()
			var res any
			var err error
			if len(args) > 0 {
				res, err = proxy.Invoke(ctx, method, args...)
			} else {
				res, err = proxy.Invoke(ctx, method)
			}
			results[idx] = indexedResult{idx, res, err}
		}(i, call.method, call.args)
	}

	wg.Wait()
	elapsed = ms(time.Since(start))
	fmt.Printf("All concurrent calls completed in %.1fms:\n", elapsed)
	for _, r := range results {
		if r.err != nil {
			fmt.Printf("  %d: ERROR: %v\n", r.index+1, r.err)
		} else {
			switch v := r.result.(type) {
			case map[string]any:
				fmt.Printf("  %d: %s\n", r.index+1, toJSON(v))
			default:
				fmt.Printf("  %d: %v\n", r.index+1, r.result)
			}
		}
	}

	fmt.Println()
	start = time.Now()
	streamCh, err := proxy.InvokeStream(ctx, "generateData", 5)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stream: %v\n", err)
		os.Exit(1)
	}
	count := 0
	for val := range streamCh {
		if val.Error != nil {
			fmt.Fprintf(os.Stderr, "  stream error: %v\n", val.Error)
			break
		}
		count++
		fmt.Printf("  Received: %s\n", toJSON(val.Data))
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("Streamed %d items in %.1fms\n", count, elapsed)

	fmt.Println()
	fmt.Println("Disconnecting main server...")
	server.Disconnect()
	fmt.Println("Main server disconnected")

	fmt.Println("Testing handlers after main disconnect...")
	time.Sleep(100 * time.Millisecond) // Give time for disconnect to propagate
	_, err = proxy.Invoke(ctx, "echo", "Should fail")
	if err != nil {
		fmt.Printf("Call failed as expected: %v\n", err)
	} else {
		fmt.Println("ERROR: Call should have failed")
	}

	fmt.Println()
	unsub()
	fmt.Println("Handlers unsubscribed")

	_, err = proxy.Invoke(ctx, "echo", "Should fail")
	if err != nil {
		fmt.Printf("Call failed as expected: %v\n", err)
	} else {
		fmt.Println("ERROR: Call should have failed")
	}

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nAll tests completed! Total time: %.1fms\n", totalElapsed)

	client.Disconnect()
	if server.IsConnected() {
		server.Disconnect()
	}
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func toJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
