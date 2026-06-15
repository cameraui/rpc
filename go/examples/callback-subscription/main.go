package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

func main() {
	totalStart := time.Now()

	fmt.Println("Callback Subscription Example")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "callback-test-server",
	})
	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Server connect error: %v\n", err)
		os.Exit(1)
	}

	handlers := map[string]any{
		"name": "events",
		"onEvents": func(prefix string, callback func(map[string]any)) (func(), error) {
			fmt.Printf("New subscriber for prefix '%s'\n", prefix)

			for i := range 5 {
				callback(map[string]any{
					"prefix": prefix,
					"index":  i,
					"type":   "event",
				})
				time.Sleep(50 * time.Millisecond)
			}

			return func() {
				fmt.Printf("Cleanup called for prefix '%s'\n", prefix)
			}, nil
		},
	}

	unsubHandler, err := server.RegisterHandler("events", handlers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Register handler error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Server connected")
	fmt.Println("Handler registered")
	fmt.Println()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "callback-test-client",
	})
	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Client connect error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Client connected")
	fmt.Println()

	proxy := client.CreateProxy("events")

	fmt.Println("Test 1: Basic Callback Subscription")
	fmt.Println()

	var received []map[string]any
	var mu sync.Mutex
	done := make(chan struct{})

	start := time.Now()
	unsub, err := proxy.InvokeCallback("onEvents", []any{"test"}, func(value any) {
		if m, ok := value.(map[string]any); ok {
			mu.Lock()
			received = append(received, m)
			count := len(received)
			mu.Unlock()
			fmt.Printf("  Received event: prefix=%v index=%v\n", m["prefix"], m["index"])
			if count >= 5 {
				select {
				case <-done:
				default:
					close(done)
				}
			}
		}
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "InvokeCallback error: %v\n", err)
		os.Exit(1)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		fmt.Println("  Timeout waiting for events")
	}

	elapsed := time.Since(start)
	mu.Lock()
	count := len(received)
	mu.Unlock()

	fmt.Printf("\n  Received %d events in %.1fms\n", count, float64(elapsed.Microseconds())/1000.0)
	fmt.Println("  Unsubscribing...")
	unsub()
	fmt.Println("  Unsubscribed!")

	fmt.Println()
	fmt.Println("Test 2: Multiple Concurrent Subscriptions")
	fmt.Println()

	counts := [3]int{}
	var countsMu sync.Mutex
	doneChans := [3]chan struct{}{}
	unsubs := [3]func(){}

	for i := range 3 {
		idx := i
		doneChans[idx] = make(chan struct{})
		u, err := proxy.InvokeCallback("onEvents", []any{fmt.Sprintf("sub-%d", i)}, func(value any) {
			countsMu.Lock()
			counts[idx]++
			c := counts[idx]
			countsMu.Unlock()
			if c >= 5 {
				select {
				case <-doneChans[idx]:
				default:
					close(doneChans[idx])
				}
			}
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "InvokeCallback error for sub-%d: %v\n", i, err)
			continue
		}
		unsubs[idx] = u
	}

	for i := range 3 {
		select {
		case <-doneChans[i]:
		case <-time.After(5 * time.Second):
		}
	}

	for _, u := range unsubs {
		if u != nil {
			u()
		}
	}

	for i := range 3 {
		fmt.Printf("  Subscriber %d received %d events\n", i, counts[i])
	}

	fmt.Println()
	fmt.Println("Test 3: Direct CallWithCallback")
	fmt.Println()

	directCount := 0
	var directMu sync.Mutex
	directDone := make(chan struct{})

	directUnsub, err := rpc.CallWithCallback[any](client, "rpc.events.onEvents", []any{"direct"}, func(value any) {
		directMu.Lock()
		directCount++
		c := directCount
		directMu.Unlock()
		if m, ok := value.(map[string]any); ok && c <= 3 {
			fmt.Printf("  Direct callback received: prefix=%v index=%v\n", m["prefix"], m["index"])
		}
		if c >= 5 {
			select {
			case <-directDone:
			default:
				close(directDone)
			}
		}
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "CallWithCallback error: %v\n", err)
	} else {
		select {
		case <-directDone:
		case <-time.After(5 * time.Second):
		}
		directUnsub()
		fmt.Printf("  Total direct callbacks received: %d\n", directCount)
	}

	fmt.Println()
	if count >= 5 {
		fmt.Println("All assertions passed!")
	} else {
		fmt.Printf("Expected at least 5 events, got %d\n", count)
	}

	fmt.Println()
	fmt.Println("Cleaning up...")
	_ = unsubHandler()
	client.Disconnect()
	server.Disconnect()

	totalElapsed := time.Since(totalStart)
	fmt.Printf("\nAll tests completed! Total time: %.1fms\n", float64(totalElapsed.Microseconds())/1000.0)
}
