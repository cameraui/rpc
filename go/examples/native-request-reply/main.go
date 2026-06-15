package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

func main() {
	totalStart := time.Now()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "server",
	})

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "client",
	})

	if err := server.Connect(ctx); err != nil {
		fatal("server connect", err)
	}
	if err := client.Connect(ctx); err != nil {
		fatal("client connect", err)
	}

	fmt.Println("Connected to NATS")
	fmt.Println()

	fmt.Println("Testing Native NATS Request/Reply")
	fmt.Println()

	unsubEcho, err := server.OnRequest("echo", func(data []byte) (any, error) {
		var req map[string]any
		rpc.Decode(data, &req)
		fmt.Printf("[Server] Received echo request: %s\n", toJSON(req))
		return map[string]any{"echo": req, "timestamp": time.Now().UnixMilli()}, nil
	})
	if err != nil {
		fatal("onRequest echo", err)
	}

	fmt.Println("[Client] Sending echo request...")
	start := time.Now()
	resp, err := client.Request(ctx, "echo", map[string]any{"message": "Hello NATS!"})
	if err != nil {
		fmt.Printf("[Client] Request failed: %v\n", err)
	} else {
		elapsed := ms(time.Since(start))
		var response map[string]any
		rpc.Decode(resp, &response)
		fmt.Printf("[Client] Got response: %s (%.1fms)\n\n", toJSON(response), elapsed)
	}

	fmt.Println("Testing Math Service")
	fmt.Println()

	unsubMath, err := server.OnRequest("math.*", func(data []byte) (any, error) {
		var req map[string]any
		rpc.Decode(data, &req)
		operation, _ := req["operation"].(string)
		fmt.Printf("[Server] Math operation: %s, data: %s\n", operation, toJSON(req))

		a := toFloat(req["a"])
		b := toFloat(req["b"])

		switch operation {
		case "add":
			return map[string]any{"result": a + b}, nil
		case "multiply":
			return map[string]any{"result": a * b}, nil
		case "divide":
			if b == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return map[string]any{"result": a / b}, nil
		default:
			return nil, fmt.Errorf("unknown operation: %s", operation)
		}
	})
	if err != nil {
		fatal("onRequest math", err)
	}

	mathTests := []struct {
		subject string
		data    map[string]any
	}{
		{"math.add", map[string]any{"a": 5, "b": 3, "operation": "add"}},
		{"math.multiply", map[string]any{"a": 4, "b": 7, "operation": "multiply"}},
		{"math.divide", map[string]any{"a": 20, "b": 4, "operation": "divide"}},
		{"math.divide", map[string]any{"a": 10, "b": 0, "operation": "divide"}},
	}

	for _, test := range mathTests {
		fmt.Printf("[Client] Requesting %s with %s\n", test.subject, toJSON(test.data))
		start = time.Now()
		resp, err := client.Request(ctx, test.subject, test.data)
		elapsed := ms(time.Since(start))
		if err != nil {
			fmt.Printf("[Client] Error: %v\n", err)
		} else {
			var result map[string]any
			rpc.Decode(resp, &result)
			fmt.Printf("[Client] Result: %s (%.1fms)\n", toJSON(result), elapsed)
		}
	}

	fmt.Println()
	fmt.Println("Testing Large Data Request/Reply")
	fmt.Println()

	unsubData, err := server.OnRequest("data.large", func(data []byte) (any, error) {
		var req map[string]any
		rpc.Decode(data, &req)
		sizeMB := toFloat(req["size"])
		sizeBytes := int(sizeMB * 1024 * 1024)
		fmt.Printf("[Server] Got request for %.1fMB of data\n", sizeMB)
		buffer := []byte(strings.Repeat("x", sizeBytes))
		return map[string]any{
			"data":     buffer,
			"size":     len(buffer),
			"checksum": len(buffer),
		}, nil
	})
	if err != nil {
		fatal("onRequest data", err)
	}

	fmt.Println("[Client] Requesting 500KB of data...")
	start = time.Now()
	resp, err = client.Request(ctx, "data.large", map[string]any{"size": 0.5}, 10*time.Second)
	if err != nil {
		fmt.Printf("[Client] Large data request failed: %v\n\n", err)
	} else {
		elapsed := ms(time.Since(start))
		var result map[string]any
		rpc.Decode(resp, &result)
		size := toFloat(result["size"])
		throughput := (size / 1024 / 1024) / (elapsed / 1000)
		fmt.Printf("[Client] Got %.1fKB in %.1fms (%.1f MB/s)\n\n", size/1024, elapsed, throughput)
	}

	fmt.Println("Testing Concurrent Requests")
	fmt.Println()

	var requestCount atomic.Int64

	unsubConcurrent, err := server.OnRequest("concurrent.test", func(data []byte) (any, error) {
		id := requestCount.Add(1)
		delay := time.Duration(rand.IntN(200)) * time.Millisecond
		fmt.Printf("[Server] Processing request %d, delay: %dms\n", id, delay.Milliseconds())

		time.Sleep(delay)

		var req map[string]any
		rpc.Decode(data, &req)

		return map[string]any{
			"requestId":      id,
			"input":          req,
			"processingTime": fmt.Sprintf("%dms", delay.Milliseconds()),
		}, nil
	})
	if err != nil {
		fatal("onRequest concurrent", err)
	}

	fmt.Println("[Client] Sending 5 concurrent requests...")
	concurrentStart := time.Now()
	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			start := time.Now()
			resp, err := client.Request(ctx, "concurrent.test", map[string]any{"index": idx})
			elapsed := ms(time.Since(start))
			if err != nil {
				fmt.Printf("[Client] Response %d: error: %v\n", idx, err)
				return
			}
			var result map[string]any
			rpc.Decode(resp, &result)
			fmt.Printf("[Client] Response %d: %s (%.1fms)\n", idx, toJSON(result), elapsed)
		}(i)
	}
	wg.Wait()
	concurrentElapsed := ms(time.Since(concurrentStart))
	fmt.Printf("\nAll concurrent requests completed in %.1fms!\n\n", concurrentElapsed)

	fmt.Println("Testing No Responders")
	fmt.Println()

	fmt.Println("[Client] Requesting non-existent service...")
	noRespStart := time.Now()
	_, err = client.Request(ctx, "does.not.exist", map[string]any{"test": true}, 1*time.Second)
	if err != nil {
		noRespElapsed := ms(time.Since(noRespStart))
		fmt.Printf("[Client] Expected error: %v (%.1fms)\n\n", err, noRespElapsed)
	}

	fmt.Println("Testing Timeout")
	fmt.Println()

	unsubSlow, err := server.OnRequest("slow.service", func(data []byte) (any, error) {
		fmt.Println("[Server] Slow service - waiting 3 seconds...")
		time.Sleep(3 * time.Second)
		return map[string]any{"done": true}, nil
	})
	if err != nil {
		fatal("onRequest slow", err)
	}

	fmt.Println("[Client] Requesting slow service with 1s timeout...")
	timeoutStart := time.Now()
	_, err = client.Request(ctx, "slow.service", map[string]any{"test": true}, 1*time.Second)
	if err != nil {
		timeoutElapsed := ms(time.Since(timeoutStart))
		fmt.Printf("[Client] Expected timeout: %v (%.1fms)\n\n", err, timeoutElapsed)
	}

	unsubEcho()
	unsubMath()
	unsubData()
	unsubConcurrent()
	unsubSlow()

	client.Disconnect()
	server.Disconnect()

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("All tests completed! Total time: %.1fms\n", totalElapsed)
}

func fatal(label string, err error) {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", label, err)
	os.Exit(1)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func toJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int8:
		return float64(n)
	case int16:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case uint8:
		return float64(n)
	case uint16:
		return float64(n)
	case uint32:
		return float64(n)
	case uint64:
		return float64(n)
	default:
		return 0
	}
}
