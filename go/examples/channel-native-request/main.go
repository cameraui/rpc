package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

var largeBuffer = []byte(strings.Repeat("x", 5*1024*1024)) // 5MB

func main() {
	totalStart := time.Now()
	ctx := context.Background()

	clientA := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "client-a",
	})

	clientB := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "client-b",
	})

	if err := clientA.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "clientA connect: %v\n", err)
		os.Exit(1)
	}
	if err := clientB.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "clientB connect: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Clients connected")
	fmt.Println()

	fmt.Println("=== Testing Public Channel with Native Request/Reply ===")
	fmt.Println()

	channelA, err := clientA.Channel("test-channel")
	if err != nil {
		fatal("channelA", err)
	}
	channelB, err := clientB.Channel("test-channel")
	if err != nil {
		fatal("channelB", err)
	}

	unsubB, err := channelB.OnRequest(func(data []byte) (any, error) {
		var req map[string]any
		rpc.Decode(data, &req)
		fmt.Printf("[Client B] Received request: %s\n", toJSON(req))

		time.Sleep(100 * time.Millisecond)

		question, _ := req["question"].(string)
		result := map[string]any{
			"answer":    strings.ToUpper(question),
			"processed": true,
		}
		fmt.Printf("[Client B] Sending reply: %s\n", toJSON(result))
		return result, nil
	})
	if err != nil {
		fatal("onRequest", err)
	}

	fmt.Println("[Client A] Sending request...")
	reqStart := time.Now()
	respBytes, err := channelA.Request(ctx, map[string]any{"question": "hello world"}, 2*time.Second)
	if err != nil {
		fmt.Printf("[Client A] Request failed: %v\n", err)
	} else {
		reqTime := time.Since(reqStart)
		var resp map[string]any
		rpc.Decode(respBytes, &resp)
		fmt.Printf("[Client A] Got response: %s\n", toJSON(resp))
		fmt.Printf("[Client A] Request took %.1fms\n", float64(reqTime.Microseconds())/1000)
	}
	fmt.Println()

	fmt.Println("=== Testing Private Channel with Native Request/Reply ===")
	fmt.Println()

	privateA, err := clientA.PrivateChannelConnect("private-chat", "client-b")
	if err != nil {
		fatal("privateA", err)
	}
	privateB, err := clientB.PrivateChannelConnect("private-chat", "client-a")
	if err != nil {
		fatal("privateB", err)
	}

	unsubPrivB, err := privateB.OnRequest(func(data []byte) (any, error) {
		var req map[string]any
		rpc.Decode(data, &req)
		fmt.Printf("[Client B Private] Received request: %s\n", toJSON(req))

		reqType, _ := req["type"].(string)
		switch reqType {
		case "calculation":
			a := toFloat(req["a"])
			b := toFloat(req["b"])
			return map[string]any{"result": a + b}, nil
		case "error-test":
			return nil, fmt.Errorf("simulated error")
		}

		return map[string]any{"echo": req}, nil
	})
	if err != nil {
		fatal("privateB onRequest", err)
	}

	fmt.Println("[Client A Private] Sending calculation request...")
	calcStart := time.Now()
	calcResp, err := privateA.Request(ctx, map[string]any{"type": "calculation", "a": 5, "b": 3})
	if err != nil {
		fmt.Printf("[Client A Private] Request failed: %v\n", err)
	} else {
		calcTime := time.Since(calcStart)
		var result map[string]any
		rpc.Decode(calcResp, &result)
		fmt.Printf("[Client A Private] Got result: %s\n", toJSON(result))
		fmt.Printf("[Client A Private] Calculation request took %.1fms\n", float64(calcTime.Microseconds())/1000)
	}
	fmt.Println()

	fmt.Println("[Client A Private] Sending error test request...")
	_, err = privateA.Request(ctx, map[string]any{"type": "error-test"})
	if err != nil {
		fmt.Printf("[Client A Private] Got expected error: %v\n", err)
	}
	fmt.Println()

	fmt.Println("=== Testing Concurrent Channel Requests ===")
	fmt.Println()

	unsubB()

	var requestCount int
	var countMu sync.Mutex
	unsubConcurrent, err := channelB.OnRequest(func(data []byte) (any, error) {
		countMu.Lock()
		requestCount++
		reqNum := requestCount
		countMu.Unlock()

		fmt.Printf("[Client B] Processing request %d...\n", reqNum)

		delay := time.Duration(rand.IntN(200)) * time.Millisecond
		time.Sleep(delay)

		var req map[string]any
		rpc.Decode(data, &req)

		return map[string]any{
			"original":       req,
			"requestNumber":  reqNum,
			"processingTime": fmt.Sprintf("%dms", delay.Milliseconds()),
		}, nil
	})
	if err != nil {
		fatal("concurrent onRequest", err)
	}

	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := channelA.Request(ctx, map[string]any{"index": idx, "timestamp": time.Now().UnixMilli()})
			if err != nil {
				fmt.Printf("[Client A] Request %d failed: %v\n", idx, err)
				return
			}
			var result map[string]any
			rpc.Decode(resp, &result)
			fmt.Printf("[Client A] Response for request %d: %s\n", idx, toJSON(result))
		}(i)
	}
	wg.Wait()
	fmt.Println()
	fmt.Println("All concurrent requests completed")
	fmt.Println()

	fmt.Println("=== Testing Channel Request/Reply with Large Data ===")
	fmt.Println()

	unsubPrivB()

	unsubLarge, err := privateB.OnRequest(func(data []byte) (any, error) {
		var req map[string]any
		rpc.Decode(data, &req)

		reqType, _ := req["type"].(string)
		if reqType == "large-data" {
			fmt.Println("[Client B] Processing large data request...")
			fmt.Printf("[Client B] Sending large response (%.1fMB)\n", float64(len(largeBuffer))/1024/1024)

			message, _ := req["message"].(string)
			return map[string]any{
				"echo": message,
				"data": largeBuffer,
				"size": len(largeBuffer),
			}, nil
		}
		return nil, nil
	})
	if err != nil {
		fatal("large onRequest", err)
	}

	fmt.Println("[Client A] Sending large data request...")
	largeStart := time.Now()
	largeResp, err := privateA.Request(ctx, map[string]any{
		"type":    "large-data",
		"message": "Please send me large data",
	}, 10*time.Second)
	if err != nil {
		fmt.Printf("[Client A] Large data request failed: %v\n", err)
	} else {
		largeTime := time.Since(largeStart)
		var result map[string]any
		rpc.Decode(largeResp, &result)
		dataBytes, _ := result["data"].([]byte)
		fmt.Printf("[Client A] Got large response: size=%.1fMB, time=%.1fms\n",
			float64(len(dataBytes))/1024/1024, float64(largeTime.Microseconds())/1000)
		fmt.Printf("[Client A] Throughput: %.2f MB/s\n", 5.0/largeTime.Seconds())
	}
	fmt.Println()

	fmt.Println("=== Testing Mixed Messages and Requests ===")
	fmt.Println()

	channelB.OnMessage(func(data any) {
		fmt.Printf("[Client B] Regular message: %s\n", toJSON(data))
	})

	channelA.Send(map[string]any{"type": "regular", "content": "This is a regular message"})

	mixResp, err := channelA.Request(ctx, map[string]any{"type": "request", "content": "This is a request"})
	if err != nil {
		fmt.Printf("[Client A] Request failed: %v\n", err)
	} else {
		var result map[string]any
		rpc.Decode(mixResp, &result)
		fmt.Printf("[Client A] Request response: %s\n", toJSON(result))
	}
	fmt.Println()

	unsubB()
	unsubPrivB()
	unsubConcurrent()
	unsubLarge()

	channelA.Close()
	channelB.Close()
	privateA.Close()
	privateB.Close()
	clientA.Disconnect()
	clientB.Disconnect()

	fmt.Printf("Test completed. Total time: %.1fms\n", float64(time.Since(totalStart).Microseconds())/1000)
}

func fatal(label string, err error) {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", label, err)
	os.Exit(1)
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
