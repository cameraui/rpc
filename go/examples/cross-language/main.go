package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

func calculateChecksum(data []byte) uint32 {
	var checksum uint32
	for _, b := range data {
		checksum = (checksum + uint32(b)) % 0xFFFFFFFF
	}
	return checksum
}

func parseTargets(args []string) []string {
	for i, arg := range args {
		if arg == "--targets" && i+1 < len(args) {
			return strings.Split(args[i+1], ",")
		}
		if after, ok := strings.CutPrefix(arg, "--targets="); ok {
			return strings.Split(after, ",")
		}
	}
	return []string{"python-service", "node-service"}
}

func infoMethodForTarget(target string) string {
	switch target {
	case "python-service":
		return "getPythonInfo"
	case "node-service":
		return "getNodeInfo"
	case "go-service":
		return "getGoInfo"
	default:
		return "getInfo"
	}
}

func main() {
	targets := parseTargets(os.Args[1:])

	fmt.Println("Go RPC Server Starting...")
	fmt.Printf("   Targets: %v\n", targets)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "go-server-unique",
	})

	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "connect error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Go server connected")

	handlers := map[string]any{
		"name": "go-service",
	}

	handlers["greet"] = func(name string) (string, error) {
		fmt.Printf("Received greet request for: %s\n", name)
		return fmt.Sprintf("Hello %s from Go!", name), nil
	}

	handlers["calculate"] = func(a, b float64, operation string) (any, error) {
		fmt.Printf("Calculate: %g %s %g\n", a, operation, b)
		switch operation {
		case "add":
			return a + b, nil
		case "subtract":
			return a - b, nil
		case "multiply":
			return a * b, nil
		case "divide":
			if b != 0 {
				return a / b, nil
			}
			return "Error: Division by zero", nil
		default:
			return "Unknown operation", nil
		}
	}

	handlers["getGoInfo"] = func() (map[string]any, error) {
		fmt.Println("Returning Go info")
		return map[string]any{
			"platform":  "Go",
			"version":   runtime.Version(),
			"timestamp": time.Now().Format(time.RFC3339),
			"pid":       os.Getpid(),
		}, nil
	}

	handlers["echoData"] = func(data any) (any, error) {
		fmt.Printf("Echoing data: %v\n", truncate(data))
		return data, nil
	}

	handlers["raiseError"] = func(message string) (any, error) {
		fmt.Printf("Raising error: %s\n", message)
		return nil, errors.New(message)
	}

	handlers["getLargeData"] = func() (map[string]any, error) {
		fmt.Println("Creating 20MB test data...")
		size := 20 * 1024 * 1024
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}
		fmt.Println("Sending 20MB data to test auto-chunking...")
		return map[string]any{
			"type":     "large-data",
			"size":     size,
			"data":     data,
			"checksum": calculateChecksum(data),
		}, nil
	}

	handlers["verifyLargeData"] = func(payload map[string]any) (map[string]any, error) {
		data := toBytes(payload["data"])
		fmt.Printf("Verifying received data: %.2fMB\n", float64(len(data))/1024/1024)

		checksum := calculateChecksum(data)
		expectedChecksum := toUint32(payload["checksum"])
		expectedSize := toInt(payload["size"])
		valid := checksum == expectedChecksum && len(data) == expectedSize

		if valid {
			fmt.Println("Verification: PASSED")
		} else {
			fmt.Println("Verification: FAILED")
		}
		return map[string]any{
			"valid":         valid,
			"receivedSize":  len(data),
			"checksumMatch": checksum == expectedChecksum,
		}, nil
	}

	handlers["onStatusUpdates"] = func(prefix string, callback func(map[string]any)) (func(), error) {
		fmt.Printf("New callback subscriber for prefix '%s'\n", prefix)
		for i := range 3 {
			callback(map[string]any{
				"source": "go",
				"prefix": prefix,
				"index":  i,
				"time":   time.Now().Format(time.RFC3339),
			})
			time.Sleep(50 * time.Millisecond)
		}
		return func() {
			fmt.Printf("Callback cleanup for prefix '%s'\n", prefix)
		}, nil
	}

	unsub, err := server.RegisterHandler("go-service", handlers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "register handler error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Go handlers registered")

	channel, err := server.Channel("cross-language-chat")
	if err != nil {
		fmt.Fprintf(os.Stderr, "channel error: %v\n", err)
		os.Exit(1)
	}

	channel.OnMessage(func(data any) {
		fmt.Printf("Received: %v\n", truncate(data))

		msg, ok := data.(map[string]any)
		if !ok {
			return
		}

		from, _ := msg["from"].(string)
		msgType, _ := msg["type"].(string)

		// Only respond to initial messages from other services, not responses
		if from != "go" && msgType != "response" {
			channel.Send(map[string]any{
				"from":     "go",
				"type":     "response",
				"original": msg,
				"message":  fmt.Sprintf("Go received: %q", msg["message"]),
			})
		}
	})
	fmt.Println("Go channel ready")

	// Wait for other services to set up
	time.Sleep(3 * time.Second)

	fmt.Printf("\nGo calling target services...\n\n")

	failures := 0
	for _, target := range targets {
		fmt.Printf("--- Calling %s ---\n", target)
		proxy := server.CreateProxy(target)

		serviceName, err := proxy.Invoke(ctx, "name")
		if err != nil {
			fmt.Printf("Error getting %s name: %v\n", target, err)
			failures++
			continue
		}
		fmt.Printf("%s service name: %v\n", target, serviceName)

		greeting, err := proxy.Invoke(ctx, "greet", "Go")
		if err != nil {
			fmt.Printf("Error calling %s greet: %v\n", target, err)
			failures++
			continue
		}
		fmt.Printf("%s greeting: %v\n", target, greeting)

		product, err := proxy.Invoke(ctx, "calculate", 6, 7, "multiply")
		if err != nil {
			fmt.Printf("Error calling %s calculate: %v\n", target, err)
			failures++
			continue
		}
		fmt.Printf("%s calculation (6 * 7): %v\n", target, product)

		infoMethod := infoMethodForTarget(target)
		info, err := proxy.Invoke(ctx, infoMethod)
		if err != nil {
			fmt.Printf("Error calling %s %s: %v\n", target, infoMethod, err)
			failures++
			continue
		}
		fmt.Printf("%s info: %v\n", target, info)

		complexData := map[string]any{
			"numbers": []int{10, 20, 30, 40, 50},
			"nested":  map[string]any{"hello": "world", "answer": 42},
			"binary":  []byte("Hello from Go"),
			"unicode": "你好世界 🌍",
		}
		echoed, err := proxy.Invoke(ctx, "echoData", complexData)
		if err != nil {
			fmt.Printf("Error calling %s echoData: %v\n", target, err)
			failures++
			continue
		}
		echoMap, _ := echoed.(map[string]any)
		nested, _ := echoMap["nested"].(map[string]any)
		valid := nested["hello"] == "world" && echoMap["unicode"] == "你好世界 🌍"
		fmt.Printf("%s echoed complex data correctly: %v\n", target, valid)

		channel.Send(map[string]any{
			"from":    "go",
			"type":    "greeting",
			"message": fmt.Sprintf("Hello %s, this is Go speaking!", target),
		})

		fmt.Printf("\nTesting 20MB data transfer with %s...\n", target)

		largeData, err := proxy.Invoke(ctx, "getLargeData")
		if err != nil {
			fmt.Printf("Error calling %s getLargeData: %v\n", target, err)
			failures++
			continue
		}
		largeMap, _ := largeData.(map[string]any)
		receivedData := toBytes(largeMap["data"])
		fmt.Printf("Received %.2fMB from %s\n", float64(len(receivedData))/1024/1024, target)

		checksum := calculateChecksum(receivedData)
		expectedChecksum := toUint32(largeMap["checksum"])
		if checksum == expectedChecksum {
			fmt.Println("Data integrity check: PASSED")
		} else {
			fmt.Println("Data integrity check: FAILED")
			failures++
		}

		testData := make([]byte, 20*1024*1024)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		verifyResult, err := proxy.Invoke(ctx, "verifyLargeData", map[string]any{
			"type":     "go-large-data",
			"size":     len(testData),
			"data":     testData,
			"checksum": calculateChecksum(testData),
		})
		if err != nil {
			fmt.Printf("Error calling %s verifyLargeData: %v\n", target, err)
			failures++
			continue
		}
		verifyMap, _ := verifyResult.(map[string]any)
		if verifyMap["valid"] == true {
			fmt.Printf("%s verification of our 20MB data: PASSED\n", target)
		} else {
			fmt.Printf("%s verification of our 20MB data: FAILED\n", target)
			failures++
		}

		fmt.Printf("Testing callback subscription with %s...\n", target)
		cbCount := 0
		cbDone := make(chan struct{})
		cbUnsub, err := proxy.InvokeCallback("onStatusUpdates", []any{fmt.Sprintf("go-to-%s", target)}, func(value any) {
			cbCount++
			if m, ok := value.(map[string]any); ok {
				fmt.Printf("Callback from %s: source=%v index=%v\n", target, m["source"], m["index"])
			}
			if cbCount >= 3 {
				close(cbDone)
			}
		})
		if err != nil {
			fmt.Printf("Error subscribing to %s callbacks: %v\n", target, err)
			failures++
		} else {
			select {
			case <-cbDone:
			case <-time.After(5 * time.Second):
				fmt.Printf("Timeout waiting for callbacks from %s\n", target)
				failures++
			}
			cbUnsub()
			fmt.Printf("Callback subscription test with %s: %d events received\n", target, cbCount)
		}

		// Error propagation: the peer handler returns/raises an error; it must
		// reach us as an error carrying the same message, not a silent success.
		errToken := fmt.Sprintf("boom go->%s", target)
		if _, errPropErr := proxy.Invoke(ctx, "raiseError", errToken); errPropErr == nil {
			fmt.Printf("%s raiseError did NOT propagate an error\n", target)
			failures++
		} else if strings.Contains(errPropErr.Error(), errToken) {
			fmt.Printf("%s error propagation: PASSED\n", target)
		} else {
			fmt.Printf("%s error propagation: wrong message: %v\n", target, errPropErr)
			failures++
		}

		fmt.Println()
	}

	fmt.Println("Go server running. Press Ctrl+C to stop")
	fmt.Println()

	<-ctx.Done()

	fmt.Println("\nShutting down Go server...")
	_ = unsub()
	_ = channel.Close()
	server.Disconnect()

	if failures > 0 {
		fmt.Printf("\nGo cross-language test FAILED (%d error(s))\n", failures)
		os.Exit(1)
	}
	fmt.Println("\nGo cross-language test passed")
}

func toBytes(val any) []byte {
	switch v := val.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		return nil
	}
}

func toUint32(val any) uint32 {
	switch v := val.(type) {
	case uint32:
		return v
	case int:
		return uint32(v)
	case int64:
		return uint32(v)
	case uint64:
		return uint32(v)
	case float64:
		return uint32(v)
	default:
		return 0
	}
}

func toInt(val any) int {
	switch v := val.(type) {
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint:
		return int(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func truncate(data any) string {
	s := fmt.Sprintf("%v", data)
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}
