package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

var buffer2MB = makePattern(2 * 1024 * 1024)

func makePattern(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i % 256)
	}
	return b
}

type MathService struct{}

func (s *MathService) Add(a, b int) (int, error) {
	return a + b, nil
}

func (s *MathService) Multiply(a, b int) (int, error) {
	return a * b, nil
}

func (s *MathService) Divide(a, b float64) (float64, error) {
	if b == 0 {
		return 0, fmt.Errorf("division by zero")
	}
	return a / b, nil
}

type StringService struct{}

func (s *StringService) Concat(a, b string) (string, error) {
	return a + b, nil
}

func (s *StringService) Reverse(str string) (string, error) {
	runes := []rune(str)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes), nil
}

func (s *StringService) GenerateWords(count int) (<-chan string, error) {
	words := []string{"hello", "world", "nats", "rpc", "service"}
	ch := make(chan string)
	go func() {
		defer close(ch)
		for i := range count {
			ch <- words[i%len(words)]
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return ch, nil
}

type DataService struct {
	Info *InfoNamespace `rpc:"info"`
}

func (s *DataService) CreateBuffer(sizeMB int) ([]byte, error) {
	if sizeMB == 2 {
		return buffer2MB, nil
	}
	return makePattern(sizeMB * 1024 * 1024), nil
}

type InfoNamespace struct{}

func (n *InfoNamespace) Version() (string, error) {
	return "1.0.0", nil
}

func (n *InfoNamespace) Status() (map[string]any, error) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return map[string]any{
		"healthy": true,
		"uptime":  time.Since(startupTime).Seconds(),
		"memory": map[string]any{
			"heapUsed": m.HeapAlloc,
		},
	}, nil
}

var startupTime = time.Now()

func main() {
	totalStart := time.Now()
	fmt.Println("Starting multi-service test...")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "multi-service-server",
	})

	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "server connect: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Server connected")

	svcManager := rpc.NewRPCService(server)

	service1, err := svcManager.RegisterHandler(rpc.ServiceConfig{
		Name:        "math",
		Version:     "1.0.0",
		Description: "Mathematical operations service",
	}, &MathService{})
	if err != nil {
		fatal("register math", err)
	}

	service2, err := svcManager.RegisterHandler(rpc.ServiceConfig{
		Name:        "string",
		Version:     "1.0.0",
		Description: "String manipulation service",
	}, &StringService{})
	if err != nil {
		fatal("register string", err)
	}

	service3, err := svcManager.RegisterHandler(rpc.ServiceConfig{
		Name:        "data",
		Version:     "2.0.0",
		Description: "Data generation and info service",
	}, &DataService{Info: &InfoNamespace{}})
	if err != nil {
		fatal("register data", err)
	}

	fmt.Println()
	fmt.Println("Registered services:")
	for _, info := range svcManager.GetAllInfo() {
		fmt.Printf("- %s v%s: %s\n", info.Name, info.Version, info.Description)
	}

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "test-client",
	})

	if err := client.Connect(ctx); err != nil {
		fatal("client connect", err)
	}
	fmt.Println()
	fmt.Println("Client connected")

	fmt.Println()
	fmt.Println("--- Service Discovery ---")
	monitor := svcManager.Monitor()

	infos, err := monitor.Info(ctx, "")
	if err != nil {
		fatal("monitor info", err)
	}
	for _, info := range infos {
		fmt.Printf("Found: %s v%s (%s)\n", info.Name, info.Version, info.ID)
	}

	fmt.Println()
	fmt.Println("--- Math Service ---")
	mathStart := time.Now()
	mathProxy, err := client.CreateServiceProxy(ctx, "math")
	if err != nil {
		fatal("create math proxy", err)
	}
	mathProxyTime := ms(time.Since(mathStart))
	fmt.Printf("Math proxy created in %.1fms\n", mathProxyTime)

	start := time.Now()
	result, err := mathProxy.Invoke(ctx, "add", 5, 3)
	if err != nil {
		fatal("add", err)
	}
	fmt.Printf("5 + 3 = %v (%.1fms)\n", result, ms(time.Since(start)))

	start = time.Now()
	result, err = mathProxy.Invoke(ctx, "multiply", 4, 7)
	if err != nil {
		fatal("multiply", err)
	}
	fmt.Printf("4 * 7 = %v (%.1fms)\n", result, ms(time.Since(start)))

	start = time.Now()
	result, err = mathProxy.Invoke(ctx, "divide", 10, 2)
	if err != nil {
		fatal("divide", err)
	}
	fmt.Printf("10 / 2 = %v (%.1fms)\n", result, ms(time.Since(start)))

	fmt.Println()
	fmt.Println("--- String Service ---")
	stringStart := time.Now()
	stringProxy, err := client.CreateServiceProxy(ctx, "string")
	if err != nil {
		fatal("create string proxy", err)
	}
	stringProxyTime := ms(time.Since(stringStart))
	fmt.Printf("String proxy created in %.1fms\n", stringProxyTime)

	start = time.Now()
	result, err = stringProxy.Invoke(ctx, "concat", "Hello", "World")
	if err != nil {
		fatal("concat", err)
	}
	fmt.Printf("concat(\"Hello\", \"World\") = %v (%.1fms)\n", result, ms(time.Since(start)))

	start = time.Now()
	result, err = stringProxy.Invoke(ctx, "reverse", "NATS")
	if err != nil {
		fatal("reverse", err)
	}
	fmt.Printf("reverse(\"NATS\") = %v (%.1fms)\n", result, ms(time.Since(start)))

	fmt.Println()
	fmt.Println("Streaming words:")
	streamStart := time.Now()
	streamCh, err := stringProxy.InvokeStream(ctx, "generateWords", 5)
	if err != nil {
		fatal("generateWords", err)
	}
	wordCount := 0
	for val := range streamCh {
		if val.Error != nil {
			fmt.Fprintf(os.Stderr, "stream error: %v\n", val.Error)
			break
		}
		fmt.Printf(" - %v\n", val.Data)
		wordCount++
	}
	streamTime := ms(time.Since(streamStart))
	fmt.Printf("Streamed %d words in %.1fms\n", wordCount, streamTime)

	fmt.Println()
	fmt.Println("--- Data Service ---")
	dataStart := time.Now()
	dataProxy, err := client.CreateServiceProxy(ctx, "data")
	if err != nil {
		fatal("create data proxy", err)
	}
	dataProxyTime := ms(time.Since(dataStart))
	fmt.Printf("Data proxy created in %.1fms\n", dataProxyTime)

	start = time.Now()
	bufResult, err := dataProxy.Invoke(ctx, "createBuffer", 2)
	if err != nil {
		fatal("createBuffer", err)
	}
	bufferTime := ms(time.Since(start))
	bufBytes := toBytes(bufResult)
	bufMB := float64(len(bufBytes)) / 1024 / 1024
	fmt.Printf("Generated buffer: %.2fMB in %.1fms (%.1f MB/s)\n",
		bufMB, bufferTime, bufMB/(bufferTime/1000))

	start = time.Now()
	infoProxy := dataProxy.Sub("info")
	version, err := infoProxy.Invoke(ctx, "version")
	if err != nil {
		fatal("version", err)
	}
	fmt.Printf("Service version: %v (%.1fms)\n", version, ms(time.Since(start)))

	status, err := infoProxy.Invoke(ctx, "status")
	if err != nil {
		fatal("status", err)
	}
	statusMap, _ := status.(map[string]any)
	uptime := toFloat(statusMap["uptime"])
	memMap, _ := statusMap["memory"].(map[string]any)
	heapUsed := toFloat(memMap["heapUsed"])
	fmt.Printf("Service status: {healthy: %v, uptime: %.2fs, memory: %.2fMB}\n",
		statusMap["healthy"], uptime, heapUsed/1024/1024)

	fmt.Println()
	fmt.Println("--- All Services Stats ---")
	allStats := svcManager.GetAllStats()
	for _, stats := range allStats {
		fmt.Printf("\n%s (%s):\n", stats.Name, stats.ID)
		fmt.Printf("  Started: %s\n", stats.Started)
		fmt.Printf("  Total endpoints: %d\n", len(stats.Endpoints))
		totalRequests := 0
		for _, ep := range stats.Endpoints {
			totalRequests += ep.NumRequests
		}
		fmt.Printf("  Total requests: %d\n", totalRequests)
	}

	fmt.Println()
	fmt.Println("--- Stopping string service ---")
	if err := svcManager.Stop("string"); err != nil {
		fmt.Printf("stop string: %v\n", err)
	}

	fmt.Println("Remaining services:")
	for _, info := range svcManager.GetAllInfo() {
		fmt.Printf("- %s v%s\n", info.Name, info.Version)
	}

	fmt.Println()
	fmt.Println("Trying to use stopped string service...")
	_, err = stringProxy.Invoke(ctx, "reverse", "test")
	if err != nil {
		fmt.Printf("Expected error: %v\n", err)
	}

	fmt.Println()
	fmt.Println("Cleaning up...")
	_ = service1.Stop()
	_ = service2.Stop()
	_ = service3.Stop()
	time.Sleep(1 * time.Second)
	svcManager.StopAll()
	client.Disconnect()
	server.Disconnect()

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nAll tests completed! Total time: %.1fms\n", totalElapsed)
}

func fatal(label string, err error) {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", label, err)
	os.Exit(1)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func toBytes(val any) []byte {
	if b, ok := val.([]byte); ok {
		return b
	}
	data, _ := rpc.Encode(val)
	return data
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	default:
		return 0
	}
}
