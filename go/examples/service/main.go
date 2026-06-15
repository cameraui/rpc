package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

var buffer5MB = makePattern(5 * 1024 * 1024)

func makePattern(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i % 256)
	}
	return b
}

type MonitorNamespace struct{}

func (m *MonitorNamespace) Cpu() (map[string]any, error) {
	return map[string]any{"usage": rand.Float64() * 100}, nil
}

func (m *MonitorNamespace) Memory() (map[string]any, error) {
	return map[string]any{
		"used":  rand.Float64() * 16 * 1024,
		"total": 16 * 1024,
	}, nil
}

type ConfigNamespace struct {
	name    *string
	Monitor *MonitorNamespace `rpc:"monitor"`
}

func (c *ConfigNamespace) Get() (map[string]any, error) {
	return map[string]any{
		"name":          *c.name,
		"maxConcurrent": 10,
		"gpu":           true,
	}, nil
}

func (c *ConfigNamespace) Set(config map[string]any) (map[string]any, error) {
	if name, ok := config["name"].(string); ok {
		*c.name = name
	}
	return map[string]any{"success": true}, nil
}

type ComputeService struct {
	name   string
	Config *ConfigNamespace `rpc:"config"`
}

func NewComputeService(name string) *ComputeService {
	svc := &ComputeService{name: name}
	svc.Config = &ConfigNamespace{
		name:    &svc.name,
		Monitor: &MonitorNamespace{},
	}
	return svc
}

func (s *ComputeService) Add(a, b int) (int, error) {
	return a + b, nil
}

func (s *ComputeService) CreateData(sizeMB int) ([]byte, error) {
	if sizeMB == 5 {
		return buffer5MB, nil
	}
	buf := make([]byte, sizeMB*1024*1024)
	for i := range buf {
		buf[i] = byte(i % 256)
	}
	return buf, nil
}

func (s *ComputeService) GenerateNumbers(count int) (<-chan map[string]any, error) {
	ch := make(chan map[string]any)
	go func() {
		defer close(ch)
		for i := range count {
			data := make([]byte, 1024*1024)
			for j := range data {
				data[j] = byte(i % 256)
			}
			ch <- map[string]any{
				"index":     i,
				"data":      data,
				"timestamp": float64(time.Now().UnixMicro()) / 1000,
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return ch, nil
}

func (s *ComputeService) FailingMethod() error {
	return fmt.Errorf("this method always fails")
}

func main() {
	fmt.Println("Starting service test...")
	fmt.Println()

	ctx := context.Background()
	totalStart := time.Now()

	server1 := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "compute-server-1",
	})

	if err := server1.Connect(ctx); err != nil {
		fatal("server1 connect", err)
	}
	fmt.Println("Server 1 connected")

	svcManager1 := rpc.NewRPCService(server1)
	service1, err := svcManager1.RegisterHandler(rpc.ServiceConfig{
		Name:        "compute",
		Version:     "1.0.0",
		Description: "High-performance compute service",
		Queue:       "compute-workers",
		Metadata: map[string]string{
			"host":   "server1",
			"gpu":    "true",
			"region": "us-east",
		},
	}, NewComputeService("GPU Compute Node 1"))
	if err != nil {
		fatal("register service 1", err)
	}
	fmt.Printf("Service 1 registered: %s v%s\n", service1.Info().Name, service1.Info().Version)

	server2 := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "compute-server-2",
	})

	if err := server2.Connect(ctx); err != nil {
		fatal("server2 connect", err)
	}

	svcManager2 := rpc.NewRPCService(server2)
	service2, err := svcManager2.RegisterHandler(rpc.ServiceConfig{
		Name:        "compute",
		Version:     "1.0.0",
		Description: "High-performance compute service",
		Queue:       "compute-workers",
		Metadata: map[string]string{
			"host":   "server2",
			"gpu":    "false",
			"region": "eu-west",
		},
	}, NewComputeService("GPU Compute Node 2"))
	if err != nil {
		fatal("register service 2", err)
	}
	fmt.Printf("Service 2 registered: %s v%s\n", service2.Info().Name, service2.Info().Version)

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "test-client",
	})

	if err := client.Connect(ctx); err != nil {
		fatal("client connect", err)
	}
	fmt.Println("\nClient connected")

	fmt.Println("\n--- Service Discovery ---")
	monitor := svcManager1.Monitor()

	infos, err := monitor.Info(ctx, "compute")
	if err != nil {
		fatal("monitor info", err)
	}
	for _, info := range infos {
		fmt.Printf("Found service: %s\n", info.ID)
		fmt.Printf("  Version: %s\n", info.Version)
		fmt.Printf("  Metadata: %v\n", info.Metadata)
		subjects := make([]string, len(info.Endpoints))
		for i, ep := range info.Endpoints {
			subjects[i] = ep.Subject
		}
		fmt.Printf("  Endpoints: %v\n", subjects)
	}

	fmt.Println("\n--- Testing Service Calls ---")
	proxyStart := time.Now()
	compute, err := client.CreateServiceProxy(ctx, "compute")
	if err != nil {
		fatal("create service proxy", err)
	}
	proxyTime := ms(time.Since(proxyStart))
	fmt.Printf("Service proxy created in %.1fms\n", proxyTime)

	start := time.Now()
	sumResult, err := compute.Invoke(ctx, "add", 5, 3)
	if err != nil {
		fatal("add", err)
	}
	addTime := ms(time.Since(start))
	fmt.Printf("Add result: %v (%.1fms)\n", sumResult, addTime)

	start = time.Now()
	configResult, err := compute.Invoke(ctx, "config.get")
	if err != nil {
		fatal("config.get", err)
	}
	configTime := ms(time.Since(start))
	fmt.Printf("Config: %v (%.1fms)\n", configResult, configTime)

	start = time.Now()
	cpuResult, err := compute.Invoke(ctx, "config.monitor.cpu")
	if err != nil {
		fatal("config.monitor.cpu", err)
	}
	cpuTime := ms(time.Since(start))
	fmt.Printf("CPU usage: %v (%.1fms)\n", cpuResult, cpuTime)

	fmt.Println("\n--- Testing Large Data Transfer ---")
	start = time.Now()
	largeData, err := compute.Invoke(ctx, "createData", 5)
	if err != nil {
		fatal("createData", err)
	}
	dataTime := ms(time.Since(start))
	dataBytes := toBytes(largeData)
	sizeMB := float64(len(dataBytes)) / 1024 / 1024
	throughput := sizeMB / (dataTime / 1000)
	fmt.Printf("Received %.1fMB of data in %.1fms (%.1f MB/s)\n", sizeMB, dataTime, throughput)

	fmt.Println("\n--- Testing Streaming ---")
	streamStart := time.Now()
	streamCh, err := compute.InvokeStream(ctx, "generateNumbers", 5)
	if err != nil {
		fatal("generateNumbers stream", err)
	}
	streamCount := 0
	for val := range streamCh {
		if val.Error != nil {
			fmt.Printf("Stream error: %v\n", val.Error)
			break
		}
		if m, ok := val.Data.(map[string]any); ok {
			chunk := toBytes(m["data"])
			fmt.Printf("Stream item %v: %.2fMB\n", m["index"], float64(len(chunk))/1024/1024)
		}
		streamCount++
	}
	streamTime := ms(time.Since(streamStart))
	fmt.Printf("Received %d stream items in %.1fms\n", streamCount, streamTime)

	fmt.Println("\n--- Testing Error Handling ---")
	start = time.Now()
	_, err = compute.Invoke(ctx, "failingMethod")
	errorTime := ms(time.Since(start))
	if err != nil {
		fmt.Printf("Caught expected error: %v (%.1fms)\n", err, errorTime)
	} else {
		fmt.Println("ERROR: Expected an error but got none!")
	}

	fmt.Println("\n--- Testing Load Balancing ---")
	fmt.Println("Making 10 requests to see distribution...")
	lbStart := time.Now()
	for i := range 10 {
		reqStart := time.Now()
		cfg, err := compute.Invoke(ctx, "config.get")
		reqTime := ms(time.Since(reqStart))
		if err != nil {
			fmt.Printf("Request %d: error: %v\n", i+1, err)
			continue
		}
		if m, ok := cfg.(map[string]any); ok {
			fmt.Printf("Request %d: Handled by %v (%.1fms)\n", i+1, m["name"], reqTime)
		}
	}
	lbTime := ms(time.Since(lbStart))
	fmt.Printf("Total load balancing test time: %.1fms\n", lbTime)

	fmt.Println("\n--- Service Stats ---")
	stats, err := monitor.Stats(ctx, "compute")
	if err != nil {
		fmt.Printf("Stats error: %v\n", err)
	} else {
		for _, stat := range stats {
			fmt.Printf("Stats for %s:\n", stat.ID)
			fmt.Printf("  Started: %s\n", stat.Started)
			fmt.Printf("  Endpoints:\n")
			for _, ep := range stat.Endpoints {
				avgMs := float64(ep.AverageProcessingMs) / 1000000
				fmt.Printf("    %s: requests=%d, errors=%d, avg=%.2fms\n",
					ep.Name, ep.NumRequests, ep.NumErrors, avgMs)
			}
		}
	}

	time.Sleep(500 * time.Millisecond)
	_ = service1.Stop()
	_ = service2.Stop()
	svcManager1.StopAll()
	svcManager2.StopAll()
	compute.Close()
	client.Disconnect()
	server1.Disconnect()
	server2.Disconnect()

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nTest completed! Total time: %.1fms\n", totalElapsed)
}

func toBytes(val any) []byte {
	if b, ok := val.([]byte); ok {
		return b
	}
	data, _ := rpc.Encode(val)
	return data
}

func fatal(label string, err error) {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", label, err)
	os.Exit(1)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
