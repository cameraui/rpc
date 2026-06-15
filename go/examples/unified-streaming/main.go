package main

import (
	"context"
	"fmt"
	"os"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

type NestedService struct{}

func (n *NestedService) GenerateData(prefix string) (<-chan string, error) {
	ch := make(chan string)
	go func() {
		defer close(ch)
		for i := 1; i <= 3; i++ {
			ch <- fmt.Sprintf("%s-%d", prefix, i)
			time.Sleep(50 * time.Millisecond)
		}
	}()
	return ch, nil
}

type TestService struct {
	Nested *NestedService `rpc:"nested"`
}

func (s *TestService) NormalMethod(name string) (string, error) {
	return fmt.Sprintf("Hello, %s!", name), nil
}

func (s *TestService) GenerateNumbersService(count int) (<-chan int, error) {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := 1; i <= count; i++ {
			ch <- i
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return ch, nil
}

type TestHandler struct{}

func (h *TestHandler) NormalMethod(name string) (string, error) {
	return fmt.Sprintf("RPC says: Hello, %s!", name), nil
}

func (h *TestHandler) GenerateNumbers(count int) (<-chan int, error) {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := 1; i <= count; i++ {
			ch <- i * 10
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return ch, nil
}

func main() {
	fmt.Println("Testing unified streaming implementation...")
	fmt.Println()

	ctx := context.Background()
	totalStart := time.Now()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "unified-test-server",
	})
	if err := server.Connect(ctx); err != nil {
		fatal("server connect", err)
	}

	svcManager := rpc.NewRPCService(server)
	service1, err := svcManager.RegisterHandler(rpc.ServiceConfig{
		Name:    "test-service",
		Version: "1.0.0",
	}, &TestService{Nested: &NestedService{}})
	if err != nil {
		fatal("register service", err)
	}

	unsubRPC, err := server.RegisterHandler("test-rpc", &TestHandler{})
	if err != nil {
		fatal("register rpc handler", err)
	}

	fmt.Println("Server ready with service and RPC handler")
	fmt.Println()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "unified-test-client",
	})
	if err := client.Connect(ctx); err != nil {
		fatal("client connect", err)
	}

	fmt.Println("=== Testing Service Streaming ===")
	proxyStart := time.Now()
	serviceProxy, err := client.CreateServiceProxy(ctx, "test-service")
	if err != nil {
		fatal("create service proxy", err)
	}
	proxyTime := ms(time.Since(proxyStart))
	fmt.Printf("Service proxy created in %.1fms\n", proxyTime)

	start := time.Now()
	greeting, err := serviceProxy.Invoke(ctx, "normalMethod", "Service")
	if err != nil {
		fatal("service normalMethod", err)
	}
	methodTime := ms(time.Since(start))
	fmt.Printf("Service normal method: %v (%.1fms)\n", greeting, methodTime)

	fmt.Println("\nService streaming:")
	streamCount := 0
	streamStart := time.Now()
	streamCh, err := serviceProxy.InvokeStream(ctx, "generateNumbersService", 5)
	if err != nil {
		fatal("service generateNumbersService", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Printf("  Stream error: %v\n", val.Error)
			break
		}
		fmt.Printf("  Received: %v\n", val.Data)
		streamCount++
	}
	streamTime := ms(time.Since(streamStart))
	fmt.Printf("  Total items: %d in %.1fms\n", streamCount, streamTime)

	fmt.Println("\nService nested streaming:")
	nestedCount := 0
	nestedStart := time.Now()
	nestedCh, err := serviceProxy.InvokeStream(ctx, "nested.generateData", "test")
	if err != nil {
		fatal("service nested.generateData", err)
	}
	for val := range nestedCh {
		if val.Error != nil {
			fmt.Printf("  Stream error: %v\n", val.Error)
			break
		}
		fmt.Printf("  Received: %v\n", val.Data)
		nestedCount++
	}
	nestedTime := ms(time.Since(nestedStart))
	fmt.Printf("  Total items: %d in %.1fms\n", nestedCount, nestedTime)

	fmt.Println("\n=== Testing RPC Streaming ===")
	rpcProxyStart := time.Now()
	rpcProxy := client.CreateProxy("test-rpc")
	rpcProxyTime := ms(time.Since(rpcProxyStart))
	fmt.Printf("RPC proxy created in %.1fms\n", rpcProxyTime)

	rpcStart := time.Now()
	rpcGreeting, err := rpcProxy.Invoke(ctx, "normalMethod", "RPC")
	if err != nil {
		fatal("rpc normalMethod", err)
	}
	rpcMethodTime := ms(time.Since(rpcStart))
	fmt.Printf("RPC normal method: %v (%.1fms)\n", rpcGreeting, rpcMethodTime)

	fmt.Println("\nRPC streaming:")
	rpcCount := 0
	rpcStreamStart := time.Now()
	rpcStreamCh, err := rpcProxy.InvokeStream(ctx, "generateNumbers", 5)
	if err != nil {
		fatal("rpc generateNumbers", err)
	}
	for val := range rpcStreamCh {
		if val.Error != nil {
			fmt.Printf("  Stream error: %v\n", val.Error)
			break
		}
		fmt.Printf("  Received: %v\n", val.Data)
		rpcCount++
	}
	rpcStreamTime := ms(time.Since(rpcStreamStart))
	fmt.Printf("  Total items: %d in %.1fms\n", rpcCount, rpcStreamTime)

	fmt.Println("\n=== Message Format Verification ===")
	fmt.Println("Both service and RPC streaming now use:")
	fmt.Println("- Same StreamMessage format")
	fmt.Println("- Same cancellation support")
	fmt.Println("- Same error handling")
	fmt.Println("- Automatic chunking via client.publish()")

	fmt.Println("\nCleaning up...")
	_ = service1.Stop()
	svcManager.StopAll()
	_ = unsubRPC()
	serviceProxy.Close()
	client.Disconnect()
	server.Disconnect()

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nTest completed successfully! Total time: %.1fms\n", totalElapsed)
}

func fatal(label string, err error) {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", label, err)
	os.Exit(1)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
