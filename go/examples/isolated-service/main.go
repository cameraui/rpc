package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

type ComputeService struct{}

func (s *ComputeService) Fibonacci(n int) (int, error) {
	fmt.Printf("[Service] Computing fibonacci(%d)\n", n)
	if n <= 1 {
		return n, nil
	}

	a, b := 0, 1
	for i := 2; i <= n; i++ {
		a, b = b, a+b
	}

	time.Sleep(100 * time.Millisecond)
	return b, nil
}

func (s *ComputeService) GeneratePrimes(limit int) (<-chan int, error) {
	fmt.Printf("[Service] Generating primes up to %d\n", limit)
	ch := make(chan int)
	go func() {
		defer close(ch)
		for num := 2; num <= limit; num++ {
			isPrime := true
			for i := 2; i <= int(math.Sqrt(float64(num))); i++ {
				if num%i == 0 {
					isPrime = false
					break
				}
			}
			if isPrime {
				ch <- num
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
	return ch, nil
}

type EchoService struct{}

func (s *EchoService) Echo(message string) (string, error) {
	fmt.Printf("[Service] Echo: %s\n", message)
	return fmt.Sprintf("Echo: %s", message), nil
}

func (s *EchoService) Ping() (string, error) {
	return "pong", nil
}

func main() {
	totalStart := time.Now()
	fmt.Println("Starting isolated service connection test...")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "service-server",
	})

	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "server connect: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Server connected")

	svcManager := rpc.NewRPCService(server)

	service1, err := svcManager.RegisterHandler(rpc.ServiceConfig{
		Name:        "compute",
		Version:     "1.0.0",
		Description: "Heavy computation service",
	}, &ComputeService{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "register compute: %v\n", err)
		os.Exit(1)
	}

	service2, err := svcManager.RegisterHandler(rpc.ServiceConfig{
		Name:        "echo",
		Version:     "1.0.0",
		Description: "Lightweight echo service",
	}, &EchoService{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "register echo: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Services registered")
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

	fmt.Println("Creating service proxies")

	compute, err := client.CreateServiceProxy(ctx, "compute", rpc.WithIsolatedServiceProxy())
	if err != nil {
		fmt.Fprintf(os.Stderr, "create compute proxy: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Compute service proxy created (isolated connection)")

	echo, err := client.CreateServiceProxy(ctx, "echo")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create echo proxy: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Echo service proxy created (shared connection)")
	fmt.Println()

	fmt.Println("Testing concurrent operations")

	fmt.Println("Starting heavy computation (fibonacci)...")
	fibStart := time.Now()
	type fibResult struct {
		value any
		err   error
	}
	fibCh := make(chan fibResult, 1)
	go func() {
		val, err := compute.Invoke(ctx, "fibonacci", 40)
		fibElapsed := ms(time.Since(fibStart))
		if err == nil {
			fmt.Printf("Fibonacci(40) = %v (%.1fms)\n", val, fibElapsed)
		}
		fibCh <- fibResult{val, err}
	}()

	fmt.Println()
	fmt.Println("Testing echo service while computing...")
	var echoTimes []float64
	for i := 1; i <= 5; i++ {
		start := time.Now()
		result, err := echo.Invoke(ctx, "echo", fmt.Sprintf("Message %d", i))
		if err != nil {
			fmt.Fprintf(os.Stderr, "echo: %v\n", err)
			os.Exit(1)
		}
		elapsed := ms(time.Since(start))
		echoTimes = append(echoTimes, elapsed)
		fmt.Printf("%v (%.1fms)\n", result, elapsed)
		time.Sleep(200 * time.Millisecond)
	}
	var avgEchoTime float64
	for _, t := range echoTimes {
		avgEchoTime += t
	}
	avgEchoTime /= float64(len(echoTimes))
	fmt.Printf("Average echo time: %.1fms\n", avgEchoTime)

	fmt.Println()
	fmt.Println("Waiting for fibonacci computation...")
	fr := <-fibCh
	if fr.err != nil {
		fmt.Fprintf(os.Stderr, "fibonacci: %v\n", fr.err)
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("Testing streaming with isolation")
	streamStart := time.Now()

	fmt.Println("Collecting primes while doing other work...")

	var collectedPrimes []int
	primeDone := make(chan struct{})
	go func() {
		defer close(primeDone)
		streamCh, err := compute.InvokeStream(ctx, "generatePrimes", 30)
		if err != nil {
			fmt.Printf("stream error: %v\n", err)
			return
		}
		for val := range streamCh {
			if val.Error != nil {
				fmt.Printf("stream value error: %v\n", val.Error)
				break
			}
			if n, ok := toInt(val.Data); ok {
				collectedPrimes = append(collectedPrimes, n)
				fmt.Printf("Received prime: %d\n", n)
			}
		}
	}()

	for i := 1; i <= 3; i++ {
		time.Sleep(100 * time.Millisecond)
		pong, err := echo.Invoke(ctx, "ping")
		if err != nil {
			fmt.Fprintf(os.Stderr, "ping: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Ping %d: %v\n", i, pong)
	}

	<-primeDone
	streamTime := ms(time.Since(streamStart))
	primeStrs := make([]string, len(collectedPrimes))
	for i, p := range collectedPrimes {
		primeStrs[i] = fmt.Sprintf("%d", p)
	}
	fmt.Printf("\nCollected %d primes in %.1fms: %s\n", len(collectedPrimes), streamTime, strings.Join(primeStrs, ", "))

	fmt.Println()
	fmt.Println("Testing connection isolation")
	fmt.Println("Disconnecting compute service (isolated connection)...")
	_ = compute.Close()
	fmt.Println("Isolated connection closed")

	fmt.Println()
	fmt.Println("Testing echo service after compute disconnect...")
	start := time.Now()
	result, err := echo.Invoke(ctx, "echo", "Still working?")
	if err != nil {
		fmt.Printf("Echo failed: %v\n", err)
	} else {
		elapsed := ms(time.Since(start))
		fmt.Printf("Echo service: %v (%.1fms)\n", result, elapsed)
	}

	fmt.Println()
	fmt.Println("Testing compute service after disconnect...")
	_, err = compute.Invoke(ctx, "fibonacci", 10)
	if err != nil {
		fmt.Printf("Expected error: %v\n", err)
	} else {
		fmt.Println("ERROR: Compute service should have failed!")
	}

	fmt.Println()
	fmt.Println("Cleaning up...")
	_ = service1.Stop()
	_ = service2.Stop()
	client.Disconnect()
	server.Disconnect()

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nAll tests completed! Total time: %.1fms\n", totalElapsed)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int8:
		return int(n), true
	case int16:
		return int(n), true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case uint8:
		return int(n), true
	case uint16:
		return int(n), true
	case uint32:
		return int(n), true
	case uint64:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}
