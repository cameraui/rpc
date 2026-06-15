package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
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
	fmt.Println("Starting server-side isolated service test...")
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

	fmt.Println("Registering compute service with isolated connection...")
	service1, err := svcManager.RegisterHandler(rpc.ServiceConfig{
		Name:        "compute",
		Version:     "1.0.0",
		Description: "Heavy computation service (isolated)",
	}, &ComputeService{}, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "register compute: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Registering echo service on main connection...")
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

	const numClients = 5
	clients := make([]*rpc.Client, numClients)
	for i := range numClients {
		clients[i] = rpc.NewClient(rpc.ClientOptions{
			Servers: []string{"nats://localhost:4222"},
			Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
			Name:    fmt.Sprintf("test-client-%d", i),
		})
		if err := clients[i].Connect(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "client %d connect: %v\n", i, err)
			os.Exit(1)
		}
	}
	fmt.Printf("%d clients connected\n\n", numClients)

	fmt.Println("Testing concurrent heavy computations")
	fmt.Println("All clients will request fibonacci calculations simultaneously...")
	fmt.Println()

	computeProxies := make([]*rpc.ServiceProxy, numClients)
	echoProxies := make([]*rpc.ServiceProxy, numClients)
	for i := range numClients {
		cp, err := clients[i].CreateServiceProxy(ctx, "compute", rpc.WithIsolatedServiceProxy())
		if err != nil {
			fmt.Fprintf(os.Stderr, "create compute proxy %d: %v\n", i, err)
			os.Exit(1)
		}
		computeProxies[i] = cp

		ep, err := clients[i].CreateServiceProxy(ctx, "echo")
		if err != nil {
			fmt.Fprintf(os.Stderr, "create echo proxy %d: %v\n", i, err)
			os.Exit(1)
		}
		echoProxies[i] = ep
	}

	computeStart := time.Now()
	type computeResult struct {
		clientID int
		n        int
		result   any
		elapsed  float64
	}
	computeResults := make(chan computeResult, numClients)
	var computeWg sync.WaitGroup

	for i := range numClients {
		n := 35 + i
		fmt.Printf("Client %d: Starting fibonacci(%d)\n", i, n)
		computeWg.Add(1)
		go func(clientID, fibN int) {
			defer computeWg.Done()
			clientStart := time.Now()
			result, err := computeProxies[clientID].Invoke(ctx, "fibonacci", fibN)
			clientElapsed := ms(time.Since(clientStart))
			if err != nil {
				fmt.Printf("Client %d: fibonacci(%d) error: %v\n", clientID, fibN, err)
				return
			}
			fmt.Printf("Client %d: fibonacci(%d) = %v (%.1fms)\n", clientID, fibN, result, clientElapsed)
			computeResults <- computeResult{clientID, fibN, result, clientElapsed}
		}(i, n)
	}

	fmt.Println()
	fmt.Println("Testing echo service responsiveness during heavy load...")
	var echoTimes [][]float64
	for round := range 3 {
		time.Sleep(100 * time.Millisecond)

		roundTimes := make([]float64, numClients)
		var echoWg sync.WaitGroup
		for i := range numClients {
			echoWg.Add(1)
			go func(clientID, roundNum int) {
				defer echoWg.Done()
				start := time.Now()
				_, err := echoProxies[clientID].Invoke(ctx, "echo", fmt.Sprintf("Round %d from client %d", roundNum+1, clientID))
				elapsed := ms(time.Since(start))
				if err != nil {
					fmt.Printf("Echo error client %d: %v\n", clientID, err)
					return
				}
				roundTimes[clientID] = elapsed
			}(i, round)
		}
		echoWg.Wait()
		echoTimes = append(echoTimes, roundTimes)

		parts := make([]string, numClients)
		for i, t := range roundTimes {
			parts[i] = fmt.Sprintf("%.1fms", t)
		}
		fmt.Printf("Round %d echo times: %s\n", round+1, joinStrings(parts, ", "))
	}

	var totalEchoTime float64
	var echoCount int
	for _, roundTimes := range echoTimes {
		for _, t := range roundTimes {
			totalEchoTime += t
			echoCount++
		}
	}
	avgEchoTime := totalEchoTime / float64(echoCount)
	fmt.Printf("Average echo time during heavy load: %.1fms\n", avgEchoTime)

	fmt.Println()
	fmt.Println("Waiting for all computations to complete...")
	computeWg.Wait()
	close(computeResults)
	computeTime := ms(time.Since(computeStart))
	fmt.Printf("All computations completed in %.1fms\n", computeTime)

	fmt.Println()
	fmt.Println("Testing concurrent streaming")
	streamStart := time.Now()
	var streamWg sync.WaitGroup

	for i := range 3 {
		streamWg.Add(1)
		go func(clientID int) {
			defer streamWg.Done()
			fmt.Printf("Client %d: Starting prime generation\n", clientID)
			clientStreamStart := time.Now()
			streamCh, err := computeProxies[clientID].InvokeStream(ctx, "generatePrimes", 20)
			if err != nil {
				fmt.Printf("Client %d: stream error: %v\n", clientID, err)
				return
			}
			var collected []int
			for val := range streamCh {
				if val.Error != nil {
					fmt.Printf("Client %d: stream value error: %v\n", clientID, val.Error)
					break
				}
				if n, ok := toInt(val.Data); ok {
					collected = append(collected, n)
				}
			}
			clientElapsed := ms(time.Since(clientStreamStart))
			fmt.Printf("Client %d: Collected %d primes in %.1fms\n", clientID, len(collected), clientElapsed)
		}(i)
	}

	streamWg.Wait()
	streamTime := ms(time.Since(streamStart))
	fmt.Printf("All streams completed in %.1fms\n", streamTime)

	fmt.Println()
	fmt.Println("Connection status")
	fmt.Printf("Main server connection active: %v\n", server.IsConnected())
	fmt.Println("Isolated service connections managed by services")

	fmt.Println()
	fmt.Println("Cleaning up...")
	_ = service1.Stop()
	_ = service2.Stop()
	for _, c := range clients {
		c.Disconnect()
	}
	svcManager.StopAll()
	server.Disconnect()

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nAll tests completed! Total time: %.1fms\n", totalElapsed)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func joinStrings(parts []string, sep string) string {
	var result strings.Builder
	for i, p := range parts {
		if i > 0 {
			result.WriteString(sep)
		}
		result.WriteString(p)
	}
	return result.String()
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
