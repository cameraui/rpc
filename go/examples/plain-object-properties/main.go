package main

import (
	"context"
	"fmt"
	"os"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

func main() {
	fmt.Println("Plain Object Property Access Test (Go)")
	fmt.Println()

	// Declare first, then assign self-referencing closures.
	handlers := map[string]any{
		"name":    "test-service",
		"version": "1.0.0",
	}
	handlers["getName"] = func() (string, error) {
		return handlers["name"].(string), nil
	}
	handlers["getVersion"] = func() (string, error) {
		return handlers["version"].(string), nil
	}
	handlers["echo"] = func(msg string) (string, error) {
		return fmt.Sprintf("Echo: %s", msg), nil
	}

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "plain-object-server",
	})

	if err := server.Connect(ctx); err != nil {
		fatal("server connect", err)
	}
	fmt.Println("Server connected")

	unsub, err := server.RegisterHandler("test", handlers)
	if err != nil {
		fatal("register handler", err)
	}
	fmt.Println("Handlers registered")
	fmt.Println()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "plain-object-client",
	})

	if err := client.Connect(ctx); err != nil {
		fatal("client connect", err)
	}
	fmt.Println("Client connected")
	fmt.Println()

	proxy := client.CreateProxy("test")

	totalStart := time.Now()

	fmt.Println("--- Testing Direct Property Access ---")

	start := time.Now()
	name, err := proxy.Invoke(ctx, "name")
	if err != nil {
		fatal("name", err)
	}
	elapsed := ms(time.Since(start))
	fmt.Printf("Name (direct): %v - Time: %.1fms\n", name, elapsed)

	start = time.Now()
	version, err := proxy.Invoke(ctx, "version")
	if err != nil {
		fatal("version", err)
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("Version (direct): %v - Time: %.1fms\n", version, elapsed)

	fmt.Println()
	fmt.Println("--- Testing Method Calls ---")

	start = time.Now()
	nameViaMethod, err := proxy.Invoke(ctx, "getName")
	if err != nil {
		fatal("getName", err)
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("Name (via method): %v - Time: %.1fms\n", nameViaMethod, elapsed)

	start = time.Now()
	echo, err := proxy.Invoke(ctx, "echo", "Hello World")
	if err != nil {
		fatal("echo", err)
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("Echo: %v - Time: %.1fms\n", echo, elapsed)

	fmt.Println()
	fmt.Println("--- Performance Test ---")
	iterations := 100

	start = time.Now()
	for range iterations {
		_, err := proxy.Invoke(ctx, "name")
		if err != nil {
			fatal("name perf", err)
		}
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("Direct property access: %d calls in %.1fms (%.2fms avg)\n", iterations, elapsed, elapsed/float64(iterations))

	start = time.Now()
	for range iterations {
		_, err := proxy.Invoke(ctx, "echo", "test")
		if err != nil {
			fatal("echo perf", err)
		}
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("Method calls: %d calls in %.1fms (%.2fms avg)\n", iterations, elapsed, elapsed/float64(iterations))

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nAll tests passed! Total time: %.1fms\n", totalElapsed)

	fmt.Println()
	fmt.Println("Cleaning up...")
	_ = unsub()
	client.Disconnect()
	server.Disconnect()
}

func fatal(label string, err error) {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", label, err)
	os.Exit(1)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
