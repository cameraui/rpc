package main

import (
	"context"
	"fmt"
	"os"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

// ConfigService exposes properties via rpc_prop tags.
// Fields tagged with `rpc_prop:"wireName"` automatically get:
//   - a getter endpoint "wireName" that returns the current value
//   - a setter endpoint "setWireName(value)" that updates the value
type ConfigService struct {
	Name           string `rpc_prop:"name"`
	Version        string `rpc_prop:"version"`
	MaxConnections int    `rpc_prop:"maxConnections"`
}

func main() {
	totalStart := time.Now()
	fmt.Println("=== RPCProperty Decorator Example (Go) ===")
	fmt.Println()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "property-server",
	})

	if err := server.Connect(ctx); err != nil {
		fatal("server connect", err)
	}
	fmt.Println("Server connected")

	unsub, err := server.RegisterHandler("config", &ConfigService{
		Name:           "Config Service",
		Version:        "1.0.0",
		MaxConnections: 100,
	})
	if err != nil {
		fatal("register handler", err)
	}
	fmt.Println("Service registered")
	fmt.Println()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "property-client",
	})

	if err := client.Connect(ctx); err != nil {
		fatal("client connect", err)
	}
	fmt.Println("Client connected")
	fmt.Println()

	config := client.CreateProxy("config")

	fmt.Println("--- Testing Property Getters ---")

	start := time.Now()
	name, err := config.Invoke(ctx, "name")
	if err != nil {
		fatal("get name", err)
	}
	fmt.Printf("Name: %v (%.1fms)\n", name, ms(time.Since(start)))

	start = time.Now()
	version, err := config.Invoke(ctx, "version")
	if err != nil {
		fatal("get version", err)
	}
	fmt.Printf("Version: %v (%.1fms)\n", version, ms(time.Since(start)))

	start = time.Now()
	maxConns, err := config.Invoke(ctx, "maxConnections")
	if err != nil {
		fatal("get maxConnections", err)
	}
	fmt.Printf("Max Connections: %v (%.1fms)\n", maxConns, ms(time.Since(start)))

	fmt.Println()
	fmt.Println("--- Updating Name ---")

	start = time.Now()
	_, err = config.Invoke(ctx, "setName", "Updated Config Service")
	if err != nil {
		fatal("setName", err)
	}
	fmt.Printf("Name updated (%.1fms)\n", ms(time.Since(start)))

	start = time.Now()
	newName, err := config.Invoke(ctx, "name")
	if err != nil {
		fatal("get new name", err)
	}
	fmt.Printf("New Name: %v (%.1fms)\n", newName, ms(time.Since(start)))

	fmt.Println()
	fmt.Println("Cleaning up...")
	_ = unsub()
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
