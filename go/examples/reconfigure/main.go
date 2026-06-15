package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "TEST FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\nALL RECONFIGURE TESTS PASSED")
}

func run() error {
	ctx := context.Background()
	valid := []string{"nats://localhost:4222"}

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: valid,
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "reconfigure-test-server",
	})
	client := rpc.NewClient(rpc.ClientOptions{
		Servers: valid,
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "reconfigure-test-client",
	})

	if err := server.Connect(ctx); err != nil {
		return fmt.Errorf("server connect: %w", err)
	}
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("client connect: %w", err)
	}
	fmt.Println("Initial connect ok")

	var received atomic.Int64
	if _, err := client.Subscribe("reconfigure.test", func(data []byte) {
		var msg map[string]int
		if err := rpc.Decode(data, &msg); err == nil {
			received.Add(int64(msg["n"]))
		}
	}); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	// Let NATS register the subscription on the server before publishing.
	time.Sleep(100 * time.Millisecond)

	if err := server.Publish("reconfigure.test", map[string]int{"n": 1}); err != nil {
		return fmt.Errorf("publish 1: %w", err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := received.Load(); got != 1 {
		return fmt.Errorf("pre-suspend publish: got %d, want 1", got)
	}
	fmt.Println("Pre-suspend publish delivered")

	if err := client.Reconfigure(rpc.ReconfigureOptions{Servers: valid}); err == nil {
		return fmt.Errorf("reconfigure while connected must error")
	}
	fmt.Println("reconfigure while connected returns error")

	if err := client.Suspend(); err != nil {
		return fmt.Errorf("suspend: %w", err)
	}
	if err := client.Reconfigure(rpc.ReconfigureOptions{Servers: valid}); err != nil {
		return fmt.Errorf("reconfigure: %w", err)
	}
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("reconnect: %w", err)
	}
	time.Sleep(100 * time.Millisecond)

	received.Store(0)
	if err := server.Publish("reconfigure.test", map[string]int{"n": 5}); err != nil {
		return fmt.Errorf("publish 5: %w", err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := received.Load(); got != 5 {
		return fmt.Errorf("post-reconfigure publish: got %d, want 5", got)
	}
	fmt.Println("Subscriptions auto-restored after suspend -> reconfigure -> connect")

	if err := client.Suspend(); err != nil {
		return fmt.Errorf("suspend 2: %w", err)
	}
	if err := client.Reconfigure(rpc.ReconfigureOptions{
		Auth: &rpc.AuthOptions{User: "server", Password: "server_password"},
	}); err != nil {
		return fmt.Errorf("reconfigure auth: %w", err)
	}
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("reconnect 2: %w", err)
	}
	time.Sleep(100 * time.Millisecond)
	received.Store(0)
	if err := server.Publish("reconfigure.test", map[string]int{"n": 9}); err != nil {
		return fmt.Errorf("publish 9: %w", err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := received.Load(); got != 9 {
		return fmt.Errorf("post-auth-replacement publish: got %d, want 9", got)
	}
	fmt.Println("reconfigure with auth replacement keeps subscriptions live")

	if err := client.Suspend(); err != nil {
		return fmt.Errorf("suspend 3: %w", err)
	}
	if err := client.Reconfigure(rpc.ReconfigureOptions{Servers: []string{"nats://other:4222"}}); err != nil {
		return fmt.Errorf("reconfigure servers: %w", err)
	}
	if got := client.Options.Servers[0]; got != "nats://other:4222" {
		return fmt.Errorf("Options.Servers mutation: got %q, want %q", got, "nats://other:4222")
	}
	fmt.Println("Options.Servers reflects Reconfigure")

	if err := client.Disconnect(); err != nil {
		return fmt.Errorf("client disconnect: %w", err)
	}
	if err := server.Disconnect(); err != nil {
		return fmt.Errorf("server disconnect: %w", err)
	}
	return nil
}
