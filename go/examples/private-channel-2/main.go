package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

var buffer10MB = []byte(strings.Repeat("x", 10*1024*1024))

func main() {
	totalStart := time.Now()
	var isConnected atomic.Bool

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

	ctx := context.Background()

	if err := clientA.Connect(ctx); err != nil {
		fatal("clientA connect", err)
	}
	if err := clientB.Connect(ctx); err != nil {
		fatal("clientB connect", err)
	}

	fmt.Println("Clients connected")

	isConnected.Store(true)

	start := time.Now()
	clientAChannel, err := clientA.PrivateChannelConnect("secret-chat", "client-b", true)
	if err != nil {
		fatal("clientA private channel", err)
	}
	channelATime := ms(time.Since(start))

	start = time.Now()
	clientBChannel, err := clientB.PrivateChannelConnect("secret-chat", "client-a", true)
	if err != nil {
		fatal("clientB private channel", err)
	}
	channelBTime := ms(time.Since(start))

	fmt.Printf("Private channels created (A: %.1fms, B: %.1fms)\n", channelATime, channelBTime)

	time.Sleep(100 * time.Millisecond)

	clientAChannel.OnMessage(func(data any) {
		m, ok := data.(map[string]any)
		if !ok {
			return
		}
		fmt.Printf("[Client A received]: %s\n", formatMsg(m))
	})

	clientBChannel.OnMessage(func(data any) {
		m, ok := data.(map[string]any)
		if !ok {
			return
		}
		fmt.Printf("[Client B received]: %s\n", formatMsg(m))
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		count := 0
		useA := true

		for isConnected.Load() {
			var fromLabel string
			var ch *rpc.PrivateChannel
			if useA {
				fromLabel = "Client A"
				ch = clientAChannel
			} else {
				fromLabel = "Client B"
				ch = clientBChannel
			}

			sendStart := time.Now()
			err := ch.Send(map[string]any{
				"from": fromLabel,
				"text": fmt.Sprintf("Message %d", count),
				"date": time.Now().Format(time.RFC3339),
				"file": buffer10MB,
			})
			if err != nil {
				if !isConnected.Load() {
					break
				}
				fmt.Printf("Error sending message: %v\n", err)
			} else {
				sendTime := ms(time.Since(sendStart))
				throughput := float64(10*1024*1024) / ((sendTime / 1000) * 1024 * 1024)
				fmt.Printf("[%s] Sent 10MB message %d in %.1fms (%.1f MB/s)\n", fromLabel, count, sendTime, throughput)
				count++
			}

			if isConnected.Load() {
				time.Sleep(1 * time.Second)
				useA = !useA
			}
		}
	}()

	time.Sleep(10 * time.Second)

	fmt.Println()
	fmt.Println("Disconnecting clients...")
	isConnected.Store(false)

	<-done

	_ = clientAChannel.Close()
	_ = clientBChannel.Close()
	clientA.Disconnect()
	clientB.Disconnect()

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nAll tests completed! Total time: %.1fms\n", totalElapsed)
}

func formatMsg(m map[string]any) string {
	parts := make([]string, 0, len(m))
	for k, v := range m {
		if b, ok := v.([]byte); ok {
			parts = append(parts, fmt.Sprintf("%s: <%d bytes>", k, len(b)))
		} else {
			parts = append(parts, fmt.Sprintf("%s: %v", k, v))
		}
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func fatal(label string, err error) {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", label, err)
	os.Exit(1)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
