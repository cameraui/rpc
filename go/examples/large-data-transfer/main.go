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

var (
	buffer10MB = []byte(strings.Repeat("x", 10*1024*1024))
	buffer20MB = []byte(strings.Repeat("x", 20*1024*1024))
)

type DataService struct{}

func (s *DataService) GenerateLargeData(sizeMB int) (<-chan []byte, error) {
	chunkSize := 1024 * 1024
	totalSize := sizeMB * 1024 * 1024

	fmt.Printf("[Server] Starting to stream %dMB of data...\n", sizeMB)

	ch := make(chan []byte)
	go func() {
		defer close(ch)
		sent := 0
		for sent < totalSize {
			remaining := totalSize - sent
			currentChunkSize := min(remaining, chunkSize)
			chunk := make([]byte, currentChunkSize)
			for i := range chunk {
				chunk[i] = 'x'
			}
			sent += currentChunkSize
			ch <- chunk

			progress := (sent * 100) / totalSize
			if progress%10 == 0 && progress > 0 && progress != 100 {
				fmt.Printf("[Server] Streamed %d%%\n", progress)
			}
		}
		fmt.Println("[Server] Streaming completed")
	}()
	return ch, nil
}

func main() {
	totalStart := time.Now()

	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "server",
	})

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "client",
	})

	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "server connect: %v\n", err)
		os.Exit(1)
	}
	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "client connect: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Connected to NATS")
	fmt.Println()
	fmt.Printf("Server max payload: %.1fMB\n", float64(server.MaxPayloadSize())/1024/1024)
	fmt.Printf("Client max payload: %.1fMB\n\n", float64(client.MaxPayloadSize())/1024/1024)

	fmt.Println("Method 1: Publish/Subscribe with Auto-Chunking")
	fmt.Println()

	type largeTransfer struct {
		Payload  []byte `msgpack:"payload"`
		Checksum int    `msgpack:"checksum"`
	}

	var receivedData atomic.Pointer[largeTransfer]

	unsub, err := server.Subscribe("large.data.transfer", func(data []byte) {
		var transfer largeTransfer
		rpc.Decode(data, &transfer)
		receivedData.Store(&transfer)
		fmt.Printf("[Server] Received %.1fMB via pub/sub\n", float64(len(transfer.Payload))/1024/1024)
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
		os.Exit(1)
	}

	largePayload := buffer10MB
	fmt.Printf("[Client] Sending %.1fMB via publish...\n", float64(len(largePayload))/1024/1024)
	start := time.Now()
	if err := client.Publish("large.data.transfer", largeTransfer{
		Payload:  largePayload,
		Checksum: len(largePayload),
	}); err != nil {
		fmt.Fprintf(os.Stderr, "publish: %v\n", err)
		os.Exit(1)
	}

	time.Sleep(500 * time.Millisecond)
	elapsed := ms(time.Since(start))

	if rd := receivedData.Load(); rd != nil {
		fmt.Printf("[Client] Transfer completed in %.1fms\n", elapsed)
		fmt.Printf("[Client] Checksum verified: %v\n\n", rd.Checksum == len(largePayload))
	} else {
		fmt.Println("[Client] Data not received")
		fmt.Println()
	}

	unsub()

	fmt.Println("Method 2: RPC Streaming for Large Data")
	fmt.Println()

	unregister, err := server.RegisterHandler("data", &DataService{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "register handler: %v\n", err)
		os.Exit(1)
	}

	proxy := client.CreateProxy("data")

	fmt.Println("[Client] Requesting 10MB of streamed data...")
	streamStart := time.Now()
	totalReceived := 0

	streamCh, err := proxy.InvokeStream(ctx, "generateLargeData", 10)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stream: %v\n", err)
		os.Exit(1)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Fprintf(os.Stderr, "stream error: %v\n", val.Error)
			break
		}
		chunk := toBytes(val.Data)
		totalReceived += len(chunk)
	}

	streamElapsed := ms(time.Since(streamStart))
	fmt.Printf("[Client] Received %.1fMB in %.1fms\n", float64(totalReceived)/1024/1024, streamElapsed)
	fmt.Printf("[Client] Transfer rate: %.1fMB/s\n\n",
		float64(totalReceived)/1024/1024/(streamElapsed/1000))

	unregister()

	fmt.Println("Method 3: Channels for Bidirectional Transfer")
	fmt.Println()

	serverChannel, err := server.Channel("large-data-channel")
	if err != nil {
		fmt.Fprintf(os.Stderr, "server channel: %v\n", err)
		os.Exit(1)
	}
	clientChannel, err := client.Channel("large-data-channel")
	if err != nil {
		fmt.Fprintf(os.Stderr, "client channel: %v\n", err)
		os.Exit(1)
	}

	var serverReceived int

	serverChannel.OnMessage(func(data any) {
		m, ok := data.(map[string]any)
		if !ok {
			return
		}
		if chunk, exists := m["chunk"]; exists {
			if b, ok := chunk.([]byte); ok {
				serverReceived += len(b)
			}
			if last, ok := m["last"].(bool); ok && last {
				fmt.Printf("[Server] Received total: %.1fMB\n", float64(serverReceived)/1024/1024)
				_ = serverChannel.Send(map[string]any{"received": serverReceived, "status": "complete"})
			}
		}
	})

	fmt.Println("[Client] Sending 5MB through channel in chunks...")
	channelStart := time.Now()
	chunkSize := 512 * 1024
	totalSize := 5 * 1024 * 1024
	sent := 0

	for sent < totalSize {
		remaining := totalSize - sent
		currentChunkSize := min(remaining, chunkSize)
		chunk := make([]byte, currentChunkSize)
		for i := range chunk {
			chunk[i] = 'x'
		}
		sent += currentChunkSize

		if err := clientChannel.Send(map[string]any{
			"chunk": chunk,
			"last":  sent >= totalSize,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "channel send: %v\n", err)
			break
		}
	}

	var confirmed atomic.Bool

	clientChannel.OnMessage(func(data any) {
		m, ok := data.(map[string]any)
		if !ok {
			return
		}
		if status, ok := m["status"].(string); ok && status == "complete" {
			confirmed.Store(true)
			channelElapsed := ms(time.Since(channelStart))
			received := toNumber(m["received"])
			fmt.Printf("[Client] Channel transfer completed in %.1fms\n", channelElapsed)
			fmt.Printf("[Client] Server confirmed %.1fMB received\n\n", float64(received)/1024/1024)
		}
	})

	for range 20 {
		if confirmed.Load() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("Method 4: Request/Reply for Large Data")
	fmt.Println()

	fmt.Println("[Client] Sending 20MB via request/reply (should fail)...")
	hugePayload := buffer20MB
	reqStart := time.Now()
	_, err = client.Request(ctx, "large.data.reply", largeTransfer{
		Payload:  hugePayload,
		Checksum: len(hugePayload),
	}, 5*time.Second)
	if err != nil {
		fmt.Printf("[Client] Request/Reply failed: %v\n\n", err)
		fmt.Println("This method is not suitable for large data transfers due to NATS max_payload limits.")
		fmt.Println()
	} else {
		reqElapsed := ms(time.Since(reqStart))
		fmt.Printf("[Client] Received response in %.1fms\n\n", reqElapsed)
	}

	_ = serverChannel.Close()
	_ = clientChannel.Close()

	fmt.Println()
	fmt.Println("Summary")
	fmt.Println("1. Publish/Subscribe: Best for one-way large data broadcasts")
	fmt.Println("2. RPC Streaming: Best for controlled data flow with backpressure")
	fmt.Println("3. Channels: Best for bidirectional communication with large data")
	fmt.Println("4. Request/Reply: Limited by NATS max_payload, no auto-chunking")

	client.Disconnect()
	server.Disconnect()

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nAll tests completed! Total time: %.1fms\n", totalElapsed)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func toNumber(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int8:
		return int(n)
	case int16:
		return int(n)
	case int32:
		return int(n)
	case int64:
		return int(n)
	case uint8:
		return int(n)
	case uint16:
		return int(n)
	case uint32:
		return int(n)
	case uint64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func toBytes(val any) []byte {
	if b, ok := val.([]byte); ok {
		return b
	}
	data, _ := rpc.Encode(val)
	return data
}
