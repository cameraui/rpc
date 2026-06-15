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
	smallBuffer    = []byte("This is a small response that fits in a single message")
	mediumBuf2MB   = makePattern(2 * 1024 * 1024)
	mediumBuf5MB   = makePattern(5 * 1024 * 1024)
	largeBuf10MB   = []byte(strings.Repeat("x", 10*1024*1024))
	chunkBuffer1MB = makePattern(1 * 1024 * 1024)
)

func makePattern(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i % 256)
	}
	return b
}

type TestService struct{}

func (s *TestService) GetSmallData() (string, error) {
	return string(smallBuffer), nil
}

func (s *TestService) GetMediumData(sizeMB int) ([]byte, error) {
	fmt.Printf("[Service] Returning %dMB buffer\n", sizeMB)
	switch sizeMB {
	case 2:
		return mediumBuf2MB, nil
	case 5:
		return mediumBuf5MB, nil
	default:
		return makePattern(sizeMB * 1024 * 1024), nil
	}
}

func (s *TestService) GetLargeData(sizeMB int) ([]byte, error) {
	return s.GetMediumData(sizeMB)
}

func (s *TestService) Echo(data []byte) ([]byte, error) {
	fmt.Printf("[Service] Echoing %.2fMB\n", float64(len(data))/1024/1024)
	return data, nil
}

func (s *TestService) GenerateLargeDataStream(chunkSizeMB, chunks int) (<-chan []byte, error) {
	fmt.Printf("[Service] Streaming %d chunks of %dMB each\n", chunks, chunkSizeMB)
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		for i := range chunks {
			if chunkSizeMB == 1 {
				fmt.Printf("[Service] Yielding chunk %d/%d - size: %d bytes\n", i+1, chunks, len(chunkBuffer1MB))
				ch <- chunkBuffer1MB
			} else {
				buf := make([]byte, chunkSizeMB*1024*1024)
				for j := range buf {
					buf[j] = byte((i + j) % 256)
				}
				fmt.Printf("[Service] Yielding chunk %d/%d - size: %d bytes\n", i+1, chunks, len(buf))
				ch <- buf
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return ch, nil
}

func (s *TestService) Ping() (string, error) {
	return fmt.Sprintf("pong at %s", time.Now().UTC().Format(time.RFC3339Nano)), nil
}

func main() {
	totalStart := time.Now()
	ctx := context.Background()
	var isConnected atomic.Bool
	isConnected.Store(false)

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "server",
	})

	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "server connect: %v\n", err)
		os.Exit(1)
	}
	unsub, err := server.RegisterHandler("test", &TestService{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "register handler: %v\n", err)
		os.Exit(1)
	}

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

	if err := clientA.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "clientA connect: %v\n", err)
		os.Exit(1)
	}
	if err := clientB.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "clientB connect: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Clients connected")
	isConnected.Store(true)

	clientAChannel, err := clientA.PrivateChannelConnect("secret-chat", "client-b")
	if err != nil {
		fmt.Fprintf(os.Stderr, "privateA: %v\n", err)
		os.Exit(1)
	}
	clientBChannel, err := clientB.PrivateChannelConnect("secret-chat", "client-a")
	if err != nil {
		fmt.Fprintf(os.Stderr, "privateB: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Private channels created")

	clientAChannel.OnMessage(func(data any) {
		m, _ := data.(map[string]any)
		if file, ok := m["file"]; ok {
			if b, ok := file.([]byte); ok {
				m["file"] = fmt.Sprintf("<%d bytes>", len(b))
			}
		}
		fmt.Printf("[Client A received]: %v\n", m)
	})

	clientBChannel.OnMessage(func(data any) {
		m, _ := data.(map[string]any)
		if file, ok := m["file"]; ok {
			if b, ok := file.([]byte); ok {
				m["file"] = fmt.Sprintf("<%d bytes>", len(b))
			}
		}
		fmt.Printf("[Client B received]: %v\n", m)
	})

	go func() {
		count := 0
		useA := true
		for isConnected.Load() {
			var ch *rpc.PrivateChannel
			var from string
			if useA {
				ch = clientAChannel
				from = "Client A"
			} else {
				ch = clientBChannel
				from = "Client B"
			}
			err := ch.Send(map[string]any{
				"from": from,
				"text": fmt.Sprintf("Message %d", count),
				"date": time.Now().UTC().Format(time.RFC3339Nano),
				"file": largeBuf10MB,
			})
			if err != nil {
				if !isConnected.Load() {
					return
				}
				fmt.Printf("Error sending message: %v\n", err)
			}
			count++
			useA = !useA
			time.Sleep(100 * time.Millisecond)
		}
	}()

	proxyA := clientA.CreateProxy("test")

	fmt.Println()
	fmt.Println("=== Testing service methods ===")

	start := time.Now()
	smallResult, err := proxyA.Invoke(ctx, "getSmallData")
	if err != nil {
		fmt.Fprintf(os.Stderr, "getSmallData: %v\n", err)
		os.Exit(1)
	}
	elapsed := time.Since(start)
	smallStr, _ := smallResult.(string)
	fmt.Printf("Small Data: %d bytes (%.1fms)\n", len(smallStr), ms(elapsed))

	start = time.Now()
	medResult, err := proxyA.Invoke(ctx, "getMediumData", 2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getMediumData: %v\n", err)
		os.Exit(1)
	}
	elapsed = time.Since(start)
	medBytes := toBytes(medResult)
	fmt.Printf("Medium Data (2MB): %d bytes (%.1fms, %.1f MB/s)\n", len(medBytes), ms(elapsed), 2000.0/ms(elapsed))

	start = time.Now()
	largeResult, err := proxyA.Invoke(ctx, "getLargeData", 5)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getLargeData: %v\n", err)
		os.Exit(1)
	}
	elapsed = time.Since(start)
	largeBytes := toBytes(largeResult)
	fmt.Printf("Large Data (5MB): %d bytes (%.1fms, %.1f MB/s)\n", len(largeBytes), ms(elapsed), 5000.0/ms(elapsed))

	start = time.Now()
	echoResult, err := proxyA.Invoke(ctx, "echo", []byte("Hello, World!"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "echo: %v\n", err)
		os.Exit(1)
	}
	elapsed = time.Since(start)
	echoBytes := toBytes(echoResult)
	fmt.Printf("Echo Data: %d bytes (%.1fms)\n", len(echoBytes), ms(elapsed))

	fmt.Println()
	fmt.Println("=== Testing streaming ===")
	start = time.Now()
	streamCh, err := proxyA.InvokeStream(ctx, "generateLargeDataStream", 1, 100)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stream: %v\n", err)
		os.Exit(1)
	}
	chunkCount := 0
	for val := range streamCh {
		if val.Error != nil {
			fmt.Fprintf(os.Stderr, "stream error: %v\n", val.Error)
			break
		}
		chunkCount++
		chunk := toBytes(val.Data)
		if chunkCount <= 3 || chunkCount > 97 {
			fmt.Printf("Received Chunk %d: %d bytes\n", chunkCount, len(chunk))
		} else if chunkCount == 4 {
			fmt.Println("... (skipping intermediate chunks) ...")
		}
	}
	elapsed = time.Since(start)
	fmt.Printf("Stream completed: %d chunks, total %dMB in %.1fms (%.1f MB/s)\n",
		chunkCount, chunkCount, ms(elapsed), float64(chunkCount)*1000.0/ms(elapsed))

	start = time.Now()
	pingResult, err := proxyA.Invoke(ctx, "ping")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ping: %v\n", err)
		os.Exit(1)
	}
	elapsed = time.Since(start)
	pingStr, _ := pingResult.(string)
	fmt.Printf("\nPing Response: %s (%.1fms)\n", pingStr, ms(elapsed))

	fmt.Println()
	fmt.Println("=== Running concurrent message test for 10 seconds ===")
	time.Sleep(10 * time.Second)

	fmt.Println()
	fmt.Println("=== Disconnecting ===")
	isConnected.Store(false)
	unsub()
	clientAChannel.Close()
	clientBChannel.Close()
	clientA.Disconnect()
	clientB.Disconnect()
	server.Disconnect()

	fmt.Printf("Test completed. Total time: %.1fms\n", ms(time.Since(totalStart)))
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
