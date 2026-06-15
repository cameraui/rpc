package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

var largeBuffer = []byte(strings.Repeat("x", 2*1024*1024)) // 2MB

func channelExample() error {
	fmt.Println("Channel Communication Example")
	fmt.Println()

	ctx := context.Background()

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
		return fmt.Errorf("clientA connect: %w", err)
	}
	if err := clientB.Connect(ctx); err != nil {
		return fmt.Errorf("clientB connect: %w", err)
	}

	fmt.Println("Connected both clients")
	fmt.Println()

	channelA, err := clientA.Channel("chat-room-123")
	if err != nil {
		return fmt.Errorf("channelA: %w", err)
	}
	channelB, err := clientB.Channel("chat-room-123")
	if err != nil {
		return fmt.Errorf("channelB: %w", err)
	}

	fmt.Println("Created channels on both sides")
	fmt.Println()

	channelA.OnMessage(func(data any) {
		msg := formatMsg(data)
		fmt.Printf("Client A received: %s\n", msg)
	})

	channelA.OnClose(func() {
		fmt.Println("Client A channel closed")
	})

	channelB.OnMessage(func(data any) {
		msg := formatMsg(data)
		fmt.Printf("Client B received: %s\n", msg)
	})

	channelB.OnClose(func() {
		fmt.Println("Client B channel closed")
	})

	fmt.Println("Sending messages")

	smallStart := time.Now()
	if err := channelA.Send(map[string]any{"from": "A", "message": "Hello from client A!"}); err != nil {
		return err
	}
	if err := channelB.Send(map[string]any{"from": "B", "message": "Hi from client B!"}); err != nil {
		return err
	}
	if err := channelA.Send(map[string]any{
		"type": "user-info",
		"user": map[string]any{"id": 1, "name": "Alice", "status": "online"},
	}); err != nil {
		return err
	}
	smallTime := time.Since(smallStart)
	fmt.Printf("Small messages sent in %.1fms\n", float64(smallTime.Microseconds())/1000)

	fmt.Println()
	fmt.Println("Sending large data (2MB)")
	largeStart := time.Now()
	if err := channelA.Send(map[string]any{
		"from":     "A",
		"type":     "file-transfer",
		"filename": "large-dataset.json",
		"content":  largeBuffer,
	}); err != nil {
		return err
	}
	largeTime := time.Since(largeStart)
	fmt.Printf("Large data (2MB) sent in %.1fms (%.2f MB/s)\n",
		float64(largeTime.Microseconds())/1000, 2000.0/float64(largeTime.Milliseconds()))

	channelA.OnError(func(err error) {
		fmt.Printf("Client A error: %v\n", err)
	})

	time.Sleep(100 * time.Millisecond)

	fmt.Println()
	fmt.Println("Closing channel from Client A")
	if err := channelA.Close(); err != nil {
		return err
	}

	if err := channelA.Send(map[string]any{"message": "This should fail"}); err != nil {
		fmt.Printf("Client A expected error: %v\n", err)
	}

	clientA.Disconnect()
	clientB.Disconnect()

	fmt.Println()
	fmt.Println("Channel communication example completed!")
	return nil
}

func chatRoomExample() error {
	fmt.Println()
	fmt.Println()
	fmt.Println("Multi-Party Chat Room Example")
	fmt.Println()

	ctx := context.Background()

	users := []string{"Alice", "Bob", "Charlie"}
	type userChannel struct {
		user    string
		channel *rpc.Channel
		client  *rpc.Client
	}
	var channels []userChannel
	roomID := "team-standup"

	for _, user := range users {
		client := rpc.NewClient(rpc.ClientOptions{
			Servers: []string{"nats://localhost:4222"},
			Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
			Name:    fmt.Sprintf("user-%s", strings.ToLower(user)),
		})
		if err := client.Connect(ctx); err != nil {
			return fmt.Errorf("connect %s: %w", user, err)
		}

		ch, err := client.Channel(roomID)
		if err != nil {
			return fmt.Errorf("channel %s: %w", user, err)
		}

		u := user
		ch.OnMessage(func(data any) {
			m, ok := data.(map[string]any)
			if !ok {
				return
			}
			from, _ := m["from"].(string)
			if from != u {
				msg, _ := m["message"].(string)
				fmt.Printf("[%s] %s: %s\n", u, from, msg)
			}
		})

		channels = append(channels, userChannel{user: user, channel: ch, client: client})
	}

	fmt.Println("All users joined the chat room")
	fmt.Println()

	channels[0].channel.Send(map[string]any{"from": "Alice", "message": "Good morning team!"})
	channels[1].channel.Send(map[string]any{"from": "Bob", "message": "Hi Alice! Ready for standup?"})
	channels[2].channel.Send(map[string]any{"from": "Charlie", "message": "Hey everyone!"})

	time.Sleep(100 * time.Millisecond)

	fmt.Println()
	fmt.Println("File sharing")
	channels[0].channel.Send(map[string]any{
		"from":    "Alice",
		"type":    "file-share",
		"message": "Sharing today's agenda",
		"file": map[string]any{
			"name":    "standup-agenda.md",
			"size":    1024,
			"content": "# Today's Standup\n1. Updates\n2. Blockers\n3. Plans",
		},
	})

	time.Sleep(100 * time.Millisecond)

	for _, uc := range channels {
		uc.channel.Close()
	}
	for _, uc := range channels {
		uc.client.Disconnect()
	}

	fmt.Println()
	fmt.Println("Chat room example completed!")
	return nil
}

func main() {
	totalStart := time.Now()

	if err := channelExample(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if err := chatRoomExample(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nAll tests completed! Total time: %.1fms\n", float64(time.Since(totalStart).Microseconds())/1000)
}

func formatMsg(data any) string {
	m, ok := data.(map[string]any)
	if !ok {
		return fmt.Sprintf("%v", data)
	}
	if content, exists := m["content"]; exists {
		switch v := content.(type) {
		case []byte:
			m["content"] = fmt.Sprintf("<%d bytes>", len(v))
		case string:
			if len(v) > 100 {
				m["content"] = fmt.Sprintf("<%d bytes>", len(v))
			}
		}
	}
	return fmt.Sprintf("%v", m)
}
