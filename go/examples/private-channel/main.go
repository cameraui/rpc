package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

var buffer3MB = []byte(strings.Repeat("x", 3*1024*1024))

func main() {
	totalStart := time.Now()

	ctx := context.Background()

	privateChannelExample(ctx)
	directMessagingExample(ctx)

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nAll tests completed! Total time: %.1fms\n", totalElapsed)
}

func privateChannelExample(ctx context.Context) {
	fmt.Println("=== Private Channel Example ===")
	fmt.Println()

	alice := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "alice",
	})

	bob := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "bob",
	})

	charlie := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "charlie",
	})

	if err := alice.Connect(ctx); err != nil {
		fatal("alice connect", err)
	}
	if err := bob.Connect(ctx); err != nil {
		fatal("bob connect", err)
	}
	if err := charlie.Connect(ctx); err != nil {
		fatal("charlie connect", err)
	}

	fmt.Println("All clients connected")
	fmt.Println()

	start := time.Now()
	aliceChannel, err := alice.PrivateChannelConnect("secret-chat", "bob")
	if err != nil {
		fatal("alice private channel", err)
	}
	aliceChannelTime := ms(time.Since(start))

	start = time.Now()
	bobChannel, err := bob.PrivateChannelConnect("secret-chat", "alice")
	if err != nil {
		fatal("bob private channel", err)
	}
	bobChannelTime := ms(time.Since(start))

	start = time.Now()
	charlieChannel, err := charlie.PrivateChannelConnect("secret-chat", "alice")
	if err != nil {
		fatal("charlie private channel", err)
	}
	charlieChannelTime := ms(time.Since(start))

	fmt.Printf("Private channels created (Alice: %.1fms, Bob: %.1fms, Charlie: %.1fms)\n\n",
		aliceChannelTime, bobChannelTime, charlieChannelTime)

	time.Sleep(50 * time.Millisecond)

	aliceChannel.OnMessage(func(data any) {
		fmt.Printf("[Alice received]: %s\n", formatMsg(data))
	})

	bobChannel.OnMessage(func(data any) {
		fmt.Printf("[Bob received]: %s\n", formatMsg(data))
	})

	charlieChannel.OnMessage(func(data any) {
		fmt.Printf("[Charlie received]: %s\n", formatMsg(data)) // Should not receive anything
	})

	fmt.Println("--- Private Messages ---")

	msgStart := time.Now()
	if err := aliceChannel.Send(map[string]any{"from": "Alice", "text": "Hi Bob, this is private!"}); err != nil {
		fatal("alice send", err)
	}
	aliceSendTime := ms(time.Since(msgStart))

	msgStart = time.Now()
	if err := bobChannel.Send(map[string]any{"from": "Bob", "text": "Hi Alice, got your private message!"}); err != nil {
		fatal("bob send", err)
	}
	bobSendTime := ms(time.Since(msgStart))

	msgStart = time.Now()
	if err := charlieChannel.Send(map[string]any{"from": "Charlie", "text": "Can anyone hear me?"}); err != nil {
		fatal("charlie send", err)
	}
	charlieSendTime := ms(time.Since(msgStart))

	time.Sleep(100 * time.Millisecond)

	fmt.Printf("\nMessage send times: Alice: %.1fms, Bob: %.1fms, Charlie: %.1fms\n",
		aliceSendTime, bobSendTime, charlieSendTime)

	fmt.Println()
	fmt.Println("--- Channel Info ---")
	fmt.Printf("Alice connected to: %s\n", aliceChannel.RemoteID())
	fmt.Printf("Bob connected to: %s\n", bobChannel.RemoteID())
	charlieRemote := charlieChannel.RemoteID()
	if charlieRemote == "" {
		charlieRemote = "nobody"
	}
	fmt.Printf("Charlie connected to: %s\n", charlieRemote)

	_ = aliceChannel.Close()
	_ = bobChannel.Close()
	_ = charlieChannel.Close()

	alice.Disconnect()
	bob.Disconnect()
	charlie.Disconnect()

	fmt.Println()
	fmt.Println("Private channel example completed!")
}

type User struct {
	client   *rpc.Client
	name     string
	channels map[string]*rpc.PrivateChannel
}

func newUser(name string) *User {
	return &User{
		client: rpc.NewClient(rpc.ClientOptions{
			Servers: []string{"nats://localhost:4222"},
			Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
			Name:    name,
		}),
		name:     name,
		channels: make(map[string]*rpc.PrivateChannel),
	}
}

func (u *User) connect(ctx context.Context) error {
	return u.client.Connect(ctx)
}

func (u *User) sendDM(recipient, message string) error {
	ids := []string{u.name, recipient}
	sort.Strings(ids)
	channelID := strings.Join(ids, "-")

	ch, exists := u.channels[recipient]
	if !exists {
		var err error
		ch, err = u.client.PrivateChannelConnect(channelID, recipient)
		if err != nil {
			return err
		}
		u.channels[recipient] = ch

		userName := u.name
		ch.OnMessage(func(data any) {
			m, ok := data.(map[string]any)
			if !ok {
				return
			}
			fmt.Printf("[%s] DM from %v: %v\n", userName, m["from"], m["message"])
		})
	}

	return ch.Send(map[string]any{"from": u.name, "message": message})
}

func (u *User) disconnect() {
	for _, ch := range u.channels {
		_ = ch.Close()
	}
	u.client.Disconnect()
}

func directMessagingExample(ctx context.Context) {
	fmt.Println()
	fmt.Println()
	fmt.Println("=== Direct Messaging System ===")
	fmt.Println()

	alice := newUser("alice")
	bob := newUser("bob")
	charlie := newUser("charlie")

	if err := alice.connect(ctx); err != nil {
		fatal("alice connect", err)
	}
	if err := bob.connect(ctx); err != nil {
		fatal("bob connect", err)
	}
	if err := charlie.connect(ctx); err != nil {
		fatal("charlie connect", err)
	}

	bobFromAlice, err := bob.client.PrivateChannelConnect("alice-bob", "alice")
	if err != nil {
		fatal("bob-from-alice channel", err)
	}
	bobFromAlice.OnMessage(func(data any) {
		m, ok := data.(map[string]any)
		if !ok {
			return
		}
		if msg, ok := m["message"]; ok {
			fmt.Printf("[bob] DM from %v: %v\n", m["from"], msg)
		} else if t, ok := m["type"].(string); ok && t == "file" {
			fmt.Printf("[bob] Received file from %v: %v (%v bytes)\n", m["from"], m["filename"], m["size"])
		}
	})

	charlieFromBob, err := charlie.client.PrivateChannelConnect("bob-charlie", "bob")
	if err != nil {
		fatal("charlie-from-bob channel", err)
	}
	charlieFromBob.OnMessage(func(data any) {
		m, ok := data.(map[string]any)
		if !ok {
			return
		}
		fmt.Printf("[charlie] DM from %v: %v\n", m["from"], m["message"])
	})

	time.Sleep(50 * time.Millisecond)

	fmt.Println("--- Direct Messages ---")

	dmStart := time.Now()
	if err := alice.sendDM("bob", "Hey Bob, are you free for lunch?"); err != nil {
		fatal("alice DM", err)
	}
	aliceDmTime := ms(time.Since(dmStart))

	dmStart = time.Now()
	if err := bob.sendDM("charlie", "Charlie, did you finish the report?"); err != nil {
		fatal("bob DM", err)
	}
	bobDmTime := ms(time.Since(dmStart))

	// Charlie sends to Alice (they don't have a channel setup, so Alice won't receive it)
	dmStart = time.Now()
	if err := charlie.sendDM("alice", "Alice, I need your help!"); err != nil {
		fatal("charlie DM", err)
	}
	charlieDmTime := ms(time.Since(dmStart))

	fmt.Printf("\nDM send times: Alice: %.1fms, Bob: %.1fms, Charlie: %.1fms\n",
		aliceDmTime, bobDmTime, charlieDmTime)

	time.Sleep(200 * time.Millisecond)

	fmt.Println()
	fmt.Println("--- Private File Transfer ---")

	largeFile := map[string]any{
		"from":     "alice",
		"type":     "file",
		"filename": "confidential.pdf",
		"size":     3 * 1024 * 1024,
		"data":     buffer3MB,
	}

	if err := alice.sendDM("bob", "Sending you the confidential file..."); err != nil {
		fatal("alice file DM", err)
	}

	// send the large file directly (will be chunked automatically)
	if ch, ok := alice.channels["bob"]; ok {
		fileStart := time.Now()
		if err := ch.Send(largeFile); err != nil {
			fatal("file send", err)
		}
		fileTime := ms(time.Since(fileStart))
		throughput := float64(3*1024*1024) / ((fileTime / 1000) * 1024 * 1024)
		fmt.Printf("\nFile transfer completed: 3MB in %.1fms (%.1f MB/s)\n", fileTime, throughput)
	}

	time.Sleep(100 * time.Millisecond)

	_ = bobFromAlice.Close()
	_ = charlieFromBob.Close()
	alice.disconnect()
	bob.disconnect()
	charlie.disconnect()

	fmt.Println()
	fmt.Println("Direct messaging example completed!")
}

func formatMsg(data any) string {
	m, ok := data.(map[string]any)
	if !ok {
		return fmt.Sprintf("%v", data)
	}
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
