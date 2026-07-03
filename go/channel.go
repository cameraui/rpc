package rpc

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Channel is a bidirectional broadcast communication channel.
type Channel struct {
	client    *Client
	channelID string
	subject   string

	mu            sync.RWMutex
	closed        bool
	unsub         func()
	requestUnsubs []func()

	onMessage func(data any)
	onClose   func()
	onError   func(err error)

	isolatedClient *Client
}

// NewChannel creates a new broadcast channel. Call Init() before use.
func NewChannel(client *Client, channelID string) *Channel {
	return &Channel{
		client:    client,
		channelID: channelID,
		subject:   "channel." + channelID,
	}
}

// ID returns the channel identifier.
func (ch *Channel) ID() string {
	return ch.channelID
}

// IsClosed returns whether the channel is closed.
func (ch *Channel) IsClosed() bool {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return ch.closed
}

// Init subscribes to the channel subject. Must be called before Send/Close.
func (ch *Channel) Init() error {
	unsub, err := ch.client.Subscribe(ch.subject, func(data []byte) {
		var msg ChannelMessage
		if err := DecodeMessageInto(data, &msg); err != nil {
			return
		}

		switch msg.Type {
		case "message":
			if ch.onMessage != nil {
				ch.onMessage(msg.Data)
			}
		case "close":
			ch.handleClose()
		case "error":
			if ch.onError != nil {
				errMsg := msg.Error
				if errMsg == "" {
					errMsg = "Channel error"
				}
				ch.onError(fmt.Errorf("%s", errMsg))
			}
		}
	})
	if err != nil {
		return err
	}
	ch.unsub = unsub
	return nil
}

// Send publishes data to the channel.
func (ch *Channel) Send(data any) error {
	ch.mu.RLock()
	if ch.closed {
		ch.mu.RUnlock()
		return fmt.Errorf("channel is closed")
	}
	ch.mu.RUnlock()

	msg := ChannelMessage{Type: "message", Data: data}
	return ch.client.Publish(ch.subject, msg)
}

// Request sends a request on the channel and waits for a reply.
func (ch *Channel) Request(ctx context.Context, data any, timeout ...time.Duration) ([]byte, error) {
	ch.mu.RLock()
	if ch.closed {
		ch.mu.RUnlock()
		return nil, fmt.Errorf("channel is closed")
	}
	ch.mu.RUnlock()

	t := 5 * time.Second
	if len(timeout) > 0 {
		t = timeout[0]
	}
	return ch.client.Request(ctx, ch.subject+".request", data, t)
}

// OnRequest registers a handler for request/reply on this channel.
// The subscription is tracked so Close()/cleanup releases it.
func (ch *Channel) OnRequest(handler func(data []byte) (any, error)) (func(), error) {
	unsub, err := ch.client.OnRequest(ch.subject+".request", handler)
	if err != nil {
		return nil, err
	}
	ch.mu.Lock()
	ch.requestUnsubs = append(ch.requestUnsubs, unsub)
	ch.mu.Unlock()
	return unsub, nil
}

// OnMessage sets the callback for incoming messages.
func (ch *Channel) OnMessage(fn func(data any)) {
	ch.onMessage = fn
}

// OnClose sets the callback for channel close events.
func (ch *Channel) OnClose(fn func()) {
	ch.onClose = fn
}

// OnError sets the callback for channel errors.
func (ch *Channel) OnError(fn func(err error)) {
	ch.onError = fn
}

// Close gracefully closes the channel.
func (ch *Channel) Close() error {
	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return nil
	}
	ch.closed = true
	ch.mu.Unlock()

	// Notify other side
	msg := ChannelMessage{Type: "close"}
	_ = ch.client.Publish(ch.subject, msg)

	return ch.cleanup()
}

func (ch *Channel) handleClose() {
	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return
	}
	ch.closed = true
	ch.mu.Unlock()

	if ch.onClose != nil {
		ch.onClose()
	}
	_ = ch.cleanup()
}

func (ch *Channel) cleanup() error {
	ch.onMessage = nil
	ch.onClose = nil
	ch.onError = nil

	// Release request/reply subscriptions collected via OnRequest.
	ch.mu.Lock()
	requestUnsubs := ch.requestUnsubs
	ch.requestUnsubs = nil
	ch.mu.Unlock()
	for _, unsub := range requestUnsubs {
		unsub()
	}

	if ch.unsub != nil {
		ch.unsub()
		ch.unsub = nil
	}

	if ch.isolatedClient != nil {
		return ch.isolatedClient.Disconnect()
	}
	return nil
}

// PrivateChannel is a 1:1 communication channel between two specific clients.
type PrivateChannel struct {
	client         *Client
	channelID      string
	clientID       string
	targetClientID string
	subject        string

	mu             sync.RWMutex
	closed         bool
	unsub          func()
	requestUnsubs  []func()
	remoteClientID string

	onMessage func(data any)
	onClose   func()
	onError   func(err error)

	isolatedClient *Client
}

// NewPrivateChannel creates a new private 1:1 channel. Call Init() before use.
func NewPrivateChannel(client *Client, channelID, targetClientID string) *PrivateChannel {
	// Derive clientID from client name, stripping any isolation suffixes
	name := client.Options.Name
	// Remove suffixes like "-channel-xxx" or "-private-xxx"
	for _, suffix := range []string{"-channel-", "-private-"} {
		if idx := strings.Index(name, suffix); idx != -1 {
			name = name[:idx]
		}
	}

	// Sort IDs to create a deterministic subject
	ids := []string{name, targetClientID}
	sort.Strings(ids)
	subject := fmt.Sprintf("channel.private.%s.%s", channelID, strings.Join(ids, "."))

	return &PrivateChannel{
		client:         client,
		channelID:      channelID,
		clientID:       name,
		targetClientID: targetClientID,
		subject:        subject,
	}
}

// ID returns the channel identifier.
func (ch *PrivateChannel) ID() string {
	return ch.channelID
}

// RemoteID returns the remote client ID once connected.
func (ch *PrivateChannel) RemoteID() string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return ch.remoteClientID
}

// IsClosed returns whether the channel is closed.
func (ch *PrivateChannel) IsClosed() bool {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return ch.closed
}

// Init subscribes to the private channel and sends a handshake.
func (ch *PrivateChannel) Init() error {
	unsub, err := ch.client.Subscribe(ch.subject, func(data []byte) {
		var msg ChannelMessage
		if err := DecodeMessageInto(data, &msg); err != nil {
			return
		}

		// Filter: skip own messages
		if msg.Sender == ch.clientID {
			return
		}
		// Only accept from target
		if msg.Sender != ch.targetClientID {
			return
		}

		ch.mu.Lock()
		if ch.remoteClientID == "" && msg.Sender != "" {
			ch.remoteClientID = msg.Sender
		}
		remote := ch.remoteClientID
		ch.mu.Unlock()

		if remote != "" && msg.Sender != remote {
			return
		}

		switch msg.Type {
		case "message":
			// Filter handshake messages
			if m, ok := msg.Data.(map[string]any); ok {
				if _, isHandshake := m["__handshake"]; isHandshake {
					return
				}
			}
			if ch.onMessage != nil {
				ch.onMessage(msg.Data)
			}
		case "close":
			ch.handleClose()
		case "error":
			if ch.onError != nil {
				errMsg := msg.Error
				if errMsg == "" {
					errMsg = "Channel error"
				}
				ch.onError(fmt.Errorf("%s", errMsg))
			}
		}
	})
	if err != nil {
		return err
	}
	ch.unsub = unsub

	// Send handshake
	_ = ch.sendRaw(ChannelMessage{
		Type:   "message",
		Data:   HandshakeData{Handshake: true},
		Sender: ch.clientID,
	})

	return nil
}

// Send publishes data to the private channel.
func (ch *PrivateChannel) Send(data any) error {
	ch.mu.RLock()
	if ch.closed {
		ch.mu.RUnlock()
		return fmt.Errorf("channel is closed")
	}
	ch.mu.RUnlock()

	return ch.sendRaw(ChannelMessage{
		Type:   "message",
		Data:   data,
		Sender: ch.clientID,
	})
}

// Request sends a request on the private channel and waits for a reply.
func (ch *PrivateChannel) Request(ctx context.Context, data any, timeout ...time.Duration) ([]byte, error) {
	ch.mu.RLock()
	if ch.closed {
		ch.mu.RUnlock()
		return nil, fmt.Errorf("channel is closed")
	}
	ch.mu.RUnlock()

	t := 5 * time.Second
	if len(timeout) > 0 {
		t = timeout[0]
	}

	ids := []string{ch.clientID, ch.targetClientID}
	sort.Strings(ids)
	subject := fmt.Sprintf("channel.private.%s.%s.request", ch.channelID, strings.Join(ids, "."))
	return ch.client.Request(ctx, subject, data, t)
}

// OnRequest registers a handler for request/reply on this private channel.
// The subscription is tracked so Close()/cleanup releases it.
func (ch *PrivateChannel) OnRequest(handler func(data []byte) (any, error)) (func(), error) {
	ids := []string{ch.clientID, ch.targetClientID}
	sort.Strings(ids)
	subject := fmt.Sprintf("channel.private.%s.%s.request", ch.channelID, strings.Join(ids, "."))
	unsub, err := ch.client.OnRequest(subject, handler)
	if err != nil {
		return nil, err
	}
	ch.mu.Lock()
	ch.requestUnsubs = append(ch.requestUnsubs, unsub)
	ch.mu.Unlock()
	return unsub, nil
}

// OnMessage sets the callback for incoming messages.
func (ch *PrivateChannel) OnMessage(fn func(data any)) {
	ch.onMessage = fn
}

// OnClose sets the callback for channel close events.
func (ch *PrivateChannel) OnClose(fn func()) {
	ch.onClose = fn
}

// OnError sets the callback for channel errors.
func (ch *PrivateChannel) OnError(fn func(err error)) {
	ch.onError = fn
}

// Close gracefully closes the private channel.
func (ch *PrivateChannel) Close() error {
	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return nil
	}
	ch.closed = true
	ch.mu.Unlock()

	_ = ch.sendRaw(ChannelMessage{
		Type:   "close",
		Sender: ch.clientID,
	})

	return ch.cleanup()
}

// IsConnectedTo checks if the channel is connected to a specific client.
func (ch *PrivateChannel) IsConnectedTo(clientID string) bool {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return ch.remoteClientID == clientID
}

func (ch *PrivateChannel) handleClose() {
	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return
	}
	ch.closed = true
	ch.mu.Unlock()

	if ch.onClose != nil {
		ch.onClose()
	}
	_ = ch.cleanup()
}

func (ch *PrivateChannel) cleanup() error {
	ch.onMessage = nil
	ch.onClose = nil
	ch.onError = nil

	// Release request/reply subscriptions collected via OnRequest.
	ch.mu.Lock()
	requestUnsubs := ch.requestUnsubs
	ch.requestUnsubs = nil
	ch.mu.Unlock()
	for _, unsub := range requestUnsubs {
		unsub()
	}

	if ch.unsub != nil {
		ch.unsub()
		ch.unsub = nil
	}

	if ch.isolatedClient != nil {
		return ch.isolatedClient.Disconnect()
	}
	return nil
}

func (ch *PrivateChannel) sendRaw(msg ChannelMessage) error {
	return ch.client.Publish(ch.subject, msg)
}

// Channel creates or joins a broadcast channel.
func (c *Client) Channel(channelID string, isolated ...bool) (*Channel, error) {
	client := c
	var isolatedClient *Client

	if len(isolated) > 0 && isolated[0] {
		client = c.createIsolatedClient("channel-" + channelID)
		c.isolatedClientsMu.Lock()
		c.isolatedClients = append(c.isolatedClients, client)
		c.isolatedClientsMu.Unlock()
		if err := client.Connect(context.Background()); err != nil {
			return nil, err
		}
		isolatedClient = client
	}

	ch := NewChannel(client, channelID)
	ch.isolatedClient = isolatedClient
	if err := ch.Init(); err != nil {
		if isolatedClient != nil {
			_ = isolatedClient.Disconnect()
			c.removeIsolatedClient(isolatedClient)
		}
		return nil, err
	}
	return ch, nil
}

// PrivateChannelConnect creates or joins a private 1:1 channel.
func (c *Client) PrivateChannelConnect(channelID, targetClientID string, isolated ...bool) (*PrivateChannel, error) {
	client := c
	var isolatedClient *Client

	if len(isolated) > 0 && isolated[0] {
		client = c.createIsolatedClient("private-" + channelID)
		c.isolatedClientsMu.Lock()
		c.isolatedClients = append(c.isolatedClients, client)
		c.isolatedClientsMu.Unlock()
		if err := client.Connect(context.Background()); err != nil {
			return nil, err
		}
		isolatedClient = client
	}

	ch := NewPrivateChannel(client, channelID, targetClientID)
	ch.isolatedClient = isolatedClient
	if err := ch.Init(); err != nil {
		if isolatedClient != nil {
			_ = isolatedClient.Disconnect()
			c.removeIsolatedClient(isolatedClient)
		}
		return nil, err
	}
	return ch, nil
}
