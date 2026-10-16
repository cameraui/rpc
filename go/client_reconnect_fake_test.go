package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func reconnectPeers(t *testing.T) (*fakeNATS, *Client, *nats.Conn) {
	t.Helper()
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("rc", map[string]any{"echo": func(v string) string { return v }}); err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, &ClientOptions{ReconnectWait: 300 * time.Millisecond, Timeout: 3 * time.Second})
	before := client.nc

	s.kick()
	waitReconnecting(t, before)
	return s, client, before
}

func waitReconnecting(t *testing.T, nc *nats.Conn) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !nc.IsReconnecting() {
		if time.Now().After(deadline) {
			t.Fatal("the client did not notice the dropped connection")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCallDuringReconnectKeepsTheConnection(t *testing.T) {
	s, client, before := reconnectPeers(t)

	got, err := client.CreateProxy("rc").Invoke(context.Background(), "echo", "back")
	if err != nil || got != "back" {
		t.Fatalf("call during reconnect = %v, %v", got, err)
	}
	if client.nc != before {
		t.Error("the call dialed a second connection")
	}
	if n := s.connCount(); n != 2 {
		t.Errorf("%d connections on the server, want 2", n)
	}
}

func TestConnectWhileReconnectingKeepsTheConnection(t *testing.T) {
	_, client, before := reconnectPeers(t)

	if err := client.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if client.nc != before {
		t.Error("Connect replaced a connection that was still reconnecting")
	}
	if err := client.Reconfigure(ReconfigureOptions{Servers: []string{"nats://127.0.0.1:1"}}); err == nil {
		t.Error("Reconfigure accepted a connection that was still reconnecting")
	}
}

func TestNeedsConnect(t *testing.T) {
	c := NewClient(ClientOptions{})
	if !c.needsConnect() {
		t.Error("a new client needs a connection")
	}
	_ = c.Disconnect()
	if c.needsConnect() {
		t.Error("a disconnected client must not reconnect on its own")
	}
}

func TestWriteBufferSize(t *testing.T) {
	s := startFakeNATS(t)
	for _, tc := range []struct {
		set, want int
	}{{0, 1 << 20}, {64 << 10, 64 << 10}, {-1, nats.DefaultWriteBufSize}} {
		c := connectFake(t, s, &ClientOptions{WriteBufferSize: tc.set})
		if got := c.nc.Opts.WriteBufferSize; got != tc.want {
			t.Errorf("WriteBufferSize %d: connection uses %d, want %d", tc.set, got, tc.want)
		}
	}
}

func TestCallDuringBrokerRestartSucceeds(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, &ClientOptions{ReconnectWait: 100 * time.Millisecond})
	if _, err := server.RegisterHandler("br", map[string]any{"echo": func(v string) string { return v }}); err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, &ClientOptions{ReconnectWait: 100 * time.Millisecond, Timeout: 3 * time.Second})
	time.Sleep(150 * time.Millisecond)

	s.setRefuse(true)
	s.kick()
	// a request written before the client sees the dropped socket is lost with it
	waitReconnecting(t, client.nc)
	result := make(chan error, 1)
	go func() {
		got, err := client.CreateProxy("br").Invoke(context.Background(), "echo", "back")
		if err == nil && got != "back" {
			err = fmt.Errorf("got %v", got)
		}
		result <- err
	}()
	time.Sleep(300 * time.Millisecond)
	s.setRefuse(false)

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("call during the outage = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the call did not finish")
	}
}
