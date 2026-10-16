package rpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func pendingCallID(t *testing.T, c *Client) string {
	t.Helper()
	var id string
	waitFor(t, "a pending call", func() bool {
		c.pendingRequests.Range(func(key, _ any) bool {
			id = key.(string)
			return false
		})
		return id != ""
	})
	return id
}

func TestConnectionClosedForGoodTearsDownAndRedials(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	if _, err := server.Subscribe("rpc.cg.silent", func([]byte) {}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.RegisterHandler("cg", map[string]any{
		"echo": func(v string) string { return v },
		"push": func(ctx context.Context) <-chan int { return parkedProducer(ctx, release, make(chan struct{})) },
		"pull": func(ctx context.Context) <-chan int { return parkedProducer(ctx, release, make(chan struct{})) },
	}); err != nil {
		t.Fatal(err)
	}
	if err := server.nc.Flush(); err != nil {
		t.Fatal(err)
	}

	client := connectFake(t, s, &ClientOptions{MaxReconnectAttempts: 1, ReconnectWait: 50 * time.Millisecond, Timeout: 5 * time.Second})
	events := make(chan []byte, 4)
	if _, err := client.Subscribe("cg.events", func(d []byte) { events <- d }); err != nil {
		t.Fatal(err)
	}
	servedDone := make(chan struct{})
	if _, err := client.RegisterHandler("cgc", map[string]any{
		"ping":   func() string { return "pong" },
		"parked": func(ctx context.Context) <-chan int { return parkedProducer(ctx, release, servedDone) },
	}); err != nil {
		t.Fatal(err)
	}
	blob := map[string]any{"blob": func(v string) int { return len(v) }}
	if _, err := NewRPCService(client).RegisterHandler(ServiceConfig{Name: "cgsvc", Version: "1.0.0"}, blob); err != nil {
		t.Fatal(err)
	}
	reconnected := make(chan struct{}, 1)
	client.OnReconnect(func() { reconnected <- struct{}{} })
	if err := client.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	entryCount := func() int {
		client.subMu.Lock()
		defer client.subMu.Unlock()
		return len(client.subEntries)
	}
	entries := entryCount()

	// a stream and a pull iterator the client consumes, and a stream it serves
	stream, err := client.CreateProxy("cg").InvokeStream(context.Background(), "push")
	if err != nil {
		t.Fatal(err)
	}
	pull, err := client.CreateProxy("cg").InvokePullIterator(context.Background(), "pull")
	if err != nil {
		t.Fatal(err)
	}
	served, err := server.CreateProxy("cgc").InvokeStream(context.Background(), "parked")
	if err != nil {
		t.Fatal(err)
	}
	<-stream
	<-pull
	<-served

	callErr := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), "rpc.cg.silent")
		callErr <- err
	}()
	id := pendingCallID(t, client)

	// a half-assembled transfer on the reply mux, the subscription and the service endpoint
	reply, err := EncodeMessage(RPCResponse{ID: id, Result: strings.Repeat("r", 1000)})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range chunkedMessages(t, "rpc.reply."+id, "m1", reply)[:2] {
		client.handleMuxMessage(m)
	}
	call, err := EncodeMessage(RPCMessage{ID: "b1", Method: "call", Params: []any{strings.Repeat("x", 1000)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range append(chunkedMessages(t, "cg.events", "e1", make([]byte, 1000))[:2], chunkedMessages(t, "blob", "s1", call)[:2]...) {
		if err := server.nc.PublishMsg(m); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "three open transfers", func() bool { return client.chunkBudget.used.Load() >= 3000 })

	s.setRefuse(true)
	s.kick()
	select {
	case err := <-callErr:
		var rerr *RPCException
		if !errors.As(err, &rerr) || rerr.Code != ErrCodeConnectionClosed {
			t.Fatalf("the pending call ended with %v, want CONNECTION_CLOSED", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the pending call was not rejected when the connection closed for good")
	}
	select {
	case v, ok := <-stream:
		if ok {
			t.Errorf("the consumed stream got %v after the close, want its end", v)
		}
	case <-time.After(time.Second):
		t.Error("the consumed stream did not end")
	}
	select {
	case v := <-pull:
		var rerr *RPCException
		if !errors.As(v.Error, &rerr) || rerr.Code != ErrCodeConnectionClosed {
			t.Errorf("the pull iterator ended with %v, want CONNECTION_CLOSED", v.Error)
		}
	case <-time.After(time.Second):
		t.Error("the pull iterator was not settled")
	}
	within(t, "the served stream's producer to stop", time.Second, servedDone)
	waitFor(t, "the ended sessions to drop their subscriptions", func() bool { return entryCount() <= entries })
	// the teardown releases the reply mux last, after the pending call already returned
	waitFor(t, "the transfers to release their bytes", func() bool { return client.chunkBudget.used.Load() == 0 })
	client.subMu.Lock()
	kept := len(client.subEntries)
	for _, e := range client.subEntries {
		if e.sub != nil || (e.chunks != nil && e.chunks.manager.Load() != nil) {
			t.Errorf("entry %s kept its subscription or chunk manager", e.pattern)
		}
	}
	client.subMu.Unlock()
	if kept != entries || client.IsClosed() {
		t.Errorf("%d of %d entries kept, closed %v; want all entries and not closed", kept, entries, client.IsClosed())
	}

	s.setRefuse(false)
	waitFor(t, "the server to reconnect", server.IsConnected)
	got, err := client.CreateProxy("cg").Invoke(context.Background(), "echo", "back")
	if err != nil || got != "back" {
		t.Fatalf("call after the close = %v, %v", got, err)
	}
	select {
	case <-reconnected:
	case <-time.After(time.Second):
		t.Error("OnReconnect did not run after the replaced connection")
	}
	if err := server.Publish("cg.events", "hello"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Error("the subscription was not restored")
	}
	if got, err := server.CreateProxy("cgc").Invoke(context.Background(), "ping"); err != nil || got != "pong" {
		t.Errorf("the handler was not restored: %v, %v", got, err)
	}
}

func TestSuspendAndDisconnectReleaseEndpointTransfers(t *testing.T) {
	s := startFakeNATS(t)
	sender := connectFake(t, s, nil)
	client := connectFake(t, s, nil)
	call, err := EncodeMessage(RPCMessage{ID: "b1", Method: "call", Params: []any{strings.Repeat("x", 1000)}})
	if err != nil {
		t.Fatal(err)
	}
	openTransfer := func(name string) {
		t.Helper()
		blob := map[string]any{"blob": func(v string) int { return len(v) }}
		if _, err := NewRPCService(client).RegisterHandler(ServiceConfig{Name: name, Version: "1.0.0"}, blob); err != nil {
			t.Fatal(err)
		}
		if err := client.nc.Flush(); err != nil {
			t.Fatal(err)
		}
		for _, m := range chunkedMessages(t, "blob", name, call)[:2] {
			if err := sender.nc.PublishMsg(m); err != nil {
				t.Fatal(err)
			}
		}
		waitFor(t, "an open endpoint transfer", func() bool { return client.chunkBudget.used.Load() > 0 })
	}

	openTransfer("first")
	if err := client.Suspend(); err != nil {
		t.Fatal(err)
	}
	if used := client.chunkBudget.used.Load(); used != 0 {
		t.Errorf("Suspend keeps %d bytes of an endpoint transfer", used)
	}

	if err := client.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	openTransfer("second")
	if err := client.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if used := client.chunkBudget.used.Load(); used != 0 {
		t.Errorf("Disconnect keeps %d bytes of an endpoint transfer", used)
	}
}

func TestOwnCloseIsNotAConnectionLoss(t *testing.T) {
	s := startFakeNATS(t)
	client := connectFake(t, s, nil)
	reconnected := make(chan struct{}, 1)
	client.OnReconnect(func() { reconnected <- struct{}{} })
	if _, err := client.Subscribe("own.close", func([]byte) {}); err != nil {
		t.Fatal(err)
	}

	before := client.nc
	if err := client.Suspend(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the suspended connection to close", before.IsClosed)
	// nats.go runs the closed handler on its own goroutine
	time.Sleep(50 * time.Millisecond)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reconnected:
		t.Error("a Connect after Suspend ran the OnReconnect handlers")
	case <-time.After(100 * time.Millisecond):
	}
	client.subMu.Lock()
	for _, e := range client.subEntries {
		if e.sub == nil || !e.sub.IsValid() {
			t.Errorf("entry %s was not restored", e.pattern)
		}
	}
	client.subMu.Unlock()
}

func TestConnectRunsAPendingCloseHandler(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.Subscribe("rpc.cp.silent", func([]byte) {}); err != nil {
		t.Fatal(err)
	}
	if err := server.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, &ClientOptions{Timeout: 5 * time.Second})
	reconnected := make(chan struct{}, 1)
	client.OnReconnect(func() { reconnected <- struct{}{} })
	callErr := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), "rpc.cp.silent")
		callErr <- err
	}()
	pendingCallID(t, client)

	// closed for good, but nats.go has not run the closed handler yet
	client.mu.RLock()
	nc := client.nc
	client.mu.RUnlock()
	nc.SetClosedHandler(nil)
	nc.Close()

	if err := client.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-callErr:
		var rerr *RPCException
		if !errors.As(err, &rerr) || rerr.Code != ErrCodeConnectionClosed {
			t.Errorf("the pending call ended with %v, want CONNECTION_CLOSED", err)
		}
	case <-time.After(time.Second):
		t.Error("Connect did not tear down the closed connection")
	}
	select {
	case <-reconnected:
	case <-time.After(time.Second):
		t.Error("OnReconnect did not run after the replaced connection")
	}
	if !client.IsConnected() {
		t.Error("Connect left the client without a connection")
	}
}
