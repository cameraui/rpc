package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestServiceDiscoveryWaitsForTheFirstReply(t *testing.T) {
	s := startFakeNATS(t)
	responders := connectFake(t, s, nil)
	nc := responders.nc
	info, _ := json.Marshal(ServiceInfoResponse{Name: "late", ID: "x1"})
	if _, err := nc.Subscribe("$SRV.INFO.late", func(msg *nats.Msg) {
		time.Sleep(30 * time.Millisecond)
		_ = msg.Respond(info)
	}); err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		reply, _ := json.Marshal(ServiceInfoResponse{Name: "many", ID: fmt.Sprint(i)})
		if _, err := nc.Subscribe("$SRV.INFO.many", func(msg *nats.Msg) { _ = msg.Respond(reply) }); err != nil {
			t.Fatal(err)
		}
	}
	_ = nc.Flush()
	monitor := NewServiceMonitor(connectFake(t, s, nil))

	if got, err := monitor.Info(context.Background(), "late"); err != nil || len(got) != 1 {
		t.Errorf("a service answering after 30ms: %d found, %v", len(got), err)
	}
	if got, err := monitor.Info(context.Background(), "many"); err != nil || len(got) != 40 {
		t.Errorf("40 instances: %d found, %v", len(got), err)
	}
	start := time.Now()
	if got, err := monitor.Info(context.Background(), "missing"); err != nil || len(got) != 0 {
		t.Errorf("a missing service: %d found, %v", len(got), err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("a missing service took %v, the no-responder status must end the lookup", took)
	}
}

type whoami struct{ name string }

func (w *whoami) Whoami() string { return w.name }

func (w *whoami) Size(b []byte) int { return len(b) }

func (w *whoami) Watch(cb func(int)) func() {
	for i := range 3 {
		cb(i)
	}
	return func() {}
}

func (w *whoami) Frames(inv *CallbackInvoker) <-chan int {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := range 3 {
			inv.Invoke("onFrame", i)
			select {
			case ch <- i:
			case <-inv.Context().Done():
				return
			}
		}
	}()
	return ch
}

func registerService(t *testing.T, client *Client, name string) {
	t.Helper()
	svc, err := NewRPCService(client).RegisterHandler(ServiceConfig{Name: name, Version: "1.0.0"}, &whoami{name: name})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop() })
}

func TestServicesSharingAMethodNameStayApart(t *testing.T) {
	s := startFakeNATS(t)
	registerService(t, connectFake(t, s, nil), "alpha")
	registerService(t, connectFake(t, s, nil), "beta")
	caller := connectFake(t, s, nil)

	for _, name := range []string{"alpha", "beta"} {
		proxy, err := caller.CreateServiceProxy(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		for range 20 {
			if got, err := proxy.Invoke(context.Background(), "whoami"); err != nil || got != name {
				t.Fatalf("%s.whoami = %v, %v", name, got, err)
			}
		}
	}
}

func TestServiceChunkedRequestReachesOneInstance(t *testing.T) {
	s := startFakeNATS(t)
	for range 3 {
		registerService(t, connectFake(t, s, nil), "big")
	}
	caller := connectFake(t, s, nil)
	proxy, err := caller.CreateServiceProxy(context.Background(), "big")
	if err != nil {
		t.Fatal(err)
	}
	caller.SetMaxPayloadSize(4096)

	big := make([]byte, 50_000)
	for i := range 10 {
		got, err := proxy.Invoke(context.Background(), "size", big)
		if err != nil || fmt.Sprint(got) != "50000" {
			t.Fatalf("chunked request %d: %v, %v", i, got, err)
		}
	}
}

func TestServiceServesCallbacksAndPullCallbacks(t *testing.T) {
	s := startFakeNATS(t)
	registerService(t, connectFake(t, s, nil), "cb")
	caller := connectFake(t, s, nil)
	proxy, err := caller.CreateServiceProxy(context.Background(), "cb")
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var got []any
	done := make(chan struct{})
	unsub, err := proxy.InvokeCallback("watch", nil, func(v any) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, v)
		if len(got) == 3 {
			close(done)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unsub()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("callback got %v, want 3 values", got)
	}

	frames := make(chan int, 3)
	batches, err := proxy.InvokePullIteratorWithCallback(context.Background(), "frames",
		PullCallbackMap{"onFrame": func(i int) { frames <- i }}, []string{"onFrame"})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for v := range batches {
		if v.Error != nil {
			t.Fatal(v.Error)
		}
		n++
	}
	if n != 3 || len(frames) != 3 {
		t.Errorf("%d batches and %d frames, want 3 and 3", n, len(frames))
	}
}

func TestServiceSurvivesABacklog(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	release := make(chan struct{})
	svc, err := NewRPCService(server).RegisterHandler(ServiceConfig{Name: "backlog", Version: "1.0.0"}, map[string]any{
		"block": func(b []byte) int { <-release; return len(b) },
		"ping":  func() string { return "pong" },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = svc.Stop() }()

	caller := connectFake(t, s, nil)
	payload := make([]byte, 900_000)
	for range 80 {
		_ = caller.Publish("block", RPCMessage{ID: caller.generateID(), Method: "call", Params: []any{payload}})
	}
	_ = caller.nc.Flush()
	time.Sleep(100 * time.Millisecond)
	close(release)

	if svc.Stopped() {
		t.Fatal("a backlog over the nats.go pending limit stopped the service")
	}
	proxy, err := caller.CreateServiceProxy(context.Background(), "backlog")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := proxy.Invoke(context.Background(), "ping"); err != nil || got != "pong" {
		t.Errorf("ping after the backlog = %v, %v", got, err)
	}
}
