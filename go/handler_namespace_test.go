package rpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type nsDB struct{}

func (d *nsDB) Find(key string) string { return "found " + key }

type nsHandler struct {
	DB *nsDB `rpc:"db"`
}

func (h *nsHandler) Echo(s string) string { return s }

func (h *nsHandler) Size(b []byte) int { return len(b) }

func TestNamespaceSubscriptionServesMethods(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("nsx", &nsHandler{DB: &nsDB{}}, WithNamespaceSubscription()); err != nil {
		t.Fatal(err)
	}
	if n := s.subsWithPrefix("rpc.nsx."); n != 1 {
		t.Errorf("%d subscriptions for the namespace, want 1", n)
	}

	caller := connectFake(t, s, nil)
	proxy := caller.CreateProxy("nsx")
	if got, err := proxy.Invoke(context.Background(), "echo", "hi"); err != nil || got != "hi" {
		t.Errorf("echo = %v, %v", got, err)
	}
	if got, err := proxy.Sub("db").Invoke(context.Background(), "find", "k"); err != nil || got != "found k" {
		t.Errorf("db.find = %v, %v", got, err)
	}
	methods, err := caller.Call(context.Background(), "rpc.nsx.__methods")
	if err != nil || len(methods.([]any)) != 3 {
		t.Errorf("__methods = %v, %v", methods, err)
	}

	// a chunked request is put together before it reaches its method
	caller.SetMaxPayloadSize(4096)
	big := make([]byte, 50_000)
	if got, err := proxy.Invoke(context.Background(), "size", big); err != nil || fmt.Sprint(got) != "50000" {
		t.Errorf("size of a chunked request = %v, %v", got, err)
	}
}

func TestNamespaceSubscriptionAnswersUnknownMethodAtOnce(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("nsu", map[string]any{"echo": func(s string) string { return s }}, WithNamespaceSubscription()); err != nil {
		t.Fatal(err)
	}
	caller := connectFake(t, s, nil)

	start := time.Now()
	_, err := caller.Call(context.Background(), "rpc.nsu.missing")
	var rerr *RPCException
	if !errors.As(err, &rerr) || rerr.Code != ErrCodeMethodNotFound {
		t.Fatalf("err = %v, want METHOD_NOT_FOUND", err)
	}
	if rerr.Msg != "Method not found: missing" {
		t.Errorf("message = %q, want the text node and python send", rerr.Msg)
	}
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Errorf("METHOD_NOT_FOUND after %v, want at once", took)
	}

	values, err := caller.CallStream(context.Background(), "rpc.nsu.missingStream")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-values:
		if !errors.As(v.Error, &rerr) || rerr.Code != ErrCodeMethodNotFound {
			t.Errorf("stream got %+v, want METHOD_NOT_FOUND", v)
		}
	case <-time.After(time.Second):
		t.Fatal("the stream of an unknown method got nothing")
	}
}

func TestNamespaceSubscriptionMethodsDoNotWaitForEachOther(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	release := make(chan struct{})
	if _, err := server.RegisterHandler("nsw", map[string]any{
		"slow": func() { <-release },
		"fast": func() string { return "ok" },
	}, WithNamespaceSubscription()); err != nil {
		t.Fatal(err)
	}
	defer close(release)
	caller := connectFake(t, s, nil)

	go func() { _, _ = caller.Call(context.Background(), "rpc.nsw.slow") }()
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	if _, err := caller.Call(context.Background(), "rpc.nsw.fast"); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Errorf("fast waited %v behind slow", took)
	}
}

func TestNamespaceSubscriptionKeepsMethodsSerial(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	var inFlight, most atomic.Int32
	work := func() {
		n := inFlight.Add(1)
		for {
			m := most.Load()
			if n <= m || most.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inFlight.Add(-1)
	}
	if _, err := server.RegisterHandler("nss", map[string]any{"serial": work, "wide": work},
		WithNamespaceSubscription(), WithMethodConcurrency(map[string]int{"wide": 3})); err != nil {
		t.Fatal(err)
	}
	caller := connectFake(t, s, nil)

	for _, tc := range []struct {
		method string
		want   int32
	}{{"serial", 1}, {"wide", 3}} {
		most.Store(0)
		var wg sync.WaitGroup
		for range 9 {
			wg.Go(func() {
				if _, err := caller.Call(context.Background(), "rpc.nss."+tc.method); err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		if got := most.Load(); got != tc.want {
			t.Errorf("%s ran %d calls at once, want %d", tc.method, got, tc.want)
		}
	}
}

func TestNamespaceSubscriptionDrainsAndRestores(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, &ClientOptions{DrainTimeout: time.Second})
	started := make(chan struct{}, 1)
	cleanup, err := server.RegisterHandler("nsd", map[string]any{
		"work": func() string {
			started <- struct{}{}
			time.Sleep(50 * time.Millisecond)
			return "done"
		},
		"echo": func(s string) string { return s },
	}, WithNamespaceSubscription())
	if err != nil {
		t.Fatal(err)
	}
	caller := connectFake(t, s, nil)

	if err := server.Suspend(); err != nil {
		t.Fatal(err)
	}
	if err := server.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, err := caller.Call(context.Background(), "rpc.nsd.echo", "back"); err != nil || got != "back" {
		t.Fatalf("echo after Suspend and Connect = %v, %v", got, err)
	}

	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := caller.Call(context.Background(), "rpc.nsd.work")
			results <- err
		}()
	}
	<-started
	time.Sleep(10 * time.Millisecond)
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Errorf("a call the handler had received when it was unregistered: %v", err)
		}
	}
}

func TestNamespaceSubscriptionRefusesInternalNamespaces(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	for _, ns := range []string{"reply", "cb", "reply.abc"} {
		_, err := server.RegisterHandler(ns, map[string]any{"x": func() {}}, WithNamespaceSubscription())
		if err == nil || !strings.Contains(err.Error(), "namespace subscription") {
			t.Errorf("%s: err = %v", ns, err)
		}
	}
}

func TestDisconnectWithoutDrainDropsQueuedLaneCalls(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  HandlerOption
	}{
		{"namespace subscription", WithNamespaceSubscription()},
		{"drop expired", WithDropExpired()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := startFakeNATS(t)
			server := connectFake(t, s, &ClientOptions{DrainTimeout: -1})
			var runs atomic.Int32
			started := make(chan struct{}, 1)
			work := func() {
				select {
				case started <- struct{}{}:
				default:
				}
				time.Sleep(30 * time.Millisecond)
				runs.Add(1)
			}
			if _, err := server.RegisterHandler("lq", map[string]any{"work": work}, tc.opt); err != nil {
				t.Fatal(err)
			}
			caller := connectFake(t, s, nil)
			for range 10 {
				go func() { _, _ = caller.CallWithOptions(context.Background(), "rpc.lq.work", noRetry) }()
			}
			<-started
			time.Sleep(10 * time.Millisecond)

			_ = server.Disconnect()
			at := runs.Load()
			time.Sleep(150 * time.Millisecond)
			if after := runs.Load() - at; after > 1 {
				t.Errorf("%d queued calls ran after Disconnect, want at most the running one", after)
			}
		})
	}
}

func TestNamespaceSubscriptionGivesEachMethodItsTransfers(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("nsc", map[string]any{
		"a": func(b []byte) int { return len(b) },
		"b": func(b []byte) int { return len(b) },
	}, WithNamespaceSubscription()); err != nil {
		t.Fatal(err)
	}
	var entry *subscriptionEntry
	server.subMu.Lock()
	for _, e := range server.subEntries {
		if e.pattern == "rpc.nsc.>" {
			entry = e
		}
	}
	server.subMu.Unlock()
	open := func(lane string) int {
		cm := entry.chunks.loaded(lane)
		if cm == nil {
			return 0
		}
		cm.mu.Lock()
		defer cm.mu.Unlock()
		return len(cm.assemblers)
	}

	caller := connectFake(t, s, nil)
	payload := make([]byte, 1000)
	for i := range maxConcurrentTransfers + 1 {
		for _, subject := range []string{"rpc.nsc.a", "rpc.nsc.missing"} {
			if err := caller.nc.PublishMsg(chunkedMessages(t, subject, fmt.Sprint("t", i), payload)[0]); err != nil {
				t.Fatal(err)
			}
		}
	}
	_ = caller.nc.Flush()
	waitFor(t, "open transfers", func() bool {
		return open("a") == maxConcurrentTransfers && open("") == maxConcurrentTransfers
	})

	// a's 32 open transfers leave b its own
	caller.SetMaxPayloadSize(4096)
	proxy := caller.CreateProxy("nsc").WithOptions(RequestOptions{Timeout: 2 * time.Second})
	if got, err := proxy.Invoke(context.Background(), "b", make([]byte, 50_000)); err != nil || fmt.Sprint(got) != "50000" {
		t.Fatalf("a chunked request to b next to 32 open transfers of a = %v, %v", got, err)
	}

	entry.chunks.lanesMu.Lock()
	lanes := len(entry.chunks.lanes)
	entry.chunks.lanesMu.Unlock()
	if lanes != 2 {
		t.Errorf("%d method scopes, want one each for a and b, none for an unknown method", lanes)
	}

	if err := server.Suspend(); err != nil {
		t.Fatal(err)
	}
	entry.chunks.lanesMu.Lock()
	lanes = len(entry.chunks.lanes)
	entry.chunks.lanesMu.Unlock()
	if lanes != 0 || entry.chunks.manager.Load() != nil {
		t.Error("Suspend kept the transfers of the namespace subscription")
	}
}
