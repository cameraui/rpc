package rpc

import (
	"context"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type concurrentHandler struct {
	limits  map[string]int
	running atomic.Int32
	peak    atomic.Int32
	mu      sync.Mutex
	seen    []int
}

func (h *concurrentHandler) RPCConcurrency() map[string]int {
	return h.limits
}

func (h *concurrentHandler) Slow(ms int) int {
	n := h.running.Add(1)
	for {
		peak := h.peak.Load()
		if n <= peak || h.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	time.Sleep(time.Duration(ms) * time.Millisecond)
	h.running.Add(-1)
	return ms
}

func (h *concurrentHandler) Record(seq int, _ []byte) {
	time.Sleep(time.Duration(rand.IntN(300)) * time.Microsecond)
	h.mu.Lock()
	h.seen = append(h.seen, seq)
	h.mu.Unlock()
}

func callSlowParallel(t *testing.T, proxy *Proxy, n int, args ...any) time.Duration {
	t.Helper()
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Go(func() {
			if _, err := proxy.Invoke(context.Background(), "slow", args...); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	return time.Since(start)
}

func concurrencyPeers(t *testing.T, h any, opts ...HandlerOption) (*Client, *Proxy, CleanupFunc) {
	t.Helper()
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	cleanup, err := server.RegisterHandler("cc", h, opts...)
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)
	return client, client.CreateProxy("cc"), cleanup
}

func TestMethodConcurrencyRunsCallsTogether(t *testing.T) {
	h := &concurrentHandler{limits: map[string]int{"slow": 4}}
	_, proxy, _ := concurrencyPeers(t, h)

	if elapsed := callSlowParallel(t, proxy, 4, 100); elapsed > 300*time.Millisecond {
		t.Errorf("4 calls of 100 ms took %v", elapsed)
	}
	if peak := h.peak.Load(); peak != 4 {
		t.Errorf("peak in flight %d, want 4", peak)
	}
}

func TestMethodConcurrencyLimitBoundsCallsInFlight(t *testing.T) {
	h := &concurrentHandler{limits: map[string]int{"slow": 2}}
	_, proxy, _ := concurrencyPeers(t, h)

	callSlowParallel(t, proxy, 6, 30)
	if peak := h.peak.Load(); peak != 2 {
		t.Errorf("peak in flight %d, want 2", peak)
	}
}

func TestWithMethodConcurrencyOverridesTheInterface(t *testing.T) {
	h := &concurrentHandler{limits: map[string]int{"slow": 4}}
	_, proxy, _ := concurrencyPeers(t, h, WithMethodConcurrency(map[string]int{"slow": 1}))

	callSlowParallel(t, proxy, 3, 30)
	if peak := h.peak.Load(); peak != 1 {
		t.Errorf("peak in flight %d, want 1 with the override", peak)
	}

	other := &concurrentHandler{}
	_, mapProxy, _ := concurrencyPeers(t, map[string]any{"slow": other.Slow}, WithMethodConcurrency(map[string]int{"slow": 3}))
	callSlowParallel(t, mapProxy, 3, 50)
	if peak := other.peak.Load(); peak != 3 {
		t.Errorf("map handler peak %d, want 3", peak)
	}
}

func TestRPCConcurrencyIsNotExposed(t *testing.T) {
	methods := ExtractMethods(&concurrentHandler{})
	for name := range methods {
		if strings.EqualFold(name, "rpcConcurrency") {
			t.Errorf("%s is exposed over RPC", name)
		}
	}
	if _, ok := methods["slow"]; !ok {
		t.Error("slow is missing")
	}
}

func TestCleanupWaitsForConcurrentCalls(t *testing.T) {
	h := &concurrentHandler{limits: map[string]int{"slow": 2}}
	_, proxy, cleanup := concurrencyPeers(t, h)

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { _, _ = proxy.Invoke(context.Background(), "slow", 200) })
	}
	waitFor(t, "both calls running", func() bool { return h.running.Load() == 2 })
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if n := h.running.Load(); n != 0 {
		t.Errorf("cleanup returned with %d calls still running", n)
	}
	wg.Wait()
}

func TestChunkedRequestsUseTheLimit(t *testing.T) {
	h := &concurrentHandler{limits: map[string]int{"slow": 3}}
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("ck", h); err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, &ClientOptions{MaxPayloadSize: 16*1024 + 8192})

	// the 100 KB extra argument makes every request a chunked transfer
	if elapsed := callSlowParallel(t, client.CreateProxy("ck"), 3, 100, make([]byte, 100*1024)); elapsed > 250*time.Millisecond {
		t.Errorf("3 chunked calls of 100 ms took %v", elapsed)
	}
	if peak := h.peak.Load(); peak != 3 {
		t.Errorf("peak in flight %d, want 3", peak)
	}
}

func TestDefaultDispatchKeepsArrivalOrder(t *testing.T) {
	h := &concurrentHandler{limits: map[string]int{"slow": 4}}
	client, _, _ := concurrencyPeers(t, h)

	const n = 300
	for i := range n {
		msg := RPCMessage{ID: client.generateID(), Method: "call", Params: []any{i, nil}}
		if err := client.Publish("rpc.cc.record", msg); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "all calls recorded", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.seen) == n
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, seq := range h.seen {
		if seq != i {
			t.Fatalf("call %d ran at position %d", seq, i)
		}
	}
}

type gatedHandler struct {
	gate    chan struct{}
	mu      sync.Mutex
	started []int
}

func (h *gatedHandler) RPCConcurrency() map[string]int {
	return map[string]int{"wait": 2}
}

func (h *gatedHandler) Wait(seq int) int {
	h.mu.Lock()
	h.started = append(h.started, seq)
	h.mu.Unlock()
	<-h.gate
	return seq
}

func (h *gatedHandler) startedCalls() []int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.started)
}

func TestMethodConcurrencyStartsCallsInArrivalOrder(t *testing.T) {
	h := &gatedHandler{gate: make(chan struct{})}
	client, _, _ := concurrencyPeers(t, h)

	const n = 20
	for i := range n {
		msg := RPCMessage{ID: client.generateID(), Method: "call", Params: []any{i}}
		if err := client.Publish("rpc.cc.wait", msg); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "two calls running", func() bool { return len(h.startedCalls()) == 2 })
	if first := h.startedCalls(); !slices.Contains(first, 0) || !slices.Contains(first, 1) {
		t.Fatalf("calls %v started first, want 0 and 1", first)
	}

	// each finished call frees one slot, which goes to the oldest waiting call
	for next := 2; next < n; next++ {
		h.gate <- struct{}{}
		waitFor(t, "the next call", func() bool { return len(h.startedCalls()) == next+1 })
		if got := h.startedCalls()[next]; got != next {
			t.Fatalf("call %d started when call %d was next", got, next)
		}
	}
	for range 2 {
		h.gate <- struct{}{}
	}
}

func TestConcurrentPanicIsAnsweredAndFreesItsSlot(t *testing.T) {
	handlers := map[string]any{"boom": func() { panic("boom") }}
	_, proxy, _ := concurrencyPeers(t, handlers, WithPanicRecovery(), WithMethodConcurrency(map[string]int{"boom": 2}))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// more calls than slots: a slot kept by a panicked call would block the third
	for range 3 {
		_, err := proxy.Invoke(ctx, "boom")
		if msg := internalErrorMessage(t, err); msg != "handler panic: boom" {
			t.Fatalf("message = %q", msg)
		}
	}
}
