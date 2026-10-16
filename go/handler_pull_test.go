package rpc

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s not within a second", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func pullSessions(c *Client) int {
	n := 0
	c.pullIteratorCleanups.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

func TestPullCallbackCancelWhileNextIsPending(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	var inactiveAt atomic.Int64
	_, err := server.RegisterHandler("pc", map[string]any{
		"play": func(inv *CallbackInvoker) (<-chan struct{}, error) {
			ch := make(chan struct{})
			go func() {
				defer close(ch)
				ch <- struct{}{}
				// a slow batch that polls Active between frames
				for range 300 {
					if !inv.Active() {
						inactiveAt.Store(time.Now().UnixNano())
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
			}()
			return ch, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := client.CreateProxy("pc").InvokePullIteratorWithCallback(ctx, "play", PullCallbackMap{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	// the client sends the second next once the boundary is read
	time.Sleep(100 * time.Millisecond)
	cancelled := time.Now()
	cancel()

	waitFor(t, "producer saw the cancel", func() bool { return inactiveAt.Load() != 0 })
	if lag := time.Duration(inactiveAt.Load() - cancelled.UnixNano()); lag > 500*time.Millisecond {
		t.Errorf("cancel reached the producer after %v", lag)
	}
	waitFor(t, "session cleanup", func() bool { return pullSessions(server) == 0 })
}

func TestPullIteratorCancelWhileNextIsPending(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	_, err := server.RegisterHandler("pi", map[string]any{
		"items": func() (<-chan int, error) {
			ch := make(chan int)
			go func() {
				ch <- 1
				<-release
			}()
			return ch, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := client.CreateProxy("pi").InvokePullIterator(ctx, "items")
	if err != nil {
		t.Fatal(err)
	}
	if v := <-ch; v.Value == nil {
		t.Fatalf("first item = %+v", v)
	}
	time.Sleep(100 * time.Millisecond)
	cancel()

	waitFor(t, "session cleanup after cancel", func() bool { return pullSessions(server) == 0 })
}

func TestPullIteratorAnswersEveryPipelinedNext(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	_, err := server.RegisterHandler("pp", map[string]any{
		"items": func() (<-chan int, error) {
			ch := make(chan int)
			go func() {
				defer close(ch)
				for i := range 3 {
					time.Sleep(20 * time.Millisecond)
					ch <- i
				}
			}()
			return ch, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	const id = "pipe.1"
	responses := make(chan PullIteratorResponse, 8)
	unsub, err := client.Subscribe("_rpc.iterator."+id+".response", func(data []byte) {
		var resp PullIteratorResponse
		if DecodeMessageInto(data, &resp) == nil {
			responses <- resp
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unsub()
	if _, err := client.Call(context.Background(), "rpc.pp.items", PullIteratorParams{PullIterator: true, IteratorID: id}); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if err := client.Publish("_rpc.iterator."+id+".request", PullIteratorRequest{ID: id, Type: "next"}); err != nil {
			t.Fatal(err)
		}
	}

	var got []string
	for range 4 {
		select {
		case r := <-responses:
			got = append(got, r.Type)
		case <-time.After(2 * time.Second):
			t.Fatalf("responses so far %v, want one per next", got)
		}
	}
	if strings.Join(got, ",") != "value,value,value,done" {
		t.Errorf("responses = %v", got)
	}
}

func TestStreamCancelEndsIdleProducerLoop(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	_, err := server.RegisterHandler("si", map[string]any{
		"idle": func() (<-chan int, error) {
			ch := make(chan int)
			go func() {
				ch <- 1
				<-release
			}()
			return ch, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := client.CreateProxy("si").InvokeStream(ctx, "idle")
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	cancel()

	waitFor(t, "the stream loop to drop its cancel subscription", func() bool {
		server.subMu.Lock()
		defer server.subMu.Unlock()
		for _, e := range server.subEntries {
			if strings.HasSuffix(e.pattern, ".cancel") {
				return false
			}
		}
		return true
	})
}

func TestStreamCancelStopsLoopWhileValuesAreReady(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	// every receive finds a value ready, so only the done checks can see the cancel
	backlog := make(chan int, 1<<16)
	for i := range cap(backlog) {
		backlog <- i
	}
	// the cancelled stream drains the backlog until it closes
	t.Cleanup(func() { close(backlog) })
	_, err := server.RegisterHandler("sa", map[string]any{
		"backlog": func() (<-chan int, error) { return backlog, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := client.CreateProxy("sa").InvokeStream(ctx, "backlog")
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		<-ch
	}
	cancel()

	waitFor(t, "the stream loop to stop", func() bool {
		server.subMu.Lock()
		defer server.subMu.Unlock()
		for _, e := range server.subEntries {
			if strings.HasSuffix(e.pattern, ".cancel") {
				return false
			}
		}
		return true
	})
	if err := server.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := s.publishes(func(subject string) bool {
		return strings.HasPrefix(subject, "stream.rpc.sa.backlog.") && !strings.HasSuffix(subject, ".cancel")
	}); n >= cap(backlog)/2 {
		t.Errorf("the loop published %d of %d items after cancel", n, cap(backlog))
	}
}

func TestRecvUnlessDone(t *testing.T) {
	ready := make(chan int, 1)
	ready <- 7
	val, ok, stopped := recvUnlessDone(reflect.ValueOf(ready), make(chan struct{}))
	if stopped || !ok || val.Int() != 7 {
		t.Errorf("ready value: %v %v %v", val, ok, stopped)
	}

	closedCh := make(chan int)
	close(closedCh)
	if _, ok, stopped := recvUnlessDone(reflect.ValueOf(closedCh), make(chan struct{})); ok || stopped {
		t.Errorf("closed channel: ok %v, stopped %v, want the end of the channel", ok, stopped)
	}

	done := make(chan struct{})
	close(done)
	ready <- 8
	if _, _, stopped := recvUnlessDone(reflect.ValueOf(ready), done); !stopped {
		t.Error("a value ready after done was delivered")
	}

	idle := make(chan int)
	later := make(chan struct{})
	time.AfterFunc(20*time.Millisecond, func() { close(later) })
	start := time.Now()
	if _, _, stopped := recvUnlessDone(reflect.ValueOf(idle), later); !stopped {
		t.Error("done did not end a blocked receive")
	}
	if waited := time.Since(start); waited > time.Second {
		t.Errorf("blocked receive ended after %v", waited)
	}

	go func() { idle <- 9 }()
	if val, ok, stopped := recvUnlessDone(reflect.ValueOf(idle), make(chan struct{})); stopped || !ok || val.Int() != 9 {
		t.Errorf("blocked receive: %v %v %v", val, ok, stopped)
	}
}

func BenchmarkRecvUnlessDoneReady(b *testing.B) {
	ch := make(chan any, 1)
	rv := reflect.ValueOf(ch)
	done := make(chan struct{})
	item := any(map[string]any{"seq": 1})
	b.ReportAllocs()
	for b.Loop() {
		ch <- item
		if _, _, stopped := recvUnlessDone(rv, done); stopped {
			b.Fatal("stopped")
		}
	}
}

func sessionIDs(m *sync.Map) []string {
	var ids []string
	m.Range(func(k, _ any) bool {
		ids = append(ids, k.(string))
		return true
	})
	return ids
}

func TestEndedSessionsLeaveTheRegistration(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	cleanup, err := server.RegisterHandler("es", map[string]any{
		"items": func() (<-chan int, error) {
			ch := make(chan int, 2)
			ch <- 1
			ch <- 2
			close(ch)
			return ch, nil
		},
		"watch": func(func(int)) func() { return func() {} },
	})
	if err != nil {
		t.Fatal(err)
	}
	proxy := connectFake(t, s, nil).CreateProxy("es")

	items, err := proxy.InvokePullIterator(context.Background(), "items")
	if err != nil {
		t.Fatal(err)
	}
	pullIDs := sessionIDs(&server.pullIteratorCleanups)
	for range items {
	}
	waitFor(t, "the pull session to end", func() bool { return pullSessions(server) == 0 })

	unsubscribe, err := proxy.InvokeCallback("watch", nil, func(any) {})
	if err != nil {
		t.Fatal(err)
	}
	callbackIDs := sessionIDs(&server.callbackCleanups)
	unsubscribe()
	waitFor(t, "the callback session to end", func() bool { return len(sessionIDs(&server.callbackCleanups)) == 0 })
	if len(pullIDs) != 1 || len(callbackIDs) != 1 {
		t.Fatalf("live sessions: %d pull, %d callback, want 1 each", len(pullIDs), len(callbackIDs))
	}

	// cleanup runs what it finds under the ids it still tracks
	var stale atomic.Int32
	server.pullIteratorCleanups.Store(pullIDs[0], func() { stale.Add(1) })
	server.callbackCleanups.Store(callbackIDs[0], func() { stale.Add(1) })
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if n := stale.Load(); n != 0 {
		t.Errorf("the registration still tracked %d ended sessions", n)
	}
}

func TestCallbackInvokerSupports(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	supports := make(chan [2]bool, 1)
	if _, err := server.RegisterHandler("sup", map[string]any{
		"play": func(inv *CallbackInvoker) <-chan int {
			supports <- [2]bool{inv.Supports("onFrame"), inv.Supports("onBatch")}
			ch := make(chan int)
			close(ch)
			return ch
		},
	}); err != nil {
		t.Fatal(err)
	}
	caller := connectFake(t, s, nil)
	values, err := caller.CallPullIteratorWithCallback(context.Background(), "rpc.sup.play",
		PullCallbackMap{"onFrame": func(int) {}}, []string{"onFrame"})
	if err != nil {
		t.Fatal(err)
	}
	for range values {
	}
	if got := <-supports; got != [2]bool{true, false} {
		t.Errorf("Supports(onFrame), Supports(onBatch) = %v, want [true false]", got)
	}
}
