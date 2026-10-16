package rpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type drainResult struct {
	value any
	err   error
	at    time.Duration
}

func drainPeers(t *testing.T, serverOpts *ClientOptions, handlers map[string]any, opts ...HandlerOption) (*fakeNATS, *Client, CleanupFunc, *Proxy) {
	t.Helper()
	s := startFakeNATS(t)
	server := connectFake(t, s, serverOpts)
	cleanup, err := server.RegisterHandler("dr", handlers, opts...)
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, &ClientOptions{Timeout: 3 * time.Second})
	proxy := client.CreateProxy("dr").WithOptions(RequestOptions{NoResponderRetry: &NoResponderRetryOptions{MaxRetries: -1}})
	return s, server, cleanup, proxy
}

// n calls in arrival order, each result carries the time it took
func callsInBackground(proxy *Proxy, method string, args []any, n int) []chan drainResult {
	results := make([]chan drainResult, 0, n)
	start := time.Now()
	for range n {
		ch := make(chan drainResult, 1)
		results = append(results, ch)
		go func() {
			v, err := proxy.Invoke(context.Background(), method, args...)
			ch <- drainResult{v, err, time.Since(start)}
		}()
		// keeps the arrival order on the serial method
		time.Sleep(5 * time.Millisecond)
	}
	return results
}

func waitStarted(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the handler did not start")
	}
}

func isConnectionClosed(err error) bool {
	var rerr *RPCException
	return errors.As(err, &rerr) && rerr.Code == ErrCodeConnectionClosed
}

func TestDisconnectAnswersCallsAlreadyReceived(t *testing.T) {
	started := make(chan struct{}, 8)
	_, server, _, proxy := drainPeers(t, nil, map[string]any{
		"slow": func(ms int) int {
			started <- struct{}{}
			time.Sleep(time.Duration(ms) * time.Millisecond)
			return ms
		},
	})

	results := callsInBackground(proxy, "slow", []any{100}, 3)
	waitStarted(t, started)
	stopped := time.Now()
	if err := server.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(stopped); took > 600*time.Millisecond {
		t.Errorf("Disconnect took %v", took)
	}

	var answered, closed int
	for i, ch := range results {
		r := <-ch
		switch {
		case r.err == nil && fmt.Sprint(r.value) == "100":
			answered++
		case isConnectionClosed(r.err):
			closed++
		default:
			t.Errorf("call %d = %v, %v after %v, want its result or CONNECTION_CLOSED", i, r.value, r.err, r.at)
		}
		if r.at > time.Second {
			t.Errorf("call %d answered after %v, the caller waited for its timeout", i, r.at)
		}
	}
	// 250 ms: the first two finish, the third runs into the deadline
	if answered < 1 || answered+closed != 3 {
		t.Errorf("%d answered, %d closed", answered, closed)
	}
}

func TestDrainServesCallsQueuedBeforeTheDeadline(t *testing.T) {
	for _, tc := range []struct {
		name string
		ms   int
	}{
		{"handed off", 50},
		{"inline", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{}, 8)
			_, server, _, proxy := drainPeers(t, nil, map[string]any{
				"work": func(ms int) int {
					started <- struct{}{}
					time.Sleep(time.Duration(ms) * time.Millisecond)
					return ms
				},
			})
			// the method's last call decides where the next one runs
			if _, err := proxy.Invoke(context.Background(), "work", tc.ms); err != nil {
				t.Fatal(err)
			}
			<-started

			results := callsInBackground(proxy, "work", []any{tc.ms}, 3)
			waitStarted(t, started)
			if err := server.Disconnect(); err != nil {
				t.Fatal(err)
			}
			for i, ch := range results {
				if r := <-ch; r.err != nil || fmt.Sprint(r.value) != fmt.Sprint(tc.ms) {
					t.Errorf("call %d = %v, %v, want its result", i, r.value, r.err)
				}
			}
		})
	}
}

func TestDrainAnswersAStreamQueuedAtTheDeadline(t *testing.T) {
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	_, server, _, proxy := drainPeers(t, nil, map[string]any{
		"feed": func(stuck bool) (<-chan any, error) {
			started <- struct{}{}
			if stuck {
				<-release
			}
			ch := make(chan any, 1)
			ch <- 1
			close(ch)
			return ch, nil
		},
	})

	running := callsInBackground(proxy, "feed", []any{true}, 1)
	waitStarted(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := proxy.InvokeStream(ctx, "feed", false)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	stopped := time.Now()
	go func() { _ = server.Disconnect() }()
	if r := <-running[0]; !isConnectionClosed(r.err) {
		t.Errorf("running call = %v, %v, want CONNECTION_CLOSED", r.value, r.err)
	}
	// the stream caller reads only its stream subject
	var last StreamValue
	for v := range stream {
		last = v
	}
	if !isConnectionClosed(last.Error) {
		t.Errorf("stream ended with %+v, want CONNECTION_CLOSED", last)
	}
	if took := time.Since(stopped); took > defaultDrainTimeout+drainGrace+150*time.Millisecond {
		t.Errorf("stream answered %v after Disconnect", took)
	}
}

func TestDrainAnswersCallsQueuedBehindALongCall(t *testing.T) {
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	_, server, _, proxy := drainPeers(t, nil, map[string]any{
		"stuck": func() bool {
			started <- struct{}{}
			<-release
			return true
		},
	})

	results := callsInBackground(proxy, "stuck", nil, 3)
	waitStarted(t, started)
	time.Sleep(20 * time.Millisecond)
	stopped := time.Now()
	if err := server.Disconnect(); err != nil {
		t.Fatal(err)
	}
	for i, ch := range results {
		r := <-ch
		if !isConnectionClosed(r.err) {
			t.Errorf("call %d = %v, %v, want CONNECTION_CLOSED", i, r.value, r.err)
		}
	}
	if took := time.Since(stopped); took > defaultDrainTimeout+150*time.Millisecond {
		t.Errorf("queued calls answered %v after Disconnect", took)
	}
}

func TestDrainRemovesTheSubscriptionFirst(t *testing.T) {
	started := make(chan struct{}, 8)
	_, server, _, proxy := drainPeers(t, nil, map[string]any{
		"slow": func() bool {
			started <- struct{}{}
			time.Sleep(150 * time.Millisecond)
			return true
		},
		"other": func() bool { return true },
	})

	running := callsInBackground(proxy, "slow", nil, 1)
	waitStarted(t, started)
	go func() { _ = server.Disconnect() }()
	time.Sleep(30 * time.Millisecond)

	// with the subscription gone the caller sees no responders at once, a
	// retry would find another instance
	begin := time.Now()
	_, err := proxy.Invoke(context.Background(), "other")
	var rerr *RPCException
	if !errors.As(err, &rerr) || rerr.Code != ErrCodeNotFound {
		t.Errorf("call during the drain = %v, want a no-responder error", err)
	}
	if took := time.Since(begin); took > 500*time.Millisecond {
		t.Errorf("no-responder after %v", took)
	}
	if r := <-running[0]; r.err != nil || r.value != true {
		t.Errorf("running call = %v, %v", r.value, r.err)
	}
}

func TestCleanupLetsRunningCallsFinish(t *testing.T) {
	started := make(chan struct{}, 8)
	_, _, cleanup, proxy := drainPeers(t, nil, map[string]any{
		"careful": func(ctx context.Context) string {
			started <- struct{}{}
			time.Sleep(80 * time.Millisecond)
			if ctx.Err() != nil {
				return "cancelled"
			}
			return "done"
		},
	})

	results := callsInBackground(proxy, "careful", nil, 1)
	waitStarted(t, started)
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if r := <-results[0]; r.err != nil || r.value != "done" {
		t.Errorf("call = %v, %v, want done", r.value, r.err)
	}
}

func TestDrainRepliesOnlyOnce(t *testing.T) {
	started := make(chan struct{}, 8)
	s, _, cleanup, proxy := drainPeers(t, nil, map[string]any{
		"late": func() string {
			started <- struct{}{}
			time.Sleep(defaultDrainTimeout + 150*time.Millisecond)
			return "late"
		},
	})

	results := callsInBackground(proxy, "late", nil, 1)
	waitStarted(t, started)
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if r := <-results[0]; !isConnectionClosed(r.err) {
		t.Errorf("call = %v, %v, want CONNECTION_CLOSED", r.value, r.err)
	}
	// the handler returns after the deadline, on a connection that is still up
	time.Sleep(250 * time.Millisecond)
	if n := s.publishes(func(subject string) bool { return strings.HasPrefix(subject, "rpc.reply.") }); n != 1 {
		t.Errorf("%d replies published, want 1", n)
	}
}

func TestDrainAnswersConcurrentCalls(t *testing.T) {
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	_, server, _, proxy := drainPeers(t, nil, map[string]any{
		"stuck": func(ctx context.Context) bool {
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return true
		},
	}, WithMethodConcurrency(map[string]int{"stuck": 2}))

	// two run, the third waits for a slot
	results := callsInBackground(proxy, "stuck", nil, 3)
	waitStarted(t, started)
	waitStarted(t, started)
	stopped := time.Now()
	if err := server.Disconnect(); err != nil {
		t.Fatal(err)
	}
	for i, ch := range results {
		if r := <-ch; !isConnectionClosed(r.err) {
			t.Errorf("call %d = %v, %v, want CONNECTION_CLOSED", i, r.value, r.err)
		}
	}
	if took := time.Since(stopped); took > defaultDrainTimeout+150*time.Millisecond {
		t.Errorf("calls answered %v after Disconnect", took)
	}
}

func TestSuspendDrainsAndServesAgainAfterConnect(t *testing.T) {
	started := make(chan struct{}, 8)
	_, server, _, proxy := drainPeers(t, nil, map[string]any{
		"slow": func(ms int) int {
			started <- struct{}{}
			time.Sleep(time.Duration(ms) * time.Millisecond)
			return ms
		},
	})

	results := callsInBackground(proxy, "slow", []any{60}, 1)
	waitStarted(t, started)
	if err := server.Suspend(); err != nil {
		t.Fatal(err)
	}
	if r := <-results[0]; r.err != nil || fmt.Sprint(r.value) != "60" {
		t.Errorf("call during Suspend = %v, %v", r.value, r.err)
	}

	if err := server.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := proxy.Invoke(context.Background(), "slow", 1)
	if err != nil || fmt.Sprint(got) != "1" {
		t.Errorf("call after Connect = %v, %v", got, err)
	}
}

func TestNegativeDrainTimeoutKeepsTheOldTeardown(t *testing.T) {
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	_, server, _, proxy := drainPeers(t, &ClientOptions{DrainTimeout: -1}, map[string]any{
		"stuck": func() bool {
			started <- struct{}{}
			<-release
			return true
		},
	})
	proxy = proxy.WithOptions(RequestOptions{Timeout: 400 * time.Millisecond, NoResponderRetry: &NoResponderRetryOptions{MaxRetries: -1}})

	results := callsInBackground(proxy, "stuck", nil, 1)
	waitStarted(t, started)
	stopped := time.Now()
	if err := server.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(stopped); took > 100*time.Millisecond {
		t.Errorf("Disconnect without drain took %v", took)
	}
	var rerr *RPCException
	if r := <-results[0]; !errors.As(r.err, &rerr) || rerr.Code != ErrCodeTimeout {
		t.Errorf("call = %v, %v, want the old timeout", r.value, r.err)
	}
}

func TestDrainAnswersAnInlineCallAtTheDeadline(t *testing.T) {
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var calls atomic.Int32
	_, server, _, proxy := drainPeers(t, nil, map[string]any{
		"sometimes": func() bool {
			if calls.Add(1) == 1 {
				return true
			}
			started <- struct{}{}
			<-release
			return true
		},
	})

	// the first call is fast, so the second runs on the delivery goroutine
	if _, err := proxy.Invoke(context.Background(), "sometimes"); err != nil {
		t.Fatal(err)
	}
	results := callsInBackground(proxy, "sometimes", nil, 1)
	waitStarted(t, started)
	stopped := time.Now()
	if err := server.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if r := <-results[0]; !isConnectionClosed(r.err) {
		t.Errorf("call = %v, %v, want CONNECTION_CLOSED", r.value, r.err)
	}
	if took := time.Since(stopped); took > defaultDrainTimeout+150*time.Millisecond {
		t.Errorf("answered %v after Disconnect", took)
	}
}
