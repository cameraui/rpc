package rpc

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func fastRetryClient() *Client {
	return NewClient(ClientOptions{NoResponderRetry: &NoResponderRetryOptions{Delays: []time.Duration{time.Millisecond}}})
}

func TestRemoteNotFoundIsNotRetried(t *testing.T) {
	c := NewClient(ClientOptions{})
	calls := 0
	start := time.Now()
	err := c.withNoResponderRetry(context.Background(), nil, methodProbe{}, func() error {
		calls++
		return NewRPCException(ErrCodeNotFound, "event not found")
	})

	if calls != 1 {
		t.Errorf("handler ran %d times, want 1", calls)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("error after %v", elapsed)
	}
	var rerr *RPCException
	if !errors.As(err, &rerr) || rerr.Code != ErrCodeNotFound || rerr.Msg != "event not found" {
		t.Errorf("err = %v, want the remote NOT_FOUND unchanged", err)
	}
}

func TestLocalNoRespondersIsRetried(t *testing.T) {
	c := fastRetryClient()
	calls := 0
	err := c.withNoResponderRetry(context.Background(), nil, methodProbe{}, func() error {
		calls++
		if calls < 3 {
			return newLocalNoResponders("rpc.ns.echo", "No responders for rpc.ns.echo")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Errorf("err %v after %d attempts, want nil after 3", err, calls)
	}
}

func TestLocalNoRespondersSurfacesAsPlainException(t *testing.T) {
	c := fastRetryClient()
	calls := 0
	err := c.withNoResponderRetry(context.Background(), nil, methodProbe{}, func() error {
		calls++
		return newLocalNoResponders("rpc.ns.echo", "No responders for rpc.ns.echo")
	})

	if calls != 4 {
		t.Errorf("%d attempts, want 4", calls)
	}
	rerr, ok := err.(*RPCException)
	if !ok {
		t.Fatalf("err is %T, want *RPCException", err)
	}
	if rerr.Code != ErrCodeNotFound || rerr.Msg != "No responders for rpc.ns.echo" {
		t.Errorf("err = %v", rerr)
	}
}

func TestNegativeMaxRetriesTurnsRetryOff(t *testing.T) {
	c := fastRetryClient()
	for _, tc := range []struct {
		maxRetries int
		want       int
	}{{-1, 1}, {0, 4}, {1, 2}} {
		calls := 0
		override := &NoResponderRetryOptions{MaxRetries: tc.maxRetries, Delays: []time.Duration{time.Millisecond}}
		_ = c.withNoResponderRetry(context.Background(), override, methodProbe{}, func() error {
			calls++
			return newLocalNoResponders("rpc.ns.echo", "No responders for rpc.ns.echo")
		})
		if calls != tc.want {
			t.Errorf("MaxRetries %d: %d attempts, want %d", tc.maxRetries, calls, tc.want)
		}
	}
}

func TestMuxNoResponderStatusIsLocal(t *testing.T) {
	c := NewClient(ClientOptions{})
	pr := &pendingRequest{done: make(chan struct{}), subject: "rpc.ns.echo"}
	c.pendingRequests.Store("p.1", pr)

	c.handleMuxMessage(&nats.Msg{Subject: "rpc.reply.p.1", Header: nats.Header{"Status": {"503"}}})

	<-pr.done
	if _, ok := errors.AsType[*localNoResponders](pr.err); !ok {
		t.Fatalf("err %v is not marked as a local no-responder", pr.err)
	}
	var rerr *RPCException
	if !errors.As(pr.err, &rerr) || rerr.Code != ErrCodeNotFound || rerr.Msg != "No responders for rpc.ns.echo" {
		t.Errorf("err = %v", pr.err)
	}
}

func TestMuxRemoteNotFoundIsNotLocal(t *testing.T) {
	c := NewClient(ClientOptions{})
	pr := &pendingRequest{done: make(chan struct{}), subject: "rpc.ns.forward"}
	c.pendingRequests.Store("p.2", pr)

	reply, err := EncodeMessage(RPCResponse{ID: "p.2", Error: &RPCError{Code: ErrCodeNotFound, Message: "No responders for rpc.downstream.x"}})
	if err != nil {
		t.Fatal(err)
	}
	c.handleMuxMessage(&nats.Msg{Subject: "rpc.reply.p.2", Data: reply})

	<-pr.done
	_, err = pr.outcome("p.2")
	if _, ok := errors.AsType[*localNoResponders](err); ok {
		t.Error("a remote error envelope was marked as a local no-responder")
	}
	var rerr *RPCException
	if !errors.As(err, &rerr) || rerr.Code != ErrCodeNotFound {
		t.Errorf("err = %v, want the remote NOT_FOUND", err)
	}
}

func TestProxyWithOptions(t *testing.T) {
	c := NewClient(ClientOptions{})
	base := c.CreateProxy("ns")
	opts := RequestOptions{Timeout: time.Second, NoResponderRetry: &NoResponderRetryOptions{MaxRetries: -1}}
	p := base.WithOptions(opts)

	if base.opts != nil {
		t.Error("WithOptions changed the original proxy")
	}
	if p.opts == nil || p.opts.Timeout != time.Second || p.opts.NoResponderRetry.MaxRetries != -1 {
		t.Fatalf("copy opts = %+v", p.opts)
	}
	if sub := p.Sub("db"); sub.opts != p.opts || sub.buildSubject("find") != "rpc.ns.db.find" {
		t.Error("Sub must keep the options and extend the path")
	}
	if p.buildSubject("echo") != "rpc.ns.echo" {
		t.Errorf("subject = %s", p.buildSubject("echo"))
	}
}

func TestCallDoesNotRetryRemoteNotFound(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	var runs atomic.Int32
	_, err := server.RegisterHandler("nf", map[string]any{"get": func() error {
		runs.Add(1)
		return NewRPCException(ErrCodeNotFound, "event not found")
	}})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	start := time.Now()
	_, err = client.CreateProxy("nf").Invoke(context.Background(), "get")
	elapsed := time.Since(start)

	var rerr *RPCException
	if !errors.As(err, &rerr) || rerr.Code != ErrCodeNotFound || rerr.Msg != "event not found" {
		t.Fatalf("err = %v", err)
	}
	if runs.Load() != 1 || elapsed > 400*time.Millisecond {
		t.Errorf("handler ran %d times, error after %v", runs.Load(), elapsed)
	}
}

func TestCallRetriesLocalNoResponders(t *testing.T) {
	s := startFakeNATS(t)
	client := connectFake(t, s, &ClientOptions{NoResponderRetry: &NoResponderRetryOptions{Delays: []time.Duration{10 * time.Millisecond}}})

	start := time.Now()
	_, err := client.CreateProxy("nobody").Invoke(context.Background(), "echo", 1)

	rerr, ok := err.(*RPCException)
	if !ok || rerr.Code != ErrCodeNotFound || rerr.Msg != "No responders for rpc.nobody.echo" {
		t.Fatalf("err = %#v", err)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Errorf("error after %v, want three retries of 10 ms", elapsed)
	}
}

func TestProxyWithOptionsTurnsRetryOff(t *testing.T) {
	s := startFakeNATS(t)
	client := connectFake(t, s, nil)
	proxy := client.CreateProxy("nobody").WithOptions(RequestOptions{NoResponderRetry: &NoResponderRetryOptions{MaxRetries: -1}})

	start := time.Now()
	_, err := proxy.Sub("db").Invoke(context.Background(), "find", "k")

	rerr, ok := err.(*RPCException)
	if !ok || rerr.Code != ErrCodeNotFound {
		t.Fatalf("err = %#v", err)
	}
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Errorf("error after %v with the retry off", elapsed)
	}
}

func subjectIs(subject string) func(string) bool {
	return func(s string) bool { return s == subject }
}

func TestEveryLocalNoResponderSiteIsRetried(t *testing.T) {
	s := startFakeNATS(t)
	client := connectFake(t, s, &ClientOptions{NoResponderRetry: &NoResponderRetryOptions{Delays: []time.Duration{10 * time.Millisecond}}})
	s.delayPongs(20 * time.Millisecond)
	ctx := context.Background()

	sites := []struct {
		name, subject string
		call          func() error
	}{
		{"request", "nobody.request", func() error {
			_, err := client.Request(ctx, "nobody.request", "hi", time.Second)
			return err
		}},
		{"service call", "nobody.service", func() error {
			_, err := client.Call(ctx, "nobody.service", "hi")
			return err
		}},
		{"stream", "rpc.nobody.stream", func() error {
			_, err := client.CallStream(ctx, "rpc.nobody.stream")
			return err
		}},
	}
	for _, site := range sites {
		t.Run(site.name, func(t *testing.T) {
			err := site.call()
			if rerr, ok := err.(*RPCException); !ok || rerr.Code != ErrCodeNotFound {
				t.Fatalf("err = %#v, want a plain NOT_FOUND", err)
			}
			if n := s.publishes(subjectIs(site.subject)); n != 4 {
				t.Errorf("%d attempts, want 4", n)
			}
		})
	}
}

func TestProxyWithOptionsReachesEveryInvoke(t *testing.T) {
	s := startFakeNATS(t)
	client := connectFake(t, s, &ClientOptions{NoResponderRetry: &NoResponderRetryOptions{Delays: []time.Duration{10 * time.Millisecond}}})
	s.delayPongs(20 * time.Millisecond)
	ctx := context.Background()

	invokes := map[string]func(p *Proxy) error{
		"invoke": func(p *Proxy) error {
			_, err := p.Invoke(ctx, "invoke")
			return err
		},
		"stream": func(p *Proxy) error {
			_, err := p.InvokeStream(ctx, "stream")
			return err
		},
		"pull": func(p *Proxy) error {
			_, err := p.InvokePullIterator(ctx, "pull")
			return err
		},
		"callback": func(p *Proxy) error {
			_, err := p.InvokeCallback("callback", nil, func(any) {})
			return err
		},
		"pullCallback": func(p *Proxy) error {
			_, err := p.InvokePullIteratorWithCallback(ctx, "pullCallback", PullCallbackMap{}, nil)
			return err
		},
	}
	off := client.CreateProxy("off").WithOptions(RequestOptions{NoResponderRetry: &NoResponderRetryOptions{MaxRetries: -1}})
	defaults := client.CreateProxy("defaults")

	for method, invoke := range invokes {
		t.Run(method, func(t *testing.T) {
			for _, tc := range []struct {
				proxy    *Proxy
				subject  string
				attempts int
			}{{off, "rpc.off." + method, 1}, {defaults, "rpc.defaults." + method, 4}} {
				var rerr *RPCException
				if err := invoke(tc.proxy); !errors.As(err, &rerr) || rerr.Code != ErrCodeNotFound {
					t.Fatalf("%s: err = %v, want NOT_FOUND", tc.subject, err)
				}
				if n := s.publishes(subjectIs(tc.subject)); n != tc.attempts {
					t.Errorf("%s: %d attempts, want %d", tc.subject, n, tc.attempts)
				}
			}
		})
	}
}

func TestRetryPlan(t *testing.T) {
	ms := time.Millisecond
	for _, tc := range []struct {
		name       string
		client     *NoResponderRetryOptions
		override   *NoResponderRetryOptions
		maxRetries int
		delays     []time.Duration
	}{
		{"default", nil, nil, 6, []time.Duration{25 * ms, 50 * ms, 100 * ms, 325 * ms, time.Second, 2 * time.Second}},
		{"empty override", nil, &NoResponderRetryOptions{}, 6, defaultRetryDelays},
		{"own delays keep 3 retries", nil, &NoResponderRetryOptions{Delays: []time.Duration{ms}}, 3, []time.Duration{ms}},
		{"own count keeps the old delays", nil, &NoResponderRetryOptions{MaxRetries: 5}, 5, []time.Duration{500 * ms, time.Second, 2 * time.Second}},
		{"off", nil, &NoResponderRetryOptions{MaxRetries: -1}, 0, legacyRetryDelays},
		{"client-wide delays", &NoResponderRetryOptions{Delays: []time.Duration{ms}}, nil, 3, []time.Duration{ms}},
	} {
		c := NewClient(ClientOptions{NoResponderRetry: tc.client})
		maxRetries, delays := c.retryPlan(tc.override)
		if maxRetries != tc.maxRetries || !slices.Equal(delays, tc.delays) {
			t.Errorf("%s: %d retries after %v, want %d after %v", tc.name, maxRetries, delays, tc.maxRetries, tc.delays)
		}
	}
}

func TestDefaultRetryReachesALateResponderEarly(t *testing.T) {
	c := NewClient(ClientOptions{})
	start := time.Now()
	var at []time.Duration
	err := c.withNoResponderRetry(context.Background(), nil, methodProbe{}, func() error {
		at = append(at, time.Since(start))
		if len(at) < 3 {
			return newLocalNoResponders("rpc.ns.late", "No responders for rpc.ns.late")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// 25 ms and 25+50 ms, the old plan needed 500 ms for the first retry
	if at[1] < 20*time.Millisecond || at[1] > 200*time.Millisecond || at[2] > 300*time.Millisecond {
		t.Errorf("attempts at %v, want the first retry after about 25 ms", at)
	}
}
