package rpc

import (
	"context"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func TestCallSendsTTL(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.Subscribe("rpc.ttl.echo", func(data []byte) {
		var msg RPCMessage
		if err := DecodeMessageInto(data, &msg); err == nil {
			_ = server.Publish("rpc.reply."+msg.ID, RPCResponse{ID: msg.ID, Result: msg.TTL})
		}
	}); err != nil {
		t.Fatal(err)
	}
	caller := connectFake(t, s, &ClientOptions{Timeout: 7 * time.Second})

	ttl := func(ctx context.Context, opts *RequestOptions) int64 {
		t.Helper()
		got, err := caller.CallWithOptions(ctx, "rpc.ttl.echo", opts)
		if err != nil {
			t.Fatal(err)
		}
		n, _ := got.(int64)
		if u, ok := got.(uint64); ok {
			n = int64(u)
		}
		return n
	}
	if got := ttl(context.Background(), nil); got != 7000 {
		t.Errorf("__ttl with the client timeout = %d, want 7000", got)
	}
	if got := ttl(context.Background(), &RequestOptions{Timeout: 1500 * time.Millisecond}); got != 1500 {
		t.Errorf("__ttl with a call timeout = %d, want 1500", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if got := ttl(ctx, nil); got <= 2500 || got > 3000 {
		t.Errorf("__ttl with a context deadline = %d, want just under 3000", got)
	}
	short, cancelShort := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShort()
	if got := ttl(short, &RequestOptions{Timeout: 7 * time.Second}); got <= 1500 || got > 2000 {
		t.Errorf("__ttl with a call timeout and a shorter context deadline = %d, want just under 2000", got)
	}
}

func TestTTLDecodesAnyNumber(t *testing.T) {
	for _, tc := range []struct {
		ttl  any
		want Milliseconds
	}{
		{int64(7000), 7000},
		{uint16(7000), 7000},
		{7000.0, 7000},
		{7500.5, 7501},
		{float32(12.5), 13},
		{-5.0, 0},
		{math.Inf(1), 0},
		{math.NaN(), 0},
		{"soon", 0},
		{[]any{1, 2}, 0},
		{nil, 0},
	} {
		raw, err := msgpack.Marshal(map[string]any{"id": "x", "method": "call", "params": []any{1}, "__ttl": tc.ttl})
		if err != nil {
			t.Fatal(err)
		}
		var msg RPCMessage
		if err := DecodeMessageInto(raw, &msg); err != nil || msg.ID != "x" || msg.TTL != tc.want {
			t.Errorf("__ttl %#v: TTL %d, id %q, err %v; want TTL %d", tc.ttl, msg.TTL, msg.ID, err, tc.want)
		}
	}
}

func TestDropExpiredSkipsCallsTheCallerGaveUpOn(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    []HandlerOption
		skipped bool
	}{
		{"opted in", []HandlerOption{WithDropExpired("slow")}, true},
		{"every method", []HandlerOption{WithDropExpired()}, true},
		{"namespace subscription", []HandlerOption{WithDropExpired("slow"), WithNamespaceSubscription()}, true},
		{"not opted in", []HandlerOption{WithDropExpired("other")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := startFakeNATS(t)
			server := connectFake(t, s, nil)
			var mu sync.Mutex
			var ran []int
			if _, err := server.RegisterHandler("exp", map[string]any{
				"slow": func(n int) int {
					mu.Lock()
					ran = append(ran, n)
					mu.Unlock()
					time.Sleep(100 * time.Millisecond)
					return n
				},
				"other": func() {},
			}, tc.opts...); err != nil {
				t.Fatal(err)
			}
			caller := connectFake(t, s, nil)

			call := func(n int, timeout time.Duration, wg *sync.WaitGroup) {
				wg.Go(func() {
					_, _ = caller.CallWithOptions(context.Background(), "rpc.exp.slow", &RequestOptions{Timeout: timeout}, n)
				})
			}
			var wg sync.WaitGroup
			call(1, 2*time.Second, &wg)
			time.Sleep(20 * time.Millisecond)
			call(2, 30*time.Millisecond, &wg)
			time.Sleep(5 * time.Millisecond)
			call(3, 2*time.Second, &wg)
			wg.Wait()

			mu.Lock()
			defer mu.Unlock()
			want := []int{1, 2, 3}
			if tc.skipped {
				want = []int{1, 3}
			}
			if !slices.Equal(ran, want) {
				t.Errorf("ran %v, want %v", ran, want)
			}
		})
	}
}
