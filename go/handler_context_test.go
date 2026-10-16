package rpc

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestCallHandlerContextFirstMapping(t *testing.T) {
	ctx := t.Context()
	var got context.Context
	withCtx := reflect.ValueOf(func(c context.Context, a string, n int) string {
		got = c
		return a + string(rune('0'+n))
	})
	if res, err := callHandler(ctx, withCtx, []any{"x", int64(4)}); err != nil || res != "x4" || got != ctx {
		t.Errorf("ctx first = %v, %v, same ctx %v", res, err, got == ctx)
	}

	variadic := reflect.ValueOf(func(_ context.Context, xs ...int) int { return len(xs) })
	for args, want := range map[int]int{0: 0, 1: 1, 3: 3} {
		wire := make([]any, args)
		for i := range wire {
			wire[i] = int64(i)
		}
		if res, err := callHandler(ctx, variadic, wire); err != nil || res != want {
			t.Errorf("variadic with %d args = %v, %v", args, res, err)
		}
	}

	// only the exact interface type is a context, a first parameter of type any stays a wire argument
	anyFirst := reflect.ValueOf(func(v any, s string) any { return v })
	if res, _ := callHandler(ctx, anyFirst, []any{"wire", "s"}); res != "wire" {
		t.Errorf("an any first parameter got %v, want the wire argument", res)
	}
}

func TestPullCallbackInvokerContext(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	done := make(chan struct{})
	gotArg := make(chan bool, 1)
	_, err := server.RegisterHandler("ic", map[string]any{
		"play": func(ctx context.Context, n int, inv *CallbackInvoker) <-chan struct{} {
			gotArg <- n == 7 && ctx == inv.Context()
			ch := make(chan struct{})
			go func() {
				defer close(done)
				defer close(ch)
				ch <- struct{}{}
				// a producer parked in a wait, no Active polling
				select {
				case <-inv.Context().Done():
				case <-time.After(5 * time.Second):
				}
			}()
			return ch
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := client.CreateProxy("ic").InvokePullIteratorWithCallback(ctx, "play", PullCallbackMap{}, nil, 7)
	if err != nil {
		t.Fatal(err)
	}
	if ok := <-gotArg; !ok {
		t.Error("the handler did not get the wire argument after ctx and the invoker's context")
	}
	<-ch
	time.Sleep(50 * time.Millisecond)
	cancel()
	within(t, "the producer waiting on the invoker context", 500*time.Millisecond, done)

	var zero CallbackInvoker
	if zero.Context() == nil {
		t.Error("a zero invoker has no context")
	}
}

func TestCallbackSubscriptionContextEndsWithTheSubscription(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	ended := make(chan struct{})
	_, err := server.RegisterHandler("cbx", map[string]any{
		"watch": func(ctx context.Context, prefix string, cb func(string)) func() {
			cb(prefix + "!")
			go func() {
				<-ctx.Done()
				close(ended)
			}()
			return func() {}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)
	got := make(chan string, 1)
	unsubscribe, err := CallWithCallback(client, "rpc.cbx.watch", []any{"hi"}, func(v string) { got <- v })
	if err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-got:
		if v != "hi!" {
			t.Errorf("callback got %q, want hi!", v)
		}
	case <-time.After(time.Second):
		t.Fatal("no callback")
	}
	unsubscribe()
	within(t, "the callback session context to end", time.Second, ended)
}

func TestUnregisterEndsTheContextOfRunningCalls(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	started := make(chan struct{})
	stopped := make(chan struct{})
	cleanup, err := server.RegisterHandler("uc", map[string]any{
		"wait": func(ctx context.Context) string {
			close(started)
			select {
			case <-ctx.Done():
				close(stopped)
			case <-time.After(5 * time.Second):
			}
			return "late"
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, &ClientOptions{Timeout: 2 * time.Second})
	go func() { _, _ = client.CreateProxy("uc").Invoke(context.Background(), "wait") }()
	within(t, "the call to start", time.Second, started)
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	within(t, "the running call to see its context end", 500*time.Millisecond, stopped)
}
