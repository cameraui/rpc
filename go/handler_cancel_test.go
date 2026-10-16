package rpc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func within(t *testing.T, what string, d time.Duration, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("%s not within %v", what, d)
	}
}

func expectConnectionClosed(t *testing.T, what string, err error) {
	t.Helper()
	var rerr *RPCException
	if !errors.As(err, &rerr) || rerr.Code != ErrCodeConnectionClosed {
		t.Errorf("%s ended with %v, want CONNECTION_CLOSED", what, err)
	}
}

// parks after the first item until ctx ends, a lib without the ctx parameter
// hands it nil and the producer waits for the test to end
func parkedProducer(ctx context.Context, release <-chan struct{}, done chan<- struct{}) <-chan int {
	ch := make(chan int)
	go func() {
		defer close(done)
		defer close(ch)
		ch <- 1
		if ctx == nil {
			<-release
			return
		}
		select {
		case <-ctx.Done():
		case <-release:
		}
	}()
	return ch
}

// ten sends, watching neither a context nor Active, so it blocks until someone receives
func unawareProducer[T any](v T, done chan<- struct{}) <-chan T {
	ch := make(chan T)
	go func() {
		defer close(done)
		defer close(ch)
		for range 10 {
			ch <- v
		}
	}()
	return ch
}

func TestContextFirstParameterGetsTheCall(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	var seen atomic.Value
	_, err := server.RegisterHandler("cx", map[string]any{
		"describe": func(ctx context.Context, name string, n int) string {
			if ctx == nil {
				return "no context"
			}
			seen.Store(ctx)
			return fmt.Sprintf("%s %d", name, n)
		},
		"join": func(ctx context.Context, sep string, parts ...string) string {
			if ctx == nil {
				return "no context"
			}
			return strings.Join(parts, sep)
		},
		"plain": func(name string) string { return name },
	})
	if err != nil {
		t.Fatal(err)
	}
	proxy := connectFake(t, s, nil).CreateProxy("cx")

	for _, tc := range []struct {
		method string
		args   []any
		want   string
	}{
		{"describe", []any{"cam", 3}, "cam 3"},
		{"join", []any{"-", "a", "b"}, "a-b"},
		{"join", []any{"-"}, ""},
		{"plain", []any{"x"}, "x"},
	} {
		got, err := proxy.Invoke(context.Background(), tc.method, tc.args...)
		if err != nil || got != tc.want {
			t.Errorf("%s%v = %v, %v, want %q", tc.method, tc.args, got, err, tc.want)
		}
	}
	if ctx, ok := seen.Load().(context.Context); !ok || ctx.Err() == nil {
		t.Error("the context of a call did not end when the method returned")
	}
}

func TestStreamCancelReachesParkedProducer(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	done := make(chan struct{})
	_, err := server.RegisterHandler("cp", map[string]any{
		"parked": func(ctx context.Context) <-chan int { return parkedProducer(ctx, release, done) },
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := client.CreateProxy("cp").InvokeStream(ctx, "parked")
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	cancel()
	within(t, "the parked producer's cleanup after the client cancelled", 500*time.Millisecond, done)
}

func TestStreamCancelDrainsProducer(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	finished := make(chan struct{})
	_, err := server.RegisterHandler("cd", map[string]any{
		"count": func() <-chan int {
			ch := make(chan int)
			go func() {
				defer close(finished)
				defer close(ch)
				for i := range 50 {
					ch <- i
					time.Sleep(5 * time.Millisecond)
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
	ch, err := client.CreateProxy("cd").InvokeStream(ctx, "count")
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	<-ch
	cancel()
	within(t, "a producer that ignores the cancel to reach its defer", time.Second, finished)
}

func TestStreamCancelDuringHandlerSetup(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	returned := make(chan struct{})
	_, err := server.RegisterHandler("cs", map[string]any{
		"slowSetup": func() <-chan int {
			defer close(returned)
			time.Sleep(200 * time.Millisecond)
			ch := make(chan int, 50)
			for i := range 50 {
				ch <- i
			}
			close(ch)
			return ch
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := client.CreateProxy("cs").InvokeStream(ctx, "slowSetup"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	within(t, "the handler setup", time.Second, returned)
	time.Sleep(100 * time.Millisecond)
	if err := server.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := s.publishes(func(subject string) bool {
		return strings.HasPrefix(subject, "stream.rpc.cs.slowSetup.") && !strings.HasSuffix(subject, ".cancel")
	}); n != 0 {
		t.Errorf("%d stream messages published for a stream cancelled during its setup", n)
	}
}

func TestPullCancelDrainsProducer(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	finished := make(chan struct{})
	_, err := server.RegisterHandler("pd", map[string]any{
		"items": func() <-chan int {
			ch := make(chan int)
			go func() {
				defer close(finished)
				defer close(ch)
				for i := range 50 {
					ch <- i
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
	ch, err := client.CreateProxy("pd").InvokePullIterator(ctx, "items")
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	cancel()
	within(t, "a producer that ignores the cancel to reach its defer", time.Second, finished)
	waitFor(t, "session cleanup", func() bool { return pullSessions(server) == 0 })
}

func TestPullCancelReachesProducerWhileNextIsPending(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	done := make(chan struct{})
	_, err := server.RegisterHandler("pc2", map[string]any{
		"parked": func(ctx context.Context) <-chan int { return parkedProducer(ctx, release, done) },
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := client.CreateProxy("pc2").InvokePullIterator(ctx, "parked")
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	// the client asks for the next value right away, it stays pending
	time.Sleep(100 * time.Millisecond)
	cancel()
	within(t, "the parked producer's cleanup after the client cancelled", 500*time.Millisecond, done)
}

func TestTeardownEndsOpenSessions(t *testing.T) {
	for _, how := range []string{"unregister", "disconnect", "suspend", "service stop"} {
		t.Run(how, func(t *testing.T) {
			s := startFakeNATS(t)
			server := connectFake(t, s, nil)
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			done := map[string]chan struct{}{}
			for _, name := range []string{"push", "pull", "batches", "unaware pull", "unaware batches"} {
				done[name] = make(chan struct{})
			}
			methods := map[string]any{
				"push": func(ctx context.Context) <-chan int { return parkedProducer(ctx, release, done["push"]) },
				"pull": func(ctx context.Context) <-chan int { return parkedProducer(ctx, release, done["pull"]) },
				"batches": func(inv *CallbackInvoker) <-chan struct{} {
					ch := make(chan struct{})
					go func() {
						defer close(done["batches"])
						defer close(ch)
						ch <- struct{}{}
						for inv.Active() {
							time.Sleep(5 * time.Millisecond)
						}
					}()
					return ch
				},
				"unawarePull":    func() <-chan int { return unawareProducer(1, done["unaware pull"]) },
				"unawareBatches": func(*CallbackInvoker) <-chan struct{} { return unawareProducer(struct{}{}, done["unaware batches"]) },
			}
			client := connectFake(t, s, nil)
			ctx := context.Background()
			var (
				stop       func() error
				pushPrefix string
				push       <-chan StreamValue
				pulls      = map[string]<-chan PullValue{}
				err        error
			)
			if how == "service stop" {
				svc, err := NewRPCService(server).RegisterHandler(ServiceConfig{Name: "tdsvc", Version: "1.0.0"}, map[string]any{
					"svcpush": methods["push"],
					"svcpull": methods["pull"],
				})
				if err != nil {
					t.Fatal(err)
				}
				stop = svc.Stop
			} else {
				cleanup, err := server.RegisterHandler("td", methods)
				if err != nil {
					t.Fatal(err)
				}
				stop = map[string]func() error{"unregister": cleanup, "disconnect": server.Disconnect, "suspend": server.Suspend}[how]
			}
			// a request that reaches the server before its subscription gets a 503
			if err := server.nc.Flush(); err != nil {
				t.Fatal(err)
			}
			if how == "service stop" {
				pushPrefix = "stream.svcpush."
				if push, err = client.CallStream(ctx, "svcpush"); err != nil {
					t.Fatal(err)
				}
				if pulls["pull"], err = client.CallPullIterator(ctx, "svcpull"); err != nil {
					t.Fatal(err)
				}
			} else {
				pushPrefix = "stream.rpc.td.push."
				proxy := client.CreateProxy("td")
				if push, err = proxy.InvokeStream(ctx, "push"); err != nil {
					t.Fatal(err)
				}
				for name, method := range map[string]string{"pull": "pull", "unaware pull": "unawarePull"} {
					if pulls[name], err = proxy.InvokePullIterator(ctx, method); err != nil {
						t.Fatal(err)
					}
				}
				for name, method := range map[string]string{"batches": "batches", "unaware batches": "unawareBatches"} {
					if pulls[name], err = proxy.InvokePullIteratorWithCallback(ctx, method, PullCallbackMap{}, nil); err != nil {
						t.Fatal(err)
					}
				}
			}
			if v, ok := <-push; !ok || v.Error != nil {
				t.Fatalf("the push stream did not start: %v", v.Error)
			}
			for name, ch := range pulls {
				if v, ok := <-ch; !ok || v.Error != nil {
					t.Fatalf("the %s did not start: %v", name, v.Error)
				}
			}
			time.Sleep(50 * time.Millisecond)

			if err := server.nc.Flush(); err != nil {
				t.Fatal(err)
			}
			subjects := []string{pushPrefix}
			for _, id := range sessionIDs(&server.pullIteratorCleanups) {
				subjects = append(subjects, "_rpc.iterator."+id+".response")
			}
			if len(subjects) != 1+len(pulls) {
				t.Fatalf("%d pull sessions open, want %d", len(subjects)-1, len(pulls))
			}
			published := func(prefix string) int {
				return s.publishes(func(subject string) bool {
					return strings.HasPrefix(subject, prefix) && !strings.HasSuffix(subject, ".cancel")
				})
			}
			before := make([]int, len(subjects))
			for i, subject := range subjects {
				before[i] = published(subject)
			}

			start := time.Now()
			if err = stop(); err != nil {
				t.Fatal(err)
			}
			if took := time.Since(start); took > 200*time.Millisecond {
				t.Errorf("%s took %v with open sessions", how, took)
			}

			select {
			case v, ok := <-push:
				if !ok {
					t.Fatal("the push stream ended without an error")
				}
				expectConnectionClosed(t, "the push stream", v.Error)
			case <-time.After(time.Second):
				t.Fatal("the push stream consumer got nothing")
			}
			// an unaware session first hands over the values fetched before the stop, the next
			// request after them may see no responders before the terminal message arrives
			for name, ch := range pulls {
				deadline := time.After(time.Second)
			read:
				for {
					select {
					case v, ok := <-ch:
						if !ok {
							t.Fatalf("the %s ended without an error", name)
						}
						if v.Error != nil {
							var rerr *RPCException
							if !strings.HasPrefix(name, "unaware") || !errors.As(v.Error, &rerr) || rerr.Code != "503" {
								expectConnectionClosed(t, "the "+name, v.Error)
							}
							break read
						}
					case <-deadline:
						t.Fatalf("the %s consumer got no error", name)
					}
				}
			}
			within(t, "the push producer's cleanup", time.Second, done["push"])
			for name := range pulls {
				within(t, "the "+name+" producer's cleanup", time.Second, done[name])
			}

			time.Sleep(50 * time.Millisecond)
			for i, subject := range subjects {
				if n := published(subject) - before[i]; n != 1 {
					t.Errorf("%d messages on %s after the %s, want the one terminal message", n, subject, how)
				}
			}
		})
	}
}

// holds the publish of the Invoke it is passed to until released
type gatedArg struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (g *gatedArg) MarshalMsgpack() ([]byte, error) {
	g.once.Do(func() { close(g.entered) })
	<-g.release
	return Encode(1)
}

func TestNoCallbackFollowsTheTerminalMessage(t *testing.T) {
	for _, how := range []string{"cancel", "unregister"} {
		t.Run(how, func(t *testing.T) {
			s := startFakeNATS(t)
			server := connectFake(t, s, nil)
			gate := &gatedArg{entered: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate.release) }) }
			t.Cleanup(release)
			cleanup, err := server.RegisterHandler("ga", map[string]any{
				"frames": func(inv *CallbackInvoker) <-chan struct{} {
					ch := make(chan struct{})
					go func() {
						defer close(ch)
						inv.Invoke("frame", gate)
					}()
					return ch
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			client := connectFake(t, s, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if _, err := client.CreateProxy("ga").InvokePullIteratorWithCallback(ctx, "frames", PullCallbackMap{"frame": func(int) {}}, []string{"frame"}); err != nil {
				t.Fatal(err)
			}
			within(t, "the invoke to start its publish", time.Second, gate.entered)

			if how == "cancel" {
				cancel()
			} else {
				go func() { _ = cleanup() }()
			}
			terminal := func() bool {
				return s.publishes(func(subject string) bool {
					return strings.HasPrefix(subject, "_rpc.iterator.") && strings.HasSuffix(subject, ".response")
				}) > 0
			}
			// time for the end to reach the session, its terminal message must wait for the invoke
			for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline) && !terminal(); {
				time.Sleep(5 * time.Millisecond)
			}
			release()
			waitFor(t, "the terminal message", terminal)
			if err := server.nc.Flush(); err != nil {
				t.Fatal(err)
			}

			ended := false
			for _, subject := range s.publishOrder() {
				switch {
				case strings.HasPrefix(subject, "_rpc.iterator.") && strings.HasSuffix(subject, ".response"):
					ended = true
				case strings.HasPrefix(subject, pullCallbackSubjectPrefix) && ended:
					t.Errorf("callback on %s after the terminal message", subject)
				}
			}
		})
	}
}

func TestEndedSessionPublishesNothing(t *testing.T) {
	s := startFakeNATS(t)
	client := connectFake(t, s, nil)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	push := &pushSession{id: "p1", subject: "stream.ended.p1", client: client, cancel: cancel, stopped: make(chan struct{})}
	push.end(&StreamMessage{ID: "p1", Type: "end"})
	if err := push.publish(StreamMessage{ID: "p1", Type: "data", Data: 1}); err != nil {
		t.Fatal(err)
	}

	pull := newPullSession("i1", client, cancel, nil, nil)
	pull.end(&PullIteratorResponse{ID: "i1", Type: "done"}, true, true)
	pull.respond(PullIteratorResponse{ID: "i1", Type: "value", Value: 1})

	if err := client.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, subject := range []string{push.subject, pull.responseSubject} {
		if n := s.publishes(subjectIs(subject)); n != 1 {
			t.Errorf("%d messages on %s, want only the terminal one", n, subject)
		}
	}
}

func TestDrainChannelReturnsForNilChannel(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		drainChannel(reflect.ValueOf((<-chan int)(nil)))
	}()
	within(t, "draining a nil channel", time.Second, done)
}

func TestPanickingSessionSetupEndsItsContext(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	ctxs := make(chan context.Context, 2)
	_, err := server.RegisterHandler("ps", map[string]any{
		"items": func(ctx context.Context) <-chan int {
			ctxs <- ctx
			panic("setup failed")
		},
		"watch": func(ctx context.Context, cb func(int)) func() {
			ctxs <- ctx
			panic("setup failed")
		},
	}, WithPanicRecovery())
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	if _, err := client.CreateProxy("ps").InvokePullIterator(context.Background(), "items"); err == nil {
		t.Error("a pull iterator whose setup panicked started")
	}
	if _, err := CallWithCallback(client, "rpc.ps.watch", nil, func(int) {}); err == nil {
		t.Error("a callback subscription whose setup panicked started")
	}
	for range 2 {
		select {
		case ctx := <-ctxs:
			within(t, "the context of a session whose setup panicked to end", time.Second, ctx.Done())
		case <-time.After(time.Second):
			t.Fatal("the handler did not run")
		}
	}
}
