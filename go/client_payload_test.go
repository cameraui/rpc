package rpc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMaxPayloadSizeOnlyLowersTheServerLimit(t *testing.T) {
	s := startFakeNATS(t)
	for _, tc := range []struct {
		name   string
		option int
		want   int
	}{
		{"server limit", 0, 1<<20 - 8192},
		{"lower cap", 64 << 10, 64<<10 - 8192},
		{"above the server", 4 << 20, 1<<20 - 8192},
	} {
		c := connectFake(t, s, &ClientOptions{MaxPayloadSize: tc.option})
		if got := c.MaxPayloadSize(); got != tc.want {
			t.Errorf("%s: chunk limit %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestMaxPayloadSizeAppliesWithoutAServerLimit(t *testing.T) {
	s := startFakeNATS(t)
	s.setMaxPayload(0)
	c := connectFake(t, s, &ClientOptions{MaxPayloadSize: 4 << 20})
	if got := c.MaxPayloadSize(); got != 4<<20-8192 {
		t.Errorf("chunk limit %d, want %d", got, 4<<20-8192)
	}
}

func TestConnectRefusesMaxPayloadSizeBelowTheFloor(t *testing.T) {
	s := startFakeNATS(t)
	for _, option := range []int{1, 8192, 16383, -1} {
		c := NewClient(ClientOptions{Servers: []string{s.url()}, MaxPayloadSize: option})
		err := c.Connect(context.Background())
		if err == nil || !strings.Contains(err.Error(), "below the minimum of 16384") {
			t.Errorf("option %d: err = %v", option, err)
		}
		if c.IsConnected() {
			t.Errorf("option %d: connected anyway", option)
			_ = c.Disconnect()
		}
	}
	if n := s.connCount(); n != 0 {
		t.Errorf("%d connections reached the server", n)
	}
	c := connectFake(t, s, &ClientOptions{MaxPayloadSize: 16384})
	if got := c.MaxPayloadSize(); got != 8192 {
		t.Errorf("chunk limit %d at the floor, want 8192", got)
	}
}

func TestSmallServerLimitSendsWholeOrFailsAtOnce(t *testing.T) {
	for _, limit := range []int{4096, 8192, 12000} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			s := startFakeNATS(t)
			s.setMaxPayload(limit)
			server := connectFake(t, s, nil)
			bytesOf := func(n int) []byte { return make([]byte, n) }
			if _, err := server.RegisterHandler("pl", map[string]any{
				"size": func(b []byte) int { return len(b) },
				"make": bytesOf,
				"items": func(n int) <-chan []byte {
					ch := make(chan []byte, 1)
					ch <- bytesOf(n)
					close(ch)
					return ch
				},
			}); err != nil {
				t.Fatal(err)
			}
			svc, err := NewRPCService(server).RegisterHandler(ServiceConfig{Name: "plsvc", Version: "1.0.0"}, map[string]any{"make": bytesOf})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = svc.Stop() })
			caller := connectFake(t, s, nil)
			if got := caller.MaxPayloadSize(); got != limit {
				t.Errorf("MaxPayloadSize = %d, want the server limit %d", got, limit)
			}
			proxy := caller.CreateProxy("pl")
			want := "Payload limit " + strconv.Itoa(limit) + " is too small for chunking"
			failsAtOnce := func(what string, start time.Time, err error) {
				t.Helper()
				var rerr *RPCException
				if !errors.As(err, &rerr) || rerr.Code != ErrCodePayloadTooLarge || rerr.Msg != want {
					t.Errorf("%s: %v", what, err)
				}
				if took := time.Since(start); took > 200*time.Millisecond {
					t.Errorf("%s failed after %v, want at once", what, took)
				}
			}

			// larger than the limit minus the 8 KiB reserve
			fits := limit - 512
			if got, err := proxy.Invoke(context.Background(), "size", bytesOf(fits)); err != nil || fmt.Sprint(got) != strconv.Itoa(fits) {
				t.Errorf("a request that fits the limit: %v, %v", got, err)
			}

			sent := s.publishes(func(subject string) bool { return subject == "rpc.pl.size" })
			start := time.Now()
			_, err = proxy.WithOptions(RequestOptions{Timeout: 2 * time.Second}).Invoke(context.Background(), "size", bytesOf(2*limit))
			failsAtOnce("a request over the limit", start, err)
			if n := s.publishes(func(subject string) bool { return subject == "rpc.pl.size" }); n != sent {
				t.Errorf("%d messages of a request over the limit reached the server", n-sent)
			}

			start = time.Now()
			_, err = proxy.WithOptions(RequestOptions{Timeout: 2 * time.Second}).Invoke(context.Background(), "make", 2*limit)
			failsAtOnce("a reply over the limit", start, err)

			pullCtx, cancelPull := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelPull()
			start = time.Now()
			values, err := caller.CallPullIterator(pullCtx, "rpc.pl.items", 2*limit)
			if err != nil {
				t.Fatal(err)
			}
			v := <-values
			failsAtOnce("a pull value over the limit", start, v.Error)

			svcProxy, err := caller.CreateServiceProxy(context.Background(), "plsvc")
			if err != nil {
				t.Fatal(err)
			}
			svcCtx, cancelSvc := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelSvc()
			start = time.Now()
			_, err = svcProxy.Invoke(svcCtx, "make", 2*limit)
			failsAtOnce("a service reply over the limit", start, err)
		})
	}
}

func TestPublishFrameNeverChunksBelowTheFloor(t *testing.T) {
	frame, err := encodeFramePooled(make([]byte, 9000))
	if err != nil {
		t.Fatal(err)
	}
	defer frame.release()
	for _, tc := range []struct {
		max       int
		unchunked bool
	}{{8192, true}, {0, false}, {-4096, false}} {
		var pub collectPublisher
		err := publishFrame(&pub, "s", "", &frame, tc.max, tc.unchunked)
		var rerr *RPCException
		if !errors.As(err, &rerr) || rerr.Code != ErrCodePayloadTooLarge {
			t.Errorf("max %d: err = %v", tc.max, err)
		}
		if pub.single != nil || pub.chunks != 0 || pub.buf != nil {
			t.Errorf("max %d: something was sent", tc.max)
		}
	}
	var pub collectPublisher
	if err := publishFrame(&pub, "s", "", &frame, frame.size, true); err != nil || len(pub.single) != frame.size {
		t.Errorf("a frame at the limit: %v, %d bytes sent whole", err, len(pub.single))
	}
}

func TestPullCallbackFrameOverASmallLimitFailsItsBatch(t *testing.T) {
	big := make([]byte, 5000)
	release := make(chan struct{})
	for _, tc := range []struct {
		name    string
		setup   func(inv *CallbackInvoker)
		produce func(ctx context.Context, inv *CallbackInvoker, ch chan<- int)
		// values before the error, the frame then fails while the next of its batch is in
		values int
		sent   int
	}{
		{name: "ahead of the next", sent: 1, produce: func(ctx context.Context, inv *CallbackInvoker, ch chan<- int) {
			for i := range 3 {
				inv.Invoke("onFrame", "small")
				inv.Invoke("onFrame", big)
				if !sendUnlessDone(ctx, ch, i) {
					return
				}
			}
		}},
		{name: "with the next in", values: 1, sent: 1, produce: func(ctx context.Context, inv *CallbackInvoker, ch chan<- int) {
			inv.Invoke("onFrame", "small")
			if !sendUnlessDone(ctx, ch, 0) {
				return
			}
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			inv.Invoke("onFrame", big)
			sendUnlessDone(ctx, ch, 1)
		}},
		{name: "instead of done", produce: func(_ context.Context, inv *CallbackInvoker, _ chan<- int) {
			inv.Invoke("onFrame", big)
		}},
		{name: "during the setup", setup: func(inv *CallbackInvoker) { inv.Invoke("onFrame", big) }, produce: func(ctx context.Context, _ *CallbackInvoker, ch chan<- int) {
			for i := 0; sendUnlessDone(ctx, ch, i); i++ {
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, server, values, stopped := playOverSmallLimit(t, tc.setup, tc.produce)
			for range tc.values {
				if v := <-values; v.Error != nil {
					t.Fatalf("a batch before the frame failed: %v", v.Error)
				}
			}
			if tc.values > 0 {
				waitFor(t, "the next of the failing batch", func() bool { return iteratorRequests(s) == tc.values+1 })
				time.Sleep(20 * time.Millisecond)
				close(release)
			}

			v := <-values
			var rerr *RPCException
			if !errors.As(v.Error, &rerr) || rerr.Code != ErrCodePayloadTooLarge || rerr.Msg != "Payload limit 4096 is too small for chunking" {
				t.Fatalf("the batch got %+v, want PAYLOAD_TOO_LARGE", v)
			}
			if v, open := <-values; open {
				t.Errorf("%+v after the error", v)
			}
			waitFor(t, "producer stopped", stopped.Load)
			waitFor(t, "session gone", func() bool { return pullSessions(server) == 0 })
			if n := s.publishes(func(subject string) bool { return strings.HasPrefix(subject, "_rpc.cb.") }); n != tc.sent {
				t.Errorf("%d callback frames sent, want %d", n, tc.sent)
			}
		})
	}
}

func TestPullCallbackFrameThatDoesNotEncodeFailsItsBatch(t *testing.T) {
	s, server, values, stopped := playOverSmallLimit(t, nil, func(ctx context.Context, inv *CallbackInvoker, ch chan<- int) {
		for i := range 3 {
			inv.Invoke("onFrame", "small")
			inv.Invoke("onFrame", make(chan int))
			if !sendUnlessDone(ctx, ch, i) {
				return
			}
		}
	})

	v := <-values
	var rerr *RPCException
	if !errors.As(v.Error, &rerr) || rerr.Code != ErrCodeInternalError || !strings.HasPrefix(rerr.Msg, "encode: ") {
		t.Fatalf("the batch got %+v, want INTERNAL_ERROR from the encoder", v)
	}
	if v, open := <-values; open {
		t.Errorf("%+v after the error", v)
	}
	waitFor(t, "producer stopped", stopped.Load)
	waitFor(t, "session gone", func() bool { return pullSessions(server) == 0 })
	if n := s.publishes(func(subject string) bool { return strings.HasPrefix(subject, "_rpc.cb.") }); n != 1 {
		t.Errorf("%d callback frames sent, want 1", n)
	}
}

// stopped turns true once the producer returned
func playOverSmallLimit(
	t *testing.T,
	setup func(inv *CallbackInvoker),
	produce func(ctx context.Context, inv *CallbackInvoker, ch chan<- int),
) (s *fakeNATS, server *Client, values <-chan PullValue, stopped *atomic.Bool) {
	t.Helper()
	s = startFakeNATS(t)
	s.setMaxPayload(4096)
	server = connectFake(t, s, nil)
	stopped = &atomic.Bool{}
	if _, err := server.RegisterHandler("pcb", map[string]any{
		"play": func(ctx context.Context, inv *CallbackInvoker) (<-chan int, error) {
			if setup != nil {
				setup(inv)
			}
			ch := make(chan int)
			go func() {
				defer stopped.Store(true)
				defer close(ch)
				produce(ctx, inv, ch)
			}()
			return ch, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	values, err := connectFake(t, s, nil).CallPullIteratorWithCallback(ctx, "rpc.pcb.play", PullCallbackMap{"onFrame": func(any) {}}, []string{"onFrame"})
	if err != nil {
		t.Fatal(err)
	}
	return s, server, values, stopped
}

func sendUnlessDone(ctx context.Context, ch chan<- int, v int) bool {
	select {
	case ch <- v:
		return true
	case <-ctx.Done():
		return false
	}
}

func iteratorRequests(s *fakeNATS) int {
	return s.publishes(func(subject string) bool {
		return strings.HasPrefix(subject, "_rpc.iterator.") && strings.HasSuffix(subject, ".request")
	})
}

// a peer that sends a next before the init reply while a frame of the setup failed
func TestPullCallbackSetupFailureWithAnEarlyNext(t *testing.T) {
	s := startFakeNATS(t)
	s.setMaxPayload(4096)
	server := connectFake(t, s, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	if _, err := server.RegisterHandler("pcb", map[string]any{
		"play": func(ctx context.Context, inv *CallbackInvoker) (<-chan int, error) {
			inv.Invoke("onFrame", make([]byte, 5000))
			close(entered)
			<-release
			ch := make(chan int)
			go func() {
				defer close(ch)
				for i := 0; sendUnlessDone(ctx, ch, i); i++ {
				}
			}()
			return ch, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	peer := connectFake(t, s, nil)
	failed := make(chan *RPCError, 4)
	if _, err := peer.Subscribe("_rpc.iterator.early.response", func(data []byte) {
		var resp PullIteratorResponse
		if DecodeMessageInto(data, &resp) == nil && resp.Type == "error" {
			failed <- resp.Error
		}
	}); err != nil {
		t.Fatal(err)
	}
	initDone := make(chan error, 1)
	go func() {
		_, err := peer.Call(context.Background(), "rpc.pcb.play", PullCallbackParams{
			PullCallback:    true,
			IteratorID:      "early",
			CallbackSubject: "_rpc.cb.early",
			CallbackMethods: []string{"onFrame"},
			OnewayMethods:   []string{"onFrame"},
		})
		initDone <- err
	}()
	<-entered
	next := PullIteratorRequest{ID: "early", Type: "next"}
	if err := peer.Publish("_rpc.iterator.early.request", next); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "early next", func() bool { return iteratorRequests(s) == 1 })
	time.Sleep(20 * time.Millisecond)
	close(release)
	if err := <-initDone; err != nil {
		t.Fatal(err)
	}

	if err := peer.Publish("_rpc.iterator.early.request", next); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-failed:
		if e == nil || e.Code != ErrCodePayloadTooLarge {
			t.Errorf("the session ended with %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("no error for the failed frame")
	}
	waitFor(t, "session gone", func() bool { return pullSessions(server) == 0 })
}
