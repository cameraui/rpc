package rpc

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func orphanServer(t *testing.T, s *fakeNATS, interval, grace time.Duration) *Client {
	t.Helper()
	return connectFake(t, s, &ClientOptions{OrphanProbe: &OrphanProbeOptions{Interval: interval, Grace: grace}})
}

func waitEnded(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not end within 2s", what)
	}
}

func staysOpen(t *testing.T, ch <-chan struct{}, d time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s ended although its consumer is alive", what)
	case <-time.After(d):
	}
}

// a handler whose callback session reports its end
func callbackHandler(ended chan struct{}, publish func(cb func(int))) map[string]any {
	return map[string]any{
		"watch": func(cb func(int)) func() {
			if publish != nil {
				go publish(cb)
			}
			return func() { close(ended) }
		},
	}
}

func TestOrphanCallbackSessionEndsAfterGrace(t *testing.T) {
	s := startFakeNATS(t)
	server := orphanServer(t, s, 10*time.Millisecond, 60*time.Millisecond)
	ended := make(chan struct{})
	if _, err := server.RegisterHandler("orphan", callbackHandler(ended, nil)); err != nil {
		t.Fatal(err)
	}

	consumer := connectFake(t, s, nil)
	if _, err := CallWithCallback(consumer, "rpc.orphan.watch", nil, func(any) {}); err != nil {
		t.Fatal(err)
	}
	gone := time.Now()
	_ = consumer.Disconnect()

	waitEnded(t, ended, "callback session")
	if since := time.Since(gone); since < 60*time.Millisecond {
		t.Errorf("session ended %v after its consumer left, before the grace of 60ms", since)
	}
}

func TestOrphanProbeKeepsLiveCallbackConsumer(t *testing.T) {
	s := startFakeNATS(t)
	server := orphanServer(t, s, 10*time.Millisecond, 40*time.Millisecond)
	idleEnded := make(chan struct{})
	busyEnded := make(chan struct{})
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	if _, err := server.RegisterHandler("idle", callbackHandler(idleEnded, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := server.RegisterHandler("busy", callbackHandler(busyEnded, func(cb func(int)) {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
				cb(i)
			}
		}
	})); err != nil {
		t.Fatal(err)
	}

	consumer := connectFake(t, s, nil)
	var idleValues, busyValues atomic.Int64
	if _, err := CallWithCallback(consumer, "rpc.idle.watch", nil, func(any) { idleValues.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if _, err := CallWithCallback(consumer, "rpc.busy.watch", nil, func(any) { busyValues.Add(1) }); err != nil {
		t.Fatal(err)
	}

	staysOpen(t, idleEnded, 200*time.Millisecond, "idle callback session")
	staysOpen(t, busyEnded, 0, "busy callback session")
	if n := idleValues.Load(); n != 0 {
		t.Errorf("the idle consumer got %d values, the probe must not reach its callback", n)
	}
	if busyValues.Load() == 0 {
		t.Error("the busy consumer got no values")
	}
}

func TestOrphanProbeRidesOnDataAtMostOncePerInterval(t *testing.T) {
	s := startFakeNATS(t)
	server := orphanServer(t, s, 20*time.Millisecond, time.Minute)
	ended := make(chan struct{})
	sent := make(chan int, 1)
	if _, err := server.RegisterHandler("rate", callbackHandler(ended, func(cb func(int)) {
		n := 0
		for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); n++ {
			cb(n)
			time.Sleep(time.Millisecond)
		}
		sent <- n
	})); err != nil {
		t.Fatal(err)
	}

	consumer := connectFake(t, s, nil)
	var callbackSubject string
	unsub, err := CallWithCallback(consumer, "rpc.rate.watch", nil, func(any) {})
	if err != nil {
		t.Fatal(err)
	}
	defer unsub()
	n := <-sent
	for _, subject := range s.publishOrder() {
		if strings.HasPrefix(subject, "rpc.cb.") && !strings.HasSuffix(subject, ".cancel") {
			callbackSubject = subject
		}
	}
	probes := s.requestsTo(callbackSubject)
	if probes < 2 || probes > 7 {
		t.Errorf("%d of %d callback messages carried the probe over 100ms with a 20ms interval, want 2 to 7", probes, n)
	}
}

func TestOrphanProbeSurvivesShortAbsence(t *testing.T) {
	s := startFakeNATS(t)
	server := orphanServer(t, s, 10*time.Millisecond, 150*time.Millisecond)
	ended := make(chan struct{})
	if _, err := server.RegisterHandler("blip", callbackHandler(ended, nil)); err != nil {
		t.Fatal(err)
	}
	consumer := connectFake(t, s, nil)
	if _, err := CallWithCallback(consumer, "rpc.blip.watch", nil, func(any) {}); err != nil {
		t.Fatal(err)
	}

	// the callback subscription comes back with the restore
	if err := consumer.Suspend(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := consumer.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	staysOpen(t, ended, 400*time.Millisecond, "callback session after a short absence")
}

func TestOrphanProbeCanBeTurnedOff(t *testing.T) {
	s := startFakeNATS(t)
	server := orphanServer(t, s, 10*time.Millisecond, 30*time.Millisecond)
	ended := make(chan struct{})
	if _, err := server.RegisterHandler("kept", callbackHandler(ended, nil), WithOrphanReaping(OrphanProbeOptions{Disabled: true})); err != nil {
		t.Fatal(err)
	}
	consumer := connectFake(t, s, nil)
	if _, err := CallWithCallback(consumer, "rpc.kept.watch", nil, func(any) {}); err != nil {
		t.Fatal(err)
	}
	_ = consumer.Disconnect()
	staysOpen(t, ended, 200*time.Millisecond, "callback session without the probe")
}

func TestOrphanPullCallbackSessionEnds(t *testing.T) {
	s := startFakeNATS(t)
	server := orphanServer(t, s, 10*time.Millisecond, 60*time.Millisecond)
	ended := make(chan struct{})
	if _, err := server.RegisterHandler("play", map[string]any{
		"frames": func(inv *CallbackInvoker) <-chan int {
			ch := make(chan int)
			go func() {
				defer close(ended)
				for i := 0; ; i++ {
					inv.Invoke("onFrame", i)
					select {
					case ch <- i:
					case <-inv.Context().Done():
						return
					}
				}
			}()
			return ch
		},
	}); err != nil {
		t.Fatal(err)
	}

	consumer := connectFake(t, s, nil)
	frames := make(chan int, 16)
	values, err := consumer.CallPullIteratorWithCallback(context.Background(), "rpc.play.frames",
		PullCallbackMap{"onFrame": func(i int) { frames <- i }}, []string{"onFrame"})
	if err != nil {
		t.Fatal(err)
	}
	if v := <-values; v.Error != nil {
		t.Fatal(v.Error)
	}
	if <-frames != 0 {
		t.Fatal("first frame missing")
	}

	// a live consumer parked between batches keeps its session
	staysOpen(t, ended, 150*time.Millisecond, "parked pull-callback session")
	if v := <-values; v.Error != nil {
		t.Fatalf("next batch after the idle probes: %v", v.Error)
	}

	_ = consumer.Disconnect()
	waitEnded(t, ended, "pull-callback session")
}

func TestOrphanPullIteratorSessionEnds(t *testing.T) {
	s := startFakeNATS(t)
	server := orphanServer(t, s, 10*time.Millisecond, 60*time.Millisecond)
	ended := make(chan struct{})
	if _, err := server.RegisterHandler("pull", map[string]any{
		"items": func(ctx context.Context) <-chan int {
			ch := make(chan int)
			go func() {
				defer close(ended)
				for i := 0; ; i++ {
					select {
					case ch <- i:
					case <-ctx.Done():
						return
					}
				}
			}()
			return ch
		},
	}); err != nil {
		t.Fatal(err)
	}

	consumer := connectFake(t, s, nil)
	values, err := consumer.CallPullIterator(context.Background(), "rpc.pull.items")
	if err != nil {
		t.Fatal(err)
	}
	if v := <-values; v.Error != nil || fmt.Sprint(v.Value) != "0" {
		t.Fatalf("first value = %+v", v)
	}
	time.Sleep(100 * time.Millisecond)
	if v := <-values; v.Error != nil || fmt.Sprint(v.Value) != "1" {
		t.Fatalf("value after the idle probes = %+v", v)
	}

	_ = consumer.Disconnect()
	waitEnded(t, ended, "pull iterator session")
}

func TestOrphanPushStreamEnds(t *testing.T) {
	s := startFakeNATS(t)
	server := orphanServer(t, s, 10*time.Millisecond, 60*time.Millisecond)
	ended := make(chan struct{})
	if _, err := server.RegisterHandler("push", map[string]any{
		"live": func(ctx context.Context) <-chan int {
			ch := make(chan int)
			go func() {
				defer close(ended)
				<-ctx.Done()
			}()
			return ch
		},
	}); err != nil {
		t.Fatal(err)
	}

	consumer := connectFake(t, s, nil)
	if _, err := consumer.CallStream(context.Background(), "rpc.push.live"); err != nil {
		t.Fatal(err)
	}
	staysOpen(t, ended, 150*time.Millisecond, "idle push stream")

	_ = consumer.Disconnect()
	waitEnded(t, ended, "push stream")
}
