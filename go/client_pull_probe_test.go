package rpc

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	pullRequests  = "_rpc.iterator."
	requestSuffix = ".request"
	answerSuffix  = ".response"
)

func pullSubject(suffix string) func(string) bool {
	return func(subject string) bool {
		return strings.HasPrefix(subject, pullRequests) && strings.HasSuffix(subject, suffix)
	}
}

// counts on demand, slow from the given step on, and reports when its session ended
func countingHandler(ended chan struct{}, slowFrom int, delay time.Duration) map[string]any {
	return map[string]any{
		"items": func(ctx context.Context) <-chan int {
			ch := make(chan int)
			go func() {
				defer close(ended)
				for i := 0; ; i++ {
					if i >= slowFrom {
						select {
						case <-time.After(delay):
						case <-ctx.Done():
							return
						}
					}
					select {
					case ch <- i:
					case <-ctx.Done():
						return
					}
				}
			}()
			return ch
		},
	}
}

func probingConsumer(t *testing.T, s *fakeNATS) *Client {
	t.Helper()
	consumer := connectFake(t, s, nil)
	consumer.pullProbeInterval = 20 * time.Millisecond
	return consumer
}

func nextValue(t *testing.T, values <-chan PullValue) PullValue {
	t.Helper()
	select {
	case v, ok := <-values:
		if !ok {
			t.Fatal("the iterator closed without a value or an error")
		}
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("no value and no error within 2s")
		return PullValue{}
	}
}

// reads on until the iterator fails, a value before that has to be the next one unless next is negative
func firstError(t *testing.T, values <-chan PullValue, next int) error {
	t.Helper()
	for range 8 {
		v := nextValue(t, values)
		if v.Error != nil {
			return v.Error
		}
		if next >= 0 {
			if fmt.Sprint(v.Value) != fmt.Sprint(next) {
				t.Fatalf("value = %+v, want %d", v, next)
			}
			next++
		}
	}
	t.Fatal("the iterator kept yielding after a message got lost")
	return nil
}

func TestPullStepWhoseRequestGotLostEndsWithAnError(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	ended := make(chan struct{})
	if _, err := server.RegisterHandler("pull", countingHandler(ended, 1<<30, 0)); err != nil {
		t.Fatal(err)
	}

	consumer := probingConsumer(t, s)
	values, err := consumer.CallPullIterator(context.Background(), "rpc.pull.items")
	if err != nil {
		t.Fatal(err)
	}
	if v := nextValue(t, values); v.Error != nil || fmt.Sprint(v.Value) != "0" {
		t.Fatalf("first value = %+v", v)
	}

	// the consumer asks one step ahead, so the values it already has still arrive
	s.loseNext(pullSubject(requestSuffix))
	if err := firstError(t, values, 1); !strings.Contains(err.Error(), "lost a message") {
		t.Fatalf("a step whose request got lost = %v, want the lost message error", err)
	}
	waitEnded(t, ended, "pull session after its consumer gave up")
}

func TestPullStepWhoseAnswerGotLostEndsWithAnError(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	ended := make(chan struct{})
	if _, err := server.RegisterHandler("pull", countingHandler(ended, 1<<30, 0)); err != nil {
		t.Fatal(err)
	}

	consumer := probingConsumer(t, s)
	values, err := consumer.CallPullIterator(context.Background(), "rpc.pull.items")
	if err != nil {
		t.Fatal(err)
	}
	if v := nextValue(t, values); v.Error != nil || fmt.Sprint(v.Value) != "0" {
		t.Fatalf("first value = %+v", v)
	}

	s.loseNext(pullSubject(answerSuffix))
	if err := firstError(t, values, 1); !strings.Contains(err.Error(), "lost a message") {
		t.Fatalf("a step whose answer got lost = %v, want the lost message error", err)
	}
	waitEnded(t, ended, "pull session after its consumer gave up")
}

func TestPullSessionWithASlowSourceOutlivesItsProbes(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	ended := make(chan struct{})
	if _, err := server.RegisterHandler("pull", countingHandler(ended, 1, 200*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	consumer := probingConsumer(t, s)
	session, leave := context.WithCancel(context.Background())
	values, err := consumer.CallPullIterator(session, "rpc.pull.items")
	if err != nil {
		t.Fatal(err)
	}
	// every step from the second one on outlasts about ten probes
	for want := range 4 {
		if v := nextValue(t, values); v.Error != nil || fmt.Sprint(v.Value) != fmt.Sprint(want) {
			t.Fatalf("value %d = %+v", want, v)
		}
	}
	if probes := s.publishes(pullSubject(requestSuffix)); probes < 10 {
		t.Fatalf("%d requests went out, the slow steps were not probed", probes)
	}

	leave()
	waitEnded(t, ended, "pull session after its consumer left")
}

func TestPullSessionOfASlowConsumerOutlivesItsProbes(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	ended := make(chan struct{})
	if _, err := server.RegisterHandler("pull", countingHandler(ended, 1<<30, 0)); err != nil {
		t.Fatal(err)
	}

	consumer := probingConsumer(t, s)
	session, leave := context.WithCancel(context.Background())
	values, err := consumer.CallPullIterator(session, "rpc.pull.items")
	if err != nil {
		t.Fatal(err)
	}
	// the handler has nothing open most of the time, its consumer takes longer than a probe interval per value
	for want := range 5 {
		if v := nextValue(t, values); v.Error != nil || fmt.Sprint(v.Value) != fmt.Sprint(want) {
			t.Fatalf("value %d = %+v", want, v)
		}
		time.Sleep(50 * time.Millisecond)
	}

	leave()
	waitEnded(t, ended, "pull session after its consumer left")
}

func TestPullStepIgnoresIdleAnswersToEarlierSteps(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	ended := make(chan struct{})
	if _, err := server.RegisterHandler("pull", countingHandler(ended, 1, 100*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	requests := make(chan string, 1)
	s.loseNext(func(subject string) bool {
		if pullSubject(requestSuffix)(subject) {
			select {
			case requests <- subject:
			default:
			}
		}
		return false
	})

	consumer := probingConsumer(t, s)
	session, leave := context.WithCancel(context.Background())
	values, err := consumer.CallPullIterator(session, "rpc.pull.items")
	if err != nil {
		t.Fatal(err)
	}
	if v := nextValue(t, values); v.Error != nil || fmt.Sprint(v.Value) != "0" {
		t.Fatalf("first value = %+v", v)
	}

	// probes that waited in a stalled connection get their answers in a burst, after the step they asked about
	id := strings.TrimSuffix(strings.TrimPrefix(<-requests, pullRequests), requestSuffix)
	for _, step := range []uint64{0, 1, 1, 1} {
		if err := server.Publish(pullRequests+id+answerSuffix, PullIteratorResponse{ID: id, Type: "idle", Step: step}); err != nil {
			t.Fatal(err)
		}
	}
	for want := 1; want < 4; want++ {
		if v := nextValue(t, values); v.Error != nil || fmt.Sprint(v.Value) != fmt.Sprint(want) {
			t.Fatalf("value %d = %+v", want, v)
		}
	}

	leave()
	waitEnded(t, ended, "pull session after its consumer left")
}

func TestPullStepOfASessionThatIsGoneEndsWithNoResponders(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("pull", map[string]any{
		"items": func() <-chan int {
			ch := make(chan int, 1)
			ch <- 0
			close(ch)
			return ch
		},
	}); err != nil {
		t.Fatal(err)
	}

	consumer := probingConsumer(t, s)
	values, err := consumer.CallPullIterator(context.Background(), "rpc.pull.items")
	if err != nil {
		t.Fatal(err)
	}
	// the handler ends its session with the second step, and its last word never arrives
	answers := 0
	s.loseNext(func(subject string) bool {
		if pullSubject(answerSuffix)(subject) {
			answers++
		}
		return answers == 2
	})
	if v := nextValue(t, values); v.Error != nil || fmt.Sprint(v.Value) != "0" {
		t.Fatalf("first value = %+v", v)
	}

	v := nextValue(t, values)
	if v.Error == nil || !strings.Contains(v.Error.Error(), "No responders") {
		t.Fatalf("a step of a session that is gone = %+v, want no responders", v)
	}
}

func TestPullCallbackStepWhoseRequestGotLostEndsWithAnError(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
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

	consumer := probingConsumer(t, s)
	values, err := consumer.CallPullIteratorWithCallback(context.Background(), "rpc.play.frames",
		PullCallbackMap{"onFrame": func(int) {}}, []string{"onFrame"})
	if err != nil {
		t.Fatal(err)
	}
	if v := nextValue(t, values); v.Error != nil {
		t.Fatal(v.Error)
	}

	s.loseNext(pullSubject(requestSuffix))
	if err := firstError(t, values, -1); !strings.Contains(err.Error(), "lost a message") {
		t.Fatalf("a batch whose request got lost = %v, want the lost message error", err)
	}
	waitEnded(t, ended, "pull-callback session after its consumer gave up")
}
