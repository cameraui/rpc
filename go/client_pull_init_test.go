package rpc

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type pullInitPeers struct {
	s            *fakeNATS
	server       *Client
	client       *Client
	entered      chan struct{}
	release      chan struct{}
	releaseSetup func()
	stopped      atomic.Bool
}

// the handler's setup blocks until release, its producer runs until its session ends
func newPullInitPeers(t *testing.T) *pullInitPeers {
	t.Helper()
	p := &pullInitPeers{s: startFakeNATS(t), entered: make(chan struct{}, 4), release: make(chan struct{})}
	p.server = connectFake(t, p.s, nil)
	released := false
	t.Cleanup(func() {
		if !released {
			close(p.release)
		}
	})
	produce := func(ctx context.Context) <-chan int {
		p.entered <- struct{}{}
		<-p.release
		ch := make(chan int)
		go func() {
			defer close(ch)
			for {
				select {
				case ch <- 1:
				case <-ctx.Done():
					p.stopped.Store(true)
					return
				}
			}
		}()
		return ch
	}
	if _, err := p.server.RegisterHandler("pi", map[string]any{
		"items": func(ctx context.Context) (<-chan int, error) { return produce(ctx), nil },
		"play":  func(ctx context.Context, _ *CallbackInvoker) (<-chan int, error) { return produce(ctx), nil },
		"plain": func() int { return 1 },
	}); err != nil {
		t.Fatal(err)
	}
	p.client = connectFake(t, p.s, nil)
	p.releaseSetup = func() {
		released = true
		close(p.release)
	}
	return p
}

func (p *pullInitPeers) open(ctx context.Context, kind, method string) error {
	var err error
	if kind == "pull-callback" {
		_, err = p.client.CallPullIteratorWithCallback(ctx, "rpc.pi."+method, PullCallbackMap{}, nil)
	} else {
		_, err = p.client.CallPullIterator(ctx, "rpc.pi."+method)
	}
	return err
}

func (p *pullInitPeers) cancels() int {
	return p.s.publishes(func(subject string) bool {
		return strings.HasPrefix(subject, "_rpc.iterator.") && strings.HasSuffix(subject, ".request")
	})
}

func endingContext(ending string) (context.Context, context.CancelFunc) {
	if ending == "deadline" {
		return context.WithTimeout(context.Background(), 50*time.Millisecond)
	}
	return context.WithCancel(context.Background())
}

func TestPullInitCancelledSendsOneCancelAfterTheReply(t *testing.T) {
	for _, kind := range []string{"pull iterator", "pull-callback"} {
		for _, ending := range []string{"cancel", "deadline"} {
			t.Run(kind+"/"+ending, func(t *testing.T) {
				testPullInitEnds(t, kind, ending)
			})
		}
	}
}

func testPullInitEnds(t *testing.T, kind, ending string) {
	p := newPullInitPeers(t)
	method := map[string]string{"pull iterator": "items", "pull-callback": "play"}[kind]
	ctx, cancel := endingContext(ending)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- p.open(ctx, kind, method) }()
	<-p.entered

	if ending == "cancel" {
		cancel()
	}
	var rerr *RPCException
	select {
	case err := <-result:
		if !errors.As(err, &rerr) || rerr.Code != ErrCodeTimeout || !strings.Contains(err.Error(), "context cancelled") {
			t.Errorf("err = %v, want a TIMEOUT for the ended context", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("the call did not return when its context ended")
	}
	// a cancel ahead of the init reply would find no session yet
	time.Sleep(20 * time.Millisecond)
	if n := p.cancels(); n != 0 {
		t.Errorf("%d cancels before the init reply", n)
	}

	p.releaseSetup()
	waitFor(t, "producer stopped", p.stopped.Load)
	waitFor(t, "session gone", func() bool { return pullSessions(p.server) == 0 })
	time.Sleep(20 * time.Millisecond)
	if n := p.cancels(); n != 1 {
		t.Errorf("%d cancels, want 1", n)
	}
}

// the second init waits on the responder behind the first one's setup, it has
// no session there yet that an early cancel could end
func TestPullInitQueuedBehindASetupEndsItsSession(t *testing.T) {
	for _, ending := range []string{"cancel", "deadline"} {
		t.Run(ending, func(t *testing.T) {
			p := newPullInitPeers(t)
			first := make(chan error, 1)
			go func() { first <- p.open(context.Background(), "pull iterator", "items") }()
			<-p.entered

			ctx, cancel := endingContext(ending)
			defer cancel()
			if ending == "cancel" {
				time.AfterFunc(50*time.Millisecond, cancel)
			}
			if err := p.open(ctx, "pull iterator", "items"); err == nil {
				t.Fatal("the second init returned a session after its context ended")
			}

			p.releaseSetup()
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			waitFor(t, "the abandoned producer stopped", p.stopped.Load)
			time.Sleep(20 * time.Millisecond)
			if n := pullSessions(p.server); n != 1 {
				t.Errorf("%d sessions on the server, want the first one only", n)
			}
		})
	}
}

func TestPullInitSendsOneCancelWhenItFails(t *testing.T) {
	p := newPullInitPeers(t)
	p.releaseSetup()
	err := p.open(context.Background(), "pull iterator", "plain")
	if err == nil || !strings.Contains(err.Error(), "init pull iterator") {
		t.Fatalf("err = %v, want the handler's refusal", err)
	}
	waitFor(t, "cancel sent", func() bool { return p.cancels() == 1 })
}

func TestPullInitWithEndedContextSendsNothing(t *testing.T) {
	p := newPullInitPeers(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, kind := range []string{"pull iterator", "pull-callback"} {
		if err := p.open(ctx, kind, "items"); err == nil || !strings.Contains(err.Error(), "context cancelled") {
			t.Errorf("%s: err = %v", kind, err)
		}
	}
	_ = p.client.nc.Flush()
	time.Sleep(20 * time.Millisecond)
	if n := p.s.publishes(func(subject string) bool { return strings.HasPrefix(subject, "rpc.pi.") }); n != 0 {
		t.Errorf("%d init calls went out with an ended context", n)
	}
	if n := p.cancels(); n != 0 {
		t.Errorf("%d cancels without an init call", n)
	}
}
