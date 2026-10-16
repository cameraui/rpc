package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func asyncErrors(t *testing.T, s *fakeNATS) (client *Client, errs <-chan *AsyncError) {
	t.Helper()
	ch := make(chan *AsyncError, 16)
	client = connectFake(t, s, &ClientOptions{OnAsyncError: func(err *AsyncError) {
		select {
		case ch <- err:
		default:
		}
	}})
	return client, ch
}

func wantSlowConsumer(t *testing.T, errs <-chan *AsyncError, subject string) {
	t.Helper()
	select {
	case err := <-errs:
		if !errors.Is(err, nats.ErrSlowConsumer) || err.Subject != subject || err.Dropped < 1 {
			t.Errorf("async error = %+v, want a slow consumer on %s with dropped messages", err, subject)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("no slow consumer reported for %s", subject)
	}
}

func TestOnAsyncErrorGetsSlowConsumer(t *testing.T) {
	s := startFakeNATS(t)
	c, errs := asyncErrors(t, s)
	release := make(chan struct{})
	defer close(release)
	if _, err := c.Subscribe("slow.subject", func([]byte) { <-release }, func(sub *nats.Subscription) {
		_ = sub.SetPendingLimits(1, -1)
	}); err != nil {
		t.Fatal(err)
	}

	publisher := connectFake(t, s, nil)
	for range 5 {
		if err := publisher.Publish("slow.subject", "x"); err != nil {
			t.Fatal(err)
		}
	}
	wantSlowConsumer(t, errs, "slow.subject")
}

func TestWithPendingLimitsCapsMethods(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []HandlerOption
	}{
		{"per method", nil},
		{"namespace subscription", []HandlerOption{WithNamespaceSubscription()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := startFakeNATS(t)
			server, errs := asyncErrors(t, s)
			release := make(chan struct{})
			defer close(release)
			opts := append([]HandlerOption{WithPendingLimits(1, -1)}, tc.opts...)
			if _, err := server.RegisterHandler("capped", map[string]any{"block": func() { <-release }}, opts...); err != nil {
				t.Fatal(err)
			}

			caller := connectFake(t, s, nil)
			for range 5 {
				go func() {
					_, _ = caller.CallWithOptions(context.Background(), "rpc.capped.block", &RequestOptions{Timeout: 500 * time.Millisecond})
				}()
			}
			wantSlowConsumer(t, errs, "rpc.capped.block")
		})
	}
}
