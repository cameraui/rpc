package rpc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPullCancelDuringHandlerSetupEndsSession(t *testing.T) {
	cases := []struct {
		name   string
		marker string
	}{
		{"pull iterator", "__pullIterator"},
		{"pull-callback", "__pullCallback"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := startFakeNATS(t)
			server := connectFake(t, s, nil)
			entered := make(chan struct{})
			release := make(chan struct{})
			released := false
			t.Cleanup(func() {
				if !released {
					close(release)
				}
			})
			var stopped atomic.Bool
			produce := func(ctx context.Context) <-chan int {
				<-release
				ch := make(chan int)
				go func() {
					defer close(ch)
					for {
						select {
						case ch <- 1:
						case <-ctx.Done():
							stopped.Store(true)
							return
						}
					}
				}()
				return ch
			}
			handlers := map[string]any{
				"items": func(ctx context.Context) (<-chan int, error) {
					close(entered)
					return produce(ctx), nil
				},
				"play": func(ctx context.Context, _ *CallbackInvoker) (<-chan int, error) {
					close(entered)
					return produce(ctx), nil
				},
			}
			if _, err := server.RegisterHandler("pc", handlers); err != nil {
				t.Fatal(err)
			}
			client := connectFake(t, s, nil)

			method := "items"
			params := map[string]any{tc.marker: true, "__iteratorId": "it1", "args": []any{}}
			if tc.marker == "__pullCallback" {
				method = "play"
				params["__callbackSubject"] = "_rpc.cb.it1"
				params["__onewayMethods"] = []any{}
			}
			if err := client.Publish("rpc.pc."+method, RPCMessage{ID: "call-it1", Method: method, Params: params}); err != nil {
				t.Fatal(err)
			}
			<-entered
			waitFor(t, "request subscription during setup", func() bool { return s.subsWithPrefix("_rpc.iterator.it1.request") == 1 })
			if err := client.Publish("_rpc.iterator.it1.request", PullIteratorRequest{ID: "it1", Type: "cancel"}); err != nil {
				t.Fatal(err)
			}
			_ = client.nc.Flush()
			waitFor(t, "cancel delivered", func() bool {
				return s.publishes(func(subject string) bool { return subject == "_rpc.iterator.it1.request" }) == 1
			})
			time.Sleep(20 * time.Millisecond)
			released = true
			close(release)

			waitFor(t, "producer stopped", stopped.Load)
			waitFor(t, "session gone", func() bool {
				return pullSessions(server) == 0 && s.subsWithPrefix("_rpc.iterator.it1.request") == 0
			})
		})
	}
}
