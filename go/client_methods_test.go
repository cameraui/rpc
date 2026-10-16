package rpc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

type methodsDB struct{}

func (methodsDB) Find(key string) string { return "found " + key }

type methodsHandler struct {
	DB *methodsDB `rpc:"db"`
}

func (h *methodsHandler) Ping() string { return "pong" }

func fastRetries() *ClientOptions {
	delays := []time.Duration{60 * time.Millisecond, 60 * time.Millisecond, 60 * time.Millisecond}
	return &ClientOptions{Timeout: 3 * time.Second, NoResponderRetry: &NoResponderRetryOptions{MaxRetries: 3, Delays: delays}}
}

func expectNoResponders(t *testing.T, what string, err error) {
	t.Helper()
	var rerr *RPCException
	if !errors.As(err, &rerr) || rerr.Code != ErrCodeNotFound {
		t.Errorf("%s = %v, want the no-responder error", what, err)
	}
}

func TestRegisterHandlerAnswersTheMethodList(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	cleanup, err := server.RegisterHandler("ml", &methodsHandler{DB: &methodsDB{}})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	got, err := client.Call(context.Background(), "rpc.ml.__methods")
	if err != nil {
		t.Fatal(err)
	}
	if want := []any{"db.find", "ping"}; !slices.Equal(got.([]any), want) {
		t.Errorf("method list = %v, want %v", got, want)
	}

	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	_, err = client.CallWithOptions(context.Background(), "rpc.ml.__methods", &RequestOptions{NoResponderRetry: &NoResponderRetryOptions{MaxRetries: -1}})
	expectNoResponders(t, "method list after cleanup", err)
}

func TestMissingMethodOnALiveNamespaceFailsAtOnce(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("mm", &methodsHandler{DB: &methodsDB{}}); err != nil {
		t.Fatal(err)
	}
	proxy := connectFake(t, s, &ClientOptions{Timeout: 3 * time.Second}).CreateProxy("mm")

	for _, tc := range []struct {
		name    string
		subject string
		call    func() error
	}{
		{"call", "rpc.mm.snapshot", func() error {
			_, err := proxy.Invoke(context.Background(), "snapshot")
			return err
		}},
		{"nested", "rpc.mm.db.drop", func() error {
			_, err := proxy.Sub("db").Invoke(context.Background(), "drop")
			return err
		}},
		{"stream", "rpc.mm.frames", func() error {
			ch, err := proxy.InvokeStream(context.Background(), "frames")
			if err != nil {
				return err
			}
			// a 503 that lands after the flush arrives through the channel
			v := <-ch
			return v.Error
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			err := tc.call()
			if took := time.Since(start); took > 200*time.Millisecond {
				t.Errorf("error after %v, want it before the first retry", took)
			}
			expectNoResponders(t, tc.name, err)
			if n := s.publishes(subjectIs(tc.subject)); n != 1 {
				t.Errorf("%d requests to %s, want 1", n, tc.subject)
			}
		})
	}

	if got, err := proxy.Sub("db").Invoke(context.Background(), "find", "k"); err != nil || got != "found k" {
		t.Errorf("served nested method = %v, %v", got, err)
	}
}

func TestEmptyNamespaceAnswersAnEmptyMethodList(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("empty", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, &ClientOptions{Timeout: 3 * time.Second})

	got, err := client.Call(context.Background(), "rpc.empty.__methods")
	if list, ok := got.([]any); err != nil || !ok || len(list) != 0 {
		t.Errorf("method list = %#v, %v, want an empty list", got, err)
	}

	start := time.Now()
	_, err = client.CreateProxy("empty").Invoke(context.Background(), "missing")
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Errorf("error after %v, want it before the first retry", took)
	}
	expectNoResponders(t, "missing method", err)
}

func TestNoResponderRetriesWhenTheProbeCannotTell(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	// lists a method nobody serves: it may be restarting, so the retry goes on
	unsub, err := server.Subscribe("rpc.listed.__methods", func(data []byte) {
		var msg RPCMessage
		if DecodeMessageInto(data, &msg) == nil {
			_ = server.Publish("rpc.reply."+msg.ID, RPCResponse{ID: msg.ID, Result: []string{"later"}})
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unsub)
	client := connectFake(t, s, fastRetries())

	for _, tc := range []struct{ name, namespace, method string }{
		{"dead namespace", "gone", "m"},
		{"method listed", "listed", "later"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.CreateProxy(tc.namespace).Invoke(context.Background(), tc.method)
			expectNoResponders(t, tc.name, err)
			subject := fmt.Sprintf("rpc.%s.%s", tc.namespace, tc.method)
			if n := s.publishes(subjectIs(subject)); n != 4 {
				t.Errorf("%d requests to %s, want 4", n, subject)
			}
		})
	}
}

func TestMissingMethodFailsOnceALateMethodListArrives(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	// a remote namespace answers later than the first retry delays
	unsub, err := server.Subscribe("rpc.late.__methods", func(data []byte) {
		var msg RPCMessage
		if DecodeMessageInto(data, &msg) == nil {
			time.AfterFunc(80*time.Millisecond, func() {
				_ = server.Publish("rpc.reply."+msg.ID, RPCResponse{ID: msg.ID, Result: []string{"other"}})
			})
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unsub)
	client := connectFake(t, s, &ClientOptions{Timeout: 3 * time.Second})

	start := time.Now()
	_, err = client.CreateProxy("late").Invoke(context.Background(), "missing")
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("error after %v, want it once the method list arrived", took)
	}
	expectNoResponders(t, "missing method", err)
}

func TestPlainCallsDoNotProbe(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("pc", &methodsHandler{}); err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, fastRetries())

	_, err := client.Call(context.Background(), "rpc.pc.snapshot")
	expectNoResponders(t, "call", err)
	if n := s.publishes(subjectIs("rpc.pc.snapshot")); n != 4 {
		t.Errorf("%d requests, want the full retry of 4", n)
	}
	if n := s.publishes(subjectIs("rpc.pc.__methods")); n != 0 {
		t.Errorf("%d probes from a plain call", n)
	}
}
