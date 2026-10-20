package rpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestFormatErrorObjectHidesLocalNoResponders(t *testing.T) {
	local := noRespondersException("rpc.cam.snapshot", "No responders for rpc.cam.snapshot")

	got := FormatErrorObject(local)
	if got.Code != ErrCodeInternalError || got.Message != "upstream unavailable" {
		t.Errorf("marked = %+v", got)
	}
	if data, _ := got.Data.(map[string]any); data["subject"] != "rpc.cam.snapshot" {
		t.Errorf("data = %v", got.Data)
	}

	wrapped := FormatErrorObject(fmt.Errorf("snapshot of cam: %w", local))
	if wrapped.Code != ErrCodeInternalError || wrapped.Message != "snapshot of cam: upstream unavailable" {
		t.Errorf("wrapped = %+v", wrapped)
	}

	inner := FormatErrorObject(newLocalNoResponders("rpc.cam.snapshot", "No responders for rpc.cam.snapshot"))
	if inner.Code != ErrCodeInternalError || inner.Message != "upstream unavailable" {
		t.Errorf("retry wrapper = %+v", inner)
	}

	remote := FormatErrorObject(NewRPCException(ErrCodeNotFound, "No responders for rpc.cam.snapshot"))
	if remote.Code != ErrCodeNotFound || remote.Message != "No responders for rpc.cam.snapshot" {
		t.Errorf("a NOT_FOUND the handler made itself = %+v", remote)
	}
	if received := RPCExceptionFromError(&RPCError{Code: ErrCodeNotFound, Message: "No responders for x"}); FormatErrorObject(received).Code != ErrCodeNotFound {
		t.Error("a NOT_FOUND decoded from the wire was rewritten")
	}
}

func TestForwardedNoResponderReachesTheCallerAsUpstreamUnavailable(t *testing.T) {
	s := startFakeNATS(t)
	middle := connectFake(t, s, nil)
	seen := make(chan error, 4)
	_, err := middle.RegisterHandler("mid", map[string]any{
		"relay": func(ctx context.Context) (any, error) {
			_, err := middle.CallWithOptions(ctx, "rpc.nobody.snapshot", &RequestOptions{NoResponderRetry: &NoResponderRetryOptions{MaxRetries: -1}})
			seen <- err
			return nil, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, fastRetries())

	start := time.Now()
	_, err = client.CreateProxy("mid").Invoke(context.Background(), "relay")
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("error after %v", took)
	}
	var rerr *RPCException
	if !errors.As(err, &rerr) || rerr.Code != ErrCodeInternalError || rerr.Msg != "upstream unavailable" {
		t.Fatalf("caller got %v, want INTERNAL_ERROR upstream unavailable", err)
	}
	if data, _ := rerr.Details.(map[string]any); data["subject"] != "rpc.nobody.snapshot" {
		t.Errorf("data = %v", rerr.Details)
	}
	if len(seen) != 1 {
		t.Errorf("the middle ran %d times, want 1", len(seen))
	}
	handlerErr := <-seen
	var inner *RPCException
	if !errors.As(handlerErr, &inner) || inner.Code != ErrCodeNotFound || inner.Msg != "No responders for rpc.nobody.snapshot" {
		t.Errorf("the handler saw %v, want the unchanged no-responder error", handlerErr)
	}
}

func serveHalfIterators(t *testing.T, s *fakeNATS, subject string) {
	t.Helper()
	half := connectFake(t, s, nil)
	_, err := half.Subscribe(subject, func(data []byte) {
		var msg RPCMessage
		if DecodeMessageInto(data, &msg) != nil {
			return
		}
		var id string
		if isPullCallbackRequest(msg.Params) {
			id, _, _, _ = extractPullCallbackParams(msg.Params, msg.ID)
		} else {
			id, _ = extractPullIteratorParams(msg.Params, msg.ID)
		}
		_ = half.Publish("rpc.reply."+msg.ID, RPCResponse{ID: msg.ID, Result: map[string]any{"iteratorId": id}})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := half.nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func firstStreamError(ch <-chan StreamValue, err error) error {
	if err != nil {
		return err
	}
	for v := range ch {
		if v.Error != nil {
			return v.Error
		}
	}
	return errors.New("the stream ended without an error")
}

func firstPullError(ch <-chan PullValue, err error) error {
	if err != nil {
		return err
	}
	for v := range ch {
		if v.Error != nil {
			return v.Error
		}
	}
	return errors.New("the iterator ended without an error")
}

func TestEveryLocalNoResponderReachesTheCallerAsUpstreamUnavailable(t *testing.T) {
	s := startFakeNATS(t)
	serveHalfIterators(t, s, "rpc.half.items")
	middle := connectFake(t, s, &ClientOptions{NoResponderRetry: &NoResponderRetryOptions{Delays: []time.Duration{5 * time.Millisecond}}})

	sites := []struct {
		method, subject, code string
		late                  bool
		run                   func(ctx context.Context) error
	}{
		{"request", "nobody.request", ErrCodeNotFound, false, func(ctx context.Context) error {
			_, err := middle.Request(ctx, "nobody.request", "hi", time.Second)
			return err
		}},
		{"service", "nobody.service", ErrCodeNotFound, false, func(ctx context.Context) error {
			_, err := middle.Call(ctx, "nobody.service", "hi")
			return err
		}},
		{"stream", "rpc.nobody.stream", ErrCodeNotFound, false, func(ctx context.Context) error {
			return firstStreamError(middle.CallStream(ctx, "rpc.nobody.stream"))
		}},
		{"streamLate", "rpc.nobody.late", ErrCodeNotFound, true, func(ctx context.Context) error {
			return firstStreamError(middle.CallStream(ctx, "rpc.nobody.late"))
		}},
		{"pullNext", "rpc.half.items", "503", false, func(ctx context.Context) error {
			return firstPullError(middle.CallPullIterator(ctx, "rpc.half.items"))
		}},
		{"pullCallbackNext", "rpc.half.items", "503", false, func(ctx context.Context) error {
			return firstPullError(middle.CallPullIteratorWithCallback(ctx, "rpc.half.items", PullCallbackMap{}, nil))
		}},
	}
	seen := make(chan error, 8)
	handlers := map[string]any{}
	for _, site := range sites {
		handlers[site.method] = func(ctx context.Context) (any, error) {
			err := site.run(ctx)
			seen <- err
			return nil, err
		}
	}
	if _, err := middle.RegisterHandler("mid", handlers); err != nil {
		t.Fatal(err)
	}
	proxy := connectFake(t, s, &ClientOptions{Timeout: 3 * time.Second}).CreateProxy("mid")
	// the 503 of a request reaches the client before the Flush behind it returns
	s.delayPongs(20 * time.Millisecond)

	for _, site := range sites {
		t.Run(site.method, func(t *testing.T) {
			if site.late {
				s.delayNoResponders(100 * time.Millisecond)
				defer s.delayNoResponders(0)
			}
			_, err := proxy.Invoke(context.Background(), site.method)
			var handlerErr error
			select {
			case handlerErr = <-seen:
			case <-time.After(time.Second):
				t.Fatal("the middle did not run")
			}
			if n := len(seen); n != 0 {
				t.Fatalf("the middle ran %d more times", n)
			}
			var inner *RPCException
			if !errors.As(handlerErr, &inner) || inner.Code != site.code || !strings.HasPrefix(inner.Msg, "No responders") {
				t.Errorf("the handler saw %v, want the unchanged no-responder error", handlerErr)
			}
			var rerr *RPCException
			if !errors.As(err, &rerr) || rerr.Code != ErrCodeInternalError || rerr.Msg != upstreamUnavailable {
				t.Fatalf("caller got %v, want INTERNAL_ERROR upstream unavailable", err)
			}
			if data, _ := rerr.Details.(map[string]any); data["subject"] != site.subject {
				t.Errorf("data = %v, want subject %s", rerr.Details, site.subject)
			}
		})
	}
}
