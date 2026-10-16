package rpc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

var noRetry = &RequestOptions{NoResponderRetry: &NoResponderRetryOptions{MaxRetries: -1}}

func TestRegisterHandlerIsReadyOnReturn(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	caller := connectFake(t, s, nil)
	s.delaySubs(5 * time.Millisecond)

	for i := range 10 {
		ns := fmt.Sprintf("ready%d", i)
		if _, err := server.RegisterHandler(ns, map[string]any{
			"a": func() string { return "a" },
			"b": func() string { return "b" },
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := caller.CallWithOptions(context.Background(), "rpc."+ns+".b", noRetry); err != nil {
			t.Fatalf("round %d: call right after RegisterHandler: %v", i, err)
		}
	}
}

func TestRegisterHandlerFlushesOnce(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	methods := map[string]any{}
	for i := range 10 {
		methods[fmt.Sprintf("m%d", i)] = func() {}
	}

	before := s.pingCount()
	if _, err := server.RegisterHandler("queued", methods, WithQueue("workers")); err != nil {
		t.Fatal(err)
	}
	if pings := s.pingCount() - before; pings != 1 {
		t.Errorf("%d PINGs for one registration of 10 queued methods, want 1", pings)
	}
}

func TestSubscribeQueueIsReadyOnReturn(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	caller := connectFake(t, s, nil)
	s.delaySubs(5 * time.Millisecond)

	if _, err := server.SubscribeQueue("work.items", "workers", func([]byte) {}); err != nil {
		t.Fatal(err)
	}
	if _, err := caller.RequestWithOptions(context.Background(), "work.items", nil, &RequestOptions{
		Timeout:          50 * time.Millisecond,
		NoResponderRetry: noRetry.NoResponderRetry,
	}); err != nil {
		var rerr *RPCException
		if errors.As(err, &rerr) && rerr.Code == ErrCodeNotFound {
			t.Fatalf("request right after SubscribeQueue: %v", err)
		}
	}
}

func TestConnectRestoreIsReadyOnReturn(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	caller := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("restored", map[string]any{"ping": func() string { return "pong" }}); err != nil {
		t.Fatal(err)
	}

	if err := server.Suspend(); err != nil {
		t.Fatal(err)
	}
	s.delaySubs(5 * time.Millisecond)
	before := s.pingCount()
	if err := server.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := caller.CallWithOptions(context.Background(), "rpc.restored.ping", noRetry); err != nil {
		t.Fatalf("call right after Connect restored the handler: %v", err)
	}
	// the handshake PING of the new connection plus one for the restore
	if pings := s.pingCount() - before; pings != 2 {
		t.Errorf("%d PINGs for a restore of 3 subscriptions, want 2", pings)
	}
}
