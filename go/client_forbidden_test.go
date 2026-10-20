package rpc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const deniedSubject = "rpc.sensors.rpc.getSensors"

func refusedPublish(subject string) error {
	// nats.go hands a refused publish to the async error callback in this form
	return fmt.Errorf("%w: %s", nats.ErrPermissionViolation, fmt.Sprintf("Permissions Violation for Publish to %q", subject))
}

func silentResponder(t *testing.T, s *fakeNATS, subjects ...string) {
	t.Helper()
	silent := connectFake(t, s, nil)
	for _, subject := range subjects {
		if _, err := silent.nc.Subscribe(subject, func(*nats.Msg) {}); err != nil {
			t.Fatal(err)
		}
	}
	if err := silent.nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestRefusedPublishFailsWaitingCallsWithForbidden(t *testing.T) {
	s := startFakeNATS(t)
	silentResponder(t, s, deniedSubject, "rpc.sensors.rpc.getSensorStates")
	client := connectFake(t, s, &ClientOptions{Timeout: 10 * time.Second})

	results := make(chan error, 3)
	for _, subject := range []string{deniedSubject, deniedSubject, "rpc.sensors.rpc.getSensorStates"} {
		go func() {
			_, err := client.Call(context.Background(), subject)
			results <- err
		}()
	}
	waitFor(t, "calls sent", func() bool {
		return s.publishes(subjectIs(deniedSubject)) == 2 && s.publishes(subjectIs("rpc.sensors.rpc.getSensorStates")) == 1
	})

	client.asyncError(nil, nil, refusedPublish(deniedSubject))

	for range 2 {
		select {
		case err := <-results:
			var rpcErr *RPCException
			if !errors.As(err, &rpcErr) || rpcErr.Code != ErrCodeForbidden {
				t.Fatalf("err = %v, want FORBIDDEN", err)
			}
		case <-time.After(time.Second):
			t.Fatal("call still waiting after the refusal")
		}
	}
	select {
	case err := <-results:
		t.Fatalf("call to an allowed subject ended: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if n := s.publishes(subjectIs(deniedSubject)); n != 2 {
		t.Errorf("%d requests to the refused subject, want 2", n)
	}
}

func TestRefusedPublishEndsStreamStart(t *testing.T) {
	s := startFakeNATS(t)
	silentResponder(t, s, deniedSubject)
	client := connectFake(t, s, nil)

	ch, err := client.CallStream(context.Background(), deniedSubject)
	if err != nil {
		t.Fatal(err)
	}
	client.asyncError(nil, nil, refusedPublish(deniedSubject))

	select {
	case v := <-ch:
		var rpcErr *RPCException
		if !errors.As(v.Error, &rpcErr) || rpcErr.Code != ErrCodeForbidden {
			t.Fatalf("stream value = %+v, want a FORBIDDEN error", v)
		}
	case <-time.After(time.Second):
		t.Fatal("stream still waiting after the refusal")
	}
	waitFor(t, "stream subscription gone", func() bool { return s.subsWithPrefix("stream."+deniedSubject) == 0 })
}

func TestRefusedSubscriptionLeavesCallsAlone(t *testing.T) {
	s := startFakeNATS(t)
	silentResponder(t, s, deniedSubject)
	client := connectFake(t, s, &ClientOptions{Timeout: 200 * time.Millisecond})

	done := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), deniedSubject)
		done <- err
	}()
	waitFor(t, "call sent", func() bool { return s.publishes(subjectIs(deniedSubject)) == 1 })
	client.asyncError(nil, nil, fmt.Errorf("%w: %s", nats.ErrPermissionViolation, fmt.Sprintf("Permissions Violation for Subscription to %q", deniedSubject)))

	err := <-done
	var rpcErr *RPCException
	if !errors.As(err, &rpcErr) || rpcErr.Code != ErrCodeTimeout {
		t.Fatalf("err = %v, want the call's own TIMEOUT", err)
	}
}
