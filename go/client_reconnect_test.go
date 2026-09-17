package rpc

import (
	"sync"
	"testing"
)

func TestOnReconnectRunsEveryHandler(t *testing.T) {
	c := &Client{}

	var mu sync.Mutex
	var fired []string
	done := make(chan struct{}, 2)
	record := func(name string) func() {
		return func() {
			mu.Lock()
			fired = append(fired, name)
			mu.Unlock()
			done <- struct{}{}
		}
	}

	c.OnReconnect(record("a"))
	c.OnReconnect(record("b"))

	c.notifyReconnect()
	<-done
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(fired) != 2 {
		t.Fatalf("both handlers must run, got %v", fired)
	}
}

func TestOnReconnectUnregisterStopsHandler(t *testing.T) {
	c := &Client{}
	kept := make(chan struct{}, 1)
	removed := make(chan struct{}, 1)

	c.OnReconnect(func() { kept <- struct{}{} })
	off := c.OnReconnect(func() { removed <- struct{}{} })
	off()

	c.notifyReconnect()
	<-kept

	select {
	case <-removed:
		t.Fatal("unregistered handler must not run")
	default:
	}
}

func TestOnReconnectIsolatesPanics(t *testing.T) {
	c := &Client{}
	ok := make(chan struct{}, 1)

	c.OnReconnect(func() { panic("boom") })
	c.OnReconnect(func() { ok <- struct{}{} })

	// a panicking handler must neither crash the process nor block the others
	c.notifyReconnect()
	<-ok
}

func TestReconnectIsOnByDefault(t *testing.T) {
	disabled := false
	enabled := true

	cases := []struct {
		name string
		opts ClientOptions
		want int
	}{
		{"unset", ClientOptions{}, -1},
		{"enabled", ClientOptions{Reconnect: &enabled}, -1},
		{"enabled with limit", ClientOptions{Reconnect: &enabled, MaxReconnectAttempts: 5}, 5},
		{"disabled", ClientOptions{Reconnect: &disabled}, 0},
	}

	for _, tc := range cases {
		if got := NewClient(tc.opts).maxReconnects(); got != tc.want {
			t.Errorf("%s: max reconnects = %d, want %d", tc.name, got, tc.want)
		}
	}
}
