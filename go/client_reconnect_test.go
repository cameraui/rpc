package rpc

import (
	"sync"
	"testing"
	"time"
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

func TestReconnectDelay(t *testing.T) {
	c := NewClient(ClientOptions{ReconnectWait: 2 * time.Second})
	between := func(what string, d, lo, hi time.Duration) {
		t.Helper()
		if d < lo || d >= hi {
			t.Errorf("%s: delay %v, want [%v, %v)", what, d, lo, hi)
		}
	}

	between("never connected", c.reconnectDelay(1), 2*time.Second, 2100*time.Millisecond)

	lived := time.Now().Add(-3 * time.Second)
	c.connectedAt.Store(&lived)
	between("first attempt after a long connection", c.reconnectDelay(1), 0, 10*time.Millisecond)
	between("second attempt", c.reconnectDelay(2), 2*time.Second, 2100*time.Millisecond)

	short := time.Now().Add(-time.Second)
	c.connectedAt.Store(&short)
	between("first attempt after a short connection", c.reconnectDelay(1), 2*time.Second, 2100*time.Millisecond)

	tls := NewClient(ClientOptions{ReconnectWait: 2 * time.Second, TLS: &TLSOptions{}})
	between("tls", tls.reconnectDelay(2), 2*time.Second, 3*time.Second)
}

func TestReconnectRunsAtOnceAfterALongConnection(t *testing.T) {
	s := startFakeNATS(t)
	wait := 400 * time.Millisecond
	for _, tc := range []struct {
		name    string
		lived   time.Duration
		atLeast time.Duration
		atMost  time.Duration
	}{
		{"long", wait + 50*time.Millisecond, 0, 200 * time.Millisecond},
		{"short", 0, wait, 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := connectFake(t, s, &ClientOptions{ReconnectWait: wait})
			reconnected := make(chan time.Time, 1)
			c.OnReconnect(func() { reconnected <- time.Now() })
			time.Sleep(tc.lived)

			kicked := time.Now()
			s.kick()
			select {
			case at := <-reconnected:
				if took := at.Sub(kicked); took < tc.atLeast || took > tc.atMost {
					t.Errorf("reconnected after %v, want [%v, %v]", took, tc.atLeast, tc.atMost)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("no reconnect")
			}
		})
	}
}
