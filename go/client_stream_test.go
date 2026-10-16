package rpc

import (
	"context"
	"strings"
	"testing"
	"time"
)

func streamPeers(t *testing.T) (*fakeNATS, *Client, *Proxy) {
	t.Helper()
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	_, err := server.RegisterHandler("st", map[string]any{
		"ints": func(n int) []int {
			out := make([]int, n)
			for i := range out {
				out[i] = i
			}
			return out
		},
		// an unencodable item ends the stream with an error after 64 items
		"fillThenFail": func() []any {
			out := make([]any, 65)
			for i := range 64 {
				out[i] = i
			}
			out[64] = make(chan int)
			return out
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)
	return s, client, client.CreateProxy("st")
}

func waitClosed(t *testing.T, ch <-chan StreamValue) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("the stream channel did not close")
		}
	}
}

func streamEntries(c *Client) (count int, limitsLifted bool) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	for _, e := range c.subEntries {
		if !strings.HasPrefix(e.pattern, "stream.") {
			continue
		}
		count++
		if e.sub != nil {
			msgs, bytes, _ := e.sub.PendingLimits()
			limitsLifted = msgs < 0 && bytes < 0
		}
	}
	return count, limitsLifted
}

func TestCallStreamCancelWithFullBuffer(t *testing.T) {
	_, _, proxy := streamPeers(t)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := proxy.InvokeStream(ctx, "ints", 2000)
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	// the 64-slot buffer fills and the next push parks on the subscription goroutine
	time.Sleep(200 * time.Millisecond)
	cancel()
	// not reading lets the cancel close the channel while the push is parked
	time.Sleep(100 * time.Millisecond)
	waitClosed(t, ch)
}

func TestCallStreamDisconnectWithFullBuffer(t *testing.T) {
	_, client, proxy := streamPeers(t)
	ch, err := proxy.InvokeStream(context.Background(), "ints", 2000)
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	time.Sleep(200 * time.Millisecond)
	if err := client.Disconnect(); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, ch)
}

func TestCallStreamReleasesSubscriptionAfterEnd(t *testing.T) {
	s, client, proxy := streamPeers(t)
	for range 20 {
		ch, err := proxy.InvokeStream(context.Background(), "ints", 3)
		if err != nil {
			t.Fatal(err)
		}
		got := 0
		for v := range ch {
			if v.Error != nil {
				t.Fatal(v.Error)
			}
			got++
		}
		if got != 3 {
			t.Fatalf("%d items, want 3", got)
		}
	}

	deadline := time.Now().Add(time.Second)
	for {
		left, _ := streamEntries(client)
		if left == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d finished streams still hold their subscription", left)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// the fake has seen every publish once the flush returns
	if err := client.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := s.publishes(func(subject string) bool { return strings.HasSuffix(subject, ".cancel") }); n != 0 {
		t.Errorf("%d streams that ended by themselves sent a cancel", n)
	}
}

func TestCallStreamLiftsPendingLimits(t *testing.T) {
	_, client, proxy := streamPeers(t)
	ctx := t.Context()
	ch, err := proxy.InvokeStream(ctx, "ints", 2000)
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	if count, lifted := streamEntries(client); count != 1 || !lifted {
		t.Errorf("stream subscriptions %d, pending limits lifted %v", count, lifted)
	}
}

func TestCallStreamCancelWithErrorParkedOnFullBuffer(t *testing.T) {
	_, client, proxy := streamPeers(t)
	ch, err := proxy.InvokeStream(context.Background(), "fillThenFail")
	if err != nil {
		t.Fatal(err)
	}
	items, last := 0, StreamValue{}
	for v := range ch {
		if v.Error == nil {
			items++
		}
		last = v
	}
	if items != 64 || last.Error == nil {
		t.Fatalf("%d items, last error %v, want 64 items then an error", items, last.Error)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch, err = proxy.InvokeStream(ctx, "fillThenFail")
	if err != nil {
		t.Fatal(err)
	}
	// the 64 items fill the buffer and the error parks behind them
	time.Sleep(200 * time.Millisecond)
	cancel()
	// without a reader, the cancel has to free the parked error by itself
	waitFor(t, "the stream to release its subscription", func() bool {
		left, _ := streamEntries(client)
		return left == 0
	})
	waitClosed(t, ch)
}
