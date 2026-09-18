package rpc

import (
	"context"
	"testing"
	"time"
)

func TestCallDeadlineUsesClientDefault(t *testing.T) {
	c := NewClient(ClientOptions{Timeout: 30 * time.Second})
	if got := c.callDeadline(context.Background(), 0); got != 30*time.Second {
		t.Errorf("callDeadline = %v, want 30s", got)
	}
}

func TestCallDeadlineFollowsContext(t *testing.T) {
	c := NewClient(ClientOptions{Timeout: 30 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	got := c.callDeadline(ctx, 0)
	if got <= 30*time.Second || got > 2*time.Minute {
		t.Errorf("callDeadline = %v, want the remaining two minutes", got)
	}
}

func TestCallDeadlinePrefersExplicitTimeout(t *testing.T) {
	c := NewClient(ClientOptions{Timeout: 30 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if got := c.callDeadline(ctx, 5*time.Second); got != 5*time.Second {
		t.Errorf("callDeadline = %v, want 5s", got)
	}
}
