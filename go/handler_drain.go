package rpc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultDrainTimeout = 250 * time.Millisecond
	// requests queued behind a call the deadline gave up on are answered within this
	drainGrace = 50 * time.Millisecond
	// a handoff costs CPU on every call, so only calls longer than this take one
	handoffAfter = drainGrace / 2
)

const (
	replyOpen int32 = iota
	replySending
	replyDone
)

type handlerCall struct {
	id     string
	state  *atomic.Int32
	cancel context.CancelFunc
}

type drainSignal struct {
	expired chan struct{}
}

type serialMethod struct {
	fast    atomic.Bool
	current atomic.Pointer[handlerCall]
}

type handlerRegistration struct {
	client    *Client
	drains    bool
	entries   []*subscriptionEntry
	discovery *subscriptionEntry
	methods   []*serialMethod
	lanes     *laneSet

	callCtx     context.Context
	cancelCalls context.CancelFunc
	signal      atomic.Pointer[drainSignal]

	mu       sync.Mutex
	inflight sync.WaitGroup
	stopped  bool
	calls    map[*handlerCall]struct{}
	draining chan struct{}
}

func newHandlerRegistration(client *Client) *handlerRegistration {
	ctx, cancel := context.WithCancel(context.Background())
	r := &handlerRegistration{
		client:      client,
		drains:      client.drainTimeout() > 0,
		callCtx:     ctx,
		cancelCalls: cancel,
		calls:       map[*handlerCall]struct{}{},
	}
	r.signal.Store(&drainSignal{expired: make(chan struct{})})
	return r
}

func (r *handlerRegistration) addMethod() *serialMethod {
	m := &serialMethod{}
	r.methods = append(r.methods, m)
	return m
}

func (r *handlerRegistration) expiry() <-chan struct{} {
	if !r.drains {
		// a nil channel never fires in a select
		return nil
	}
	return r.signal.Load().expired
}

func (r *handlerRegistration) closeCall(state *atomic.Int32, id string) {
	if id == "" || !state.CompareAndSwap(replyOpen, replyDone) {
		return
	}
	_ = r.client.Publish("rpc.reply."+id, RPCResponse{ID: id, Error: connectionClosedError()})
}

func (r *handlerRegistration) closeStream(id string, params any) {
	subject, _ := extractStreamParams(params)
	if !isControlSubject(subject, streamSubjectPrefix) {
		return
	}
	_ = r.client.Publish(subject, StreamMessage{ID: id, Type: "error", Error: connectionClosedError()})
}

func (r *handlerRegistration) track(call *handlerCall) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return false
	}
	r.inflight.Add(1)
	if r.drains {
		r.calls[call] = struct{}{}
	}
	return true
}

func (r *handlerRegistration) untrack(call *handlerCall) {
	r.mu.Lock()
	delete(r.calls, call)
	r.mu.Unlock()
	r.inflight.Done()
}

func (r *handlerRegistration) stop() {
	r.mu.Lock()
	// a late delivery is no longer counted, so the wait ends
	r.stopped = true
	r.mu.Unlock()
	r.inflight.Wait()
}

func (r *handlerRegistration) beginDrain() (done <-chan struct{}, started bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.draining != nil {
		return r.draining, false
	}
	r.draining = make(chan struct{})
	return nil, true
}

func (r *handlerRegistration) endDrain() {
	r.mu.Lock()
	close(r.draining)
	r.draining = nil
	r.mu.Unlock()
}

func (r *handlerRegistration) drainSubscriptions() []*nats.Subscription {
	r.client.subMu.Lock()
	defer r.client.subMu.Unlock()
	if r.discovery != nil && r.discovery.sub != nil {
		_ = r.discovery.sub.Unsubscribe()
	}
	subs := make([]*nats.Subscription, 0, len(r.entries))
	for _, entry := range r.entries {
		if entry.sub != nil && entry.sub.Drain() == nil {
			subs = append(subs, entry.sub)
		}
	}
	return subs
}

func (r *handlerRegistration) idle() bool {
	r.mu.Lock()
	calls := len(r.calls)
	r.mu.Unlock()
	return calls == 0 && r.lanes.idle()
}

func (r *handlerRegistration) expire() {
	r.mu.Lock()
	sig := r.signal.Load()
	select {
	case <-sig.expired:
	default:
		close(sig.expired)
	}
	calls := make([]*handlerCall, 0, len(r.calls)+len(r.methods))
	for call := range r.calls {
		calls = append(calls, call)
	}
	r.mu.Unlock()
	for _, m := range r.methods {
		if call := m.current.Load(); call != nil {
			calls = append(calls, call)
		}
	}

	// closeCall answers once, whichever of this sweep and a slow serial call's delivery goroutine comes first
	for _, call := range calls {
		r.closeCall(call.state, call.id)
		call.cancel()
	}
}

func (r *handlerRegistration) resume() {
	r.lanes.clear()
	r.signal.Store(&drainSignal{expired: make(chan struct{})})
}

func (c *Client) drainTimeout() time.Duration {
	if c.Options.DrainTimeout == 0 {
		return defaultDrainTimeout
	}
	return c.Options.DrainTimeout
}

func (c *Client) addHandlerRegistration(r *handlerRegistration) {
	c.subMu.Lock()
	if c.handlerRegs == nil {
		c.handlerRegs = make(map[*handlerRegistration]struct{})
	}
	c.handlerRegs[r] = struct{}{}
	c.subMu.Unlock()
}

func (c *Client) removeHandlerRegistration(r *handlerRegistration) {
	c.subMu.Lock()
	delete(c.handlerRegs, r)
	c.subMu.Unlock()
}

func (c *Client) handlerRegistrations() []*handlerRegistration {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	regs := make([]*handlerRegistration, 0, len(c.handlerRegs))
	for r := range c.handlerRegs {
		regs = append(regs, r)
	}
	return regs
}

func (c *Client) drainHandlers(regs []*handlerRegistration) {
	timeout := c.drainTimeout()
	c.mu.RLock()
	nc := c.nc
	c.mu.RUnlock()
	if timeout <= 0 || nc == nil || !nc.IsConnected() || len(regs) == 0 {
		return
	}
	deadline := time.Now().Add(timeout)

	var (
		mine   []*handlerRegistration
		others []<-chan struct{}
		subs   []*nats.Subscription
	)
	for _, r := range regs {
		if !r.drains {
			continue
		}
		if done, started := r.beginDrain(); !started {
			others = append(others, done)
			continue
		}
		mine = append(mine, r)
		subs = append(subs, r.drainSubscriptions()...)
	}

	if len(mine) > 0 {
		// after the PONG nothing more arrives for the drained subscriptions
		if left := time.Until(deadline); left > 0 {
			_ = nc.FlushTimeout(left)
		}
		idle := waitUntil(deadline, func() bool {
			if !subscriptionsIdle(subs) {
				return false
			}
			for _, r := range mine {
				if !r.idle() {
					return false
				}
			}
			return true
		})
		for _, r := range mine {
			r.expire()
		}
		if !idle {
			waitUntil(time.Now().Add(drainGrace), func() bool {
				if !subscriptionsIdle(subs) {
					return false
				}
				for _, r := range mine {
					if !r.lanes.idle() {
						return false
					}
				}
				return true
			})
		}
		for _, r := range mine {
			r.endDrain()
		}
	}
	for _, done := range others {
		<-done
	}
}

func subscriptionsIdle(subs []*nats.Subscription) bool {
	for _, sub := range subs {
		// a removed subscription errors here and counts as idle
		if n, _, err := sub.Pending(); err == nil && n > 0 {
			return false
		}
	}
	return true
}

func waitUntil(deadline time.Time, done func() bool) bool {
	pause := 250 * time.Microsecond
	for !done() {
		left := time.Until(deadline)
		if left <= 0 {
			return false
		}
		time.Sleep(min(pause, left))
		pause = min(2*pause, 5*time.Millisecond)
	}
	return true
}
