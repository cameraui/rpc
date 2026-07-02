package rpc

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// PullCallbackMap registers callback handlers invoked by the server during
// a pull-iterator-with-callbacks session.
//
// Keys are method names the server will call; values must be functions.
// Each function is invoked via reflection when a matching CallbackInvocation
// arrives on the callback subject.
type PullCallbackMap map[string]any

// CallPullIteratorWithCallback makes a pull-iterator-with-callbacks RPC call.
//
// Combines client-driven pull iteration (1 RTT per batch) with a oneway
// callback channel (fire-and-forget server→client) for low-latency data
// delivery with coarse-grained backpressure.
//
// The returned channel yields one PullValue per batch boundary the server
// produces. The Value field is always nil — meaningful data is dispatched
// through the callbacks map. Cancel via the context to stop early.
func (c *Client) CallPullIteratorWithCallback(
	ctx context.Context,
	subject string,
	callbacks PullCallbackMap,
	oneway []string,
	args ...any,
) (<-chan PullValue, error) {
	if !c.IsConnected() && !c.IsClosed() {
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

	// Validate callbacks.
	for name, fn := range callbacks {
		if reflect.ValueOf(fn).Kind() != reflect.Func {
			return nil, fmt.Errorf("callback %q is not a function", name)
		}
	}

	c.ensureMuxSubscription(true)

	iteratorID := c.generateID()
	requestSubject := fmt.Sprintf("_rpc.iterator.%s.request", iteratorID)
	responseSubject := fmt.Sprintf("_rpc.iterator.%s.response", iteratorID)
	callbackSubject := fmt.Sprintf("_rpc.cb.%s", iteratorID)
	// 503 status inbox for `next` requests — see CallPullIterator.
	statusInbox := "rpc.reply." + iteratorID

	methodNames := make([]string, 0, len(callbacks))
	for k := range callbacks {
		methodNames = append(methodNames, k)
	}

	// Sync subscription for callbacks: the iterator goroutine drains
	// pending callback messages serially before each yield. This is what
	// gives true end-to-end backpressure — a slow callback handler blocks
	// the drain loop, which stalls the next `next()` request, which
	// suspends the server at its own `yield`.
	//
	// Async Subscribe() would dispatch callbacks on a separate goroutine
	// in parallel with the iterator loop — callback delays would not
	// propagate to the server.
	c.mu.RLock()
	nc := c.nc
	c.mu.RUnlock()
	if nc == nil {
		return nil, fmt.Errorf("not connected")
	}

	cbSub, err := nc.SubscribeSync(callbackSubject)
	if err != nil {
		return nil, err
	}
	// The drain loop only runs at batch boundaries, so a full batch of
	// callback messages queues up in the subscription first. With the
	// nats.go defaults (64MB) large batches (e.g. 1000 x 100KB frames)
	// would be silently dropped as "slow consumer". Lift the limits —
	// memory stays bounded by the pull iterator's one-batch-in-flight
	// granularity. Node (unbounded) and Python (128MB + continuous
	// dispatch) do not drop here either.
	_ = cbSub.SetPendingLimits(-1, -1)
	cbUnsub := func() {
		_ = cbSub.Unsubscribe()
	}

	// Init request via regular Call.
	initParams := PullCallbackParams{
		PullCallback:    true,
		IteratorID:      iteratorID,
		CallbackSubject: callbackSubject,
		CallbackMethods: methodNames,
		OnewayMethods:   oneway,
		Args:            args,
	}
	initResult, err := c.Call(ctx, subject, initParams)
	if err != nil {
		cbUnsub()
		return nil, fmt.Errorf("init pull-callback iterator: %w", err)
	}

	if m, ok := initResult.(map[string]any); ok {
		if retID, ok := m["iteratorId"].(string); !ok || retID != iteratorID {
			cbUnsub()
			return nil, fmt.Errorf("failed to initialize pull-callback iterator")
		}
	} else {
		cbUnsub()
		return nil, fmt.Errorf("failed to initialize pull-callback iterator: unexpected response type")
	}

	// Unbuffered: the send blocks until the consumer reads, which prevents
	// the client loop from sending the next `next` request until the caller
	// has actually processed the current batch boundary. Buffering here
	// would push batches one ahead of the consumer and break backpressure.
	ch := make(chan PullValue)

	// See CallPullIterator: force-settle a consumer parked in a next() wait
	// when the connection tears down (Disconnect/Suspend).
	disconnected := make(chan struct{})
	var settleOnce sync.Once
	c.pullIteratorSettles.Store(iteratorID, func() {
		settleOnce.Do(func() { close(disconnected) })
	})

	go func() {
		defer close(ch)
		defer c.pullIteratorSettles.Delete(iteratorID)
		defer cbUnsub()

		// ended is written by this goroutine and read by the NATS
		// subscription goroutine — atomic.Bool avoids the race.
		respCh := make(chan PullIteratorResponse, 8)
		var ended atomic.Bool

		respUnsub, err := c.Subscribe(responseSubject, func(data []byte) {
			var resp PullIteratorResponse
			if err := Decode(data, &resp); err != nil {
				return
			}
			if !ended.Load() {
				respCh <- resp
			}
		})
		if err != nil {
			ch <- PullValue{Error: err}
			return
		}
		defer respUnsub()

		// No-responder detection for `next` requests via the muxed reply
		// inbox (one-shot) — see CallPullIterator.
		c.statusHandlers.Store(iteratorID, func(error) {
			if !ended.Load() {
				respCh <- PullIteratorResponse{
					ID:    iteratorID,
					Type:  "error",
					Error: &RPCError{Code: "503", Message: "No responders for " + subject},
				}
			}
		})
		defer c.statusHandlers.Delete(iteratorID)

		sendCancel := func() {
			cancelReq := PullIteratorRequest{ID: iteratorID, Type: "cancel"}
			_ = c.Publish(requestSubject, cancelReq)
		}

		// drainCallbacks consumes every pending message from the sync
		// callback subscription and dispatches the handler synchronously.
		// Called before each yield — any backpressure in the handler
		// (e.g. blocking channel send) propagates to the server via the
		// stalled `next()` request.
		drainCallbacks := func() {
			for {
				msg, err := cbSub.NextMsg(0)
				if err != nil {
					// nats.ErrTimeout means the queue is empty right now.
					// Any other error means the subscription is gone;
					// either way, stop draining.
					return
				}
				var inv CallbackInvocation
				if err := Decode(msg.Data, &inv); err != nil {
					continue
				}
				fn, ok := callbacks[inv.Method]
				if !ok {
					continue
				}
				dispatchCallback(fn, inv.Args)
			}
		}

		// sendDisconnected delivers the connection error to a parked
		// consumer without blocking; if nobody is waiting, the closed
		// channel terminates the consumer's range loop instead.
		sendDisconnected := func() {
			ended.Store(true)
			select {
			case ch <- PullValue{Error: NewRPCException(ErrCodeConnectionClosed, "Connection closed")}:
			default:
			}
		}

		// sendToConsumer wraps a channel send in a ctx-aware select so that
		// a consumer that has stopped reading doesn't permanently park the
		// goroutine. Without this wrap, ctx cancellation would not preempt
		// a blocked `ch <- v` — the goroutine would leak until (hopefully)
		// the NATS connection drops. Returns true if the send succeeded.
		sendToConsumer := func(v PullValue) bool {
			select {
			case ch <- v:
				return true
			case <-ctx.Done():
				sendCancel()
				ended.Store(true)
				return false
			case <-disconnected:
				ended.Store(true)
				return false
			}
		}

		for {
			// `reply` points at the status inbox so a vanished responder
			// surfaces as a 503 on the mux instead of a silent hang.
			nextReq := PullIteratorRequest{ID: iteratorID, Type: "next"}
			if err := c.publishInternal(requestSubject, nextReq, statusInbox); err != nil {
				sendToConsumer(PullValue{Error: err})
				return
			}

			select {
			case resp := <-respCh:
				switch resp.Type {
				case "value":
					// Drain all callbacks for this batch before handing
					// control to the caller. Callback messages for the
					// batch arrive on TCP before the value response, so by
					// the time we see "value" they are already queued in
					// cbSub.
					drainCallbacks()
					if !sendToConsumer(PullValue{}) {
						return
					}
				case "done":
					drainCallbacks()
					return
				case "error":
					var pv PullValue
					if resp.Error != nil {
						pv = PullValue{Error: RPCExceptionFromError(resp.Error)}
					} else {
						pv = PullValue{Error: NewRPCException(ErrCodeStreamError, "Iterator error")}
					}
					sendToConsumer(pv)
					return
				}
			case <-ctx.Done():
				sendCancel()
				ended.Store(true)
				return
			case <-disconnected:
				sendDisconnected()
				return
			}
		}
	}()

	return ch, nil
}

// callArgsPool recycles the []reflect.Value argument slice used by
// dispatchCallback — callbacks fire once per frame in the pull-callback hot
// path, so the per-invocation slice allocation adds up.
var callArgsPool = sync.Pool{
	New: func() any {
		s := make([]reflect.Value, 0, 8)
		return &s
	},
}

// dispatchCallback invokes a registered callback via reflection, coercing
// msgpack-decoded args to the function's declared parameter types.
func dispatchCallback(fn any, args []any) {
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		return
	}
	t := v.Type()
	numIn := t.NumIn()

	argsPtr := callArgsPool.Get().(*[]reflect.Value)
	callArgs := (*argsPtr)[:0]

	// Recover — callback errors must not escape. Log-and-continue semantics
	// per protocol spec (callback dispatch errors never propagate server-ward).
	// The same defer returns the pooled slice; reflect.Value.Call copies the
	// arguments into the new frame and does not retain the slice. Clear the
	// values first so the pool doesn't keep the args (e.g. frames) alive.
	defer func() {
		_ = recover()
		clear(callArgs)
		*argsPtr = callArgs[:0]
		callArgsPool.Put(argsPtr)
	}()

	for i := range numIn {
		paramType := t.In(i)
		if i < len(args) {
			callArgs = append(callArgs, coerceValue(args[i], paramType))
		} else {
			callArgs = append(callArgs, reflect.Zero(paramType))
		}
	}

	_ = v.Call(callArgs)
}
