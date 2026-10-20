package rpc

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// PullCallbackMap maps the method names the server calls to the functions that handle them.
type PullCallbackMap map[string]any

// CallPullIteratorWithCallback makes a pull-iterator call whose data arrives through oneway callbacks, its channel yields one empty PullValue per batch.
func (c *Client) CallPullIteratorWithCallback(
	ctx context.Context,
	subject string,
	callbacks PullCallbackMap,
	oneway []string,
	args ...any,
) (<-chan PullValue, error) {
	return c.callPullIteratorWithCallback(ctx, subject, nil, callbacks, oneway, args...)
}

func (c *Client) callPullIteratorWithCallback(
	ctx context.Context,
	subject string,
	opts *RequestOptions,
	callbacks PullCallbackMap,
	oneway []string,
	args ...any,
) (<-chan PullValue, error) {
	if c.needsConnect() {
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

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
	statusInbox := "rpc.reply." + iteratorID

	methodNames := make([]string, 0, len(callbacks))
	for k := range callbacks {
		methodNames = append(methodNames, k)
	}

	// a sync subscription drained before each yield, so a slow callback stalls the next request and with it the server
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
	// a whole batch queues up before the drain, the nats.go limits would drop a large one as slow consumer
	_ = cbSub.SetPendingLimits(-1, -1)
	cbUnsub := func() {
		_ = cbSub.Unsubscribe()
	}

	initParams := PullCallbackParams{
		PullCallback:    true,
		IteratorID:      iteratorID,
		CallbackSubject: callbackSubject,
		CallbackMethods: methodNames,
		OnewayMethods:   oneway,
		Args:            args,
	}
	if err := c.initPullSession(ctx, subject, "pull-callback iterator", iteratorID, opts, initParams); err != nil {
		cbUnsub()
		return nil, err
	}

	// unbuffered, so the next request waits until the caller has processed this batch
	ch := make(chan PullValue)

	// settles a consumer parked in next() when the connection is torn down
	disconnected := make(chan struct{})
	var settleOnce sync.Once
	c.pullIteratorSettles.Store(iteratorID, func() {
		settleOnce.Do(func() { close(disconnected) })
	})

	go func() {
		defer close(ch)
		defer c.pullIteratorSettles.Delete(iteratorID)
		defer cbUnsub()

		respCh := make(chan PullIteratorResponse, 8)
		var ended atomic.Bool

		respUnsub, err := c.Subscribe(responseSubject, func(data []byte) {
			var resp PullIteratorResponse
			if err := DecodeMessageInto(data, &resp); err != nil {
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

		noResponders := &RPCError{Code: "503", Message: "No responders for " + subject}
		c.statusHandlers.Store(iteratorID, func(error) {
			if !ended.Load() {
				respCh <- PullIteratorResponse{ID: iteratorID, Type: "error", Error: noResponders}
			}
		})
		defer c.statusHandlers.Delete(iteratorID)

		sendCancel := func() {
			cancelReq := PullIteratorRequest{ID: iteratorID, Type: "cancel"}
			_ = c.Publish(requestSubject, cancelReq)
		}

		drainCallbacks := func() {
			for {
				msg, err := cbSub.NextMsg(0)
				if err != nil {
					// ErrTimeout means empty for now, any other error means the subscription is gone
					return
				}
				var inv CallbackInvocation
				if err := DecodeMessageInto(msg.Data, &inv); err != nil {
					continue
				}
				fn, ok := callbacks[inv.Method]
				if !ok {
					continue
				}
				dispatchCallback(fn, inv.Args)
			}
		}

		// with nobody waiting, the closed channel ends the consumer's range loop instead
		sendDisconnected := func() {
			ended.Store(true)
			select {
			case ch <- PullValue{Error: NewRPCException(ErrCodeConnectionClosed, "Connection closed")}:
			default:
			}
		}

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
			// reply points at the status inbox so a vanished responder shows as a 503 instead of a hang
			nextReq := PullIteratorRequest{ID: iteratorID, Type: "next"}
			if err := c.publishInternal(requestSubject, nextReq, statusInbox); err != nil {
				sendToConsumer(PullValue{Error: err})
				return
			}

			select {
			case resp := <-respCh:
				switch resp.Type {
				case "value":
					// the batch's callbacks arrive before its value response, so they are queued in cbSub by now
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
						pv = PullValue{Error: pullError(resp.Error, noResponders, subject)}
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

var callArgsPool = sync.Pool{
	New: func() any {
		s := make([]reflect.Value, 0, 8)
		return &s
	},
}

func dispatchCallback(fn any, args []any) {
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		return
	}
	t := v.Type()
	numIn := t.NumIn()

	argsPtr := callArgsPool.Get().(*[]reflect.Value)
	callArgs := (*argsPtr)[:0]

	// callback errors never reach the server; clear the args so the pool does not keep frames alive
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
