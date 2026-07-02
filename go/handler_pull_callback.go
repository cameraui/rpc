package rpc

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// CallbackInvoker is provided to a pull-callback handler as its last
// argument. Handlers invoke methods through Invoke() to fire oneway
// callback messages to the client.
//
// Safe for concurrent use. Invoke() becomes a no-op after cancellation
// or when the underlying client disconnects.
type CallbackInvoker struct {
	subject string
	client  *Client
	oneway  map[string]struct{}
	active  atomic.Bool
}

// Invoke fires a oneway callback. Only methods registered as oneway by the
// client are dispatched; unknown methods are silently dropped to match the
// protocol's at-most-once semantics.
func (ci *CallbackInvoker) Invoke(method string, args ...any) {
	if !ci.active.Load() {
		return
	}
	if _, ok := ci.oneway[method]; !ok {
		return
	}
	if !ci.client.IsConnected() {
		return
	}
	msg := CallbackInvocation{Method: method, Args: args}
	_ = ci.client.Publish(ci.subject, msg)
}

// Active reports whether further Invoke() calls will be dispatched.
// Handlers can poll this inside loops to exit early on cancel.
func (ci *CallbackInvoker) Active() bool {
	return ci.active.Load()
}

// handlePullCallbackRequestGo handles a pull-iterator-with-callbacks request.
//
// Builds a CallbackInvoker whose Invoke() publishes oneway to callbackSubject,
// calls the handler with (...args, invoker), then drives the returned channel
// from iterator `next`/`cancel` requests. The iterator response carries no
// value — it is purely a batch-boundary signal.
// onFinished is invoked once when the iterator ends naturally (done/cancel) so
// the caller can drop its cleanup-map entry for this session.
func handlePullCallbackRequestGo(
	fn reflect.Value,
	args []any,
	iteratorID, callbackSubject string,
	onewayMethods []string,
	client *Client,
	onFinished func(),
) (func(), error) {
	onewaySet := make(map[string]struct{}, len(onewayMethods))
	for _, m := range onewayMethods {
		onewaySet[m] = struct{}{}
	}

	invoker := &CallbackInvoker{
		subject: callbackSubject,
		client:  client,
		oneway:  onewaySet,
	}
	invoker.active.Store(true)

	// Build handler arguments: original args + invoker as last positional.
	fnType := fn.Type()
	numIn := fnType.NumIn()
	if numIn == 0 {
		return nil, NewRPCException(ErrCodeInternalError,
			"pull-callback handler must accept at least one argument (CallbackInvoker)")
	}

	invokerParamType := fnType.In(numIn - 1)
	// Accept either *CallbackInvoker or a compatible interface.
	invokerValue := reflect.ValueOf(invoker)
	if !invokerValue.Type().AssignableTo(invokerParamType) {
		return nil, NewRPCException(ErrCodeInternalError,
			fmt.Sprintf("pull-callback handler's last parameter must be *CallbackInvoker, got %s", invokerParamType))
	}

	callArgs := make([]reflect.Value, numIn)
	for i := 0; i < numIn-1; i++ {
		paramType := fnType.In(i)
		if i < len(args) {
			callArgs[i] = coerceValue(args[i], paramType)
		} else {
			callArgs[i] = reflect.Zero(paramType)
		}
	}
	callArgs[numIn-1] = invokerValue

	// Invoke handler.
	results := fn.Call(callArgs)
	result, handlerErr := processResults(results)
	if handlerErr != nil {
		invoker.active.Store(false)
		return nil, handlerErr
	}

	if result == nil {
		invoker.active.Store(false)
		return nil, NewRPCException(ErrCodeInternalError, "pull-callback handler must return a channel")
	}

	rv := reflect.ValueOf(result)
	if rv.Kind() != reflect.Chan {
		invoker.active.Store(false)
		return nil, NewRPCException(ErrCodeInternalError,
			fmt.Sprintf("pull-callback handler must return a channel, got %s", rv.Kind()))
	}

	requestSubject := fmt.Sprintf("_rpc.iterator.%s.request", iteratorID)
	responseSubject := fmt.Sprintf("_rpc.iterator.%s.response", iteratorID)

	active := true

	// subUnsub is assigned after Subscribe returns but read from the NATS
	// callback goroutine — guard it against that race.
	var subUnsubMu sync.Mutex
	var subUnsub func()

	// Natural end (done/cancel): see handlePullIteratorRequestGo — drop the
	// request subscription and the caller's cleanup-map entry per finished
	// session.
	var finishOnce sync.Once
	finish := func() {
		finishOnce.Do(func() {
			active = false
			invoker.active.Store(false)
			subUnsubMu.Lock()
			u := subUnsub
			subUnsubMu.Unlock()
			if u != nil {
				u()
			}
			if onFinished != nil {
				onFinished()
			}
		})
	}

	unsub, err := client.Subscribe(requestSubject, func(data []byte) {
		if !active || !client.IsConnected() {
			return
		}

		var req PullIteratorRequest
		if err := Decode(data, &req); err != nil {
			return
		}

		switch req.Type {
		case "cancel":
			finish()
			// Drain the handler channel via reflection so its producer goroutine
			// unblocks on send and can exit. Works for any chan element type.
			go func() {
				for {
					if _, ok := rv.Recv(); !ok {
						return
					}
				}
			}()
			resp := PullIteratorResponse{ID: iteratorID, Type: "done"}
			_ = client.Publish(responseSubject, resp)

		case "next":
			// Receive the next batch-boundary signal from the handler channel.
			_, ok := rv.Recv()
			if !ok {
				finish()
				resp := PullIteratorResponse{ID: iteratorID, Type: "done"}
				_ = client.Publish(responseSubject, resp)
			} else {
				// Value is ignored by the protocol — batch boundary only.
				resp := PullIteratorResponse{ID: iteratorID, Type: "value"}
				_ = client.Publish(responseSubject, resp)
			}
		}
	})
	if err != nil {
		invoker.active.Store(false)
		return nil, err
	}
	subUnsubMu.Lock()
	subUnsub = unsub
	subUnsubMu.Unlock()

	cleanup := func() {
		finishOnce.Do(func() {
			active = false
			invoker.active.Store(false)
			unsub()
			// No onFinished here — the caller is already sweeping its map.
		})
	}

	return cleanup, nil
}
