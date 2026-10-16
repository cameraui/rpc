package rpc

import (
	"context"
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
	ctx     context.Context
	probe   *orphanProbe
	fail    func(err error)
	// held by every Invoke across its publish, so stop waits for the ones in
	// flight and none follows the session's terminal message
	mu sync.RWMutex
}

// Invoke fires a oneway callback. Only methods registered as oneway by the
// client are dispatched; unknown methods are silently dropped to match the
// protocol's at-most-once semantics. A frame that cannot go out while
// connected, one that does not encode or is larger than a payload limit too
// small for chunking, ends the session: later calls send nothing and the
// batch it belongs to fails with that error, INTERNAL_ERROR or
// PAYLOAD_TOO_LARGE.
func (ci *CallbackInvoker) Invoke(method string, args ...any) {
	if !ci.active.Load() {
		return
	}
	if _, ok := ci.oneway[method]; !ok {
		return
	}
	// an error while disconnected means the connection is gone, the teardown answers
	if err := ci.publish(method, args); err != nil && ci.client.IsConnected() {
		ci.fail(err)
	}
}

func (ci *CallbackInvoker) publish(method string, args []any) error {
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	if !ci.active.Load() || !ci.client.IsConnected() {
		return nil
	}
	return ci.client.publishInternal(ci.subject, CallbackInvocation{Method: method, Args: args}, ci.probe.replyFor())
}

// Supports reports whether the client registered method as a oneway
// callback, so Invoke sends it. A producer checks it before it switches to a
// form the client may not handle, for example batched frames.
//
// Example:
//
//	if inv.Supports("onBatch") {
//		inv.Invoke("onBatch", frames)
//	} else {
//		for _, f := range frames {
//			inv.Invoke("onFrame", f)
//		}
//	}
func (ci *CallbackInvoker) Supports(method string) bool {
	_, ok := ci.oneway[method]
	return ok
}

// Active reports whether further Invoke() calls will be dispatched.
// Handlers can poll this inside loops to exit early on cancel.
func (ci *CallbackInvoker) Active() bool {
	return ci.active.Load()
}

// Context is cancelled when the session ends: the caller cancels it or is gone
// for good, the producer closes its channel, the handler is unregistered, the
// service stops, or the Client serving it disconnects, suspends or loses its
// connection for good. A producer waiting for data selects on its Done channel
// to stop at once instead of at its next Active check. It is the same context
// a handler gets in a context.Context first parameter.
func (ci *CallbackInvoker) Context() context.Context {
	if ci.ctx == nil {
		return context.Background()
	}
	return ci.ctx
}

func (ci *CallbackInvoker) stop() {
	ci.mu.Lock()
	ci.active.Store(false)
	ci.mu.Unlock()
}

// handlePullCallbackRequestGo handles a pull-iterator-with-callbacks request.
//
// Builds a CallbackInvoker whose Invoke() publishes oneway to callbackSubject,
// calls the handler with ([ctx], ...args, invoker), then drives the returned channel
// from iterator `next`/`cancel` requests. The iterator response carries no
// value — it is purely a batch-boundary signal.
// onFinished is invoked once when the iterator ends naturally (done/cancel) so
// the caller can drop its cleanup-map entry for this session. A nil teardown
// with a nil error: the caller cancelled during the setup and the session is over.
func handlePullCallbackRequestGo(
	parent context.Context,
	fn reflect.Value,
	args []any,
	iteratorID, callbackSubject string,
	onewayMethods []string,
	client *Client,
	orphan *orphanConfig,
	onFinished func(),
) (func(), error) {
	if !isControlSubject(callbackSubject, pullCallbackSubjectPrefix) {
		return nil, NewRPCException(ErrCodeInvalidParams, "callback subject must start with "+pullCallbackSubjectPrefix)
	}

	onewaySet := make(map[string]struct{}, len(onewayMethods))
	for _, m := range onewayMethods {
		onewaySet[m] = struct{}{}
	}

	ctx, cancel := context.WithCancel(parent)
	var s *pullSession
	invoker := &CallbackInvoker{
		subject: callbackSubject,
		client:  client,
		oneway:  onewaySet,
		ctx:     ctx,
		probe:   client.newOrphanProbe(orphan, func(reply string) error { return s.ping(reply) }),
		fail:    func(err error) { s.fail(err) },
	}
	invoker.active.Store(true)
	s = newPullSession(iteratorID, client, cancel, invoker.probe, onFinished)
	s.invoker = invoker
	started := false
	defer func() {
		if !started {
			s.abandon()
			invoker.active.Store(false)
			cancel()
		}
	}()

	// Build handler arguments: [ctx] + original args + invoker as last positional.
	fnType := fn.Type()
	numIn := fnType.NumIn()
	first := contextParams(fnType)
	if numIn <= first {
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
	if first == 1 {
		callArgs[0] = reflect.ValueOf(ctx)
	}
	for i := first; i < numIn-1; i++ {
		paramType := fnType.In(i)
		if a := i - first; a < len(args) {
			callArgs[i] = coerceValue(args[a], paramType)
		} else {
			callArgs[i] = reflect.Zero(paramType)
		}
	}
	callArgs[numIn-1] = invokerValue

	if err := s.listen(); err != nil {
		return nil, err
	}
	results := fn.Call(callArgs)
	result, handlerErr := processResults(results)
	if handlerErr != nil {
		return nil, handlerErr
	}

	if result == nil {
		return nil, NewRPCException(ErrCodeInternalError, "pull-callback handler must return a channel")
	}

	rv := reflect.ValueOf(result)
	if rv.Kind() != reflect.Chan {
		return nil, NewRPCException(ErrCodeInternalError,
			fmt.Sprintf("pull-callback handler must return a channel, got %s", rv.Kind()))
	}

	started = true
	if !s.serve(rv) {
		return nil, nil
	}
	return s.teardown, nil
}
