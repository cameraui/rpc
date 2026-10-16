package rpc

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// handleCallbackRequestGo handles a callback subscription request.
// It creates a wrapper function matching the handler's func(T) parameter,
// calls the handler with it, and manages the subscription lifecycle.
// onFinished is invoked when the client cancels the subscription so the
// caller can drop its cleanup-map entry for this session.
func handleCallbackRequestGo(parent context.Context, fn reflect.Value, args []any, callbackSubject, requestID string, client *Client, orphan *orphanConfig, onFinished func()) (func(), error) {
	if !isControlSubject(callbackSubject, callbackSubjectPrefix) {
		return nil, NewRPCException(ErrCodeInvalidParams, "callback subject must start with "+callbackSubjectPrefix)
	}

	fnType := fn.Type()
	first := contextParams(fnType)

	// Find the func(T) parameter position
	callbackParamIdx := -1
	for i := first; i < fnType.NumIn(); i++ {
		if fnType.In(i).Kind() == reflect.Func {
			callbackParamIdx = i
			break
		}
	}
	if callbackParamIdx < 0 {
		return nil, NewRPCException(ErrCodeInternalError, "handler has no func parameter for callback")
	}

	// Get the callback parameter type: func(T) or func(T) error
	callbackType := fnType.In(callbackParamIdx)
	hasErrorReturn := callbackType.NumOut() == 1 && callbackType.Out(0).Implements(reflect.TypeFor[error]())
	if callbackType.NumIn() != 1 || (callbackType.NumOut() != 0 && !hasErrorReturn) {
		return nil, NewRPCException(ErrCodeInternalError,
			fmt.Sprintf("callback parameter must be func(T) or func(T) error, got %s", callbackType))
	}

	// every consumer skips a callback message of an unknown type
	probe := client.newOrphanProbe(orphan, func(reply string) error {
		return client.publishInternal(callbackSubject, CallbackMessage{ID: requestID, Type: "ping"}, reply)
	})

	// Create wrapper function that publishes to callbackSubject
	wrapperFunc := reflect.MakeFunc(callbackType, func(wrapperArgs []reflect.Value) []reflect.Value {
		if !client.IsConnected() {
			if hasErrorReturn {
				return []reflect.Value{reflect.Zero(callbackType.Out(0))}
			}
			return nil
		}
		msg := CallbackMessage{
			ID:   requestID,
			Type: "data",
			Data: wrapperArgs[0].Interface(),
		}
		err := client.publishInternal(callbackSubject, msg, probe.replyFor())
		if hasErrorReturn {
			if err != nil {
				return []reflect.Value{reflect.ValueOf(err)}
			}
			return []reflect.Value{reflect.Zero(callbackType.Out(0))}
		}
		return nil
	})

	ctx, cancel := context.WithCancel(parent)
	// a panicking handler ends the context too
	started := false
	defer func() {
		if !started {
			cancel()
		}
	}()

	// Build call arguments: [ctx], then the wrapper at the callback position
	numIn := fnType.NumIn()
	callArgs := make([]reflect.Value, numIn)
	argIdx := 0
	for i := range numIn {
		switch {
		case i < first:
			callArgs[i] = reflect.ValueOf(ctx)
		case i == callbackParamIdx:
			callArgs[i] = wrapperFunc
		default:
			if argIdx < len(args) {
				callArgs[i] = coerceValue(args[argIdx], fnType.In(i))
			} else {
				callArgs[i] = reflect.Zero(fnType.In(i))
			}
			argIdx++
		}
	}

	// Call handler
	results := fn.Call(callArgs)
	handlerResult, handlerErr := processResults(results)
	if handlerErr != nil {
		return nil, handlerErr
	}

	// Extract cleanup function from result
	var handlerCleanup func()
	if handlerResult != nil {
		if fn, ok := handlerResult.(func()); ok {
			handlerCleanup = fn
		}
	}

	// Handler cleanup must run exactly once — the cancel message and a later
	// RegisterHandler/Disconnect cleanup would otherwise both invoke it.
	var cleanupOnce sync.Once
	runHandlerCleanup := func() {
		cleanupOnce.Do(func() {
			probe.stop()
			cancel()
			if handlerCleanup != nil {
				handlerCleanup()
			}
		})
	}
	started = true

	// Subscribe to cancel subject for cleanup. cancelUnsub is assigned after
	// Subscribe returns but read from the NATS callback goroutine — guard it.
	var cancelUnsubMu sync.Mutex
	var cancelUnsub func()
	finish := func() {
		cancelUnsubMu.Lock()
		u := cancelUnsub
		cancelUnsubMu.Unlock()
		if u != nil {
			u()
		}
		if onFinished != nil {
			onFinished()
		}
		runHandlerCleanup()
	}
	unsub, err := client.Subscribe(callbackSubject+".cancel", func([]byte) { finish() })
	if err != nil {
		runHandlerCleanup()
		return nil, fmt.Errorf("failed to setup cancel listener: %w", err)
	}
	cancelUnsubMu.Lock()
	cancelUnsub = unsub
	cancelUnsubMu.Unlock()
	probe.start(finish)

	// Return combined cleanup
	cleanup := func() {
		unsub()
		runHandlerCleanup()
	}

	return cleanup, nil
}
