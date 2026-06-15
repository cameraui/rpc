package rpc

import (
	"fmt"
	"reflect"
)

// handleCallbackRequestGo handles a callback subscription request.
// It creates a wrapper function matching the handler's func(T) parameter,
// calls the handler with it, and manages the subscription lifecycle.
func handleCallbackRequestGo(fn reflect.Value, args []any, callbackSubject, requestID string, client *Client) (func(), error) {
	fnType := fn.Type()

	// Find the func(T) parameter position
	callbackParamIdx := -1
	for i := 0; i < fnType.NumIn(); i++ {
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
		err := client.Publish(callbackSubject, msg)
		if hasErrorReturn {
			if err != nil {
				return []reflect.Value{reflect.ValueOf(err)}
			}
			return []reflect.Value{reflect.Zero(callbackType.Out(0))}
		}
		return nil
	})

	// Build call arguments: insert wrapper at the callback position
	numIn := fnType.NumIn()
	callArgs := make([]reflect.Value, numIn)
	argIdx := 0
	for i := range numIn {
		if i == callbackParamIdx {
			callArgs[i] = wrapperFunc
		} else {
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

	// Subscribe to cancel subject for cleanup
	cancelUnsub, err := client.Subscribe(callbackSubject+".cancel", func(data []byte) {
		if handlerCleanup != nil {
			handlerCleanup()
		}
	})
	if err != nil {
		if handlerCleanup != nil {
			handlerCleanup()
		}
		return nil, fmt.Errorf("failed to setup cancel listener: %w", err)
	}

	// Return combined cleanup
	cleanup := func() {
		cancelUnsub()
		if handlerCleanup != nil {
			handlerCleanup()
		}
	}

	return cleanup, nil
}
