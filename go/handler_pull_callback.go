package rpc

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// CallbackInvoker is passed to a pull-callback handler as its last argument to fire oneway callbacks.
type CallbackInvoker struct {
	subject string
	client  *Client
	oneway  map[string]struct{}
	active  atomic.Bool
	ctx     context.Context
	probe   *orphanProbe
	fail    func(err error)
	mu      sync.RWMutex
}

// Invoke fires a oneway callback, a method the client did not register as oneway is dropped.
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

// Supports reports whether the client registered method as a oneway callback, so Invoke sends it.
func (ci *CallbackInvoker) Supports(method string) bool {
	_, ok := ci.oneway[method]
	return ok
}

// Active reports whether further Invoke calls will be dispatched.
func (ci *CallbackInvoker) Active() bool {
	return ci.active.Load()
}

// Context is cancelled when the session ends.
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

	fnType := fn.Type()
	numIn := fnType.NumIn()
	first := contextParams(fnType)
	if numIn <= first {
		return nil, NewRPCException(ErrCodeInternalError,
			"pull-callback handler must accept at least one argument (CallbackInvoker)")
	}

	invokerParamType := fnType.In(numIn - 1)
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
		// the caller cancelled during the setup, the session is over
		return nil, nil
	}
	return s.teardown, nil
}
