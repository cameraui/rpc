package rpc

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"
)

const (
	streamSubjectPrefix       = "stream."
	callbackSubjectPrefix     = "rpc.cb."
	pullCallbackSubjectPrefix = "_rpc.cb."
	methodsSubject            = "__methods"
	expiredSlack              = 5 * time.Millisecond // clock granularity
)

// CleanupFunc is returned by RegisterHandler to clean up subscriptions.
type CleanupFunc func() error

// RPCMethodAllowlist lets a struct handler restrict which of its exported methods are reachable over RPC.
type RPCMethodAllowlist interface {
	RPCMethods() []string
}

// RPCMethodConcurrency lets a struct handler serve several calls of a method at once.
type RPCMethodConcurrency interface {
	RPCConcurrency() map[string]int
}

// RegisterHandler registers all exported methods of handler as RPC endpoints under the given namespace.
func (c *Client) RegisterHandler(namespace string, handler any, opts ...HandlerOption) (CleanupFunc, error) {
	cfg := &handlerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.namespaceSubscription && !namespaceSubscribable(namespace) {
		return nil, fmt.Errorf("namespace %q overlaps internal subjects and cannot take a namespace subscription", namespace)
	}

	client := c
	if cfg.isolatedConnection {
		client = c.createIsolatedClient("handler-" + namespace)
		if err := client.Connect(context.Background()); err != nil {
			_ = client.Disconnect()
			return nil, fmt.Errorf("connect isolated client: %w", err)
		}
		c.isolatedClientsMu.Lock()
		c.isolatedClients = append(c.isolatedClients, client)
		c.isolatedClientsMu.Unlock()
	}

	methods := ExtractMethods(handler)
	methodNames := slices.Sorted(maps.Keys(methods))
	if methodNames == nil {
		// a caller reads anything but a list as an old responder and keeps retrying
		methodNames = []string{}
	}

	prefix := "rpc." + namespace + "."

	limits := methodConcurrency(handler, cfg.concurrency)
	orphan := client.orphanConfig(cfg.orphanProbe)

	var unsubscribers []func()
	owner := newSessionOwner()
	reg := newHandlerRegistration(client)
	runners := make(map[string]func(data []byte, arrived time.Time), len(methods))

	for methodName, fn := range methods {
		fnCopy := fn
		takesCtx := takesContext(fn.Type())
		allMethodNames := methodNames
		var slots chan struct{}
		if limit := limits[methodName]; limit > 1 {
			slots = make(chan struct{}, limit)
		}
		serial := reg.addMethod()

		runners[methodName] = func(data []byte, arrived time.Time) {
			var msg RPCMessage
			if err := DecodeMessageInto(data, &msg); err != nil {
				return
			}

			response := RPCResponse{ID: msg.ID}
			replySubject := "rpc.reply." + msg.ID
			// one reply per request: the drain deadline may answer a call that still runs
			var state atomic.Int32

			expiry := reg.expiry()
			select {
			case <-expiry:
				if isStreamRequest(msg.Params) {
					reg.closeStream(msg.ID, msg.Params)
				} else {
					reg.closeCall(&state, msg.ID)
				}
				return
			default:
			}

			// an unencodable result still gets an error reply
			reply := func() {
				if !state.CompareAndSwap(replyOpen, replySending) {
					return
				}
				err := client.Publish(replySubject, response)
				state.Store(replyDone)
				if err != nil && client.IsConnected() {
					_ = client.Publish(replySubject, RPCResponse{ID: msg.ID, Error: FormatErrorObject(err)})
				}
			}

			answerPanic := func() {
				r := recover()
				if r == nil {
					return
				}
				if state.CompareAndSwap(replyOpen, replyDone) || state.CompareAndSwap(replySending, replyDone) {
					_ = client.Publish(replySubject, RPCResponse{ID: msg.ID, Error: &RPCError{
						Code:    ErrCodeInternalError,
						Message: fmt.Sprintf("handler panic: %v", r),
					}})
				}
				if !cfg.recoverPanics {
					client.flushTimeout(time.Second)
					panic(r)
				}
			}
			defer answerPanic()

			// only a proxy with an empty method cache sends __discover, old clients never do
			if msg.Discover {
				response.Methods = allMethodNames
			}

			setError := func(err error) {
				response.Error = FormatErrorObject(err)
				if response.Error != nil && response.Error.Code == ErrCodeMethodNotFound {
					response.Methods = allMethodNames
				}
			}

			if isStreamRequest(msg.Params) {
				streamSubject, args := extractStreamParams(msg.Params)
				go handleStreamRequestGo(owner, fnCopy, args, streamSubject, msg.ID, client, orphan, cfg.recoverPanics)
				return
			}

			if isPullIteratorRequest(msg.Params) {
				iteratorID, args := extractPullIteratorParams(msg.Params, msg.ID)
				cleanup, err := handlePullIteratorRequestGo(owner.ctx, fnCopy, args, iteratorID, client, orphan, func() {
					owner.untrack(&client.pullIteratorCleanups, iteratorID)
				})
				if err == nil && cleanup != nil {
					err = owner.keep(&client.pullIteratorCleanups, iteratorID, cleanup)
				}
				if err != nil {
					setError(err)
					reply()
					return
				}
				response.Result = map[string]any{"iteratorId": iteratorID}
				reply()
				return
			}

			if isPullCallbackRequest(msg.Params) {
				iteratorID, callbackSubject, oneway, args := extractPullCallbackParams(msg.Params, msg.ID)
				cleanup, err := handlePullCallbackRequestGo(owner.ctx, fnCopy, args, iteratorID, callbackSubject, oneway, client, orphan, func() {
					owner.untrack(&client.pullIteratorCleanups, iteratorID)
				})
				if err == nil && cleanup != nil {
					err = owner.keep(&client.pullIteratorCleanups, iteratorID, cleanup)
				}
				if err != nil {
					setError(err)
					reply()
					return
				}
				response.Result = map[string]any{"iteratorId": iteratorID}
				reply()
				return
			}

			if isCallbackRequest(msg.Params) {
				callbackSubject, args := extractCallbackParams(msg.Params)
				requestID := msg.ID
				cleanup, err := handleCallbackRequestGo(owner.ctx, fnCopy, args, callbackSubject, requestID, client, orphan, func() {
					owner.untrack(&client.callbackCleanups, requestID)
				})
				if err == nil {
					err = owner.keep(&client.callbackCleanups, requestID, cleanup)
				}
				if err != nil {
					setError(err)
					reply()
					return
				}
				response.Result = map[string]any{"ok": true}
				reply()
				return
			}

			serve := func(ctx context.Context) {
				if expired(arrived, msg.TTL) {
					state.Store(replyDone)
					return
				}
				var start int64
				if msg.Timing {
					start = time.Now().UnixMilli()
				}
				result, err := callHandler(ctx, fnCopy, msg.Params)
				if msg.Timing {
					response.Timing = &RPCTiming{Start: start, End: time.Now().UnixMilli()}
				}
				if err != nil {
					setError(err)
				} else {
					response.Result = result
				}

				reply()
			}

			ctx, cancel := callContext(reg.callCtx, takesCtx)
			if slots == nil && !reg.drains {
				defer cancel()
				serve(ctx)
				return
			}

			if slots == nil {
				call := &handlerCall{id: msg.ID, state: &state, cancel: cancel}
				serial.current.Store(call)
				defer serial.current.CompareAndSwap(call, nil)
				if serial.fast.Load() {
					defer cancel()
					start := time.Now()
					serve(ctx)
					serial.fast.Store(time.Since(start) < handoffAfter)
					return
				}
				// slow methods leave the delivery goroutine free for requests queued behind a call the drain gave up on
				done := make(chan struct{})
				go func() {
					defer close(done)
					defer cancel()
					defer answerPanic()
					start := time.Now()
					serve(ctx)
					serial.fast.Store(time.Since(start) < handoffAfter)
				}()
				select {
				case <-done:
				case <-expiry:
					reg.closeCall(&state, msg.ID)
					cancel()
				}
				return
			}

			call := &handlerCall{id: msg.ID, state: &state, cancel: cancel}
			if !reg.track(call) {
				defer cancel()
				serve(ctx)
				return
			}

			// take the slot here: calls start in arrival order and a full method backs up into the nats.go pending limits
			select {
			case slots <- struct{}{}:
			case <-expiry:
				reg.untrack(call)
				reg.closeCall(&state, msg.ID)
				cancel()
				return
			}
			go func() {
				defer reg.untrack(call)
				defer func() { <-slots }()
				defer cancel()
				defer answerPanic()
				serve(ctx)
			}()
		}
	}

	answerMethods := func(data []byte) {
		var msg RPCMessage
		if err := DecodeMessageInto(data, &msg); err != nil || msg.ID == "" {
			return
		}
		_ = client.Publish("rpc.reply."+msg.ID, RPCResponse{ID: msg.ID, Result: methodNames})
	}

	subscribe := func(entry *subscriptionEntry) error {
		unsub, err := client.subscribeEntry(entry)
		if err != nil {
			for _, u := range unsubscribers {
				u()
			}
			reg.cancelCalls()
			if cfg.isolatedConnection {
				_ = c.releaseIsolated(client)
			}
			return fmt.Errorf("subscribe %s: %w", entry.pattern, err)
		}
		unsubscribers = append(unsubscribers, unsub)
		return nil
	}

	// expiring methods are served from a lane, so their queue wait is measured on this side
	reg.lanes = newLaneSet(client, prefix, cfg.pendingMsgs, cfg.pendingBytes, func(method string) func([]byte, time.Time) {
		if !cfg.namespaceSubscription && !cfg.dropsExpired(method) {
			return nil
		}
		return runners[method]
	})

	if cfg.namespaceSubscription {
		entry := &subscriptionEntry{pattern: prefix + ">", queue: cfg.queue, routed: func(subject string, data []byte) {
			method := strings.TrimPrefix(subject, prefix)
			if method == methodsSubject {
				answerMethods(data)
			} else if lane := reg.lanes.lane(method); lane != nil {
				var arrived time.Time
				if cfg.dropsExpired(method) {
					arrived = time.Now()
				}
				lane.push(data, arrived)
			} else {
				answerUnknownMethod(client, method, data, methodNames)
			}
		}}
		// keyed by the methods the handler has, so a caller cannot add scopes
		entry.chunkLane = func(subject string) string {
			method := strings.TrimPrefix(subject, prefix)
			if _, ok := runners[method]; ok {
				return method
			}
			return ""
		}
		if err := subscribe(entry); err != nil {
			return nil, err
		}
		reg.entries = append(reg.entries, entry)
	} else {
		var opts []func(*nats.Subscription)
		if cfg.pendingMsgs != 0 || cfg.pendingBytes != 0 {
			opts = append(opts, pendingLimits(cfg.pendingMsgs, cfg.pendingBytes))
		}
		for methodName, run := range runners {
			handle := func(data []byte) { run(data, time.Time{}) }
			if cfg.dropsExpired(methodName) {
				lane := reg.lanes.lane(methodName)
				handle = func(data []byte) { lane.push(data, time.Now()) }
			}
			entry := &subscriptionEntry{pattern: prefix + methodName, queue: cfg.queue, opts: opts, handler: handle}
			if err := subscribe(entry); err != nil {
				return nil, err
			}
			reg.entries = append(reg.entries, entry)
		}

		// after the methods, so a caller that sees the list also reaches them
		reg.discovery = &subscriptionEntry{pattern: prefix + methodsSubject, queue: cfg.queue, handler: answerMethods}
		if err := subscribe(reg.discovery); err != nil {
			return nil, err
		}
		// the method list goes first on cleanup
		last := len(unsubscribers) - 1
		discovery := unsubscribers[last]
		copy(unsubscribers[1:], unsubscribers[:last])
		unsubscribers[0] = discovery
	}

	// one PING for all subscriptions, so a caller on another connection reaches every method once this returns
	client.flushTimeout(registerFlushTimeout)

	client.addHandlerRegistration(reg)

	cleanup := func() error {
		// first, so running sessions see their context end at once
		owner.stop()
		if !reg.drains {
			reg.cancelCalls()
		}
		client.drainHandlers([]*handlerRegistration{reg})
		reg.cancelCalls()
		client.removeHandlerRegistration(reg)
		for _, unsub := range unsubscribers {
			unsub()
		}
		reg.lanes.close()
		reg.stop()

		if cfg.isolatedConnection {
			return c.releaseIsolated(client)
		}
		return nil
	}

	return cleanup, nil
}

// ExtractMethods extracts all RPC-callable methods from a handler.
func ExtractMethods(handler any) map[string]reflect.Value {
	methods := make(map[string]reflect.Value)
	extractMethodsRecursive(handler, "", methods, make(map[uintptr]bool))
	return methods
}

func extractMethodsRecursive(handler any, prefix string, methods map[string]reflect.Value, visited map[uintptr]bool) {
	v := reflect.ValueOf(handler)
	t := v.Type()

	if t.Kind() == reflect.Map && t.Key().Kind() == reflect.String {
		for _, key := range v.MapKeys() {
			name := key.String()
			if strings.HasPrefix(name, "_") {
				continue
			}
			val := v.MapIndex(key)
			if val.Kind() == reflect.Interface {
				val = val.Elem()
			}

			fullName := name
			if prefix != "" {
				fullName = prefix + "." + name
			}

			switch {
			case val.Kind() == reflect.Func:
				methods[fullName] = val
			case val.Kind() == reflect.Map || (val.Kind() == reflect.Pointer && val.Elem().Kind() == reflect.Struct):
				extractMethodsRecursive(val.Interface(), fullName, methods, visited)
			default:
				captured := val.Interface()
				methods[fullName] = reflect.ValueOf(func() any { return captured })
			}
		}
		return
	}

	if t.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		ptr := v.Pointer()
		if visited[ptr] {
			return
		}
		visited[ptr] = true
		t = t.Elem()
		// v stays a pointer for the method set
	}

	if t.Kind() != reflect.Struct {
		return
	}

	var allowed map[string]bool
	if provider, ok := handler.(RPCMethodAllowlist); ok {
		names := provider.RPCMethods()
		allowed = make(map[string]bool, len(names))
		for _, n := range names {
			allowed[n] = true
		}
	}

	_, hasConcurrency := handler.(RPCMethodConcurrency)

	// the method set of the original type, pointer receivers included
	methodType := reflect.ValueOf(handler).Type()
	for i := 0; i < methodType.NumMethod(); i++ {
		m := methodType.Method(i)
		if !m.IsExported() || (hasConcurrency && m.Name == "RPCConcurrency") {
			continue
		}
		wireName := toCamelCase(m.Name)
		if allowed != nil && !allowed[wireName] {
			continue
		}
		fullName := wireName
		if prefix != "" {
			fullName = prefix + "." + wireName
		}
		methods[fullName] = reflect.ValueOf(handler).Method(i)
	}

	structVal := reflect.Indirect(reflect.ValueOf(handler))
	structType := structVal.Type()
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		if propName := field.Tag.Get("rpc_prop"); propName != "" && propName != "-" {
			fieldVal := structVal.Field(i)
			if !fieldVal.CanInterface() {
				continue
			}

			getterName := propName
			if prefix != "" {
				getterName = prefix + "." + propName
			}

			methods[getterName] = reflect.ValueOf(fieldVal.Interface)

			if fieldVal.CanSet() {
				setterName := "set" + strings.ToUpper(propName[:1]) + propName[1:]
				if prefix != "" {
					setterName = prefix + "." + setterName
				}
				fieldType := field.Type
				methods[setterName] = reflect.ValueOf(func(val any) error {
					fieldVal.Set(coerceValue(val, fieldType))
					return nil
				})
			}
			continue
		}

		tag := field.Tag.Get("rpc")
		if tag == "" || tag == "-" {
			continue
		}
		fieldVal := structVal.Field(i)
		if !fieldVal.CanInterface() {
			continue
		}

		nestedPrefix := tag
		if prefix != "" {
			nestedPrefix = prefix + "." + tag
		}
		extractMethodsRecursive(fieldVal.Interface(), nestedPrefix, methods, visited)
	}
}

func expired(arrived time.Time, ttl Milliseconds) bool {
	return ttl > 0 && !arrived.IsZero() && time.Since(arrived) > time.Duration(ttl)*time.Millisecond+expiredSlack
}

func pendingLimits(msgs, bytes int) func(*nats.Subscription) {
	if msgs == 0 {
		msgs = nats.DefaultSubPendingMsgsLimit
	}
	if bytes == 0 {
		bytes = nats.DefaultSubPendingBytesLimit
	}
	return func(sub *nats.Subscription) { _ = sub.SetPendingLimits(msgs, bytes) }
}

func namespaceSubscribable(namespace string) bool {
	// rpc.reply.> and rpc.cb.> carry every client's replies and callbacks
	first, _, _ := strings.Cut(namespace, ".")
	return first != "reply" && first != "cb"
}

func answerUnknownMethod(client *Client, method string, data []byte, methods []string) {
	var msg RPCMessage
	if err := DecodeMessageInto(data, &msg); err != nil || msg.ID == "" {
		return
	}
	rpcErr := &RPCError{Code: ErrCodeMethodNotFound, Message: "Method not found: " + method}
	if isStreamRequest(msg.Params) {
		if subject, _ := extractStreamParams(msg.Params); isControlSubject(subject, streamSubjectPrefix) {
			_ = client.Publish(subject, StreamMessage{ID: msg.ID, Type: "error", Error: rpcErr})
		}
		return
	}
	_ = client.Publish("rpc.reply."+msg.ID, RPCResponse{ID: msg.ID, Error: rpcErr, Methods: methods})
}

func methodConcurrency(handler any, overrides map[string]int) map[string]int {
	limits := map[string]int{}
	if provider, ok := handler.(RPCMethodConcurrency); ok {
		maps.Copy(limits, provider.RPCConcurrency())
	}
	maps.Copy(limits, overrides)
	return limits
}

func toCamelCase(name string) string {
	if name == "" {
		return name
	}
	runes := []rune(name)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

func callHandler(ctx context.Context, fn reflect.Value, params any) (any, error) {
	var args []any
	switch p := params.(type) {
	case []any:
		args = p
	case nil:
		args = nil
	default:
		args = []any{params}
	}

	fnType := fn.Type()
	numIn := fnType.NumIn()
	first := contextParams(fnType)

	// JS undefined must not reach a handler as an empty struct; scalar args and maps for a struct param skip the walk
	for i := range args {
		switch args[i].(type) {
		case map[string]any:
			if !structParam(paramTypeOf(fnType, first+i)) {
				args[i] = NormalizeUndefined(args[i])
			}
		case msgpackrUndefined, *msgpackrUndefined, []any:
			args[i] = NormalizeUndefined(args[i])
		}
	}

	callArgs := make([]reflect.Value, numIn)
	if first == 1 {
		callArgs[0] = reflect.ValueOf(ctx)
	}
	for i := first; i < numIn; i++ {
		paramType := fnType.In(i)
		if a := i - first; a < len(args) {
			callArgs[i] = coerceValue(args[a], paramType)
		} else {
			callArgs[i] = reflect.Zero(paramType)
		}
	}

	if fnType.IsVariadic() && len(args) > numIn-1-first {
		callArgs = callArgs[:numIn-1]
		for i := numIn - 1 - first; i < len(args); i++ {
			elemType := fnType.In(numIn - 1).Elem()
			callArgs = append(callArgs, coerceValue(args[i], elemType))
		}
		results := fn.Call(callArgs)
		return processResults(results)
	}

	// no variadic args: callArgs already ends with the (empty) slice
	if fnType.IsVariadic() {
		return processResults(fn.CallSlice(callArgs))
	}
	results := fn.Call(callArgs)
	return processResults(results)
}

func paramTypeOf(fnType reflect.Type, i int) reflect.Type {
	n := fnType.NumIn()
	if fnType.IsVariadic() && i >= n-1 {
		return fnType.In(n - 1).Elem()
	}
	if i < n {
		return fnType.In(i)
	}
	return nil
}

func structParam(t reflect.Type) bool {
	if t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t != nil && t.Kind() == reflect.Struct
}

func processResults(results []reflect.Value) (any, error) {
	switch len(results) {
	case 0:
		return nil, nil
	case 1:
		v := results[0]
		if v.Type().Implements(reflect.TypeFor[error]()) {
			if v.IsNil() {
				return nil, nil
			}
			return nil, v.Interface().(error)
		}
		return v.Interface(), nil
	default:
		last := results[len(results)-1]
		if last.Type().Implements(reflect.TypeFor[error]()) {
			var err error
			if !last.IsNil() {
				err = last.Interface().(error)
			}
			if len(results) == 2 {
				return results[0].Interface(), err
			}
			vals := make([]any, len(results)-1)
			for i := 0; i < len(results)-1; i++ {
				vals[i] = results[i].Interface()
			}
			return vals, err
		}
		if len(results) == 1 {
			return results[0].Interface(), nil
		}
		vals := make([]any, len(results))
		for i := range results {
			vals[i] = results[i].Interface()
		}
		return vals, nil
	}
}

func coerceValue(val any, target reflect.Type) reflect.Value {
	if val == nil {
		return reflect.Zero(target)
	}

	v := reflect.ValueOf(val)

	if v.Type().AssignableTo(target) {
		return v
	}

	if v.Type().ConvertibleTo(target) {
		return v.Convert(target)
	}

	if isNumericKind(v.Kind()) && isNumericKind(target.Kind()) {
		return v.Convert(target)
	}

	if v.Kind() == reflect.Slice && target.Kind() == reflect.Slice {
		if items, ok := val.([]any); ok {
			switch target {
			case float64SliceType:
				return reflect.ValueOf(coerceFloat64s(items))
			case stringSliceType:
				return reflect.ValueOf(coerceStrings(items))
			}
		}
		result := reflect.MakeSlice(target, v.Len(), v.Len())
		for i := range v.Len() {
			elem := coerceValue(v.Index(i).Interface(), target.Elem())
			result.Index(i).Set(elem)
		}
		return result
	}

	if (v.Kind() == reflect.Map || v.Kind() == reflect.Interface) && target.Kind() == reflect.Struct {
		if newVal, ok := convertToStruct(val, target); ok {
			return newVal.Elem()
		}
	}

	if (v.Kind() == reflect.Map || v.Kind() == reflect.Interface) && target.Kind() == reflect.Pointer && target.Elem().Kind() == reflect.Struct {
		if newVal, ok := convertToStruct(val, target.Elem()); ok {
			return newVal
		}
	}

	return reflect.Zero(target)
}

func convertToStruct(val any, t reflect.Type) (reflect.Value, bool) {
	newVal := reflect.New(t)
	if assignGeneric(newVal.Elem(), val) {
		return newVal, true
	}
	// a failed assign leaves newVal half written, the round trip starts from a fresh one
	newVal = reflect.New(t)
	if err := convertPooled(coerceValues(val), newVal.Interface()); err != nil {
		return reflect.Value{}, false
	}
	return newVal, true
}

func coerceFloat64s(items []any) []float64 {
	out := make([]float64, len(items))
	for i, item := range items {
		switch f := item.(type) {
		case float64:
			out[i] = f
		case int64:
			out[i] = float64(f)
		default:
			out[i] = coerceValue(item, float64Type).Float()
		}
	}
	return out
}

func coerceStrings(items []any) []string {
	out := make([]string, len(items))
	for i, item := range items {
		if s, ok := item.(string); ok {
			out[i] = s
		} else {
			out[i] = coerceValue(item, stringType).String()
		}
	}
	return out
}

func isNumericKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func isStreamRequest(params any) bool {
	switch p := params.(type) {
	case map[string]any:
		if v, ok := p["__stream"]; ok {
			if b, ok := v.(bool); ok && b {
				return true
			}
		}
	case StreamParams:
		return p.Stream
	}

	if slice, ok := params.([]any); ok && len(slice) > 0 {
		if m, ok := slice[0].(map[string]any); ok {
			if v, ok := m["__stream"]; ok {
				if b, ok := v.(bool); ok && b {
					return true
				}
			}
		}
	}
	return false
}

func isPullIteratorRequest(params any) bool {
	switch p := params.(type) {
	case map[string]any:
		if v, ok := p["__pullIterator"]; ok {
			if b, ok := v.(bool); ok && b {
				return true
			}
		}
	case PullIteratorParams:
		return p.PullIterator
	}

	if slice, ok := params.([]any); ok && len(slice) > 0 {
		if m, ok := slice[0].(map[string]any); ok {
			if v, ok := m["__pullIterator"]; ok {
				if b, ok := v.(bool); ok && b {
					return true
				}
			}
		}
	}
	return false
}

func extractStreamParams(params any) (subject string, args []any) {
	switch p := params.(type) {
	case map[string]any:
		subject, _ := p["__streamSubject"].(string)
		args, _ := p["args"].([]any)
		return subject, args
	case StreamParams:
		return p.StreamSubject, p.Args
	}
	return "", nil
}

func extractPullIteratorParams(params any, defaultID string) (iteratorID string, args []any) {
	extract := func(m map[string]any) (string, []any) {
		iteratorID, _ := m["__iteratorId"].(string)
		if iteratorID == "" {
			iteratorID = defaultID
		}
		args, _ := m["args"].([]any)
		return iteratorID, args
	}

	switch p := params.(type) {
	case map[string]any:
		if _, ok := p["__pullIterator"]; ok {
			return extract(p)
		}
	case PullIteratorParams:
		id := p.IteratorID
		if id == "" {
			id = defaultID
		}
		return id, p.Args
	}

	if slice, ok := params.([]any); ok && len(slice) > 0 {
		if m, ok := slice[0].(map[string]any); ok {
			if _, ok := m["__pullIterator"]; ok {
				return extract(m)
			}
		}
	}
	return defaultID, nil
}

func isPullCallbackRequest(params any) bool {
	switch p := params.(type) {
	case map[string]any:
		if v, ok := p["__pullCallback"]; ok {
			if b, ok := v.(bool); ok && b {
				return true
			}
		}
	case PullCallbackParams:
		return p.PullCallback
	}

	if slice, ok := params.([]any); ok && len(slice) > 0 {
		if m, ok := slice[0].(map[string]any); ok {
			if v, ok := m["__pullCallback"]; ok {
				if b, ok := v.(bool); ok && b {
					return true
				}
			}
		}
	}
	return false
}

func extractPullCallbackParams(params any, defaultID string) (iteratorID, callbackSubject string, oneway []string, args []any) {
	extract := func(m map[string]any) (string, string, []string, []any) {
		id, _ := m["__iteratorId"].(string)
		if id == "" {
			id = defaultID
		}
		cbSubject, _ := m["__callbackSubject"].(string)

		var onewayList []string
		if raw, ok := m["__onewayMethods"].([]any); ok {
			onewayList = make([]string, 0, len(raw))
			for _, v := range raw {
				if s, ok := v.(string); ok {
					onewayList = append(onewayList, s)
				}
			}
		}

		a, _ := m["args"].([]any)
		return id, cbSubject, onewayList, a
	}

	switch p := params.(type) {
	case map[string]any:
		if _, ok := p["__pullCallback"]; ok {
			return extract(p)
		}
	case PullCallbackParams:
		id := p.IteratorID
		if id == "" {
			id = defaultID
		}
		return id, p.CallbackSubject, p.OnewayMethods, p.Args
	}

	if slice, ok := params.([]any); ok && len(slice) > 0 {
		if m, ok := slice[0].(map[string]any); ok {
			if _, ok := m["__pullCallback"]; ok {
				return extract(m)
			}
		}
	}
	return defaultID, "", nil, nil
}

func isCallbackRequest(params any) bool {
	switch p := params.(type) {
	case map[string]any:
		if v, ok := p["__callback"]; ok {
			if b, ok := v.(bool); ok && b {
				return true
			}
		}
	case CallbackParams:
		return p.Callback
	}

	if slice, ok := params.([]any); ok && len(slice) > 0 {
		if m, ok := slice[0].(map[string]any); ok {
			if v, ok := m["__callback"]; ok {
				if b, ok := v.(bool); ok && b {
					return true
				}
			}
		}
	}
	return false
}

func extractCallbackParams(params any) (subject string, args []any) {
	if slice, ok := params.([]any); ok && len(slice) > 0 {
		if m, ok := slice[0].(map[string]any); ok {
			subject, _ := m["__callbackSubject"].(string)
			args, _ := m["args"].([]any)
			return subject, args
		}
	}

	switch p := params.(type) {
	case map[string]any:
		subject, _ := p["__callbackSubject"].(string)
		args, _ := p["args"].([]any)
		return subject, args
	case CallbackParams:
		return p.CallbackSubject, p.Args
	}
	return "", nil
}

func isControlSubject(subject, prefix string) bool {
	// the caller names this subject and the handler publishes there with its own credentials
	return len(subject) > len(prefix) && strings.HasPrefix(subject, prefix)
}
