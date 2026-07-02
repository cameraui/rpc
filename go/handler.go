package rpc

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
)

// CleanupFunc is returned by RegisterHandler to clean up subscriptions.
type CleanupFunc func() error

// RegisterHandler registers all exported methods of handler as RPC endpoints
// under the given namespace. Methods are converted to camelCase wire names.
//
// Supported handler types:
//   - struct pointer: all exported methods are exposed
//   - map[string]any: values must be functions
//
// Struct fields tagged with `rpc:"name"` are treated as nested namespaces.
//
// Example:
//
//	type MyHandler struct {
//	    DB *DBHandler `rpc:"db"`
//	}
//	func (h *MyHandler) GetSnapshot(name string) ([]byte, error) { ... }
//
// This exposes: rpc.{namespace}.getSnapshot and rpc.{namespace}.db.*
func (c *Client) RegisterHandler(namespace string, handler any, opts ...HandlerOption) (CleanupFunc, error) {
	cfg := &handlerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	client := c
	if cfg.isolatedConnection {
		client = c.createIsolatedClient("handler-" + namespace)
		if err := client.Connect(context.Background()); err != nil {
			return nil, fmt.Errorf("connect isolated client: %w", err)
		}
		c.isolatedClientsMu.Lock()
		c.isolatedClients = append(c.isolatedClients, client)
		c.isolatedClientsMu.Unlock()
	}

	methods := ExtractMethods(handler)
	methodNames := make([]string, 0, len(methods))
	for name := range methods {
		methodNames = append(methodNames, name)
	}

	var unsubscribers []func()

	// Track the iterator/callback sessions created by THIS RegisterHandler
	// call. The client maps are shared across RegisterHandler calls; sweeping
	// them wholesale on cleanup would kill the live sessions of every other
	// namespace.
	var (
		idsMu           sync.Mutex
		pullIteratorIDs []string
		callbackIDs     []string
	)

	for methodName, fn := range methods {
		subject := fmt.Sprintf("rpc.%s.%s", namespace, methodName)
		fnCopy := fn
		allMethodNames := methodNames

		msgHandler := func(data []byte) {
			var msg RPCMessage
			if err := Decode(data, &msg); err != nil {
				return
			}

			response := RPCResponse{ID: msg.ID}

			// Method discovery on demand: only a request whose envelope
			// carries __discover (a proxy with an empty method cache) pays
			// for the namespace's method list — attaching it to every
			// response would be dead wire weight once the proxy cache is
			// filled. Old clients never send __discover and never read
			// __methods on this path.
			if msg.Discover {
				response.Methods = allMethodNames
			}

			// setError formats err into the response and, as a diagnostic
			// aid, attaches the method list to METHOD_NOT_FOUND errors —
			// discovery requested or not (rare, small).
			setError := func(err error) {
				response.Error = FormatErrorObject(err)
				if response.Error != nil && response.Error.Code == ErrCodeMethodNotFound {
					response.Methods = allMethodNames
				}
			}

			// Check for stream request
			if isStreamRequest(msg.Params) {
				streamSubject, args := extractStreamParams(msg.Params)
				go handleStreamRequestGo(fnCopy, args, streamSubject, msg.ID, client)
				return
			}

			// Check for pull iterator request
			if isPullIteratorRequest(msg.Params) {
				iteratorID, args := extractPullIteratorParams(msg.Params, msg.ID)
				cleanup, err := handlePullIteratorRequestGo(fnCopy, args, iteratorID, client, func() {
					client.pullIteratorCleanups.Delete(iteratorID)
				})
				if err != nil {
					setError(err)
					replySubject := "rpc.reply." + msg.ID
					_ = client.Publish(replySubject, response)
					return
				}
				client.pullIteratorCleanups.Store(iteratorID, cleanup)
				idsMu.Lock()
				pullIteratorIDs = append(pullIteratorIDs, iteratorID)
				idsMu.Unlock()
				response.Result = map[string]any{"iteratorId": iteratorID}
				replySubject := "rpc.reply." + msg.ID
				_ = client.Publish(replySubject, response)
				return
			}

			// Check for pull-iterator-with-callbacks request
			if isPullCallbackRequest(msg.Params) {
				iteratorID, callbackSubject, oneway, args := extractPullCallbackParams(msg.Params, msg.ID)
				cleanup, err := handlePullCallbackRequestGo(fnCopy, args, iteratorID, callbackSubject, oneway, client, func() {
					client.pullIteratorCleanups.Delete(iteratorID)
				})
				if err != nil {
					setError(err)
					replySubject := "rpc.reply." + msg.ID
					_ = client.Publish(replySubject, response)
					return
				}
				client.pullIteratorCleanups.Store(iteratorID, cleanup)
				idsMu.Lock()
				pullIteratorIDs = append(pullIteratorIDs, iteratorID)
				idsMu.Unlock()
				response.Result = map[string]any{"iteratorId": iteratorID}
				replySubject := "rpc.reply." + msg.ID
				_ = client.Publish(replySubject, response)
				return
			}

			// Check for callback subscription request
			if isCallbackRequest(msg.Params) {
				callbackSubject, args := extractCallbackParams(msg.Params)
				requestID := msg.ID
				cleanup, err := handleCallbackRequestGo(fnCopy, args, callbackSubject, requestID, client, func() {
					client.callbackCleanups.Delete(requestID)
				})
				if err != nil {
					setError(err)
					replySubject := "rpc.reply." + msg.ID
					_ = client.Publish(replySubject, response)
					return
				}
				client.callbackCleanups.Store(requestID, cleanup)
				idsMu.Lock()
				callbackIDs = append(callbackIDs, requestID)
				idsMu.Unlock()
				response.Result = map[string]any{"ok": true}
				replySubject := "rpc.reply." + msg.ID
				_ = client.Publish(replySubject, response)
				return
			}

			// Normal RPC call
			result, err := callHandler(fnCopy, msg.Params)
			if err != nil {
				setError(err)
			} else {
				response.Result = result
			}

			replySubject := "rpc.reply." + msg.ID
			_ = client.Publish(replySubject, response)
		}

		var unsub func()
		var err error
		if cfg.queue != "" {
			unsub, err = client.SubscribeQueue(subject, cfg.queue, msgHandler)
		} else {
			unsub, err = client.Subscribe(subject, msgHandler)
		}
		if err != nil {
			// Cleanup already-registered handlers
			for _, u := range unsubscribers {
				u()
			}
			return nil, fmt.Errorf("subscribe %s: %w", subject, err)
		}
		unsubscribers = append(unsubscribers, unsub)
	}

	cleanup := func() error {
		for _, unsub := range unsubscribers {
			unsub()
		}

		idsMu.Lock()
		pids := pullIteratorIDs
		pullIteratorIDs = nil
		cids := callbackIDs
		callbackIDs = nil
		idsMu.Unlock()

		// Cleanup pull iterators — only the sessions this RegisterHandler
		// call created; the client maps are shared across namespaces.
		for _, id := range pids {
			if v, ok := client.pullIteratorCleanups.LoadAndDelete(id); ok {
				if fn, ok := v.(func()); ok {
					fn()
				}
			}
		}

		// Cleanup callbacks
		for _, id := range cids {
			if v, ok := client.callbackCleanups.LoadAndDelete(id); ok {
				if fn, ok := v.(func()); ok {
					fn()
				}
			}
		}

		if cfg.isolatedConnection {
			err := client.Disconnect()
			c.removeIsolatedClient(client)
			return err
		}
		return nil
	}

	return cleanup, nil
}

// ExtractMethods extracts all RPC-callable methods from a handler.
// For struct handlers, exported methods are converted to camelCase.
// For map handlers, keys are used as-is.
// Nested structs with `rpc:"name"` tags create dotted paths.
func ExtractMethods(handler any) map[string]reflect.Value {
	methods := make(map[string]reflect.Value)
	extractMethodsRecursive(handler, "", methods, make(map[uintptr]bool))
	return methods
}

func extractMethodsRecursive(handler any, prefix string, methods map[string]reflect.Value, visited map[uintptr]bool) {
	v := reflect.ValueOf(handler)
	t := v.Type()

	// Handle map[string]any
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
				// Non-function value: wrap as a getter that returns the value
				captured := val.Interface()
				methods[fullName] = reflect.ValueOf(func() any { return captured })
			}
		}
		return
	}

	// Dereference pointer
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
		// v stays as pointer for method set
	}

	if t.Kind() != reflect.Struct {
		return
	}

	// Extract exported methods from the method set of the original (possibly pointer) type
	methodType := reflect.ValueOf(handler).Type()
	for i := 0; i < methodType.NumMethod(); i++ {
		m := methodType.Method(i)
		if !m.IsExported() {
			continue
		}
		wireName := toCamelCase(m.Name)
		fullName := wireName
		if prefix != "" {
			fullName = prefix + "." + wireName
		}
		methods[fullName] = reflect.ValueOf(handler).Method(i)
	}

	// Recurse into fields with `rpc` tag for nested namespaces
	// and expose fields with `rpc_prop` tag as getter/setter methods.
	structVal := reflect.Indirect(reflect.ValueOf(handler))
	structType := structVal.Type()
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		// Handle rpc_prop tag: auto-generate getter and setter
		if propName := field.Tag.Get("rpc_prop"); propName != "" && propName != "-" {
			fieldVal := structVal.Field(i)
			if !fieldVal.CanInterface() {
				continue
			}

			getterName := propName
			if prefix != "" {
				getterName = prefix + "." + propName
			}

			// Getter: returns current field value
			methods[getterName] = reflect.ValueOf(fieldVal.Interface)

			// Setter: setXxx(value) — only if field is settable
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

// toCamelCase converts a Go PascalCase name to JavaScript camelCase.
// Examples: GetSnapshot → getSnapshot, GenerateFrames → generateFrames
func toCamelCase(name string) string {
	if name == "" {
		return name
	}
	runes := []rune(name)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

// callHandler invokes a handler function with the given params.
func callHandler(fn reflect.Value, params any) (any, error) {
	// Convert params to a slice of arguments
	var args []any
	switch p := params.(type) {
	case []any:
		args = p
	case nil:
		args = nil
	default:
		args = []any{params}
	}

	// JS `undefined` arrives as msgpackrUndefined (ext type 0). Normalize to
	// nil deeply so it never reaches a handler as an empty struct (which would
	// serialize back out as `{}`).
	//
	// Gate: after a generic msgpack decode, `undefined` can only appear as the
	// arg itself or nested inside map[string]any/[]any containers (the only
	// container types the decoder produces for `any`). Scalar and []byte args
	// — the NVR frame hot path — skip the recursive walk entirely.
	for i := range args {
		switch args[i].(type) {
		case msgpackrUndefined, *msgpackrUndefined, map[string]any, []any:
			args[i] = NormalizeUndefined(args[i])
		}
	}

	fnType := fn.Type()
	numIn := fnType.NumIn()

	// Build call arguments
	callArgs := make([]reflect.Value, numIn)
	for i := range numIn {
		paramType := fnType.In(i)
		if i < len(args) {
			callArgs[i] = coerceValue(args[i], paramType)
		} else {
			callArgs[i] = reflect.Zero(paramType)
		}
	}

	// Handle variadic functions
	if fnType.IsVariadic() && len(args) > numIn-1 {
		callArgs = callArgs[:numIn-1]
		for i := numIn - 1; i < len(args); i++ {
			elemType := fnType.In(numIn - 1).Elem()
			callArgs = append(callArgs, coerceValue(args[i], elemType))
		}
		results := fn.Call(callArgs)
		return processResults(results)
	}

	results := fn.Call(callArgs)
	return processResults(results)
}

// processResults processes the return values from a handler call.
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
		// Last return value is an error by convention
		last := results[len(results)-1]
		if last.Type().Implements(reflect.TypeFor[error]()) {
			var err error
			if !last.IsNil() {
				err = last.Interface().(error)
			}
			if len(results) == 2 {
				return results[0].Interface(), err
			}
			// Multiple return values — pack into slice
			vals := make([]any, len(results)-1)
			for i := 0; i < len(results)-1; i++ {
				vals[i] = results[i].Interface()
			}
			return vals, err
		}
		// No error return — return all
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

// coerceValue attempts to convert a generic value to the target type.
func coerceValue(val any, target reflect.Type) reflect.Value {
	if val == nil {
		return reflect.Zero(target)
	}

	v := reflect.ValueOf(val)

	// Direct assignment
	if v.Type().AssignableTo(target) {
		return v
	}

	// Convertible
	if v.Type().ConvertibleTo(target) {
		return v.Convert(target)
	}

	// Handle numeric conversions from MessagePack (often comes as int64/float64)
	if isNumericKind(v.Kind()) && isNumericKind(target.Kind()) {
		return v.Convert(target)
	}

	// Handle slice to slice conversion (e.g. []any → []float64, []any → [][]float64)
	if v.Kind() == reflect.Slice && target.Kind() == reflect.Slice {
		result := reflect.MakeSlice(target, v.Len(), v.Len())
		for i := range v.Len() {
			elem := coerceValue(v.Index(i).Interface(), target.Elem())
			result.Index(i).Set(elem)
		}
		return result
	}

	// Handle map to struct conversion via re-encoding
	if (v.Kind() == reflect.Map || v.Kind() == reflect.Interface) && target.Kind() == reflect.Struct {
		newVal := reflect.New(target)
		coerced := coerceValues(val)
		data, err := Encode(coerced)
		if err == nil {
			if err := Decode(data, newVal.Interface()); err == nil {
				return newVal.Elem()
			}
		}
	}

	// Handle map to pointer-to-struct
	if (v.Kind() == reflect.Map || v.Kind() == reflect.Interface) && target.Kind() == reflect.Pointer && target.Elem().Kind() == reflect.Struct {
		newVal := reflect.New(target.Elem())
		coerced := coerceValues(val)
		data, err := Encode(coerced)
		if err == nil {
			if err := Decode(data, newVal.Interface()); err == nil {
				return newVal
			}
		}
	}

	// Fallback: zero value
	return reflect.Zero(target)
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

// isStreamRequest checks if params contain the __stream marker.
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

	// Check if params is a slice containing a StreamParams-like map
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

// isPullIteratorRequest checks if params contain the __pullIterator marker.
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

// extractStreamParams extracts stream subject and args from params.
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

// extractPullIteratorParams extracts iterator ID and args from params.
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

// isPullCallbackRequest checks if params contain the __pullCallback marker.
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

// extractPullCallbackParams extracts iterator ID, callback subject, oneway
// method list and args from params.
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

// isCallbackRequest checks if params contain the __callback marker.
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

// extractCallbackParams extracts callback subject and args from params.
func extractCallbackParams(params any) (subject string, args []any) {
	// Check array-wrapped variant first (from client.Call)
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
