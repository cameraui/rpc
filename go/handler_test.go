package rpc

import (
	"reflect"
	"testing"
)

func TestToCamelCase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"GetSnapshot", "getSnapshot"},
		{"GenerateFrames", "generateFrames"},
		{"ID", "iD"},
		{"Name", "name"},
		{"", ""},
		{"a", "a"},
		{"ABC", "aBC"},
	}

	for _, tt := range tests {
		got := toCamelCase(tt.input)
		if got != tt.want {
			t.Errorf("toCamelCase(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

type testHandler struct {
	DB *testDBHandler `rpc:"db"`
}

func (h *testHandler) GetSnapshot(name string) (string, error) {
	return "snapshot:" + name, nil
}

func (h *testHandler) GenerateFrames(camera string) (<-chan string, error) {
	ch := make(chan string, 3)
	ch <- "frame1"
	ch <- "frame2"
	ch <- "frame3"
	close(ch)
	return ch, nil
}

type testDBHandler struct{}

func (h *testDBHandler) Find(key string) (string, error) {
	return "found:" + key, nil
}

func (h *testDBHandler) Delete(key string) error {
	return nil
}

func TestExtractMethodsStruct(t *testing.T) {
	handler := &testHandler{
		DB: &testDBHandler{},
	}

	methods := ExtractMethods(handler)

	// Should have: getSnapshot, generateFrames, db.find, db.delete
	expectedMethods := []string{"getSnapshot", "generateFrames", "db.find", "db.delete"}

	for _, name := range expectedMethods {
		if _, ok := methods[name]; !ok {
			t.Errorf("missing method %q", name)
		}
	}

	if len(methods) != len(expectedMethods) {
		t.Errorf("expected %d methods, got %d: %v", len(expectedMethods), len(methods), methodNames(methods))
	}
}

func TestExtractMethodsMap(t *testing.T) {
	handler := map[string]any{
		"greet": func(name string) string { return "hello " + name },
		"add":   func(a, b int) int { return a + b },
	}

	methods := ExtractMethods(handler)

	if _, ok := methods["greet"]; !ok {
		t.Error("missing method greet")
	}
	if _, ok := methods["add"]; !ok {
		t.Error("missing method add")
	}
}

type allowlistHandler struct{}

func (h *allowlistHandler) Exposed() string     { return "exposed" }
func (h *allowlistHandler) AlsoExposed() string { return "also" }
func (h *allowlistHandler) Hidden() string      { return "hidden" }
func (h *allowlistHandler) RPCMethods() []string {
	return []string{"exposed", "alsoExposed"}
}

func TestExtractMethodsAllowlist(t *testing.T) {
	methods := ExtractMethods(&allowlistHandler{})

	if _, ok := methods["exposed"]; !ok {
		t.Error("allowlisted method 'exposed' missing")
	}
	if _, ok := methods["alsoExposed"]; !ok {
		t.Error("allowlisted method 'alsoExposed' missing")
	}
	if _, ok := methods["hidden"]; ok {
		t.Error("non-allowlisted method 'hidden' was exposed")
	}
	if _, ok := methods["rpcMethods"]; ok {
		t.Error("the RPCMethods marker itself must not be exposed")
	}
	if len(methods) != 2 {
		t.Errorf("expected exactly 2 exposed methods, got %d: %v", len(methods), methodNames(methods))
	}
}

func TestCallHandler(t *testing.T) {
	handler := &testHandler{DB: &testDBHandler{}}
	methods := ExtractMethods(handler)

	fn := methods["getSnapshot"]
	result, err := callHandler(fn, []any{"cam1"})
	if err != nil {
		t.Fatal(err)
	}
	if result != "snapshot:cam1" {
		t.Errorf("result = %v, want snapshot:cam1", result)
	}
}

func TestCallHandlerNested(t *testing.T) {
	handler := &testHandler{DB: &testDBHandler{}}
	methods := ExtractMethods(handler)

	fn := methods["db.find"]
	result, err := callHandler(fn, []any{"mykey"})
	if err != nil {
		t.Fatal(err)
	}
	if result != "found:mykey" {
		t.Errorf("result = %v, want found:mykey", result)
	}
}

func TestIsStreamRequest(t *testing.T) {
	tests := []struct {
		name   string
		params any
		want   bool
	}{
		{"map with __stream true", map[string]any{"__stream": true, "__streamSubject": "stream.test"}, true},
		{"map without __stream", map[string]any{"foo": "bar"}, false},
		{"StreamParams struct", StreamParams{Stream: true, StreamSubject: "stream.test"}, true},
		{"nil", nil, false},
		{"slice with stream map", []any{map[string]any{"__stream": true}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isStreamRequest(tt.params)
			if got != tt.want {
				t.Errorf("isStreamRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsPullIteratorRequest(t *testing.T) {
	tests := []struct {
		name   string
		params any
		want   bool
	}{
		{"map with __pullIterator true", map[string]any{"__pullIterator": true, "__iteratorId": "abc"}, true},
		{"map without __pullIterator", map[string]any{"foo": "bar"}, false},
		{"nil", nil, false},
		{"slice with pull map", []any{map[string]any{"__pullIterator": true}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isPullIteratorRequest(tt.params)
			if got != tt.want {
				t.Errorf("isPullIteratorRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func methodNames(methods map[string]reflect.Value) []string {
	names := make([]string, 0, len(methods))
	for name := range methods {
		names = append(names, name)
	}
	return names
}

func TestIsCallbackRequest(t *testing.T) {
	tests := []struct {
		name   string
		params any
		want   bool
	}{
		{"map with __callback true", map[string]any{"__callback": true, "__callbackSubject": "rpc.cb.test"}, true},
		{"map without __callback", map[string]any{"foo": "bar"}, false},
		{"CallbackParams struct", CallbackParams{Callback: true, CallbackSubject: "rpc.cb.test"}, true},
		{"nil", nil, false},
		{"slice with callback map", []any{map[string]any{"__callback": true}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCallbackRequest(tt.params)
			if got != tt.want {
				t.Errorf("isCallbackRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractCallbackParams(t *testing.T) {
	tests := []struct {
		name        string
		params      any
		wantSubject string
		wantArgs    []any
	}{
		{
			"map format",
			map[string]any{"__callback": true, "__callbackSubject": "rpc.cb.123", "args": []any{"hello"}},
			"rpc.cb.123",
			[]any{"hello"},
		},
		{
			"CallbackParams struct",
			CallbackParams{Callback: true, CallbackSubject: "rpc.cb.456", Args: []any{42}},
			"rpc.cb.456",
			[]any{42},
		},
		{
			"nil params",
			nil,
			"",
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject, args := extractCallbackParams(tt.params)
			if subject != tt.wantSubject {
				t.Errorf("extractCallbackParams() subject = %v, want %v", subject, tt.wantSubject)
			}
			if tt.wantArgs == nil && args != nil {
				t.Errorf("extractCallbackParams() args = %v, want nil", args)
			}
			if tt.wantArgs != nil && len(args) != len(tt.wantArgs) {
				t.Errorf("extractCallbackParams() args len = %d, want %d", len(args), len(tt.wantArgs))
			}
		})
	}
}

// Test handler with func(T) parameter for callback
type testCallbackHandler struct{}

type testEvent struct {
	Value string
}

func (h *testCallbackHandler) OnEvents(prefix string, callback func(testEvent)) (func(), error) {
	// Simulate: store callback, return cleanup
	callback(testEvent{Value: prefix + ":initial"})
	return func() { /* cleanup */ }, nil
}

func TestExtractMethodsWithCallback(t *testing.T) {
	handler := &testCallbackHandler{}
	methods := ExtractMethods(handler)

	if _, ok := methods["onEvents"]; !ok {
		t.Error("missing method onEvents")
	}
}

func TestCallbackHandlerParamDetection(t *testing.T) {
	handler := &testCallbackHandler{}
	methods := ExtractMethods(handler)
	fn := methods["onEvents"]

	// Verify the function has a func parameter
	fnType := fn.Type()
	hasFuncParam := false
	for i := 0; i < fnType.NumIn(); i++ {
		if fnType.In(i).Kind() == reflect.Func {
			hasFuncParam = true
			break
		}
	}
	if !hasFuncParam {
		t.Error("onEvents should have a func parameter")
	}
}
