package rpc

import (
	"errors"
	"reflect"
	"testing"
)

func TestCoerceValueNumeric(t *testing.T) {
	tests := []struct {
		name   string
		val    any
		target reflect.Type
		want   any
	}{
		{"int64 to int", int64(5), reflect.TypeFor[int](), 5},
		{"int64 to float64", int64(5), reflect.TypeFor[float64](), float64(5)},
		{"float64 to int", float64(7), reflect.TypeFor[int](), 7},
		{"float64 to float32", float64(1.5), reflect.TypeFor[float32](), float32(1.5)},
		{"int64 to uint", int64(9), reflect.TypeFor[uint](), uint(9)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := coerceValue(tt.val, tt.target).Interface()
			if got != tt.want {
				t.Errorf("coerceValue = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestCoerceValueNil(t *testing.T) {
	got := coerceValue(nil, reflect.TypeFor[string]())
	if got.Interface() != "" {
		t.Errorf("coerceValue(nil) = %v, want zero string", got.Interface())
	}
}

func TestCoerceValueDirectAssign(t *testing.T) {
	got := coerceValue("hello", reflect.TypeFor[string]())
	if got.Interface() != "hello" {
		t.Errorf("coerceValue = %v, want hello", got.Interface())
	}
}

func TestCoerceValueSliceOfFloats(t *testing.T) {
	in := []any{int64(1), int64(2), int64(3)}
	got := coerceValue(in, reflect.TypeFor[[]float64]())
	out, ok := got.Interface().([]float64)
	if !ok {
		t.Fatalf("type = %T, want []float64", got.Interface())
	}
	if !reflect.DeepEqual(out, []float64{1, 2, 3}) {
		t.Errorf("got %v, want [1 2 3]", out)
	}
}

func TestCoerceValueMapToStruct(t *testing.T) {
	type point struct {
		X int `msgpack:"x"`
		Y int `msgpack:"y"`
	}
	in := map[string]any{"x": int64(3), "y": int64(4)}
	got := coerceValue(in, reflect.TypeFor[point]())
	out, ok := got.Interface().(point)
	if !ok {
		t.Fatalf("type = %T, want point", got.Interface())
	}
	if out.X != 3 || out.Y != 4 {
		t.Errorf("got %+v, want {3 4}", out)
	}
}

func TestIsNumericKind(t *testing.T) {
	numeric := []reflect.Kind{
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
	}
	for _, k := range numeric {
		if !isNumericKind(k) {
			t.Errorf("isNumericKind(%v) = false, want true", k)
		}
	}
	for _, k := range []reflect.Kind{reflect.String, reflect.Bool, reflect.Slice, reflect.Map, reflect.Struct} {
		if isNumericKind(k) {
			t.Errorf("isNumericKind(%v) = true, want false", k)
		}
	}
}

type helperHandler struct{}

func (h *helperHandler) Sum(nums ...int) int {
	s := 0
	for _, n := range nums {
		s += n
	}
	return s
}

func (h *helperHandler) Multi() (s string, n int, err error) {
	return "x", 7, nil
}

func (h *helperHandler) NoReturn(name string) {}

func (h *helperHandler) Fail() error {
	return errors.New("boom")
}

func (h *helperHandler) Echo(s string) string {
	return s
}

func TestCallHandlerVariadic(t *testing.T) {
	methods := ExtractMethods(&helperHandler{})
	got, err := callHandler(methods["sum"], []any{int64(1), int64(2), int64(3), int64(4)})
	if err != nil {
		t.Fatal(err)
	}
	if got != 10 {
		t.Errorf("sum = %v, want 10", got)
	}
}

func TestCallHandlerMultiReturn(t *testing.T) {
	methods := ExtractMethods(&helperHandler{})
	got, err := callHandler(methods["multi"], nil)
	if err != nil {
		t.Fatal(err)
	}
	vals, ok := got.([]any)
	if !ok {
		t.Fatalf("type = %T, want []any", got)
	}
	if len(vals) != 2 || vals[0] != "x" || vals[1] != 7 {
		t.Errorf("multi = %v, want [x 7]", vals)
	}
}

func TestCallHandlerNoReturn(t *testing.T) {
	methods := ExtractMethods(&helperHandler{})
	got, err := callHandler(methods["noReturn"], []any{"name"})
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("noReturn result = %v, want nil", got)
	}
}

func TestCallHandlerErrorReturn(t *testing.T) {
	methods := ExtractMethods(&helperHandler{})
	_, err := callHandler(methods["fail"], nil)
	if err == nil || err.Error() != "boom" {
		t.Errorf("err = %v, want boom", err)
	}
}

func TestCallHandlerScalarParam(t *testing.T) {
	methods := ExtractMethods(&helperHandler{})
	// Scalar (non-slice) params are wrapped into a single-arg call.
	got, err := callHandler(methods["echo"], "direct")
	if err != nil {
		t.Fatal(err)
	}
	if got != "direct" {
		t.Errorf("echo = %v, want direct", got)
	}
}

func TestCallHandlerMissingArgsZeroFilled(t *testing.T) {
	methods := ExtractMethods(&helperHandler{})
	got, err := callHandler(methods["echo"], []any{})
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("echo = %v, want empty string", got)
	}
}

type propHandler struct {
	Count int `rpc_prop:"count"`
}

func TestExtractMethodsRPCProp(t *testing.T) {
	h := &propHandler{Count: 5}
	methods := ExtractMethods(h)

	if _, ok := methods["count"]; !ok {
		t.Error("missing getter count")
	}
	if _, ok := methods["setCount"]; !ok {
		t.Error("missing setter setCount")
	}

	got, err := callHandler(methods["count"], nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Errorf("getter = %v, want 5", got)
	}

	if _, err := callHandler(methods["setCount"], []any{int64(9)}); err != nil {
		t.Fatal(err)
	}
	if h.Count != 9 {
		t.Errorf("after setCount = %d, want 9", h.Count)
	}
}

func TestExtractMethodsMapNonFuncValue(t *testing.T) {
	handler := map[string]any{
		"version": "1.0.0",
		"greet":   func(name string) string { return "hi " + name },
		"_hidden": func() {},
	}
	methods := ExtractMethods(handler)

	if _, ok := methods["_hidden"]; ok {
		t.Error("underscore-prefixed key should be skipped")
	}

	got, err := callHandler(methods["version"], nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "1.0.0" {
		t.Errorf("version getter = %v, want 1.0.0", got)
	}
}

func TestExtractMethodsNilPointer(t *testing.T) {
	var h *helperHandler
	methods := ExtractMethods(h)
	if len(methods) != 0 {
		t.Errorf("nil pointer handler produced %d methods", len(methods))
	}
}

func TestExtractStreamParams(t *testing.T) {
	t.Run("map", func(t *testing.T) {
		subj, args := extractStreamParams(map[string]any{
			"__streamSubject": "stream.1",
			"args":            []any{"a", int64(2)},
		})
		if subj != "stream.1" {
			t.Errorf("subject = %v, want stream.1", subj)
		}
		if len(args) != 2 {
			t.Errorf("args len = %d, want 2", len(args))
		}
	})

	t.Run("struct", func(t *testing.T) {
		subj, args := extractStreamParams(StreamParams{
			Stream: true, StreamSubject: "stream.2", Args: []any{"x"},
		})
		if subj != "stream.2" || len(args) != 1 {
			t.Errorf("subject=%v args=%v", subj, args)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		subj, args := extractStreamParams("nope")
		if subj != "" || args != nil {
			t.Errorf("expected empty, got %q %v", subj, args)
		}
	})
}

func TestExtractPullIteratorParams(t *testing.T) {
	t.Run("explicit id", func(t *testing.T) {
		id, args := extractPullIteratorParams(map[string]any{
			"__pullIterator": true, "__iteratorId": "it1", "args": []any{int64(1)},
		}, "def")
		if id != "it1" || len(args) != 1 {
			t.Errorf("id=%v args=%v", id, args)
		}
	})

	t.Run("default id", func(t *testing.T) {
		id, _ := extractPullIteratorParams(map[string]any{"__pullIterator": true}, "def")
		if id != "def" {
			t.Errorf("id = %v, want def", id)
		}
	})

	t.Run("struct", func(t *testing.T) {
		id, args := extractPullIteratorParams(PullIteratorParams{
			PullIterator: true, IteratorID: "", Args: []any{"a"},
		}, "fallback")
		if id != "fallback" || len(args) != 1 {
			t.Errorf("id=%v args=%v", id, args)
		}
	})

	t.Run("slice wrapped", func(t *testing.T) {
		id, _ := extractPullIteratorParams([]any{
			map[string]any{"__pullIterator": true, "__iteratorId": "wrapped"},
		}, "def")
		if id != "wrapped" {
			t.Errorf("id = %v, want wrapped", id)
		}
	})
}

func TestExtractPullCallbackParams(t *testing.T) {
	t.Run("map", func(t *testing.T) {
		id, subj, oneway, args := extractPullCallbackParams(map[string]any{
			"__pullCallback":    true,
			"__iteratorId":      "it1",
			"__callbackSubject": "cb.1",
			"__onewayMethods":   []any{"onData", "onEnd"},
			"args":              []any{int64(1)},
		}, "def")
		if id != "it1" || subj != "cb.1" {
			t.Errorf("id=%v subj=%v", id, subj)
		}
		if len(oneway) != 2 || oneway[0] != "onData" {
			t.Errorf("oneway = %v", oneway)
		}
		if len(args) != 1 {
			t.Errorf("args len = %d", len(args))
		}
	})

	t.Run("struct", func(t *testing.T) {
		id, subj, oneway, _ := extractPullCallbackParams(PullCallbackParams{
			PullCallback:    true,
			IteratorID:      "",
			CallbackSubject: "cb.2",
			OnewayMethods:   []string{"x"},
		}, "fallback")
		if id != "fallback" || subj != "cb.2" || len(oneway) != 1 {
			t.Errorf("id=%v subj=%v oneway=%v", id, subj, oneway)
		}
	})

	t.Run("non-pull-callback", func(t *testing.T) {
		id, subj, oneway, args := extractPullCallbackParams("nope", "def")
		if id != "def" || subj != "" || oneway != nil || args != nil {
			t.Errorf("unexpected: %v %v %v %v", id, subj, oneway, args)
		}
	})
}

func TestIsPullCallbackRequest(t *testing.T) {
	tests := []struct {
		name   string
		params any
		want   bool
	}{
		{"map true", map[string]any{"__pullCallback": true}, true},
		{"map false", map[string]any{"__pullCallback": false}, false},
		{"struct", PullCallbackParams{PullCallback: true}, true},
		{"slice wrapped", []any{map[string]any{"__pullCallback": true}}, true},
		{"nil", nil, false},
		{"unrelated map", map[string]any{"foo": "bar"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPullCallbackRequest(tt.params); got != tt.want {
				t.Errorf("isPullCallbackRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToCamelCaseUnicode(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"Über", "über"},
		{"X", "x"},
		{"已Done", "已Done"},
	}
	for _, tt := range tests {
		if got := toCamelCase(tt.input); got != tt.want {
			t.Errorf("toCamelCase(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
