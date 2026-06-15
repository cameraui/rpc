package rpc

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

func encodeDecodeRaw(t *testing.T, v any) any {
	t.Helper()
	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode(%v): %v", v, err)
	}
	decoded, err := DecodeRaw(encoded)
	if err != nil {
		t.Fatalf("DecodeRaw: %v", err)
	}
	return decoded
}

func TestEncodeDecodeStrings(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"ascii", "hello world"},
		{"unicode", "你好世界 🌍"},
		{"emoji", "🎉🎊"},
		{"control chars", "line1\nline2\ttab"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeDecodeRaw(t, tt.in)
			s, ok := got.(string)
			if !ok {
				t.Fatalf("decoded type = %T, want string", got)
			}
			if s != tt.in {
				t.Errorf("round-trip = %q, want %q", s, tt.in)
			}
		})
	}
}

func TestEncodeDecodeIntegers(t *testing.T) {
	tests := []struct {
		name string
		in   int64
	}{
		{"zero", 0},
		{"positive", 42},
		{"negative", -12345},
		{"max int64", math.MaxInt64},
		{"min int64", math.MinInt64},
		{"js max safe integer", 9007199254740991},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeDecodeRaw(t, tt.in)
			n, ok := got.(int64)
			if !ok {
				if u, uok := got.(uint64); uok && tt.in >= 0 && uint64(tt.in) == u {
					return
				}
				t.Fatalf("decoded type = %T (%v), want int64", got, got)
			}
			if n != tt.in {
				t.Errorf("round-trip = %d, want %d", n, tt.in)
			}
		})
	}
}

func TestEncodeDecodeFloats(t *testing.T) {
	tests := []struct {
		name string
		in   float64
	}{
		{"pi", 3.14159},
		{"small negative", -0.0001},
		{"large", 1.7976931348623157e308},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeDecodeRaw(t, tt.in)
			f, ok := got.(float64)
			if !ok {
				t.Fatalf("decoded type = %T, want float64", got)
			}
			if math.Abs(f-tt.in) > 1e-9*math.Abs(tt.in)+1e-12 {
				t.Errorf("round-trip = %v, want %v", f, tt.in)
			}
		})
	}
}

func TestEncodeDecodeBool(t *testing.T) {
	for _, in := range []bool{true, false} {
		got := encodeDecodeRaw(t, in)
		b, ok := got.(bool)
		if !ok {
			t.Fatalf("decoded type = %T, want bool", got)
		}
		if b != in {
			t.Errorf("round-trip = %v, want %v", b, in)
		}
	}
}

func TestEncodeDecodeNil(t *testing.T) {
	encoded, err := Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRaw(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != nil {
		t.Errorf("round-trip = %v, want nil", decoded)
	}
}

func TestEncodeDecodeBytes(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		{"empty", []byte{}},
		{"edge values", []byte{0, 1, 2, 254, 255}},
		{"large", bytes.Repeat([]byte{0xAB}, 10000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := Encode(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			var decoded []byte
			if err := Decode(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decoded, tt.in) {
				t.Errorf("round-trip = %v, want %v", decoded, tt.in)
			}
		})
	}
}

func TestEncodeDecodeEmptyArray(t *testing.T) {
	encoded, err := Encode([]any{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRaw(encoded)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := decoded.([]any)
	if !ok {
		t.Fatalf("decoded type = %T, want []any", decoded)
	}
	if len(s) != 0 {
		t.Errorf("len = %d, want 0", len(s))
	}
}

func TestEncodeDecodeFlatArray(t *testing.T) {
	original := []any{int64(1), int64(2), int64(3), "a", true, nil}
	decoded := encodeDecodeRaw(t, original)
	s, ok := decoded.([]any)
	if !ok {
		t.Fatalf("decoded type = %T, want []any", decoded)
	}
	if !reflect.DeepEqual(s, original) {
		t.Errorf("round-trip = %v, want %v", s, original)
	}
}

func TestEncodeDecodeEmptyMap(t *testing.T) {
	encoded, err := Encode(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeToMap(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 0 {
		t.Errorf("len = %d, want 0", len(decoded))
	}
}

func TestEncodeDecodeNestedMap(t *testing.T) {
	original := map[string]any{
		"name": "service",
		"meta": map[string]any{
			"version": int64(2),
			"tags":    []any{"x", "y"},
			"nested":  map[string]any{"deep": true},
		},
		"items": []any{
			map[string]any{"id": int64(1)},
			map[string]any{"id": int64(2)},
		},
	}

	encoded, err := Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeToMap(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, original) {
		t.Errorf("round-trip = %#v, want %#v", decoded, original)
	}
}

func TestEncodeDecodeMixedPayload(t *testing.T) {
	msg := RPCMessage{
		ID:     "12345-abcde",
		Method: "rpc.app.doThing",
		Params: []any{int64(1), int64(-2), 3.5, "text", "你好", true, nil, []any{int64(4), int64(5)}, map[string]any{"k": "v"}},
	}

	encoded, err := Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	var decoded RPCMessage
	if err := Decode(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ID != msg.ID || decoded.Method != msg.Method {
		t.Errorf("header mismatch: %+v", decoded)
	}
	params, ok := decoded.Params.([]any)
	if !ok {
		t.Fatalf("Params type = %T, want []any", decoded.Params)
	}
	if len(params) != 9 {
		t.Fatalf("Params len = %d, want 9", len(params))
	}
	if params[3] != "text" || params[4] != "你好" || params[5] != true || params[6] != nil {
		t.Errorf("params mismatch: %v", params)
	}
}

func TestDecodeToMapOnNonMap(t *testing.T) {
	encoded, err := Encode("not a map")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeToMap(encoded); err == nil {
		t.Error("expected error decoding string into map")
	}
}

func TestDecodeRawError(t *testing.T) {
	if _, err := DecodeRaw([]byte{0xc1}); err == nil {
		t.Error("expected error decoding invalid msgpack code")
	}
}

func TestNormalizeUndefined(t *testing.T) {
	nested := map[string]any{
		"a": &msgpackrUndefined{},
		"b": "keep",
		"c": []any{msgpackrUndefined{}, int64(1)},
		"d": map[string]any{"inner": &msgpackrUndefined{}},
	}

	out := NormalizeUndefined(nested).(map[string]any)
	if out["a"] != nil {
		t.Errorf("a = %v, want nil", out["a"])
	}
	if out["b"] != "keep" {
		t.Errorf("b = %v, want keep", out["b"])
	}
	list := out["c"].([]any)
	if list[0] != nil || list[1] != int64(1) {
		t.Errorf("c = %v, want [nil 1]", list)
	}
	inner := out["d"].(map[string]any)
	if inner["inner"] != nil {
		t.Errorf("d.inner = %v, want nil", inner["inner"])
	}
}

func TestNormalizeUndefinedPassthrough(t *testing.T) {
	if got := NormalizeUndefined("plain"); got != "plain" {
		t.Errorf("NormalizeUndefined(plain) = %v", got)
	}
	if got := NormalizeUndefined(int64(7)); got != int64(7) {
		t.Errorf("NormalizeUndefined(7) = %v", got)
	}
}
