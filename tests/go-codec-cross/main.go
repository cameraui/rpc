package main

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

type expectedEntry struct {
	name     string
	expected any
	typeOnly bool
}

func main() {
	entries := []expectedEntry{
		{name: "string", expected: "Hello World"},
		{name: "empty_string", expected: ""},
		{name: "number_int", expected: int64(42)},
		{name: "number_negative", expected: int64(-42)},
		{name: "number_zero", expected: int64(0)},
		{name: "number_large", expected: int64(math.MaxInt32)},
		{name: "float", expected: 3.14159},
		{name: "float_negative", expected: -3.14159},
		{name: "float_zero", expected: float64(0.0)},
		{name: "float_inf", expected: math.Inf(1)},
		{name: "float_neg_inf", expected: math.Inf(-1)},
		{name: "float_nan", expected: math.NaN()},
		{name: "boolean_true", expected: true},
		{name: "boolean_false", expected: false},
		{name: "null", expected: nil},

		{name: "empty_array", expected: []any{}},
		{name: "array", expected: []any{int64(1), int64(2), int64(3), int64(4), int64(5)}},
		{name: "mixed_array", expected: []any{"hello", int64(42), true, nil}},
		{name: "nested_array", expected: []any{
			[]any{int64(1), int64(2)},
			[]any{int64(3), int64(4)},
			[]any{int64(5), int64(6)},
		}},
		{name: "array_with_objects", expected: []any{
			map[string]any{"a": int64(1)},
			map[string]any{"b": int64(2)},
		}},
		{name: "tuple", expected: []any{int64(1), int64(2), int64(3)}},
		{name: "nested_tuple", expected: []any{[]any{int64(1), int64(2)}, []any{int64(3), int64(4)}}},

		{name: "empty_object", expected: map[string]any{}},
		{name: "simple_object", expected: map[string]any{"key": "value", "number": int64(123)}},
		{name: "nested_object", expected: map[string]any{
			"outer": map[string]any{"inner": map[string]any{"value": int64(42)}},
		}},
		{name: "object_mixed_keys", expected: map[string]any{
			"str": "text", "num": int64(42), "bool": true, "null": nil,
		}},

		{name: "datetime", expected: nil, typeOnly: true},
		{name: "date", expected: "", typeOnly: true},
		{name: "time", expected: "", typeOnly: true},
		{name: "timestamp", expected: float64(0), typeOnly: true},

		{name: "enum", expected: "INTERNAL_ERROR"},
		{name: "enum_in_dict", expected: map[string]any{"error": "TIMEOUT", "code": int64(408)}},

		{name: "complex", expected: nil, typeOnly: true},

		{name: "binary", expected: []byte("Hello binary world")},
		{name: "empty_binary", expected: []byte{}},
		{name: "binary_with_nulls", expected: []byte{0, 1, 2, 3, 4}},

		{name: "unicode", expected: "你好世界 🌍"},
		{name: "emoji", expected: "🎉🎊🎈🎁🎀"},
		{name: "special_chars", expected: "äöü ñ é à ß"},
		{name: "escape_chars", expected: "line1\nline2\ttab\r\nwindows"},
		{name: "quotes", expected: `He said "Hello" and she said 'Hi'`},

		{name: "very_long_string", expected: strings.Repeat("x", 10000)},
		{name: "deeply_nested", expected: map[string]any{
			"l1": map[string]any{"l2": map[string]any{"l3": map[string]any{"l4": map[string]any{"l5": map[string]any{"value": "deep"}}}}},
		}},
		{name: "large_array", expected: nil, typeOnly: true},

		{name: "max_safe_int", expected: int64(9007199254740991)},
		{name: "min_safe_int", expected: int64(-9007199254740991)},

		{name: "rpc_message", expected: map[string]any{
			"id":     "1234567890-abcdef",
			"method": "test.method",
			"params": []any{int64(1), "two", map[string]any{"three": int64(3)}},
			"error":  nil,
		}},
		{name: "stream_message", expected: map[string]any{
			"id":   "stream-123",
			"type": "data",
			"data": map[string]any{"chunk": int64(1), "total": int64(10)},
		}},

		{name: "js_timestamp", expected: int64(1708786800000)},
		{name: "date_ext", expected: nil, typeOnly: true}, // time.Time from ext
		{name: "camera_config", expected: map[string]any{
			"cameraId":     "cam-abc-123",
			"fps":          int64(30),
			"eventTimeout": int64(30),
			"timestamp":    int64(1708786800000),
			"confidence":   0.85,
			"enabled":      true,
			"name":         "Front Door",
		}},
		{name: "detection_event", expected: map[string]any{
			"type": "start",
			"data": map[string]any{
				"id":        "evt-abc-123",
				"state":     "active",
				"types":     []any{"motion", "audio"},
				"startTime": int64(1708786800000),
				"endTime":   int64(0),
				"triggers": []any{
					map[string]any{"type": "motion", "timestamp": int64(1708786800000), "data": map[string]any{"score": 0.95}},
					map[string]any{"type": "audio", "timestamp": int64(1708786800500), "data": map[string]any{"decibels": -25.5}},
				},
				"segments": []any{},
			},
		}},
		{name: "map_with_nil", expected: map[string]any{"key": "value", "optional": nil, "count": int64(0), "flag": false}},
		{name: "nested_ints", expected: map[string]any{
			"a": map[string]any{"b": map[string]any{"c": int64(42)}},
			"d": []any{int64(1), int64(2), int64(3)},
			"e": int64(100),
		}},
		{name: "empty_nested", expected: map[string]any{
			"a": map[string]any{},
			"b": []any{},
			"c": map[string]any{"d": map[string]any{}},
		}},
		{name: "sensor_list", expected: []any{
			map[string]any{"id": "sensor-1", "type": "motion", "online": true, "score": 0.95},
			map[string]any{"id": "sensor-2", "type": "audio", "online": false, "score": 0.0},
			map[string]any{"id": "sensor-3", "type": "object", "online": true, "score": 0.87},
		}},
		{name: "mixed_numerics", expected: nil, typeOnly: true}, // float_zero encoding varies
	}

	totalPassed := 0
	totalFailed := 0

	fmt.Println("Go cross-language decode: Python data")
	fmt.Println(strings.Repeat("=", 60))
	p, f := runCrossTest(entries, "/tmp/py-encoded-%s.msgpack")
	totalPassed += p
	totalFailed += f

	fmt.Println()

	fmt.Println("Go cross-language decode: Node.js data")
	fmt.Println(strings.Repeat("=", 60))
	p, f = runCrossTest(entries, "/tmp/node-encoded-%s.msgpack")
	totalPassed += p
	totalFailed += f

	fmt.Println()
	fmt.Printf("Total: %d passed, %d failed\n", totalPassed, totalFailed)

	if totalFailed > 0 {
		os.Exit(1)
	}
}

func runCrossTest(entries []expectedEntry, pathPattern string) (passed, failed int) {
	for _, e := range entries {
		filepath := fmt.Sprintf(pathPattern, e.name)

		data, err := os.ReadFile(filepath)
		if err != nil {
			fmt.Printf("  SKIP %s: file not found\n", e.name)
			continue
		}

		decoded, err := rpc.DecodeRaw(data)
		if err != nil {
			fmt.Printf("  FAIL %s: decode error - %v\n", e.name, err)
			failed++
			continue
		}

		success := false
		if e.typeOnly {
			success = checkTypeOnly(e.name, decoded)
		} else {
			success = crossCompare(e.expected, decoded, e.name)
		}

		if success {
			fmt.Printf("  OK %s\n", e.name)
			passed++
		} else {
			fmt.Printf("  FAIL %s\n", e.name)
			fmt.Printf("    Expected type: %T\n", e.expected)
			fmt.Printf("    Got type:      %T\n", decoded)
			fmt.Printf("    Got value:     %v\n", truncate(decoded))
			failed++
		}
	}
	return
}

func checkTypeOnly(name string, decoded any) bool {
	switch name {
	case "datetime":
		switch decoded.(type) {
		case time.Time:
			return true
		case string:
			return len(decoded.(string)) > 0
		}
		return false

	case "date":
		s, ok := decoded.(string)
		return ok && len(s) > 0

	case "time":
		s, ok := decoded.(string)
		return ok && len(s) > 0

	case "timestamp":
		_, ok := toFloat64(decoded)
		if ok {
			return true
		}
		_, ok = toInt64(decoded)
		return ok

	case "complex":
		m, ok := decoded.(map[string]any)
		if !ok {
			return false
		}
		for _, k := range []string{"id", "method", "params", "nested", "metadata"} {
			if _, exists := m[k]; !exists {
				return false
			}
		}
		if id, ok := m["id"].(string); !ok || id != "test-123" {
			return false
		}
		return true

	case "large_array":
		if arr, ok := decoded.([]any); ok {
			return len(arr) == 1000
		}
		return false

	case "date_ext":
		switch decoded.(type) {
		case time.Time:
			t := decoded.(time.Time)
			expected := time.Date(2024, 2, 24, 12, 0, 0, 0, time.UTC)
			return math.Abs(t.Sub(expected).Seconds()) < 2
		}
		return false

	case "mixed_numerics":
		m, ok := decoded.(map[string]any)
		if !ok {
			return false
		}
		if fv, ok := toFloat64(m["float_val"]); !ok || fv != 3.14 {
			return false
		}
		if iv, ok := toInt64(m["integer"]); !ok || iv != 42 {
			return false
		}
		return true
	}
	return false
}

func crossCompare(expected, actual any, testName string) bool {
	if expected == nil {
		return actual == nil
	}
	if actual == nil {
		return false
	}

	if testName == "float_nan" {
		if f, ok := toFloat64(actual); ok {
			return math.IsNaN(f)
		}
		return false
	}
	if testName == "float_inf" {
		if f, ok := toFloat64(actual); ok {
			return math.IsInf(f, 1)
		}
		return false
	}
	if testName == "float_neg_inf" {
		if f, ok := toFloat64(actual); ok {
			return math.IsInf(f, -1)
		}
		return false
	}
	if testName == "float_zero" {
		if f, ok := toFloat64(actual); ok {
			return f == 0.0
		}
		if i, ok := toInt64(actual); ok {
			return i == 0
		}
		return false
	}

	// Binary — DecodeRaw with UseLooseInterfaceDecoding decodes msgpack bin as string
	if eb, ok := expected.([]byte); ok {
		if ab, ok := actual.([]byte); ok {
			if len(eb) == 0 && len(ab) == 0 {
				return true
			}
			return reflect.DeepEqual(eb, ab)
		}
		if as, ok := actual.(string); ok {
			return as == string(eb)
		}
		if len(eb) == 0 && actual == nil {
			return true
		}
		return false
	}

	if isNumeric(expected) && isNumeric(actual) {
		return compareNumeric(expected, actual)
	}

	if isSlice(expected) && isSlice(actual) {
		return compareSlices(expected, actual)
	}

	if isMap(expected) && isMap(actual) {
		return compareMaps(expected, actual)
	}

	return reflect.DeepEqual(expected, actual)
}

func compareSlices(expected, actual any) bool {
	ev := reflect.ValueOf(expected)
	av := reflect.ValueOf(actual)
	if ev.Len() != av.Len() {
		return false
	}
	for i := 0; i < ev.Len(); i++ {
		if !crossCompare(ev.Index(i).Interface(), av.Index(i).Interface(), "") {
			return false
		}
	}
	return true
}

func compareMaps(expected, actual any) bool {
	ev := reflect.ValueOf(expected)
	av := reflect.ValueOf(actual)
	if ev.Len() != av.Len() {
		return false
	}
	for _, key := range ev.MapKeys() {
		avVal := av.MapIndex(key)
		if !avVal.IsValid() {
			return false
		}
		if !crossCompare(ev.MapIndex(key).Interface(), avVal.Interface(), "") {
			return false
		}
	}
	return true
}

func compareNumeric(a, b any) bool {
	ai, aIsInt := toInt64(a)
	bi, bIsInt := toInt64(b)
	if aIsInt && bIsInt {
		return ai == bi
	}
	af, aIsFloat := toFloat64(a)
	bf, bIsFloat := toFloat64(b)
	if aIsFloat && bIsFloat {
		if math.IsNaN(af) && math.IsNaN(bf) {
			return true
		}
		if math.IsInf(af, 1) && math.IsInf(bf, 1) {
			return true
		}
		if math.IsInf(af, -1) && math.IsInf(bf, -1) {
			return true
		}
		return af == bf
	}
	return false
}

func isNumeric(v any) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	}
	return false
}

func isSlice(v any) bool {
	if v == nil {
		return false
	}
	return reflect.TypeOf(v).Kind() == reflect.Slice
}

func isMap(v any) bool {
	if v == nil {
		return false
	}
	return reflect.TypeOf(v).Kind() == reflect.Map
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n <= uint64(math.MaxInt64) {
			return int64(n), true
		}
		return 0, false
	case float64:
		if n == math.Trunc(n) && !math.IsInf(n, 0) && !math.IsNaN(n) {
			return int64(n), true
		}
		return 0, false
	case float32:
		f := float64(n)
		if f == math.Trunc(f) && !math.IsInf(f, 0) && !math.IsNaN(f) {
			return int64(n), true
		}
		return 0, false
	}
	return 0, false
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	}
	return 0, false
}

func truncate(v any) string {
	s := fmt.Sprintf("%v", v)
	if len(s) > 100 {
		return s[:100] + "..."
	}
	return s
}
