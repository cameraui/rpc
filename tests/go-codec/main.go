package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"time"

	rpc "github.com/cameraui/rpc/go"
	"github.com/vmihailenco/msgpack/v5"
)

type testCase struct {
	name string
	data any
}

func main() {
	now := time.Now()

	largeArray := make([]any, 1000)
	for i := 0; i < 1000; i++ {
		largeArray[i] = i
	}

	testCases := []testCase{
		{name: "string", data: "Hello World"},
		{name: "empty_string", data: ""},
		{name: "number_int", data: 42},
		{name: "number_negative", data: -42},
		{name: "number_zero", data: 0},
		{name: "number_large", data: int(math.MaxInt32)},
		{name: "float", data: 3.14159},
		{name: "float_negative", data: -3.14159},
		{name: "float_zero", data: float64(0.0)},
		{name: "float_inf", data: math.Inf(1)},
		{name: "float_neg_inf", data: math.Inf(-1)},
		{name: "float_nan", data: math.NaN()},
		{name: "boolean_true", data: true},
		{name: "boolean_false", data: false},
		{name: "null", data: nil},

		{name: "empty_array", data: []any{}},
		{name: "array", data: []any{1, 2, 3, 4, 5}},
		{name: "mixed_array", data: []any{"hello", 42, true, nil}},
		{name: "nested_array", data: []any{[]any{1, 2}, []any{3, 4}, []any{5, 6}}},
		{name: "array_with_objects", data: []any{map[string]any{"a": 1}, map[string]any{"b": 2}}},
		{name: "tuple", data: []any{1, 2, 3}},
		{name: "nested_tuple", data: []any{[]any{1, 2}, []any{3, 4}}},

		{name: "empty_object", data: map[string]any{}},
		{name: "simple_object", data: map[string]any{"key": "value", "number": 123}},
		{name: "nested_object", data: map[string]any{"outer": map[string]any{"inner": map[string]any{"value": 42}}}},
		{name: "object_mixed_keys", data: map[string]any{"str": "text", "num": 42, "bool": true, "null": nil}},

		{name: "datetime", data: now},
		{name: "date", data: now.Format("2006-01-02")},
		{name: "time", data: now.Format("15:04:05")},
		{name: "timestamp", data: float64(now.Unix()) + float64(now.Nanosecond())/1e9},

		{name: "enum", data: "INTERNAL_ERROR"},
		{name: "enum_in_dict", data: map[string]any{"error": "TIMEOUT", "code": 408}},

		{name: "complex", data: map[string]any{
			"id":        "test-123",
			"method":    "greet",
			"params":    []any{"Python"},
			"nested":    map[string]any{"foo": "bar", "baz": []any{1, 2, 3}},
			"timestamp": now,
			"metadata": map[string]any{
				"version":  1.0,
				"features": []any{"streaming", "chunking"},
				"limits":   map[string]any{"max_size": 10485760, "timeout": 30000},
			},
		}},

		{name: "binary", data: []byte("Hello binary world")},
		{name: "empty_binary", data: []byte{}},
		{name: "binary_with_nulls", data: []byte{0, 1, 2, 3, 4}},

		{name: "unicode", data: "你好世界 🌍"},
		{name: "emoji", data: "🎉🎊🎈🎁🎀"},
		{name: "special_chars", data: "äöü ñ é à ß"},
		{name: "escape_chars", data: "line1\nline2\ttab\r\nwindows"},
		{name: "quotes", data: `He said "Hello" and she said 'Hi'`},

		{name: "very_long_string", data: strings.Repeat("x", 10000)},
		{name: "deeply_nested", data: map[string]any{
			"l1": map[string]any{"l2": map[string]any{"l3": map[string]any{"l4": map[string]any{"l5": map[string]any{"value": "deep"}}}}},
		}},
		{name: "large_array", data: largeArray},

		{name: "max_safe_int", data: int64(9007199254740991)},
		{name: "min_safe_int", data: int64(-9007199254740991)},

		{name: "rpc_message", data: map[string]any{
			"id":     "1234567890-abcdef",
			"method": "test.method",
			"params": []any{1, "two", map[string]any{"three": 3}},
			"error":  nil,
		}},
		{name: "stream_message", data: map[string]any{
			"id":   "stream-123",
			"type": "data",
			"data": map[string]any{"chunk": 1, "total": 10},
		}},

		{name: "js_timestamp", data: int64(1708786800000)},
		{name: "date_ext", data: time.Date(2024, 2, 24, 12, 0, 0, 0, time.UTC)},
		{name: "camera_config", data: map[string]any{
			"cameraId":     "cam-abc-123",
			"fps":          30,
			"eventTimeout": 30,
			"timestamp":    int64(1708786800000),
			"confidence":   0.85,
			"enabled":      true,
			"name":         "Front Door",
		}},
		{name: "detection_event", data: map[string]any{
			"type": "start",
			"data": map[string]any{
				"id":        "evt-abc-123",
				"state":     "active",
				"types":     []any{"motion", "audio"},
				"startTime": int64(1708786800000),
				"endTime":   0,
				"triggers": []any{
					map[string]any{"type": "motion", "timestamp": int64(1708786800000), "data": map[string]any{"score": 0.95}},
					map[string]any{"type": "audio", "timestamp": int64(1708786800500), "data": map[string]any{"decibels": -25.5}},
				},
				"segments": []any{},
			},
		}},
		{name: "map_with_nil", data: map[string]any{"key": "value", "optional": nil, "count": 0, "flag": false}},
		{name: "nested_ints", data: map[string]any{"a": map[string]any{"b": map[string]any{"c": 42}}, "d": []any{1, 2, 3}, "e": 100}},
		{name: "empty_nested", data: map[string]any{"a": map[string]any{}, "b": []any{}, "c": map[string]any{"d": map[string]any{}}}},
		{name: "sensor_list", data: []any{
			map[string]any{"id": "sensor-1", "type": "motion", "online": true, "score": 0.95},
			map[string]any{"id": "sensor-2", "type": "audio", "online": false, "score": 0.0},
			map[string]any{"id": "sensor-3", "type": "object", "online": true, "score": 0.87},
		}},
		{name: "mixed_numerics", data: map[string]any{"integer": 42, "float_val": 3.14, "zero_int": 0, "zero_float": 0.0, "negative": -10, "neg_float": -2.5}},
	}

	fmt.Println("Go codec test")
	fmt.Println()

	fmt.Println("Phase 1: Encoding + roundtrip...")

	passed := 0
	failed := 0

	for _, tc := range testCases {
		encoded, err := rpc.Encode(tc.data)
		if err != nil {
			fmt.Printf("  FAIL %s: encode error - %v\n", tc.name, err)
			failed++
			continue
		}

		err = os.WriteFile(fmt.Sprintf("/tmp/go-encoded-%s.msgpack", tc.name), encoded, 0644)
		if err != nil {
			fmt.Printf("  FAIL %s: write error - %v\n", tc.name, err)
			failed++
			continue
		}

		decoded, err := rpc.DecodeRaw(encoded)
		if err != nil {
			fmt.Printf("  FAIL %s: decode error - %v\n", tc.name, err)
			failed++
			continue
		}

		if compareValues(tc.data, decoded, tc.name) {
			fmt.Printf("  OK %s (%d bytes)\n", tc.name, len(encoded))
			passed++
		} else {
			fmt.Printf("  FAIL %s: roundtrip mismatch\n", tc.name)
			fmt.Printf("    Expected: %v (%T)\n", truncate(tc.data), tc.data)
			fmt.Printf("    Got:      %v (%T)\n", truncate(decoded), decoded)
			failed++
		}
	}

	fmt.Println()
	fmt.Printf("Roundtrip results: %d passed, %d failed\n", passed, failed)

	fmt.Println()
	fmt.Println("Phase 2: Struct decode (float->int coercion)...")

	structPassed, structFailed := testStructDecode()

	fmt.Println()
	fmt.Printf("Struct decode results: %d passed, %d failed\n", structPassed, structFailed)

	totalFailed := failed + structFailed
	if totalFailed > 0 {
		os.Exit(1)
	}
}

// Tests rpc.Decode() into typed structs, including the float→int coercion
// fallback that handles JavaScript's IEEE 754 number encoding.

type CameraConfig struct {
	CameraID     string  `msgpack:"cameraId"`
	FPS          int     `msgpack:"fps"`
	EventTimeout int     `msgpack:"eventTimeout"`
	Timestamp    int64   `msgpack:"timestamp"`
	Confidence   float64 `msgpack:"confidence"`
	Enabled      bool    `msgpack:"enabled"`
	Name         string  `msgpack:"name"`
}

type DetectionTrigger struct {
	Type      string         `msgpack:"type"`
	Timestamp int64          `msgpack:"timestamp"`
	Data      map[string]any `msgpack:"data"`
}

type DetectionEventData struct {
	ID        string             `msgpack:"id"`
	State     string             `msgpack:"state"`
	Types     []string           `msgpack:"types"`
	StartTime int64              `msgpack:"startTime"`
	EndTime   int64              `msgpack:"endTime"`
	Triggers  []DetectionTrigger `msgpack:"triggers"`
	Segments  []any              `msgpack:"segments"`
}

type DetectionEvent struct {
	Type string             `msgpack:"type"`
	Data DetectionEventData `msgpack:"data"`
}

type SimpleStruct struct {
	Count int     `msgpack:"count"`
	Size  int64   `msgpack:"size"`
	Score float64 `msgpack:"score"`
	Name  string  `msgpack:"name"`
}

func testStructDecode() (passed, failed int) {
	{
		testName := "float64_to_int_coercion"
		data := map[string]any{
			"count": float64(42),
			"size":  float64(1708786800000),
			"score": float64(0.95),
			"name":  "test",
		}
		encoded, err := msgpack.Marshal(data)
		if err != nil {
			fmt.Printf("  FAIL %s: encode error - %v\n", testName, err)
			failed++
		} else {
			var s SimpleStruct
			err = rpc.Decode(encoded, &s)
			if err != nil {
				fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
				failed++
			} else if s.Count != 42 || s.Size != 1708786800000 || s.Score != 0.95 || s.Name != "test" {
				fmt.Printf("  FAIL %s: wrong values - got %+v\n", testName, s)
				failed++
			} else {
				fmt.Printf("  OK %s\n", testName)
				passed++
			}
		}
	}

	{
		testName := "camera_config_ints"
		data := map[string]any{
			"cameraId":     "cam-abc-123",
			"fps":          30,
			"eventTimeout": 30,
			"timestamp":    int64(1708786800000),
			"confidence":   0.85,
			"enabled":      true,
			"name":         "Front Door",
		}
		encoded, err := rpc.Encode(data)
		if err != nil {
			fmt.Printf("  FAIL %s: encode error - %v\n", testName, err)
			failed++
		} else {
			var cfg CameraConfig
			err = rpc.Decode(encoded, &cfg)
			if err != nil {
				fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
				failed++
			} else if cfg.CameraID != "cam-abc-123" || cfg.FPS != 30 || cfg.Timestamp != 1708786800000 || cfg.Confidence != 0.85 {
				fmt.Printf("  FAIL %s: wrong values - got %+v\n", testName, cfg)
				failed++
			} else {
				fmt.Printf("  OK %s\n", testName)
				passed++
			}
		}
	}

	{
		testName := "camera_config_floats"
		data := map[string]any{
			"cameraId":     "cam-abc-123",
			"fps":          float64(30),
			"eventTimeout": float64(30),
			"timestamp":    float64(1708786800000),
			"confidence":   float64(0.85),
			"enabled":      true,
			"name":         "Front Door",
		}
		encoded, err := msgpack.Marshal(data)
		if err != nil {
			fmt.Printf("  FAIL %s: encode error - %v\n", testName, err)
			failed++
		} else {
			var cfg CameraConfig
			err = rpc.Decode(encoded, &cfg)
			if err != nil {
				fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
				failed++
			} else if cfg.FPS != 30 || cfg.EventTimeout != 30 || cfg.Timestamp != 1708786800000 {
				fmt.Printf("  FAIL %s: wrong values - got %+v\n", testName, cfg)
				failed++
			} else {
				fmt.Printf("  OK %s\n", testName)
				passed++
			}
		}
	}

	{
		testName := "detection_event_floats"
		data := map[string]any{
			"type": "start",
			"data": map[string]any{
				"id":        "evt-abc-123",
				"state":     "active",
				"types":     []any{"motion", "audio"},
				"startTime": float64(1708786800000),
				"endTime":   float64(0),
				"triggers": []any{
					map[string]any{
						"type":      "motion",
						"timestamp": float64(1708786800000),
						"data":      map[string]any{"score": float64(0.95)},
					},
				},
				"segments": []any{},
			},
		}
		encoded, err := msgpack.Marshal(data)
		if err != nil {
			fmt.Printf("  FAIL %s: encode error - %v\n", testName, err)
			failed++
		} else {
			var evt DetectionEvent
			err = rpc.Decode(encoded, &evt)
			if err != nil {
				fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
				failed++
			} else if evt.Type != "start" || evt.Data.ID != "evt-abc-123" || evt.Data.StartTime != 1708786800000 {
				fmt.Printf("  FAIL %s: wrong values - got %+v\n", testName, evt)
				failed++
			} else if len(evt.Data.Triggers) != 1 || evt.Data.Triggers[0].Timestamp != 1708786800000 {
				fmt.Printf("  FAIL %s: wrong trigger values - got %+v\n", testName, evt.Data.Triggers)
				failed++
			} else {
				fmt.Printf("  OK %s\n", testName)
				passed++
			}
		}
	}

	{
		testName := "cross_node_camera_config"
		data, err := os.ReadFile("/tmp/node-encoded-camera_config.msgpack")
		if err != nil {
			fmt.Printf("  SKIP %s: file not available\n", testName)
		} else {
			var cfg CameraConfig
			err = rpc.Decode(data, &cfg)
			if err != nil {
				fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
				failed++
			} else if cfg.CameraID != "cam-abc-123" || cfg.FPS != 30 || cfg.Timestamp != 1708786800000 {
				fmt.Printf("  FAIL %s: wrong values - got %+v\n", testName, cfg)
				failed++
			} else {
				fmt.Printf("  OK %s\n", testName)
				passed++
			}
		}
	}

	{
		testName := "cross_py_camera_config"
		data, err := os.ReadFile("/tmp/py-encoded-camera_config.msgpack")
		if err != nil {
			fmt.Printf("  SKIP %s: file not available\n", testName)
		} else {
			var cfg CameraConfig
			err = rpc.Decode(data, &cfg)
			if err != nil {
				fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
				failed++
			} else if cfg.CameraID != "cam-abc-123" || cfg.FPS != 30 || cfg.Timestamp != 1708786800000 {
				fmt.Printf("  FAIL %s: wrong values - got %+v\n", testName, cfg)
				failed++
			} else {
				fmt.Printf("  OK %s\n", testName)
				passed++
			}
		}
	}

	{
		testName := "cross_node_detection_event"
		data, err := os.ReadFile("/tmp/node-encoded-detection_event.msgpack")
		if err != nil {
			fmt.Printf("  SKIP %s: file not available\n", testName)
		} else {
			var evt DetectionEvent
			err = rpc.Decode(data, &evt)
			if err != nil {
				fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
				failed++
			} else if evt.Type != "start" || evt.Data.StartTime != 1708786800000 || len(evt.Data.Triggers) != 2 {
				fmt.Printf("  FAIL %s: wrong values - type=%s startTime=%d triggers=%d\n",
					testName, evt.Type, evt.Data.StartTime, len(evt.Data.Triggers))
				failed++
			} else {
				fmt.Printf("  OK %s\n", testName)
				passed++
			}
		}
	}

	{
		testName := "cross_py_detection_event"
		data, err := os.ReadFile("/tmp/py-encoded-detection_event.msgpack")
		if err != nil {
			fmt.Printf("  SKIP %s: file not available\n", testName)
		} else {
			var evt DetectionEvent
			err = rpc.Decode(data, &evt)
			if err != nil {
				fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
				failed++
			} else if evt.Type != "start" || evt.Data.StartTime != 1708786800000 || len(evt.Data.Triggers) != 2 {
				fmt.Printf("  FAIL %s: wrong values - type=%s startTime=%d triggers=%d\n",
					testName, evt.Type, evt.Data.StartTime, len(evt.Data.Triggers))
				failed++
			} else {
				fmt.Printf("  OK %s\n", testName)
				passed++
			}
		}
	}

	{
		testName := "time_roundtrip"
		now := time.Now().UTC().Truncate(time.Second)
		type TimeStruct struct {
			Created time.Time `msgpack:"created"`
			Name    string    `msgpack:"name"`
		}
		original := TimeStruct{Created: now, Name: "test"}
		encoded, err := rpc.Encode(original)
		if err != nil {
			fmt.Printf("  FAIL %s: encode error - %v\n", testName, err)
			failed++
		} else {
			var decoded TimeStruct
			err = rpc.Decode(encoded, &decoded)
			if err != nil {
				fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
				failed++
			} else if !decoded.Created.Equal(now) || decoded.Name != "test" {
				fmt.Printf("  FAIL %s: expected %v, got %v\n", testName, now, decoded.Created)
				failed++
			} else {
				fmt.Printf("  OK %s\n", testName)
				passed++
			}
		}
	}

	{
		testName := "cross_node_date_ext"
		data, err := os.ReadFile("/tmp/node-encoded-date_ext.msgpack")
		if err != nil {
			fmt.Printf("  SKIP %s: file not available\n", testName)
		} else {
			decoded, err := rpc.DecodeRaw(data)
			if err != nil {
				fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
				failed++
			} else if t, ok := decoded.(time.Time); !ok {
				fmt.Printf("  FAIL %s: expected time.Time, got %T (%v)\n", testName, decoded, decoded)
				failed++
			} else {
				expected := time.Date(2024, 2, 24, 12, 0, 0, 0, time.UTC)
				if math.Abs(t.Sub(expected).Seconds()) > 1 {
					fmt.Printf("  FAIL %s: time mismatch - expected %v, got %v\n", testName, expected, t)
					failed++
				} else {
					fmt.Printf("  OK %s\n", testName)
					passed++
				}
			}
		}
	}

	{
		testName := "cross_py_date_ext"
		data, err := os.ReadFile("/tmp/py-encoded-date_ext.msgpack")
		if err != nil {
			fmt.Printf("  SKIP %s: file not available\n", testName)
		} else {
			decoded, err := rpc.DecodeRaw(data)
			if err != nil {
				fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
				failed++
			} else if t, ok := decoded.(time.Time); !ok {
				fmt.Printf("  FAIL %s: expected time.Time, got %T (%v)\n", testName, decoded, decoded)
				failed++
			} else {
				expected := time.Date(2024, 2, 24, 12, 0, 0, 0, time.UTC)
				if math.Abs(t.Sub(expected).Seconds()) > 1 {
					fmt.Printf("  FAIL %s: time mismatch - expected %v, got %v\n", testName, expected, t)
					failed++
				} else {
					fmt.Printf("  OK %s\n", testName)
					passed++
				}
			}
		}
	}

	{
		testName := "undefined_in_string_field"
		type PluginConfig struct {
			ID       string `msgpack:"id"`
			Name     string `msgpack:"name"`
			Optional string `msgpack:"optional"`
			Count    int    `msgpack:"count"`
		}
		encoded := buildMsgpackWithUndefined(map[string]any{
			"id":    "plugin-1",
			"name":  "test-plugin",
			"count": 5,
		}, "optional")
		var cfg PluginConfig
		err := rpc.Decode(encoded, &cfg)
		if err != nil {
			fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
			failed++
		} else if cfg.ID != "plugin-1" || cfg.Name != "test-plugin" || cfg.Count != 5 {
			fmt.Printf("  FAIL %s: wrong values - got %+v\n", testName, cfg)
			failed++
		} else {
			fmt.Printf("  OK %s (optional=%q)\n", testName, cfg.Optional)
			passed++
		}
	}

	{
		testName := "undefined_in_int_field"
		type Config struct {
			Name    string `msgpack:"name"`
			Timeout int    `msgpack:"timeout"`
		}
		encoded := buildMsgpackWithUndefined(map[string]any{
			"name": "test",
		}, "timeout")
		var cfg Config
		err := rpc.Decode(encoded, &cfg)
		if err != nil {
			fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
			failed++
		} else if cfg.Name != "test" || cfg.Timeout != 0 {
			fmt.Printf("  FAIL %s: wrong values - got %+v\n", testName, cfg)
			failed++
		} else {
			fmt.Printf("  OK %s (timeout=%d)\n", testName, cfg.Timeout)
			passed++
		}
	}

	{
		testName := "undefined_and_float_mixed"
		type MixedConfig struct {
			Name      string `msgpack:"name"`
			Optional  string `msgpack:"optional"`
			Timestamp int64  `msgpack:"timestamp"`
			FPS       int    `msgpack:"fps"`
		}
		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		_ = enc.EncodeMapLen(4)
		_ = enc.EncodeString("name")
		_ = enc.EncodeString("camera-1")
		_ = enc.EncodeString("optional")
		buf.Write([]byte{0xd4, 0x00, 0x00}) // fixext 1, type 0 = undefined
		_ = enc.EncodeString("timestamp")
		_ = enc.EncodeFloat64(1708786800000)
		_ = enc.EncodeString("fps")
		_ = enc.EncodeFloat64(30)
		encoded := buf.Bytes()

		var cfg MixedConfig
		err := rpc.Decode(encoded, &cfg)
		if err != nil {
			fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
			failed++
		} else if cfg.Name != "camera-1" || cfg.Timestamp != 1708786800000 || cfg.FPS != 30 {
			fmt.Printf("  FAIL %s: wrong values - got %+v\n", testName, cfg)
			failed++
		} else {
			fmt.Printf("  OK %s\n", testName)
			passed++
		}
	}

	{
		testName := "undefined_in_map_field"
		type Inner struct {
			Value string `msgpack:"value"`
			Count int    `msgpack:"count"`
		}
		type Outer struct {
			Name   string `msgpack:"name"`
			Config Inner  `msgpack:"config"`
			ID     int    `msgpack:"id"`
		}
		encoded := buildMsgpackWithUndefined(map[string]any{
			"name": "test-device",
			"id":   42,
		}, "config")
		var out Outer
		err := rpc.Decode(encoded, &out)
		if err != nil {
			fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
			failed++
		} else if out.Name != "test-device" || out.ID != 42 || out.Config.Value != "" || out.Config.Count != 0 {
			fmt.Printf("  FAIL %s: wrong values - got %+v\n", testName, out)
			failed++
		} else {
			fmt.Printf("  OK %s (config=%+v)\n", testName, out.Config)
			passed++
		}
	}

	{
		testName := "undefined_in_map_any_field"
		type Config struct {
			Name       string         `msgpack:"name"`
			Properties map[string]any `msgpack:"properties"`
			Enabled    bool           `msgpack:"enabled"`
		}
		encoded := buildMsgpackWithUndefined(map[string]any{
			"name":    "sensor-1",
			"enabled": true,
		}, "properties")
		var cfg Config
		err := rpc.Decode(encoded, &cfg)
		if err != nil {
			fmt.Printf("  FAIL %s: decode error - %v\n", testName, err)
			failed++
		} else if cfg.Name != "sensor-1" || !cfg.Enabled || cfg.Properties != nil {
			fmt.Printf("  FAIL %s: wrong values - got %+v\n", testName, cfg)
			failed++
		} else {
			fmt.Printf("  OK %s (properties=%v)\n", testName, cfg.Properties)
			passed++
		}
	}

	return
}

// buildMsgpackWithUndefined builds msgpack bytes for a map where the given undefinedKey
// has a JS undefined value (fixext 1, type 0 — 0xd4 0x00 0x00).
func buildMsgpackWithUndefined(fields map[string]any, undefinedKey string) []byte {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeMapLen(len(fields) + 1)

	_ = enc.EncodeString(undefinedKey)
	buf.Write([]byte{0xd4, 0x00, 0x00}) // fixext 1, type 0 = JS undefined

	for k, v := range fields {
		_ = enc.EncodeString(k)
		_ = enc.Encode(v)
	}

	return buf.Bytes()
}

func compareValues(expected, actual any, testName string) bool {
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
	if testName == "datetime" {
		if _, ok := expected.(time.Time); ok {
			if at, ok := actual.(time.Time); ok {
				return math.Abs(expected.(time.Time).Sub(at).Seconds()) < 1
			}
			return false
		}
	}
	if testName == "timestamp" {
		ef, eOk := toFloat64(expected)
		af, aOk := toFloat64(actual)
		return eOk && aOk && math.Abs(ef-af) < 2
	}
	if testName == "date" || testName == "time" {
		if s, ok := actual.(string); ok {
			return len(s) > 0
		}
		return false
	}
	if testName == "complex" {
		return compareComplex(expected, actual)
	}
	if testName == "date_ext" {
		if et, ok := expected.(time.Time); ok {
			if at, ok := actual.(time.Time); ok {
				return math.Abs(et.Sub(at).Seconds()) < 1
			}
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

func compareComplex(expected, actual any) bool {
	em, ok1 := expected.(map[string]any)
	am, ok2 := actual.(map[string]any)
	if !ok1 || !ok2 {
		return false
	}
	for k, ev := range em {
		if k == "timestamp" {
			if _, ok := am[k]; !ok {
				return false
			}
			continue
		}
		av, ok := am[k]
		if !ok {
			return false
		}
		if !compareValues(ev, av, k) {
			return false
		}
	}
	return true
}

func compareSlices(expected, actual any) bool {
	ev := reflect.ValueOf(expected)
	av := reflect.ValueOf(actual)
	if ev.Len() != av.Len() {
		return false
	}
	for i := 0; i < ev.Len(); i++ {
		if !compareValues(ev.Index(i).Interface(), av.Index(i).Interface(), "") {
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
		if !compareValues(ev.MapIndex(key).Interface(), avVal.Interface(), "") {
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
