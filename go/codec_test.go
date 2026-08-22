package rpc

import (
	"reflect"
	"testing"
)

func TestEncodeDecodeMap(t *testing.T) {
	original := map[string]any{
		"name":    "test",
		"value":   int64(42),
		"nested":  map[string]any{"key": "val"},
		"list":    []any{int64(1), int64(2), int64(3)},
		"enabled": true,
	}

	encoded, err := Encode(original)
	if err != nil {
		t.Fatal(err)
	}

	var decoded map[string]any
	if err := Decode(encoded, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded["name"] != "test" {
		t.Errorf("name = %v, want test", decoded["name"])
	}
	if decoded["enabled"] != true {
		t.Errorf("enabled = %v, want true", decoded["enabled"])
	}
}

func TestEncodeDecodeRPCMessage(t *testing.T) {
	msg := RPCMessage{
		ID:     "123-abcdefghi",
		Method: "call",
		Params: []any{"arg1", int64(42)},
	}

	encoded, err := Encode(msg)
	if err != nil {
		t.Fatal(err)
	}

	var decoded RPCMessage
	if err := Decode(encoded, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.ID != msg.ID {
		t.Errorf("ID = %v, want %v", decoded.ID, msg.ID)
	}
	if decoded.Method != msg.Method {
		t.Errorf("Method = %v, want %v", decoded.Method, msg.Method)
	}
}

func TestEncodeDecodeRPCResponse(t *testing.T) {
	resp := RPCResponse{
		ID:      "123-abcdefghi",
		Result:  map[string]any{"status": "ok"},
		Methods: []string{"getSnapshot", "generateFrames"},
	}

	encoded, err := Encode(resp)
	if err != nil {
		t.Fatal(err)
	}

	var decoded RPCResponse
	if err := Decode(encoded, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.ID != resp.ID {
		t.Errorf("ID = %v, want %v", decoded.ID, resp.ID)
	}
	if !reflect.DeepEqual(decoded.Methods, resp.Methods) {
		t.Errorf("Methods = %v, want %v", decoded.Methods, resp.Methods)
	}
}

func TestEncodeDecodeRPCResponseError(t *testing.T) {
	resp := RPCResponse{
		ID: "123-abcdefghi",
		Error: &RPCError{
			Code:    ErrCodeMethodNotFound,
			Message: "Method not found",
		},
	}

	encoded, err := Encode(resp)
	if err != nil {
		t.Fatal(err)
	}

	var decoded RPCResponse
	if err := Decode(encoded, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.Error == nil {
		t.Fatal("expected error, got nil")
	}
	if decoded.Error.Code != ErrCodeMethodNotFound {
		t.Errorf("Error.Code = %v, want %v", decoded.Error.Code, ErrCodeMethodNotFound)
	}
}

func TestEncodeDecodeStreamMessage(t *testing.T) {
	msg := StreamMessage{
		ID:   "123",
		Type: "data",
		Data: map[string]any{"frame": int64(1)},
	}

	encoded, err := Encode(msg)
	if err != nil {
		t.Fatal(err)
	}

	var decoded StreamMessage
	if err := Decode(encoded, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.Type != "data" {
		t.Errorf("Type = %v, want data", decoded.Type)
	}
}

func TestDecodeRaw(t *testing.T) {
	original := map[string]any{"hello": "world"}
	encoded, err := Encode(original)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeRaw(encoded)
	if err != nil {
		t.Fatal(err)
	}

	m, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", decoded)
	}
	if m["hello"] != "world" {
		t.Errorf("hello = %v, want world", m["hello"])
	}
}

func TestEncodeDecodeChannelMessage(t *testing.T) {
	msg := ChannelMessage{
		Type:   "message",
		Data:   "hello",
		Sender: "client-1",
	}

	encoded, err := Encode(msg)
	if err != nil {
		t.Fatal(err)
	}

	var decoded ChannelMessage
	if err := Decode(encoded, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.Type != "message" {
		t.Errorf("Type = %v, want message", decoded.Type)
	}
	if decoded.Sender != "client-1" {
		t.Errorf("Sender = %v, want client-1", decoded.Sender)
	}
}

func TestDecodeFallbackIntoPartiallyFilledStruct(t *testing.T) {
	type entry struct {
		Value     any   `msgpack:"value"`
		Timestamp int64 `msgpack:"timestamp"`
	}

	// the float timestamp forces the coercion fallback, and the first attempt
	// has already put a bool behind the any field by then
	encoded, err := Encode([]any{
		map[string]any{"value": true, "timestamp": float64(1755800000000)},
		map[string]any{"value": false, "timestamp": float64(1755800001000)},
	})
	if err != nil {
		t.Fatal(err)
	}

	var entries []entry
	if err := Decode(encoded, &entries); err != nil {
		t.Fatal(err)
	}

	if len(entries) != 2 {
		t.Fatalf("len = %d, want 2", len(entries))
	}
	if entries[0].Value != true || entries[1].Value != false {
		t.Errorf("values = %v, %v, want true, false", entries[0].Value, entries[1].Value)
	}
	if entries[0].Timestamp != 1755800000000 {
		t.Errorf("Timestamp = %d, want 1755800000000", entries[0].Timestamp)
	}
}
