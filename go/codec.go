package rpc

import (
	"bytes"
	"math"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	// Register msgpackr-specific extension type for cross-language compatibility.
	// msgpackr (Node.js) encodes JavaScript `undefined` as msgpack ext type 0.
	// The msgpack timestamp extension (type -1 / 0xff) is handled natively
	msgpack.RegisterExt(0, (*msgpackrUndefined)(nil))
}

// msgpackrUndefined represents JavaScript's `undefined` value.
// msgpackr encodes `undefined` as msgpack ext type 0 with empty data.
type msgpackrUndefined struct{}

func (u msgpackrUndefined) MarshalMsgpack() ([]byte, error) {
	return []byte{0}, nil
}

func (u *msgpackrUndefined) UnmarshalMsgpack(b []byte) error {
	return nil
}

// Encode serializes data to MessagePack binary format.
// Compatible with msgpackr (useRecords=false, bundleStrings=false).
func Encode(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func Decode(data []byte, v any) error {
	err := msgpack.Unmarshal(data, v)
	if err == nil {
		return nil
	}

	// Check if the error is a type mismatch (wire type doesn't match Go struct field type).
	// vmihailenco/msgpack produces "invalid code=XX decoding YYY" for these cases:
	//   0xca/0xcb: float32/float64 where int expected (JS number encoding)
	//   0xd4-0xd8: fixext where string/int expected (JS undefined)
	//   0xc7-0xc9: ext where string/int expected
	if !isTypeMismatch(err) {
		return err
	}

	// Fallback: decode as generic any with loose interface typing, normalize values
	// (float64→int64, undefined→nil), re-encode, and decode into the target struct.
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	dec.UseLooseInterfaceDecoding(true)

	var raw any
	if rerr := dec.Decode(&raw); rerr != nil {
		return err // return original error
	}

	coerced := coerceValues(raw)
	reencoded, rerr := msgpack.Marshal(coerced)
	if rerr != nil {
		return err // return original error
	}
	return msgpack.Unmarshal(reencoded, v)
}

func isTypeMismatch(err error) bool {
	msg := err.Error()
	// vmihailenco/msgpack uses these patterns for wire type mismatches:
	//   "invalid code=XX decoding YYY"      — type decoders (string, int, float, bool, nil, array, ext)
	//   "unexpected code=XX decoding YYY"    — struct/map decoder
	//   "unsupported code=XX decoding YYY"   — query decoder
	// Matching " code=" + " decoding " catches all variants without matching structural
	// errors (EOF, corruption, truncation) which use different formats.
	return strings.Contains(msg, " code=") && strings.Contains(msg, " decoding ")
}

func coerceValues(v any) any {
	switch val := v.(type) {
	case float64:
		if val == math.Trunc(val) && !math.IsInf(val, 0) && !math.IsNaN(val) &&
			val >= math.MinInt64 && val <= math.MaxInt64 {
			return int64(val)
		}
		return val
	case float32:
		f := float64(val)
		if f == math.Trunc(f) && !math.IsInf(f, 0) && !math.IsNaN(f) {
			return int64(f)
		}
		return val
	case *msgpackrUndefined:
		return nil
	case msgpackrUndefined:
		return nil
	case map[string]any:
		for k, item := range val {
			val[k] = coerceValues(item)
		}
		return val
	case []any:
		for i, item := range val {
			val[i] = coerceValues(item)
		}
		return val
	default:
		return v
	}
}

// NormalizeUndefined recursively replaces msgpackr's `undefined` ext values
// (decoded as msgpackrUndefined structs) with Go nil, descending into maps and
// slices. The struct otherwise leaks through generic (`any`) decode paths that
// succeed without the coerceValues fallback, and serializes back out as `{}`
// (an empty struct), corrupting string/number config values into "[object Object]".
func NormalizeUndefined(v any) any {
	switch val := v.(type) {
	case *msgpackrUndefined:
		return nil
	case msgpackrUndefined:
		return nil
	case map[string]any:
		for k, item := range val {
			val[k] = NormalizeUndefined(item)
		}
		return val
	case []any:
		for i, item := range val {
			val[i] = NormalizeUndefined(item)
		}
		return val
	default:
		return v
	}
}

// DecodeRaw deserializes MessagePack binary data into a generic interface.
// Uses loose interface decoding for consistent numeric types (int64, uint64, float64).
func DecodeRaw(data []byte) (any, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	dec.UseLooseInterfaceDecoding(true)

	var v any
	err := dec.Decode(&v)
	return v, err
}

// DecodeToMap deserializes MessagePack binary data into a map.
// Uses loose interface decoding for consistent numeric types.
func DecodeToMap(data []byte) (map[string]any, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	dec.UseLooseInterfaceDecoding(true)

	var v map[string]any
	err := dec.Decode(&v)
	return v, err
}
