package rpc

import (
	"bytes"
	"math"
	"reflect"
	"strings"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	// msgpackr encodes JavaScript undefined as ext type 0
	msgpack.RegisterExt(0, (*msgpackrUndefined)(nil))
}

type msgpackrUndefined struct{}

func (u msgpackrUndefined) MarshalMsgpack() ([]byte, error) {
	return []byte{0}, nil
}

func (u *msgpackrUndefined) UnmarshalMsgpack(b []byte) error {
	return nil
}

// Encode serializes data to MessagePack, compatible with msgpackr (useRecords=false, bundleStrings=false).
func Encode(v any) ([]byte, error) {
	data, err := encodeToBytes(func(enc *msgpack.Encoder) error { return encodeFast(enc, v) })
	if err != nil {
		return nil, err
	}
	return data, nil
}

var encodeBufPool = sync.Pool{ // put a buffer back only after its last publish returned
	New: func() any { return new(bytes.Buffer) },
}

// a bigger buffer is dropped so one oversized payload does not pin memory
const maxPooledEncodeBuf = 4 << 20

func encodePooled(v any) (encoded []byte, release func(), err error) {
	buf, err := encodeToPooledBuf(v)
	if err != nil {
		return nil, nil, err
	}
	return buf.Bytes(), func() { releaseEncodeBuf(buf) }, nil
}

func encodeToPooledBuf(v any) (*bytes.Buffer, error) {
	buf := encodeBufPool.Get().(*bytes.Buffer)
	buf.Reset()

	enc := msgpack.GetEncoder()
	enc.Reset(buf)
	err := encodeFast(enc, v)
	msgpack.PutEncoder(enc)

	if err != nil {
		releaseEncodeBuf(buf)
		return nil, err
	}
	return buf, nil
}

func releaseEncodeBuf(buf *bytes.Buffer) {
	if buf.Cap() <= maxPooledEncodeBuf {
		encodeBufPool.Put(buf)
	}
}

func convertPooled(src, dst any) error {
	buf, err := encodeToPooledBuf(src)
	if err != nil {
		return err
	}
	err = Decode(buf.Bytes(), dst)
	// msgpack decoding copies bytes and strings out of its input, nothing keeps a view of buf
	releaseEncodeBuf(buf)
	return err
}

func Decode(data []byte, v any) error {
	err := msgpack.Unmarshal(data, v)
	if err == nil {
		return nil
	}

	// JS numbers arrive as floats and undefined as an ext where the struct wants an int or a string
	if !isTypeMismatch(err) {
		return err
	}

	dec := msgpack.NewDecoder(bytes.NewReader(data))
	dec.UseLooseInterfaceDecoding(true)

	var raw any
	if rerr := dec.Decode(&raw); rerr != nil {
		return err
	}

	coerced := coerceValues(raw)
	buf, rerr := encodeToPooledBuf(coerced)
	if rerr != nil {
		return err
	}

	// the failed decode left values behind, msgpack panics decoding into a non-nil any field
	resetValue(v)

	err = msgpack.Unmarshal(buf.Bytes(), v)
	releaseEncodeBuf(buf)
	return err
}

func resetValue(v any) {
	pointer := reflect.ValueOf(v)
	if pointer.Kind() != reflect.Pointer || pointer.IsNil() {
		return
	}
	target := pointer.Elem()
	if target.CanSet() {
		target.SetZero()
	}
}

func isTypeMismatch(err error) bool {
	msg := err.Error()
	// the invalid, unexpected and unsupported "code=XX decoding YYY" errors, not EOF or corruption
	return strings.Contains(msg, " code=") && strings.Contains(msg, " decoding ")
}

func coerceValues(v any) any {
	switch val := v.(type) {
	case float64:
		if val == math.Trunc(val) && !math.IsInf(val, 0) && !math.IsNaN(val) &&
			val >= math.MinInt64 && val <= math.MaxInt64 {
			return int64(val)
		}
		// v, not val: boxing val again allocates for every float in the tree
		return v
	case float32:
		f := float64(val)
		if f == math.Trunc(f) && !math.IsInf(f, 0) && !math.IsNaN(f) {
			return int64(f)
		}
		return v
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

// NormalizeUndefined replaces msgpackr's undefined ext values with nil, descending into maps and slices.
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

// DecodeRaw decodes MessagePack into a generic value with loose interface decoding (int64, uint64, float64).
func DecodeRaw(data []byte) (any, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	dec.UseLooseInterfaceDecoding(true)

	var v any
	err := dec.Decode(&v)
	return v, err
}

// DecodeToMap decodes MessagePack into a map with loose interface decoding.
func DecodeToMap(data []byte) (map[string]any, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	dec.UseLooseInterfaceDecoding(true)

	var v map[string]any
	err := dec.Decode(&v)
	return v, err
}
