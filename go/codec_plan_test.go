package rpc

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type planScalars struct {
	B   bool    `msgpack:"b"`
	S   string  `msgpack:"s"`
	I   int     `msgpack:"i"`
	I8  int8    `msgpack:"i8"`
	I16 int16   `msgpack:"i16"`
	I32 int32   `msgpack:"i32"`
	I64 int64   `msgpack:"i64"`
	U   uint    `msgpack:"u"`
	U8  uint8   `msgpack:"u8"`
	U16 uint16  `msgpack:"u16"`
	U32 uint32  `msgpack:"u32"`
	U64 uint64  `msgpack:"u64"`
	F32 float32 `msgpack:"f32"`
	F64 float64
}

type planOmit struct {
	B     bool              `msgpack:"b,omitempty"`
	S     string            `msgpack:"s,omitempty"`
	I     int               `msgpack:"i,omitempty"`
	U16   uint16            `msgpack:"u16,omitempty"`
	F     float64           `msgpack:"f,omitempty"`
	Bin   []byte            `msgpack:"bin,omitempty"`
	List  []string          `msgpack:"list,omitempty"`
	Map   map[string]string `msgpack:"map,omitempty"`
	Ptr   *planScalars      `msgpack:"ptr,omitempty"`
	Any   any               `msgpack:"any,omitempty"`
	Inner planInner         `msgpack:"inner,omitempty"`
	When  time.Time         `msgpack:"when,omitempty"`
	Zero  planZeroer        `msgpack:"zero,omitempty"`
	Arr   [2]int            `msgpack:"arr,omitempty"`
}

type planInner struct {
	A string `msgpack:"a,omitempty"`
	N int    `msgpack:"n,omitempty"`
}

type planZeroer struct{ V int }

func (z planZeroer) IsZero() bool { return z.V == 7 }

type planCustom struct{ V string }

func (c planCustom) MarshalMsgpack() ([]byte, error) { return msgpack.Marshal("custom:" + c.V) }

type planText struct{ V int }

func (p planText) MarshalText() ([]byte, error) { return fmt.Append(nil, "t", p.V), nil }

type planEmbedded struct {
	planInner
	Name string `msgpack:"name"`
}

type planAsArray struct {
	_msgpack struct{} `msgpack:",as_array"`
	A        int
	B        string
}

type planAllOmit struct {
	_msgpack struct{} `msgpack:",omitempty"`
	A        int
	B        string
}

var _ = []any{planAsArray{_msgpack: struct{}{}}, planAllOmit{_msgpack: struct{}{}}}

type planTagged struct {
	Name  string `msgpack:"name,intern"`
	Alias string `msgpack:"x,alias:y"`
}

type planDuplicate struct {
	A string `msgpack:"same"`
	B string `msgpack:"same"`
}

type planNode struct {
	Name string      `msgpack:"name"`
	Next *planNode   `msgpack:"next,omitempty"`
	Kids []planNode  `msgpack:"kids"`
	Ptrs []*planNode `msgpack:"ptrs,omitempty"`
}

type planNamedSlice []planInner

type planCode string

type planMixed struct {
	Code     planCode          `msgpack:"code"`
	Floats   []float64         `msgpack:"floats"`
	Bins     [][]byte          `msgpack:"bins"`
	Items    []planInner       `msgpack:"items"`
	Ptrs     []*planInner      `msgpack:"ptrs"`
	Named    planNamedSlice    `msgpack:"named"`
	Custom   planCustom        `msgpack:"custom"`
	CustomP  *planCustom       `msgpack:"customP"`
	Text     planText          `msgpack:"text"`
	Err      error             `msgpack:"err"`
	When     time.Time         `msgpack:"when"`
	Dur      time.Duration     `msgpack:"dur"`
	Anys     []any             `msgpack:"anys"`
	Props    map[string]any    `msgpack:"props"`
	Embedded planEmbedded      `msgpack:"embedded"`
	ArrStr   [2]string         `msgpack:"arrStr"`
	Undef    msgpackrUndefined `msgpack:"undef"`
	hidden   string
	Skipped  string `msgpack:"-"`
}

type planRegistered struct{ V int }

func init() {
	msgpack.Register(planRegistered{},
		func(e *msgpack.Encoder, v reflect.Value) error { return e.EncodeString("registered") },
		nil)
}

func planFixtures() map[string]any {
	scalarsMax := planScalars{
		B: true, S: strings.Repeat("x", 300), I: math.MaxInt64, I8: math.MinInt8, I16: math.MaxInt16, I32: math.MinInt32, I64: math.MaxInt64,
		U: math.MaxUint64, U8: math.MaxUint8, U16: math.MaxUint16, U32: math.MaxUint32, U64: math.MaxUint64, F32: -1.5, F64: math.Inf(1),
	}
	when := time.Date(2026, 10, 4, 1, 2, 3, 4, time.UTC)
	node := planNode{Name: "root", Next: &planNode{Name: "next"}, Kids: []planNode{{Name: "kid"}}, Ptrs: []*planNode{nil, {Name: "p"}}}
	full := planOmit{
		B: true, S: "s", I: -1, U16: 9, F: 0.5, Bin: []byte{1}, List: []string{"a"}, Map: map[string]string{"k": "v"},
		Ptr: &planScalars{}, Any: map[string]any{"x": 1}, Inner: planInner{N: 1}, When: when, Zero: planZeroer{V: 1}, Arr: [2]int{0, 1},
	}
	mixed := planMixed{
		Code: "c", Floats: []float64{1, 2.5}, Bins: [][]byte{nil, {1, 2}}, Items: []planInner{{A: "a"}}, Ptrs: []*planInner{nil, {N: 2}},
		Named: planNamedSlice{{A: "n"}}, Custom: planCustom{V: "v"}, CustomP: &planCustom{V: "p"}, Text: planText{V: 3},
		Err: errors.New("boom"), When: when, Dur: time.Second, Anys: []any{nil, 1, "s", planInner{A: "x"}, &planInner{}, []byte{1}, planCustom{V: "a"}},
		Props: map[string]any{"one": planInner{A: "p"}}, Embedded: planEmbedded{planInner: planInner{A: "e"}, Name: "n"},
		ArrStr: [2]string{"a", "b"}, hidden: "h", Skipped: "s",
	}
	return map[string]any{
		"scalars zero":           planScalars{},
		"scalars max":            scalarsMax,
		"scalars pointer":        &scalarsMax,
		"omit empty":             planOmit{},
		"omit zeroer":            planOmit{Zero: planZeroer{V: 7}},
		"omit full":              full,
		"omit any zero values":   []planOmit{{Any: 0}, {Any: ""}, {Any: planInner{}}, {Any: &planInner{}}, {Any: planZeroer{V: 7}}, {Any: (*planInner)(nil)}},
		"omit any embedded":      planOmit{Any: planEmbedded{}},
		"omit any as array":      planOmit{Any: planAsArray{}},
		"mixed":                  mixed,
		"mixed zero":             planMixed{},
		"embedded":               planEmbedded{planInner: planInner{N: 3}, Name: "x"},
		"as array":               planAsArray{A: 1, B: "b"},
		"all omit":               []planAllOmit{{}, {A: 1}},
		"tagged":                 planTagged{Name: "n", Alias: "a"},
		"duplicate":              planDuplicate{A: "a", B: "b"},
		"recursive":              node,
		"recursive slice":        []planNode{node, {}},
		"named slice":            planNamedSlice{{A: "a"}},
		"registered":             planRegistered{V: 1},
		"registered in field":    struct{ R planRegistered }{R: planRegistered{V: 2}},
		"registered in any":      planOmit{Any: planRegistered{V: 3}},
		"anonymous struct":       struct{ A, B int }{1, 2},
		"slice of any":           []any{planInner{A: "x"}, nil, 1.5},
		"nil slice of structs":   []planInner(nil),
		"empty slice of structs": []planInner{},
		"response":               RPCResponse{ID: "1", Result: []planInner{{A: "r"}}, Methods: []string{"a"}, Timing: &RPCTiming{Start: 1, End: 2}},
		"response error":         &RPCResponse{ID: "2", Error: &RPCError{Code: "X", Message: "m", Data: planInner{}}},
		"response error data":    RPCResponse{ID: "3", Error: &RPCError{Code: "X", Message: "m", Data: map[string]any{"subject": "s"}}},
		"message":                RPCMessage{ID: "1", Method: "call", Params: []any{"cam", planScalars{I: 3}, &mixed}, Discover: true},
		"stream":                 StreamMessage{ID: "1", Type: "data", Data: []planOmit{full}},
		"pull":                   PullIteratorResponse{ID: "1", Type: "value", Value: node},
		"chunk header":           ChunkedTransferHeader{Type: "chunked", TransferID: "t", TotalChunks: 3, TotalSize: 1 << 20, ChunkSize: 1 << 18},
	}
}

func expectSameBytes(t *testing.T, name string, v any) {
	t.Helper()
	want, wantErr := msgpack.Marshal(v)
	got, gotErr := Encode(v)
	if (wantErr != nil) != (gotErr != nil) {
		t.Fatalf("%s: vmihailenco err %v, Encode err %v", name, wantErr, gotErr)
	}
	if !bytes.Equal(want, got) {
		t.Fatalf("%s: bytes differ\nwant %x\ngot  %x", name, want, got)
	}
	pooled, release, err := encodePooled(v)
	if err == nil {
		if !bytes.Equal(want, pooled) {
			t.Fatalf("%s: pooled bytes differ", name)
		}
		release()
	}
}

func TestEncodePlanWritesTheBytesOfVmihailenco(t *testing.T) {
	for name, v := range planFixtures() {
		expectSameBytes(t, name, v)
	}
}

func TestEncodePlanRandomValues(t *testing.T) {
	types := []reflect.Type{
		reflect.TypeFor[planScalars](), reflect.TypeFor[planOmit](), reflect.TypeFor[planMixed](),
		reflect.TypeFor[planNode](), reflect.TypeFor[[]planOmit](), reflect.TypeFor[RPCResponse](),
		reflect.TypeFor[RPCMessage](), reflect.TypeFor[*planOmit](), reflect.TypeFor[[]*planNode](),
	}
	rng := rand.New(rand.NewPCG(7, 11))
	for i := range 3000 {
		typ := types[rng.IntN(len(types))]
		v := reflect.New(typ).Elem()
		fillRandom(rng, v, 3)
		expectSameBytes(t, fmt.Sprintf("random %d %s", i, typ), v.Interface())
	}
}

func TestEncodePlanSkipsTypesItDoesNotModel(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeFor[planEmbedded](), reflect.TypeFor[planAsArray](), reflect.TypeFor[planAllOmit](),
		reflect.TypeFor[planTagged](), reflect.TypeFor[planDuplicate](), reflect.TypeFor[planCustom](),
		reflect.TypeFor[planRegistered](), reflect.TypeFor[planNamedSlice](), reflect.TypeFor[time.Time](),
	} {
		if planFor(typ) != nil {
			t.Errorf("%s got a plan", typ)
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[planMixed](), reflect.TypeFor[RPCResponse](), reflect.TypeFor[[]planNode]()} {
		if planFor(typ) == nil {
			t.Errorf("%s got no plan", typ)
		}
	}
}

// one-key maps only: vmihailenco writes map entries in iteration order
func fillRandom(rng *rand.Rand, v reflect.Value, depth int) {
	if !v.CanSet() {
		return
	}
	if rng.IntN(4) == 0 {
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(rng.IntN(2) == 0)
	case reflect.String:
		v.SetString(strings.Repeat("s", []int{0, 1, 31, 32, 255, 256}[rng.IntN(6)]))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		edges := []int64{0, 1, -1, 127, 128, -32, -33, 255, 256, math.MaxInt16, math.MinInt32, math.MaxInt64, math.MinInt64}
		n := edges[rng.IntN(len(edges))]
		if !v.OverflowInt(n) {
			v.SetInt(n)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		edges := []uint64{0, 1, 127, 128, 255, 256, math.MaxUint16, math.MaxUint32, math.MaxUint64}
		n := edges[rng.IntN(len(edges))]
		if !v.OverflowUint(n) {
			v.SetUint(n)
		}
	case reflect.Float32, reflect.Float64:
		v.SetFloat([]float64{0, -0.5, 1e9, math.NaN(), math.Inf(-1)}[rng.IntN(5)])
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			v.SetBytes(make([]byte, []int{0, 3, 300}[rng.IntN(3)]))
			return
		}
		n := rng.IntN(3)
		s := reflect.MakeSlice(v.Type(), n, n)
		if depth > 0 {
			for i := range n {
				fillRandom(rng, s.Index(i), depth-1)
			}
		}
		v.Set(s)
	case reflect.Array:
		for i := range v.Len() {
			fillRandom(rng, v.Index(i), depth)
		}
	case reflect.Map:
		m := reflect.MakeMap(v.Type())
		if depth > 0 && rng.IntN(2) == 0 {
			key := reflect.New(v.Type().Key()).Elem()
			fillRandom(rng, key, 0)
			val := reflect.New(v.Type().Elem()).Elem()
			fillRandom(rng, val, depth-1)
			m.SetMapIndex(key, val)
		}
		v.Set(m)
	case reflect.Pointer:
		if depth > 0 {
			p := reflect.New(v.Type().Elem())
			fillRandom(rng, p.Elem(), depth-1)
			v.Set(p)
		}
	case reflect.Interface:
		if v.NumMethod() > 0 {
			if v.Type() == reflect.TypeFor[error]() && rng.IntN(2) == 0 {
				v.Set(reflect.ValueOf(errors.New("e")))
			}
			return
		}
		choices := []func() any{
			func() any { return nil }, func() any { return rng.IntN(300) }, func() any { return "str" },
			func() any { return planInner{A: "x"} }, func() any { return &planInner{N: 1} }, func() any { return planEmbedded{} },
			func() any { return planZeroer{V: 7} }, func() any { return []any{1, planInner{}} }, func() any { return map[string]any{"k": planInner{}} },
			func() any { return planCustom{V: "c"} }, func() any { return planRegistered{} }, func() any { return time.Unix(5, 6) },
		}
		if x := choices[rng.IntN(len(choices))](); x != nil {
			v.Set(reflect.ValueOf(x))
		}
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[time.Time]() {
			if rng.IntN(2) == 0 {
				v.Set(reflect.ValueOf(time.Unix(int64(rng.IntN(1e9)), 0)))
			}
			return
		}
		for _, field := range v.Fields() {
			fillRandom(rng, field, depth)
		}
	}
}

func TestEncodePlanBuildsConcurrently(t *testing.T) {
	for round := range 20 {
		typ := reflect.StructOf([]reflect.StructField{
			{Name: "Name", Type: reflect.TypeFor[string](), Tag: `msgpack:"name"`},
			{Name: "Node", Type: reflect.TypeFor[*planNode](), Tag: `msgpack:"node,omitempty"`},
			{Name: "Items", Type: reflect.TypeFor[[]planOmit](), Tag: `msgpack:"items"`},
			{Name: "Round", Type: reflect.TypeFor[int](), Tag: reflect.StructTag(fmt.Sprintf(`msgpack:"r%d"`, round))},
		})
		v := reflect.New(typ).Elem()
		v.Field(0).SetString("x")
		v.Field(1).Set(reflect.ValueOf(&planNode{Name: "n", Kids: []planNode{{}}}))
		v.Field(2).Set(reflect.ValueOf([]planOmit{{S: "s"}, {}}))
		value := v.Interface()
		want, err := msgpack.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		errs := make(chan error, 8)
		for range 8 {
			go func() {
				got, err := Encode(value)
				if err == nil && !bytes.Equal(got, want) {
					err = fmt.Errorf("bytes differ: %x", got)
				}
				errs <- err
			}()
		}
		for range 8 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
	}
}

type planPtrRegistered struct{ V int }

func init() {
	msgpack.Register((*planPtrRegistered)(nil),
		func(e *msgpack.Encoder, v reflect.Value) error { return e.EncodeString("pointer registered") },
		nil)
}

func TestEncodePlanHonoursAPointerRegistration(t *testing.T) {
	expectSameBytes(t, "registered pointer", &planPtrRegistered{V: 1})
	expectSameBytes(t, "registered pointer in a slice", []*planPtrRegistered{{V: 2}, nil})
	if planFor(reflect.TypeFor[*planPtrRegistered]()) != nil {
		t.Error("the registered pointer type got a plan")
	}
}

type planMoney struct{ cents *int64 }

// assumes a constructed value, as some marshalers do
func (m planMoney) MarshalMsgpack() ([]byte, error) { return msgpack.Marshal(*m.cents) }

type planOrder struct {
	ID    string
	Price planMoney
}

func TestEncodePlanSurvivesAMarshalerThatPanicsOnZero(t *testing.T) {
	cents := int64(4200)
	expectSameBytes(t, "order", planOrder{ID: "a", Price: planMoney{cents: &cents}})
	if planFor(reflect.TypeFor[planOrder]()) != nil {
		t.Error("a type whose zero value panics got a plan")
	}
}

// the generic encoder allocates per field, a plan only when the buffer grows
func TestEncodePathsUseThePlans(t *testing.T) {
	events := RPCResponse{ID: "p.1", Result: benchEvents(500)}
	if allocs := testing.AllocsPerRun(10, func() { _, _ = Encode(events) }); allocs > 30 {
		t.Errorf("Encode: %v allocations for 500 events", allocs)
	}
	if allocs := testing.AllocsPerRun(10, func() {
		_, release, err := encodeMessagePooled(events)
		if err != nil {
			t.Fatal(err)
		}
		release()
	}); allocs > 30 {
		t.Errorf("encodePooled: %v allocations for 500 events", allocs)
	}
	// the extraction copies the reply, the events keep their plan in the frame
	frame := RPCResponse{ID: "p.2", Result: map[string]any{"events": benchEvents(200), "image": make([]byte, 64<<10)}}
	if allocs := testing.AllocsPerRun(10, func() {
		_, release, err := encodeMessagePooled(frame)
		if err != nil {
			t.Fatal(err)
		}
		release()
	}); allocs > 800 {
		t.Errorf("encodeFrame: %v allocations for 200 events and a binary", allocs)
	}
}
