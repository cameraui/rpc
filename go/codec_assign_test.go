package rpc

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type asgState string

type asgTags []string

type asgPoint [2]float64

type asgBox struct {
	X      float64 `msgpack:"x"`
	Y      float64 `msgpack:"y"`
	Width  float64 `msgpack:"width"`
	Height float64 `msgpack:"height"`
}

type asgTrigger struct {
	Type  string  `msgpack:"type"`
	Label string  `msgpack:"label"`
	Score float64 `msgpack:"score,omitempty"`
}

type asgDetection struct {
	Label     string   `msgpack:"label"`
	Score     float64  `msgpack:"score"`
	MaxCount  int      `msgpack:"maxCount"`
	Moving    *bool    `msgpack:"moving,omitempty"`
	Box       *asgBox  `msgpack:"box,omitempty"`
	TrackID   int      `msgpack:"trackId,omitempty"`
	Zones     []string `msgpack:"zones,omitempty"`
	FirstSeen int64    `msgpack:"firstSeen,omitempty"`
	Alert     *bool    `msgpack:"alert,omitempty"`
}

type asgAttribute struct {
	Type          string    `msgpack:"type"`
	Label         string    `msgpack:"label"`
	Confidence    float64   `msgpack:"confidence,omitempty"`
	ParentTrackID int       `msgpack:"parentTrackId,omitempty"`
	Embedding     []float64 `msgpack:"embedding,omitempty"`
	Color         string    `msgpack:"color,omitempty"`
}

type asgEmbedding struct {
	TrackID   int       `msgpack:"trackId"`
	Embedding []float64 `msgpack:"embedding"`
	Model     string    `msgpack:"embeddingModel"`
}

type asgSegment struct {
	FirstSeen   int64          `msgpack:"firstSeen"`
	LastSeen    int64          `msgpack:"lastSeen"`
	StripWindow *asgBox        `msgpack:"stripWindow,omitempty"`
	Detections  []asgDetection `msgpack:"detections"`
	Attributes  []asgAttribute `msgpack:"attributes"`
	Zones       []string       `msgpack:"zones,omitempty"`
	Embeddings  []asgEmbedding `msgpack:"personEmbeddings,omitempty"`
}

type asgEvent struct {
	ID           string       `msgpack:"id"`
	CameraID     string       `msgpack:"cameraId"`
	State        asgState     `msgpack:"state"`
	StartTime    int64        `msgpack:"startTime"`
	EndTime      int64        `msgpack:"endTime,omitempty"`
	LastUpdate   int64        `msgpack:"lastUpdate"`
	Types        []string     `msgpack:"types"`
	Triggers     []asgTrigger `msgpack:"triggers"`
	Segments     []asgSegment `msgpack:"segments"`
	SegmentIndex int          `msgpack:"segmentIndex,omitempty"`
	EpisodeIDs   asgTags      `msgpack:"episodeIds,omitempty"`
	Favorite     bool         `msgpack:"favorite,omitempty"`
}

type asgTraceDetection struct {
	Label string    `msgpack:"label"`
	Score float64   `msgpack:"score"`
	Box   []float64 `msgpack:"box"`
	Track int       `msgpack:"track,omitempty"`
}

type asgTraceInput struct {
	TMs   int64   `msgpack:"tMs"`
	Boxes [][]any `msgpack:"boxes,omitempty"`
}

type asgTraceTick struct {
	TMs        int64                `msgpack:"tMs"`
	RTP        *uint32              `msgpack:"rtp,omitempty"`
	Src        string               `msgpack:"src,omitempty"`
	ObjectRan  bool                 `msgpack:"objectRan,omitempty"`
	Detections []asgTraceDetection  `msgpack:"detections"`
	Between    []asgTraceInput      `msgpack:"between,omitempty"`
	World      []map[string]any     `msgpack:"world"`
	Confidence map[string]float64   `msgpack:"confidence,omitempty"`
	Zones      map[string][]string  `msgpack:"zones,omitempty"`
	Boxes      map[asgState]*asgBox `msgpack:"boxes,omitempty"`
}

type asgAttachments struct {
	Scene      []byte          `msgpack:"scene,omitempty"`
	Strip      []byte          `msgpack:"strip,omitempty"`
	Card       []byte          `msgpack:"card,omitempty"`
	Attributes [][]byte        `msgpack:"attributes,omitempty"`
	Landmarks  [][]asgPoint    `msgpack:"attributeLandmarks,omitempty"`
	Trace      []asgTraceTick  `msgpack:"trace,omitempty"`
	Extra      map[string]any  `msgpack:"extra,omitempty"`
	Named      map[string]bool `msgpack:"named,omitempty"`
}

type asgNumbers struct {
	I      int               `msgpack:"i"`
	I8     int8              `msgpack:"i8"`
	U8     uint8             `msgpack:"u8"`
	U64    uint64            `msgpack:"u64"`
	I64    int64             `msgpack:"i64"`
	F32    float32           `msgpack:"f32"`
	F64    float64           `msgpack:"f64"`
	B      bool              `msgpack:"b"`
	S      string            `msgpack:"s"`
	Bin    []byte            `msgpack:"bin"`
	Any    any               `msgpack:"any"`
	Anys   []any             `msgpack:"anys"`
	Map    map[string]any    `msgpack:"map"`
	Ints   map[string]int    `msgpack:"ints"`
	Strs   map[string]string `msgpack:"strs"`
	Named  asgState          `msgpack:"named"`
	Tags   asgTags           `msgpack:"tags"`
	Floats []float64         `msgpack:"floats"`
	F32s   []float32         `msgpack:"f32s"`
	Nested [][]float64       `msgpack:"nested"`
	Untag  string
	hidden string
}

type AsgFrame struct {
	Width  int    `msgpack:"width"`
	Height int    `msgpack:"height"`
	Data   []byte `msgpack:"data"`
}

type AsgRuntime struct {
	Runtime string `msgpack:"runtime"`
	Label   string `msgpack:"label"`
}

type asgInline struct {
	AsgFrame `msgpack:",inline"`
	Box      asgBox `msgpack:"box"`
}

type asgAutoInline struct {
	AsgFrame
	AsgRuntime
	Label string `msgpack:"label"`
}

type asgShadowed struct {
	Width int `msgpack:"width"`
	AsgFrame
}

type asgExplicitShadowed struct {
	Width    string `msgpack:"width"`
	AsgFrame `msgpack:",inline"`
}

type asgNoInline struct {
	AsgFrame `msgpack:"frame,noinline"`
	Label    string `msgpack:"label"`
}

type asgDoubleInline struct {
	asgInline `msgpack:"-"`
	Outer     asgAutoInline `msgpack:"outer"`
	Inner     asgInline     `msgpack:"inner"`
}

type asgAlias struct {
	Name string `msgpack:"name,alias:title"`
}

type asgTimed struct {
	At time.Time `msgpack:"at"` // alone: msgpack panics on nil into time.Time, a second field makes reaching it map-order dependent
}

type asgCustomValue struct {
	N int
}

func (c *asgCustomValue) DecodeMsgpack(d *msgpack.Decoder) error {
	if err := d.Skip(); err != nil {
		return err
	}
	c.N = 7
	return nil
}

type asgCustom struct {
	V asgCustomValue  `msgpack:"v"`
	P *asgCustomValue `msgpack:"p"`
}

type asgEmbedPtr struct {
	*AsgFrame
	Label string `msgpack:"label"`
}

type asgArray struct {
	A [4]byte    `msgpack:"a"`
	F [2]float64 `msgpack:"f"`
}

type asgOddMaps struct {
	M map[int]string `msgpack:"m"`
}

type asgErr struct {
	E error `msgpack:"e"`
}

type asgAsArray struct {
	_msgpack struct{} `msgpack:",as_array"`
	A        int      `msgpack:"a"`
	B        string   `msgpack:"b"`
}

type asgPtrs struct {
	P  *asgBox  `msgpack:"p"`
	PP **asgBox `msgpack:"pp"`
	PI *int     `msgpack:"pi"`
	PS *string  `msgpack:"ps"`
	PA *any     `msgpack:"pa"`
}

type AsgLabels []string

type asgSliceEmbed struct {
	AsgLabels
	Label string `msgpack:"label"`
}

type asgSliceInline struct {
	AsgLabels `msgpack:",inline"`
}

type asgUnexportedEmbed struct {
	asgBox
	Label string `msgpack:"label"`
}

type asgTimeEmbed struct {
	time.Time
	Label string `msgpack:"label"`
}

type AsgEncodeOnly struct {
	N int `msgpack:"n"`
}

func (AsgEncodeOnly) EncodeMsgpack(e *msgpack.Encoder) error {
	return e.EncodeInt(1)
}

type asgEncodeOnlyEmbed struct {
	AsgEncodeOnly
	Label string `msgpack:"label"`
}

type AsgAliased struct {
	Name string `msgpack:"name,alias:title"`
}

type asgAliasedEmbed struct {
	AsgAliased
	Label string `msgpack:"label"`
}

type asgGen struct {
	r *rand.Rand
}

var _ = []any{asgNumbers{hidden: ""}, asgAsArray{_msgpack: struct{}{}}} // msgpack reads these through reflection only

var asgTargets = []reflect.Type{
	reflect.TypeFor[asgEvent](),
	reflect.TypeFor[*asgAttachments](),
	reflect.TypeFor[asgTraceTick](),
	reflect.TypeFor[asgNumbers](),
	reflect.TypeFor[*asgNumbers](),
	reflect.TypeFor[asgInline](),
	reflect.TypeFor[asgAutoInline](),
	reflect.TypeFor[asgShadowed](),
	reflect.TypeFor[asgExplicitShadowed](),
	reflect.TypeFor[asgNoInline](),
	reflect.TypeFor[asgDoubleInline](),
	reflect.TypeFor[asgAlias](),
	reflect.TypeFor[asgTimed](),
	reflect.TypeFor[asgCustom](),
	reflect.TypeFor[asgEmbedPtr](),
	reflect.TypeFor[asgArray](),
	reflect.TypeFor[asgOddMaps](),
	reflect.TypeFor[asgErr](),
	reflect.TypeFor[asgAsArray](),
	reflect.TypeFor[asgPtrs](),
	reflect.TypeFor[asgSliceEmbed](),
	reflect.TypeFor[asgUnexportedEmbed](),
	reflect.TypeFor[asgTimeEmbed](),
	reflect.TypeFor[asgEncodeOnlyEmbed](),
	reflect.TypeFor[asgAliasedEmbed](),
	reflect.TypeFor[[]asgDetection](),
	reflect.TypeFor[[][]float64](),
	reflect.TypeFor[[]float64](),
	reflect.TypeFor[[]string](),
	reflect.TypeFor[map[string]string](),
}

func legacyCoerceValue(val any, target reflect.Type, pooled bool) reflect.Value {
	if val == nil {
		return reflect.Zero(target)
	}
	v := reflect.ValueOf(val)
	if v.Type().AssignableTo(target) {
		return v
	}
	if v.Type().ConvertibleTo(target) {
		return v.Convert(target)
	}
	if isNumericKind(v.Kind()) && isNumericKind(target.Kind()) {
		return v.Convert(target)
	}
	if v.Kind() == reflect.Slice && target.Kind() == reflect.Slice {
		result := reflect.MakeSlice(target, v.Len(), v.Len())
		for i := range v.Len() {
			result.Index(i).Set(legacyCoerceValue(v.Index(i).Interface(), target.Elem(), pooled))
		}
		return result
	}
	roundTrip := func(dst any) error {
		coerced := coerceValues(val)
		if pooled {
			return convertPooled(coerced, dst)
		}
		data, err := msgpack.Marshal(coerced)
		if err != nil {
			return err
		}
		return Decode(data, dst)
	}
	if v.Kind() == reflect.Map && target.Kind() == reflect.Struct {
		newVal := reflect.New(target)
		if roundTrip(newVal.Interface()) == nil {
			return newVal.Elem()
		}
	}
	if v.Kind() == reflect.Map && target.Kind() == reflect.Pointer && target.Elem().Kind() == reflect.Struct {
		newVal := reflect.New(target.Elem())
		if roundTrip(newVal.Interface()) == nil {
			return newVal
		}
	}
	return reflect.Zero(target)
}

func decodeArg(t testing.TB, wire []byte) any {
	t.Helper()
	arg, err := tryDecodeArg(wire)
	if err != nil {
		t.Fatalf("DecodeMessageInto: %v", err)
	}
	return arg
}

func tryDecodeArg(wire []byte) (any, error) {
	var msg RPCMessage
	if err := DecodeMessageInto(wire, &msg); err != nil {
		return nil, err
	}
	return NormalizeUndefined(msg.Params.([]any)[0]), nil
}

func encodeArg(t testing.TB, arg any) []byte {
	t.Helper()
	wire, err := EncodeMessage(RPCMessage{ID: "1", Method: "m", Params: []any{arg}})
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	return wire
}

func coerceOrPanic(fn func() reflect.Value) (result any, panicked string) {
	defer func() {
		if r := recover(); r != nil {
			panicked = fmt.Sprint(r)
		}
	}()
	return fn().Interface(), ""
}

func checkCoerceParity(t testing.TB, target reflect.Type, wire []byte) {
	t.Helper()
	wireNew := bytes.Clone(wire)
	argNew, errNew := tryDecodeArg(wireNew)
	argRef, errRef := tryDecodeArg(wire)
	if errNew != nil || errRef != nil {
		if (errNew == nil) != (errRef == nil) {
			t.Fatalf("decode differs: %v, %v", errNew, errRef)
		}
		return
	}

	got, gotPanic := coerceOrPanic(func() reflect.Value { return coerceValue(argNew, target) })
	want, wantPanic := coerceOrPanic(func() reflect.Value { return legacyCoerceValue(argRef, target, false) })
	if gotPanic != wantPanic {
		t.Fatalf("%v: panic %q, reference panic %q", target, gotPanic, wantPanic)
	}
	// a struct field must never view the frame
	if k := target.Kind(); k == reflect.Struct || k == reflect.Pointer {
		for i := range wireNew {
			wireNew[i] = 0xAA
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%v: mismatch\n got  %#v\n want %#v", target, got, want)
	}

	// callHandler hands a struct parameter the map as decoded, undefined included
	if structParam(target) {
		raw, err := rawDecodeArg(bytes.Clone(wire))
		if err != nil {
			t.Fatal(err)
		}
		gotRaw, rawPanic := coerceOrPanic(func() reflect.Value { return coerceValue(raw, target) })
		if rawPanic != wantPanic || !reflect.DeepEqual(gotRaw, want) {
			t.Fatalf("%v: mismatch without normalizing\n got  %#v (panic %q)\n want %#v", target, gotRaw, rawPanic, want)
		}
	}
}

func rawDecodeArg(wire []byte) (any, error) {
	var msg RPCMessage
	if err := DecodeMessageInto(wire, &msg); err != nil {
		return nil, err
	}
	return msg.Params.([]any)[0], nil
}

func TestAssignGenericMatchesRoundTripEdges(t *testing.T) {
	big := bytes.Repeat([]byte{7}, binaryExtractThreshold+5)
	cases := []struct {
		name   string
		target reflect.Type
		arg    any
	}{
		{"integral float in any", reflect.TypeFor[asgNumbers](), map[string]any{"any": 3.0, "anys": []any{4.0, 2.5}}},
		{"fraction into int", reflect.TypeFor[asgNumbers](), map[string]any{"i": 2.5, "s": "kept?"}},
		{"double into float32", reflect.TypeFor[asgNumbers](), map[string]any{"f32": 0.75, "s": "x"}},
		{"float32 into float32", reflect.TypeFor[asgNumbers](), map[string]any{"f32": float32(0.75), "f32s": []any{float32(0.5), int8(3), nil}}},
		{"uint8 wraps", reflect.TypeFor[asgNumbers](), map[string]any{"u8": 300, "i8": int64(200)}},
		{"negative into uint64", reflect.TypeFor[asgNumbers](), map[string]any{"u64": -1}},
		{"big uint64 into int", reflect.TypeFor[asgNumbers](), map[string]any{"i64": uint64(math.MaxUint64), "i": uint64(1 << 63), "f64": uint64(math.MaxUint64)}},
		{"node timestamp", reflect.TypeFor[asgEvent](), map[string]any{"startTime": 1.759e12, "lastUpdate": float64(1759000000123)}},
		{"huge float into int", reflect.TypeFor[asgNumbers](), map[string]any{"i64": 1e300}},
		{"string and bytes swap", reflect.TypeFor[asgNumbers](), map[string]any{"s": []byte("abc"), "bin": "def", "strs": map[string]any{"a": []byte("x"), "b": nil}}},
		{"empty and nil", reflect.TypeFor[asgNumbers](), map[string]any{"bin": []byte{}, "anys": []any{}, "map": map[string]any{}, "ints": map[string]any{}, "floats": []any{}, "tags": nil, "s": ""}},
		{"empty string into bytes", reflect.TypeFor[asgNumbers](), map[string]any{"bin": ""}},
		{"unknown keys", reflect.TypeFor[asgNumbers](), map[string]any{"nope": 1, "Untag": "u", "hidden": "h"}},
		{"bool mismatch", reflect.TypeFor[asgNumbers](), map[string]any{"b": 1}},
		{"string into int", reflect.TypeFor[asgNumbers](), map[string]any{"i": "1"}},
		{"array into struct field", reflect.TypeFor[asgPtrs](), map[string]any{"p": []any{1.0, 2.0, 3.0, 4.0}}},
		{"pointers", reflect.TypeFor[asgPtrs](), map[string]any{"p": map[string]any{"x": 1}, "pp": map[string]any{"y": 2.5}, "pi": 3, "ps": "s", "pa": []any{1, "a"}}},
		{"nil pointers", reflect.TypeFor[asgPtrs](), map[string]any{"p": nil, "pp": nil, "pi": nil}},
		{"undefined", reflect.TypeFor[asgNumbers](), map[string]any{"s": &msgpackrUndefined{}, "any": &msgpackrUndefined{}, "anys": []any{&msgpackrUndefined{}}}},
		{"time in any", reflect.TypeFor[asgNumbers](), map[string]any{"any": time.Unix(1759000000, 5)}},
		{"bytes in any", reflect.TypeFor[asgNumbers](), map[string]any{"any": big, "anys": []any{big, []byte{1}}, "map": map[string]any{"b": big}}},
		{"cuib fields", reflect.TypeFor[*asgAttachments](), map[string]any{"scene": big, "attributes": []any{big, nil, []byte{1, 2}}}},
		{"explicit inline", reflect.TypeFor[asgInline](), map[string]any{"width": 640, "data": big, "box": map[string]any{"x": 0.5}}},
		{"inline whole key", reflect.TypeFor[asgInline](), map[string]any{"width": 640, "AsgFrame": map[string]any{"height": 2}}},
		{"auto inline", reflect.TypeFor[asgAutoInline](), map[string]any{"width": 1, "runtime": "coreml", "label": "outer"}},
		{"auto inline shadowed", reflect.TypeFor[asgShadowed](), map[string]any{"width": 1, "height": 2, "AsgFrame": map[string]any{"width": 3}}},
		{"explicit inline shadowed", reflect.TypeFor[asgExplicitShadowed](), map[string]any{"width": "w", "height": 2}},
		{"noinline", reflect.TypeFor[asgNoInline](), map[string]any{"frame": map[string]any{"width": 3}, "width": 9}},
		{"nested inline", reflect.TypeFor[asgDoubleInline](), map[string]any{"outer": map[string]any{"width": 1, "label": "l"}, "inner": map[string]any{"height": 4}}},
		{"alias", reflect.TypeFor[asgAlias](), map[string]any{"title": "t"}},
		{"time field", reflect.TypeFor[asgTimed](), map[string]any{"at": time.Unix(5, 0)}},
		{"nil time field", reflect.TypeFor[asgTimed](), map[string]any{"at": nil}},
		{"custom decoder", reflect.TypeFor[asgCustom](), map[string]any{"v": "x", "p": nil}},
		{"embedded pointer", reflect.TypeFor[asgEmbedPtr](), map[string]any{"width": 2, "label": "l"}},
		{"embedded slice", reflect.TypeFor[asgSliceEmbed](), map[string]any{"AsgLabels": []any{"a", nil}, "label": "l"}},
		{"inline slice", reflect.TypeFor[asgSliceInline](), map[string]any{"AsgLabels": []any{"a"}}},
		{"unexported embed", reflect.TypeFor[asgUnexportedEmbed](), map[string]any{"x": 0.5, "height": 2, "label": "l"}},
		{"embedded time", reflect.TypeFor[asgTimeEmbed](), map[string]any{"Time": time.Unix(5, 0), "label": "l"}},
		{"embed with encoder", reflect.TypeFor[asgEncodeOnlyEmbed](), map[string]any{"AsgEncodeOnly": map[string]any{"n": 3}, "n": 4, "label": "l"}},
		{"embed with alias", reflect.TypeFor[asgAliasedEmbed](), map[string]any{"name": "n", "label": "l"}},
		{"arrays", reflect.TypeFor[asgArray](), map[string]any{"a": []byte{1, 2}, "f": []any{1.5}}},
		{"array from string", reflect.TypeFor[asgArray](), map[string]any{"a": "abcd", "f": []any{1, float32(2)}}},
		{"array too long", reflect.TypeFor[asgArray](), map[string]any{"f": []any{1.5, 2.5, 3.5}, "a": []byte{1}}},
		{"byte array too long", reflect.TypeFor[asgArray](), map[string]any{"a": []byte{1, 2, 3, 4, 5}}},
		{"landmarks", reflect.TypeFor[*asgAttachments](), map[string]any{"attributeLandmarks": []any{[]any{[]any{0.5, 1}}, nil, []any{[]any{0.1}}}}},
		{"int keyed map", reflect.TypeFor[asgOddMaps](), map[string]any{"m": map[string]any{"1": "a"}}},
		{"error field", reflect.TypeFor[asgErr](), map[string]any{"e": "boom"}},
		{"as_array", reflect.TypeFor[asgAsArray](), map[string]any{"a": 1, "b": "b"}},
		{"named map keys", reflect.TypeFor[asgTraceTick](), map[string]any{"boxes": map[string]any{"a": map[string]any{"x": 1}, "b": nil}}},
		{"faces", reflect.TypeFor[[][]float64](), []any{[]any{0.5, 1, int8(-2), float32(0.25), nil, "x", true}, nil, []any{}}},
		{"strings", reflect.TypeFor[[]string](), []any{"a", []byte("b"), nil, int64(65), 1.5}},
		{"detections", reflect.TypeFor[[]asgDetection](), []any{map[string]any{"label": "person", "score": 0.9}, "bad", nil}},
		{"map target", reflect.TypeFor[map[string]string](), map[string]any{"a": "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checkCoerceParity(t, tc.target, encodeArg(t, tc.arg))
		})
	}
}

func TestAssignGenericMatchesRoundTripRandom(t *testing.T) {
	g := asgGen{r: rand.New(rand.NewPCG(1, 2))}
	iterations := 400
	if testing.Short() {
		iterations = 50
	}
	for _, target := range asgTargets {
		t.Run(target.String(), func(t *testing.T) {
			for range iterations {
				checkCoerceParity(t, target, encodeArg(t, g.forType(target, 0)))
			}
		})
	}
}

func FuzzAssignGenericMatchesRoundTrip(f *testing.F) {
	g := asgGen{r: rand.New(rand.NewPCG(3, 4))}
	for _, target := range asgTargets {
		for range 4 {
			wire, err := Encode(g.forType(target, 0))
			if err != nil {
				f.Fatal(err)
			}
			f.Add(wire)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var arg any
		if err := Decode(data, &arg); err != nil || hasOrderDependentKey(arg) {
			return
		}
		wire, err := EncodeMessage(RPCMessage{ID: "1", Method: "m", Params: []any{arg}})
		if err != nil {
			return
		}
		for _, target := range asgTargets {
			checkCoerceParity(t, target, wire)
		}
	})
}

func hasOrderDependentKey(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		for key, item := range x {
			if key == "AsgFrame" || key == "AsgRuntime" || key == "asgBox" || key == "AsgAliased" || key == "title" || hasOrderDependentKey(item) {
				return true
			}
		}
	case []any:
		if slices.ContainsFunc(x, hasOrderDependentKey) {
			return true
		}
	}
	return false
}

func TestAssignGenericTakesIngestShapes(t *testing.T) {
	for _, fx := range ingestFixtures(t) {
		arg := coerceValues(decodeArg(t, fx.wire))
		if !assignGeneric(reflect.New(fx.target).Elem(), arg) {
			t.Fatalf("%s: assigner fell back on a plain ingest shape", fx.name)
		}
	}
}

func TestAssignGenericFallsBackWhereMsgpackDiffers(t *testing.T) {
	cases := []struct {
		target reflect.Type
		arg    any
	}{
		{reflect.TypeFor[asgNumbers](), map[string]any{"i": 2.5}},
		{reflect.TypeFor[asgNumbers](), map[string]any{"f32": 0.75}},
		{reflect.TypeFor[asgNumbers](), map[string]any{"any": time.Unix(1, 0)}},
		{reflect.TypeFor[asgInline](), map[string]any{"AsgFrame": map[string]any{}}},
		{reflect.TypeFor[asgAlias](), map[string]any{}},
		{reflect.TypeFor[asgTimed](), map[string]any{"at": time.Unix(1, 0)}},
		{reflect.TypeFor[asgCustom](), map[string]any{"v": "x"}},
		{reflect.TypeFor[asgEmbedPtr](), map[string]any{}},
		{reflect.TypeFor[asgSliceInline](), map[string]any{}},
		{reflect.TypeFor[asgUnexportedEmbed](), map[string]any{}},
		{reflect.TypeFor[asgTimeEmbed](), map[string]any{}},
		{reflect.TypeFor[asgEncodeOnlyEmbed](), map[string]any{}},
		{reflect.TypeFor[asgAliasedEmbed](), map[string]any{}},
		{reflect.TypeFor[asgAsArray](), map[string]any{}},
		{reflect.TypeFor[asgPtrs](), map[string]any{"p": []any{1.0}}},
		{reflect.TypeFor[asgArray](), map[string]any{"f": []any{1.5, 2.5, 3.5}}},
	}
	for _, tc := range cases {
		if assignGeneric(reflect.New(tc.target).Elem(), tc.arg) {
			t.Errorf("%v %v: assigned, want a fall back to the round trip", tc.target, tc.arg)
		}
	}
}

func TestCoerceStructAllocatesLessThanRoundTrip(t *testing.T) {
	for _, fx := range ingestFixtures(t) {
		if fx.target.Kind() == reflect.Slice {
			continue
		}
		arg := coerceValues(decodeArg(t, fx.wire))
		current := testing.AllocsPerRun(20, func() { coerceValue(arg, fx.target) })
		reference := testing.AllocsPerRun(20, func() { legacyCoerceValue(arg, fx.target, false) })
		if current > reference*3/4 {
			t.Errorf("%s: %.0f allocs, round trip %.0f", fx.name, current, reference)
		}
	}
}

func TestConvertPooledAllocatesOnlyTheDecodedCopy(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	src := map[string]any{"data": bytes.Repeat([]byte{1}, 1<<20), "width": int64(4)}
	var dst AsgFrame
	if err := convertPooled(src, &dst); err != nil || len(dst.Data) != 1<<20 {
		t.Fatalf("convertPooled: %v, %d bytes", err, len(dst.Data))
	}
	// TotalAlloc is process wide and other tests' goroutines only add to it, so the least of several batches counts
	perCall := func(fn func()) uint64 {
		least := uint64(math.MaxUint64)
		for range 5 {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for range 10 {
				fn()
			}
			runtime.ReadMemStats(&after)
			least = min(least, (after.TotalAlloc-before.TotalAlloc)/10)
		}
		return least
	}
	pooled := perCall(func() { _ = convertPooled(src, &AsgFrame{}) })
	marshal := perCall(func() {
		data, _ := msgpack.Marshal(src)
		_ = Decode(data, &AsgFrame{})
	})
	if pooled > marshal*3/4 {
		t.Fatalf("convertPooled allocates %d bytes per 1 MiB call, Marshal+Decode %d", pooled, marshal)
	}
}

func TestDecodeFallbackStillCoercesThroughPool(t *testing.T) {
	type target struct {
		N int64  `msgpack:"n"`
		S string `msgpack:"s"`
	}
	data, err := msgpack.Marshal(map[string]any{"n": 1.759e12, "s": &msgpackrUndefined{}})
	if err != nil {
		t.Fatal(err)
	}
	var got target
	if err := Decode(data, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.N != 1759000000000 || got.S != "" {
		t.Fatalf("got %+v", got)
	}
}

func TestCallWithCallbackConvertsPushedValues(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	_, err := server.RegisterHandler("cbc", map[string]any{
		"frames": func(cb func(map[string]any)) func() {
			cb(map[string]any{"width": 640, "data": []byte{1, 2, 3}})
			return func() {}
		},
		"ticks": func(cb func(float64)) func() {
			cb(1.759e12)
			return func() {}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	frames := make(chan AsgFrame, 1)
	unsubFrames, err := CallWithCallback(client, "rpc.cbc.frames", nil, func(v AsgFrame) { frames <- v })
	if err != nil {
		t.Fatal(err)
	}
	defer unsubFrames()
	ticks := make(chan int64, 1)
	unsubTicks, err := CallWithCallback(client, "rpc.cbc.ticks", nil, func(v int64) { ticks <- v })
	if err != nil {
		t.Fatal(err)
	}
	defer unsubTicks()

	select {
	case got := <-frames:
		if got.Width != 640 || !bytes.Equal(got.Data, []byte{1, 2, 3}) {
			t.Errorf("frame %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("no frame pushed")
	}
	select {
	case got := <-ticks:
		if got != 1759000000000 {
			t.Errorf("tick %d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("no tick pushed")
	}
}

func BenchmarkCoerceValue(b *testing.B) {
	for _, fx := range ingestFixtures(b) {
		arg := coerceValues(decodeArg(b, fx.wire))
		b.Run("fixture="+fx.name+"/impl=roundtrip", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				legacyCoerceValue(arg, fx.target, false)
			}
		})
		b.Run("fixture="+fx.name+"/impl=pooled", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				legacyCoerceValue(arg, fx.target, true)
			}
		})
		b.Run("fixture="+fx.name+"/impl=assign", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				coerceValue(arg, fx.target)
			}
		})
	}
}

func BenchmarkConvertPooled(b *testing.B) {
	g := asgGen{r: rand.New(rand.NewPCG(7, 8))}
	cases := []struct {
		name string
		src  any
		dst  func() any
	}{
		{"attachments", g.attachments(25), func() any { return &asgAttachments{} }},
		{"bin1MB", map[string]any{"data": g.payload(1 << 20), "width": int64(4)}, func() any { return &AsgFrame{} }},
		{"detections30", g.detections(30), func() any { return &[]asgDetection{} }},
	}
	for _, tc := range cases {
		src := coerceValues(tc.src)
		b.Run("value="+tc.name+"/impl=marshal", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				data, err := msgpack.Marshal(src)
				if err != nil {
					b.Fatal(err)
				}
				if err := Decode(data, tc.dst()); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("value="+tc.name+"/impl=pooled", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := convertPooled(src, tc.dst()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type asgFixture struct {
	name   string
	target reflect.Type
	wire   []byte
}

func ingestFixtures(t testing.TB) []asgFixture {
	t.Helper()
	g := asgGen{r: rand.New(rand.NewPCG(5, 6))}
	frame := func(size int) map[string]any {
		return map[string]any{"width": 320.0, "height": 180.0, "data": g.payload(size)}
	}
	out := make([]asgFixture, 0, 11)
	for _, kb := range []int{2, 58, 307, 1229} {
		out = append(out, asgFixture{fmt.Sprintf("frame%dKB", kb), reflect.TypeFor[AsgFrame](), encodeArg(t, frame(kb<<10))})
	}
	out = append(out,
		asgFixture{"eventUpdate", reflect.TypeFor[asgEvent](), encodeArg(t, g.event(2))},
		asgFixture{"eventEnd20Segments", reflect.TypeFor[asgEvent](), encodeArg(t, g.event(20))},
		asgFixture{"attachUpdate60KB", reflect.TypeFor[*asgAttachments](), encodeArg(t, map[string]any{"scene": g.payload(60 << 10)})},
		asgFixture{"attachSegEnd110KB40Ticks", reflect.TypeFor[*asgAttachments](), encodeArg(t, g.attachments(40))},
		asgFixture{"attachEnd400Ticks", reflect.TypeFor[*asgAttachments](), encodeArg(t, g.attachments(400))},
		asgFixture{"matchFaces3x512", reflect.TypeFor[[][]float64](), encodeArg(t, []any{g.embedding(), g.embedding(), g.embedding()})},
		asgFixture{"detections30", reflect.TypeFor[[]asgDetection](), encodeArg(t, g.detections(30))},
	)
	return out
}

func (g *asgGen) chance(p float64) bool {
	return g.r.Float64() < p
}

func (g *asgGen) payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(g.r.UintN(256))
	}
	return b
}

func (g *asgGen) embedding() []any {
	out := make([]any, 512)
	for i := range out {
		out[i] = g.r.Float64()*2 - 1
	}
	return out
}

func (g *asgGen) timestamp() float64 {
	return float64(1759000000000 + g.r.Int64N(10_000_000))
}

func (g *asgGen) box() map[string]any {
	return map[string]any{"x": g.r.Float64(), "y": g.r.Float64(), "width": g.r.Float64(), "height": g.r.Float64()}
}

func (g *asgGen) detections(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = map[string]any{
			"label": "person", "score": g.r.Float64(), "maxCount": 1.0, "box": g.box(),
			"trackId": float64(i + 1), "zones": []any{"yard"}, "firstSeen": g.timestamp(), "moving": true,
		}
	}
	return out
}

func (g *asgGen) event(segments int) map[string]any {
	segs := make([]any, segments)
	for i := range segs {
		segs[i] = map[string]any{
			"firstSeen": g.timestamp(), "lastSeen": g.timestamp(), "stripWindow": g.box(),
			"detections":       g.detections(3),
			"attributes":       []any{map[string]any{"type": "face", "label": "anna", "confidence": 0.8, "embedding": g.embedding()}},
			"zones":            []any{"yard", "door"},
			"personEmbeddings": []any{map[string]any{"trackId": 1.0, "embedding": g.embedding(), "embeddingModel": "osnet"}},
		}
	}
	return map[string]any{
		"id": "evt-1", "cameraId": "cam-1", "state": "active", "startTime": g.timestamp(), "lastUpdate": g.timestamp(),
		"types": []any{"person"}, "triggers": []any{map[string]any{"type": "object", "label": "person", "score": 0.9}},
		"segments": segs, "segmentIndex": float64(segments - 1), "favorite": false, "endTime": &msgpackrUndefined{},
	}
}

func (g *asgGen) attachments(ticks int) map[string]any {
	trace := make([]any, ticks)
	for i := range trace {
		trace[i] = map[string]any{
			"tMs": g.timestamp(), "rtp": float64(g.r.Uint32()), "src": "low", "objectRan": true,
			"detections": []any{map[string]any{"label": "person", "score": 0.8, "box": []any{0.1, 0.2, 0.3, 0.4}, "track": 1.0}},
			"between":    []any{map[string]any{"tMs": g.timestamp(), "boxes": []any{[]any{0.1, 0.2, "person", 0.9}}}},
			"world":      []any{map[string]any{"id": 1.0, "label": "person", "state": "visible"}},
			"confidence": map[string]any{"person": 0.5},
		}
	}
	return map[string]any{
		"scene": g.payload(60 << 10), "strip": g.payload(30 << 10), "card": g.payload(20 << 10),
		"attributes":         []any{g.payload(8 << 10), nil},
		"attributeLandmarks": []any{[]any{[]any{0.1, 0.2}, []any{0.3, 0.4}}, &msgpackrUndefined{}},
		"trace":              trace,
	}
}

func (g *asgGen) forType(t reflect.Type, depth int) any {
	if g.chance(0.04) {
		return nil
	}
	if g.chance(0.03) {
		return g.anyValue(depth + 1)
	}
	switch t.Kind() {
	case reflect.Bool:
		return g.r.IntN(2) == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return g.number(true)
	case reflect.Float32, reflect.Float64:
		return g.number(false)
	case reflect.String:
		if g.chance(0.1) {
			return []byte(g.word())
		}
		return g.word()
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return g.binary()
		}
		n := g.r.IntN(4)
		if t.Kind() == reflect.Array {
			n = t.Len() + g.r.IntN(3) - 1
		}
		items := make([]any, n)
		for i := range items {
			items[i] = g.forType(t.Elem(), depth+1)
		}
		return items
	case reflect.Map:
		m := map[string]any{}
		for range g.r.IntN(3) {
			m[g.word()] = g.forType(t.Elem(), depth+1)
		}
		return m
	case reflect.Struct:
		if t == timeType {
			return time.Unix(g.r.Int64N(1<<32), g.r.Int64N(1e9))
		}
		m := map[string]any{}
		for _, f := range reflect.VisibleFields(t) {
			// an embedded struct's own name overlaps its inlined fields, msgpack's result then depends on map order
			if !f.IsExported() || f.Anonymous || f.Tag.Get("msgpack") == "-" || !g.chance(0.75) {
				continue
			}
			name, _, _ := strings.Cut(f.Tag.Get("msgpack"), ",")
			if name == "" {
				name = f.Name
			}
			m[name] = g.forType(f.Type, depth+1)
		}
		if g.chance(0.1) {
			m["unknown"] = g.anyValue(depth + 1)
		}
		return m
	case reflect.Pointer:
		return g.forType(t.Elem(), depth)
	default:
		return g.anyValue(depth + 1)
	}
}

func (g *asgGen) number(integral bool) any {
	switch g.r.IntN(12) {
	case 0:
		return int8(g.r.IntN(256) - 128)
	case 1:
		return int16(g.r.IntN(1 << 16))
	case 2:
		return g.r.Int64N(1<<40) - 1<<39
	case 3:
		return uint8(g.r.UintN(256))
	case 4:
		return uint32(g.r.Uint32())
	case 5:
		return g.r.Uint64()
	case 6:
		return g.timestamp()
	case 7:
		return float64(g.r.IntN(2000) - 1000)
	case 8:
		return float32(g.r.IntN(64)) / 4
	case 9:
		if integral {
			return 2.5
		}
		return g.r.Float64()
	case 10:
		return -1e19
	default:
		return int64(g.r.IntN(1000))
	}
}

func (g *asgGen) word() string {
	words := []string{"", "a", "person", "yard", "width", "label", "data", "x", "frame"}
	return words[g.r.IntN(len(words))]
}

func (g *asgGen) binary() any {
	switch g.r.IntN(6) {
	case 0:
		return []byte{}
	case 1:
		return g.word()
	case 2:
		return g.payload(binaryExtractThreshold + g.r.IntN(64))
	default:
		return g.payload(g.r.IntN(48))
	}
}

func (g *asgGen) anyValue(depth int) any {
	n := 13
	if depth > 3 {
		n = 10
	}
	switch g.r.IntN(n) {
	case 0:
		return nil
	case 1:
		return g.chance(0.5)
	case 2, 3:
		return g.number(g.chance(0.5))
	case 4:
		return g.word()
	case 5:
		return g.binary()
	case 6:
		return &msgpackrUndefined{}
	case 7:
		return float32(0.75)
	case 8:
		if g.chance(0.2) {
			return time.Unix(g.r.Int64N(1<<32), 0)
		}
		return 1.5
	case 9:
		return "boom"
	case 10, 11:
		items := make([]any, g.r.IntN(4))
		for i := range items {
			items[i] = g.anyValue(depth + 1)
		}
		return items
	default:
		m := map[string]any{}
		for range g.r.IntN(4) {
			m[g.word()] = g.anyValue(depth + 1)
		}
		return m
	}
}

func BenchmarkHandlerArg(b *testing.B) {
	for _, fx := range ingestFixtures(b) {
		fn := reflect.MakeFunc(reflect.FuncOf([]reflect.Type{fx.target}, nil, false), func([]reflect.Value) []reflect.Value { return nil })
		b.Run("fixture="+fx.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var msg RPCMessage
				if err := DecodeMessageInto(fx.wire, &msg); err != nil {
					b.Fatal(err)
				}
				if _, err := callHandler(b.Context(), fn, msg.Params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestCallHandlerAssignsStructsWithoutTheCoerceRound(t *testing.T) {
	for _, fx := range ingestFixtures(t) {
		if fx.name != "eventEnd20Segments" && fx.name != "attachEnd400Ticks" {
			continue
		}
		t.Run(fx.name, func(t *testing.T) {
			var msg RPCMessage
			if err := DecodeMessageInto(fx.wire, &msg); err != nil {
				t.Fatal(err)
			}
			arg := msg.Params.([]any)[0]
			target := fx.target
			if target.Kind() == reflect.Pointer {
				target = target.Elem()
			}
			fn := reflect.MakeFunc(reflect.FuncOf([]reflect.Type{fx.target}, nil, false), func([]reflect.Value) []reflect.Value { return nil })
			now := testing.AllocsPerRun(10, func() { _, _ = callHandler(context.Background(), fn, msg.Params) })
			coerced := testing.AllocsPerRun(10, func() { convertToStruct(coerceValues(arg), target) })
			if now >= coerced {
				t.Errorf("callHandler %v allocations, coerceValues plus assign %v", now, coerced)
			}
		})
	}
}

func TestCallHandlerLeavesTheMapOfAStructParameterUnwalked(t *testing.T) {
	var got asgEvent
	fn := reflect.ValueOf(func(e asgEvent) { got = e })
	arg := map[string]any{"id": "e1", "endTime": &msgpackrUndefined{}}
	if _, err := callHandler(context.Background(), fn, []any{arg}); err != nil {
		t.Fatal(err)
	}
	if got.ID != "e1" || got.EndTime != 0 {
		t.Errorf("event = %+v", got)
	}
	// the assigner reads undefined as nil, a walk for it would be a pass over the whole argument
	if _, ok := arg["endTime"].(*msgpackrUndefined); !ok {
		t.Errorf("endTime = %#v, the argument was walked for undefined", arg["endTime"])
	}
}
