package rpc

import (
	"math/rand/v2"
	"testing"
	"time"
)

type containsOpaque struct{ Data []byte }

func (containsOpaque) MarshalMsgpack() ([]byte, error) { return []byte{0xc0}, nil }

type containsInner struct {
	Frame []byte `msgpack:"frame,omitempty"`
}

type containsHidden struct {
	Data []byte
}

type containsOuter struct {
	*containsInner
	containsHidden
	Name    string `msgpack:"name"`
	Skipped []byte `msgpack:"-"`
	secret  []byte
	Opaque  containsOpaque   `msgpack:"opaque"`
	When    time.Time        `msgpack:"when"`
	Any     any              `msgpack:"any,omitempty"`
	Items   []containsInner  `msgpack:"items"`
	Ptrs    []*containsInner `msgpack:"ptrs"`
	ByName  map[string][]byte
	Fixed   [20000]byte
}

type namedBytes []byte

type namedProps map[string]any

func containsFixtures() map[string]any {
	big := make([]byte, binaryExtractThreshold)
	small := make([]byte, binaryExtractThreshold-1)
	return map[string]any{
		"nil":               nil,
		"big bytes":         big,
		"small bytes":       small,
		"named big":         namedBytes(big),
		"pointer to big":    &big,
		"byte array":        [binaryExtractThreshold]byte{},
		"nested map":        map[string]any{"a": map[string]any{"b": []any{1, "x", big}}},
		"nested small":      map[string]any{"a": map[string]any{"b": []any{1, "x", small}}},
		"typed map":         map[string][]byte{"a": small, "b": big},
		"int keyed map":     map[int]any{1: big},
		"named map":         namedProps{"frame": big},
		"typed slices":      [][]byte{small, big},
		"embedded pointer":  containsOuter{containsInner: &containsInner{Frame: big}},
		"embedded nil":      containsOuter{},
		"embedded value":    containsOuter{containsHidden: containsHidden{Data: big}},
		"skipped field":     containsOuter{Skipped: big},
		"unexported field":  containsOuter{secret: big},
		"opaque field":      containsOuter{Opaque: containsOpaque{Data: big}},
		"interface field":   containsOuter{Any: &containsInner{Frame: big}},
		"nil in interface":  containsOuter{Any: (*containsInner)(nil)},
		"struct slice":      containsOuter{Items: []containsInner{{}, {Frame: big}}},
		"pointer slice":     containsOuter{Ptrs: []*containsInner{nil, {Frame: big}}},
		"map field":         containsOuter{ByName: map[string][]byte{"x": big}},
		"response":          RPCResponse{ID: "1", Result: []any{map[string]any{"jpeg": big}}},
		"response pointer":  &RPCResponse{ID: "1", Error: &RPCError{Code: "X", Data: big}},
		"message":           RPCMessage{ID: "1", Params: []any{containsOuter{Ptrs: []*containsInner{{Frame: small}}}}},
		"message with big":  RPCMessage{ID: "1", Params: []any{"cam", namedBytes(big)}},
		"pull response":     PullIteratorResponse{ID: "1", Type: "value", Value: big},
		"stream frame":      &StreamMessage{ID: "1", Type: "data", Data: map[string]any{"chunk": small}},
		"callback batch":    []any{[]any{[]any{big}}},
		"time":              time.Now(),
		"opaque at the top": containsOpaque{Data: big},
	}
}

func extractsSomething(v any) bool {
	var segs [][]byte
	_, changed := extractValue(v, &segs)
	return changed || len(segs) > 0
}

func TestContainsBinaryMatchesTheExtraction(t *testing.T) {
	for name, v := range containsFixtures() {
		if got, want := containsBinary(v), extractsSomething(v); got != want {
			t.Errorf("%s: containsBinary %v, extraction %v", name, got, want)
		}
	}

	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 2000 {
		v := randomContainsValue(rng, 4)
		if got, want := containsBinary(v), extractsSomething(v); got != want {
			t.Fatalf("random value %d: containsBinary %v, extraction %v", i, got, want)
		}
	}
}

func randomContainsValue(rng *rand.Rand, depth int) any {
	if depth == 0 {
		return rng.IntN(10)
	}
	switch rng.IntN(9) {
	case 0:
		return make([]byte, binaryExtractThreshold-2+rng.IntN(4))
	case 1:
		m := map[string]any{}
		for i := range rng.IntN(4) {
			m[string(rune('a'+i))] = randomContainsValue(rng, depth-1)
		}
		return m
	case 2:
		s := make([]any, rng.IntN(4))
		for i := range s {
			s[i] = randomContainsValue(rng, depth-1)
		}
		return s
	case 3:
		return &containsInner{Frame: make([]byte, binaryExtractThreshold-1+rng.IntN(2))}
	case 4:
		items := make([]containsInner, rng.IntN(3))
		for i := range items {
			items[i].Frame = make([]byte, binaryExtractThreshold-1+rng.IntN(2))
		}
		return containsOuter{Items: items, Any: randomContainsValue(rng, depth-1)}
	case 5:
		return RPCResponse{ID: "x", Result: randomContainsValue(rng, depth-1)}
	case 6:
		return namedProps{"v": randomContainsValue(rng, depth-1)}
	case 7:
		return "text"
	default:
		return nil
	}
}

func TestContainsBinaryDoesNotAllocate(t *testing.T) {
	metas := make([]benchMeta, 200)
	reply := &RPCResponse{ID: "x", Result: map[string]any{"faces": metas, "total": 200}}
	if allocs := testing.AllocsPerRun(50, func() { containsBinary(reply) }); allocs != 0 {
		t.Errorf("%v allocations for a reply without binaries", allocs)
	}
}

func TestExtractBinariesSkipsTheWalkWithoutBinaries(t *testing.T) {
	var reply any = &RPCResponse{ID: "x", Result: benchEvents(200)}
	if allocs := testing.AllocsPerRun(50, func() { extractBinaries(reply) }); allocs != 0 {
		t.Errorf("%v allocations for a reply without binaries", allocs)
	}
}
