package rpc

// Behavioral spec: node/src/codec.ts + node/test/codec.test.ts ("message
// codec (CUIB wire format)" suite). Keep the two in sync.

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// Extractable sizes are expressed relative to the threshold so the tests
// survive threshold tuning.
const T = binaryExtractThreshold

func bytesN(n int, fill byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = fill
	}
	return b
}

func mustEncodeMessage(t *testing.T, v any) []byte {
	t.Helper()
	data, err := EncodeMessage(v)
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	return data
}

func mustDecodeMessage(t *testing.T, data []byte) any {
	t.Helper()
	v, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	return v
}

func messageRoundtrip(t *testing.T, v any) any {
	t.Helper()
	return mustDecodeMessage(t, mustEncodeMessage(t, v))
}

func envelopeOf(t *testing.T, frame []byte) (env map[string]any, envLen int) {
	t.Helper()
	if !bytes.Equal(frame[:4], cuibMagic[:]) {
		t.Fatalf("frame missing CUIB magic: % x", frame[:4])
	}
	envLen = int(binary.LittleEndian.Uint32(frame[4:8]))
	if err := Decode(frame[8:8+envLen], &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	return env, envLen
}

func asBytes(t *testing.T, v any) []byte {
	t.Helper()
	b, ok := v.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", v)
	}
	return b
}

func TestEncodeMessagePlainMsgpackWithoutLargeBinaries(t *testing.T) {
	// Byte-identity to Encode() cannot be asserted for Go maps (iteration
	// order randomizes per encode); assert the format instead: no CUIB frame,
	// plain msgpack, content preserved. The struct test below checks
	// byte-identity on a deterministic encoding.
	message := map[string]any{
		"id":     "abc",
		"method": "call",
		"params": []any{1, "x", map[string]any{"nested": true}, []byte{1, 2, 3}},
	}
	framed := mustEncodeMessage(t, message)
	if bytes.Equal(framed[:4], cuibMagic[:]) {
		t.Fatal("binary-free message must not be CUIB-framed")
	}
	var roundtripped map[string]any
	if err := Decode(framed, &roundtripped); err != nil {
		t.Fatalf("output is not plain msgpack: %v", err)
	}
	params := roundtripped["params"].([]any)
	if !bytes.Equal(asBytes(t, params[3]), []byte{1, 2, 3}) {
		t.Fatal("small binary should stay inline")
	}
}

func TestEncodeMessageStructByteIdenticalWithoutLargeBinaries(t *testing.T) {
	message := RPCMessage{ID: "abc", Method: "call", Params: []any{"snapshot", []byte{1, 2, 3}}}
	plain, err := Encode(message)
	if err != nil {
		t.Fatal(err)
	}
	framed := mustEncodeMessage(t, message)
	if !bytes.Equal(plain, framed) {
		t.Fatal("expected struct message without large binaries to be byte-identical to Encode()")
	}
}

func TestEncodeMessageFrameLayout(t *testing.T) {
	bin := bytesN(T+1024, 9)
	message := map[string]any{"id": "abc", "params": []any{bin}}
	encoded := mustEncodeMessage(t, message)

	env, envLen := envelopeOf(t, encoded)
	if env["id"] != "abc" {
		t.Fatalf("envelope id = %v", env["id"])
	}
	params := env["params"].([]any)
	ph, ok := params[0].(map[string]any)
	if !ok {
		t.Fatalf("placeholder missing, got %T", params[0])
	}
	index, length, ok := placeholderInfo(ph)
	if !ok || index != 0 || length != T+1024 {
		t.Fatalf("placeholder = %v (index=%d length=%d ok=%v)", ph, index, length, ok)
	}

	if len(encoded) != 8+envLen+T+1024 {
		t.Fatalf("frame size = %d, want %d", len(encoded), 8+envLen+T+1024)
	}
	if !bytes.Equal(encoded[8+envLen:], bin) {
		t.Fatal("segment does not lie back-to-back after the envelope")
	}
}

func TestEncodeMessageMultipleSegmentsInTraversalOrder(t *testing.T) {
	// []any traversal is deterministic (unlike Go maps), so the indices and
	// segment layout are fixed.
	a := bytesN(T, 1)
	b := bytesN(T+500, 2)
	c := bytesN(T+4096, 3)
	message := map[string]any{"params": []any{a, map[string]any{"deep": []any{b}}, c}}
	encoded := mustEncodeMessage(t, message)

	env, envLen := envelopeOf(t, encoded)
	params := env["params"].([]any)
	i0, l0, _ := placeholderInfo(params[0].(map[string]any))
	deep := params[1].(map[string]any)["deep"].([]any)
	i1, l1, _ := placeholderInfo(deep[0].(map[string]any))
	i2, l2, _ := placeholderInfo(params[2].(map[string]any))
	if i0 != 0 || l0 != T || i1 != 1 || l1 != T+500 || i2 != 2 || l2 != T+4096 {
		t.Fatalf("placeholder order mismatch: (%d,%d) (%d,%d) (%d,%d)", i0, l0, i1, l1, i2, l2)
	}

	base := 8 + envLen
	if encoded[base] != 1 || encoded[base+T] != 2 || encoded[base+T+(T+500)] != 3 {
		t.Fatal("segments not back-to-back in index order")
	}
	if len(encoded) != base+T+(T+500)+(T+4096) {
		t.Fatalf("frame size = %d", len(encoded))
	}
}

func TestEncodeMessageDoesNotMutateInput(t *testing.T) {
	bin := bytesN(T, 7)
	inner := map[string]any{"inner": bin}
	params := []any{bin, inner}
	message := map[string]any{"params": params}
	mustEncodeMessage(t, message)

	if &params[0] != &message["params"].([]any)[0] {
		t.Fatal("params slice was replaced")
	}
	if _, isPh := params[0].([]byte); !isPh {
		t.Fatalf("params[0] mutated to %T", params[0])
	}
	if _, isPh := inner["inner"].([]byte); !isPh {
		t.Fatalf("inner map mutated to %T", inner["inner"])
	}
}

func TestMessageRoundtripBinaryInArgsArray(t *testing.T) {
	bin := bytesN(T+4096, 42)
	message := map[string]any{"id": "x1", "method": "call", "params": []any{"snapshot", bin, map[string]any{"quality": 80}}}
	result := messageRoundtrip(t, message).(map[string]any)
	params := result["params"].([]any)
	if result["id"] != "x1" || params[0] != "snapshot" {
		t.Fatalf("envelope fields lost: %v", result)
	}
	if !bytes.Equal(asBytes(t, params[1]), bin) {
		t.Fatal("binary content mismatch")
	}
	quality := params[2].(map[string]any)["quality"]
	if n, ok := asNonNegativeInt(quality); !ok || n != 80 {
		t.Fatalf("quality = %v", quality)
	}
}

func TestMessageRoundtripBinaryInNestedMap(t *testing.T) {
	bin := bytesN(T+10_000, 5)
	message := map[string]any{"result": map[string]any{"frame": map[string]any{"data": bin, "pts": 1234}, "ok": true}}
	result := messageRoundtrip(t, message).(map[string]any)
	res := result["result"].(map[string]any)
	frame := res["frame"].(map[string]any)
	if pts, ok := asNonNegativeInt(frame["pts"]); !ok || pts != 1234 {
		t.Fatalf("pts = %v", frame["pts"])
	}
	if res["ok"] != true {
		t.Fatal("ok lost")
	}
	if !bytes.Equal(asBytes(t, frame["data"]), bin) {
		t.Fatal("binary content mismatch")
	}
}

func TestMessageRoundtripMultipleBinariesDistinct(t *testing.T) {
	a := bytesN(T, 1)
	b := bytesN(T+2048, 2)
	c := bytesN(T+1, 3)
	message := map[string]any{"params": []any{a, map[string]any{"b": b}, []any{c}}}
	result := messageRoundtrip(t, message).(map[string]any)
	params := result["params"].([]any)
	if !bytes.Equal(asBytes(t, params[0]), a) {
		t.Fatal("segment a mismatch")
	}
	if !bytes.Equal(asBytes(t, params[1].(map[string]any)["b"]), b) {
		t.Fatal("segment b mismatch")
	}
	if !bytes.Equal(asBytes(t, params[2].([]any)[0]), c) {
		t.Fatal("segment c mismatch")
	}
}

func TestMessageKeeps1023ByteBinaryInline(t *testing.T) {
	bin := bytesN(T-1, 7)
	message := map[string]any{"params": []any{bin}}
	encoded := mustEncodeMessage(t, message)
	plain, _ := Encode(message)
	if !bytes.Equal(encoded, plain) {
		t.Fatal("threshold-1 byte binary should stay inline (plain msgpack)")
	}
	result := mustDecodeMessage(t, encoded).(map[string]any)
	if !bytes.Equal(asBytes(t, result["params"].([]any)[0]), bin) {
		t.Fatal("content mismatch")
	}
}

func TestMessageExtracts1024ByteBinary(t *testing.T) {
	bin := bytesN(T, 7)
	message := map[string]any{"params": []any{bin}}
	encoded := mustEncodeMessage(t, message)
	if !bytes.Equal(encoded[:4], cuibMagic[:]) {
		t.Fatal("threshold-sized binary should be extracted (CUIB frame)")
	}
	result := mustDecodeMessage(t, encoded).(map[string]any)
	if !bytes.Equal(asBytes(t, result["params"].([]any)[0]), bin) {
		t.Fatal("content mismatch")
	}
}

func TestMessageRoundtripRootLevelBinary(t *testing.T) {
	bin := bytesN(T+3000, 6)
	result := messageRoundtrip(t, bin)
	if !bytes.Equal(asBytes(t, result), bin) {
		t.Fatal("root-level binary mismatch")
	}
}

func TestMessageRoundtripNamedByteSliceType(t *testing.T) {
	type rawFrame []byte
	bin := rawFrame(bytesN(T+2000, 0xcd))
	result := messageRoundtrip(t, map[string]any{"params": []any{bin}}).(map[string]any)
	if !bytes.Equal(asBytes(t, result["params"].([]any)[0]), bin) {
		t.Fatal("named []byte type mismatch")
	}
}

type testFrame struct {
	Data []byte `msgpack:"data"`
	PTS  int64  `msgpack:"pts"`
	Name string `msgpack:"name,omitempty"`
}

func TestMessageRoundtripStructResult(t *testing.T) {
	bin := bytesN(100_000, 0xab)
	message := RPCResponse{ID: "r1", Result: testFrame{Data: bin, PTS: 42}}
	encoded := mustEncodeMessage(t, message)
	if !bytes.Equal(encoded[:4], cuibMagic[:]) {
		t.Fatal("struct-borne binary was not extracted")
	}

	var resp RPCResponse
	if err := DecodeMessageInto(encoded, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID != "r1" || resp.Error != nil {
		t.Fatalf("envelope fields lost: %+v", resp)
	}
	frame := resp.Result.(map[string]any)
	if !bytes.Equal(asBytes(t, frame["data"]), bin) {
		t.Fatal("frame data mismatch")
	}
	if pts, ok := asNonNegativeInt(frame["pts"]); !ok || pts != 42 {
		t.Fatalf("pts = %v", frame["pts"])
	}
	if _, present := frame["name"]; present {
		t.Fatal("omitempty field should be omitted from re-emitted struct map")
	}
}

func TestMessageStructOmitEmptySiblingsPreserved(t *testing.T) {
	// RPCResponse with nil Error/Methods (both omitempty): the re-emitted
	// envelope map must omit them, like the struct encoder would.
	bin := bytesN(T+2048, 1)
	encoded := mustEncodeMessage(t, RPCResponse{ID: "x", Result: []any{bin}})
	env, _ := envelopeOf(t, encoded)
	if _, present := env["error"]; present {
		t.Fatal("nil Error should be omitted")
	}
	if _, present := env["__methods"]; present {
		t.Fatal("empty Methods should be omitted")
	}
	if env["id"] != "x" {
		t.Fatalf("id = %v", env["id"])
	}
}

func TestMessageRoundtripStructPointerAndNestedSlice(t *testing.T) {
	bin := bytesN(T+5000, 0x11)
	type wrapper struct {
		Frames []testFrame `msgpack:"frames"`
	}
	message := RPCResponse{ID: "p", Result: &wrapper{Frames: []testFrame{{Data: bin, PTS: 1}, {Data: []byte{1}, PTS: 2}}}}
	var resp RPCResponse
	if err := DecodeMessageInto(mustEncodeMessage(t, message), &resp); err != nil {
		t.Fatal(err)
	}
	frames := resp.Result.(map[string]any)["frames"].([]any)
	first := frames[0].(map[string]any)
	if !bytes.Equal(asBytes(t, first["data"]), bin) {
		t.Fatal("nested struct slice binary mismatch")
	}
	second := frames[1].(map[string]any)
	if !bytes.Equal(asBytes(t, second["data"]), []byte{1}) {
		t.Fatal("small binary should survive inline")
	}
}

func TestMessageRoundtripIntoTypedEnvelope(t *testing.T) {
	// The dispatch paths decode into RPCMessage/StreamMessage etc. — the
	// placeholder lands in the `any` payload field and must be restored there.
	bin := bytesN(T+8192, 0x77)
	msg := RPCMessage{ID: "42", Method: "call", Params: []any{bin, "tail"}}
	encoded := mustEncodeMessage(t, msg)

	var decoded RPCMessage
	if err := DecodeMessageInto(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ID != "42" || decoded.Method != "call" {
		t.Fatalf("envelope fields lost: %+v", decoded)
	}
	params := decoded.Params.([]any)
	if !bytes.Equal(asBytes(t, params[0]), bin) {
		t.Fatal("params binary mismatch")
	}
	if params[1] != "tail" {
		t.Fatalf("params[1] = %v", params[1])
	}
}

func TestDecodeMessageReturnsZeroCopyViews(t *testing.T) {
	bin := bytesN(T+2048, 0x11)
	encoded := mustEncodeMessage(t, map[string]any{"params": []any{bin}})
	result := mustDecodeMessage(t, encoded).(map[string]any)
	view := asBytes(t, result["params"].([]any)[0])

	offset := len(encoded) - (T + 2048)
	if &view[0] != &encoded[offset] {
		t.Fatal("expected zero-copy subslice view into the received buffer")
	}
	// Mutating the receive buffer is visible through the view (no copy).
	encoded[len(encoded)-1] = 0x99
	if view[len(view)-1] != 0x99 {
		t.Fatal("view does not alias the receive buffer")
	}
}

func TestCollisionUserMapWithoutLengthKey(t *testing.T) {
	message := map[string]any{"params": []any{map[string]any{placeholderKey: 0}, bytesN(T, 7)}}
	result := messageRoundtrip(t, message).(map[string]any)
	userMap := result["params"].([]any)[0].(map[string]any)
	if n, ok := asNonNegativeInt(userMap[placeholderKey]); !ok || n != 0 || len(userMap) != 1 {
		t.Fatalf("user map was altered: %v", userMap)
	}
}

func TestCollisionUserMapWithNonIntegerLength(t *testing.T) {
	message := map[string]any{"params": []any{map[string]any{placeholderKey: 0, "l": "nope"}, bytesN(T, 7)}}
	result := messageRoundtrip(t, message).(map[string]any)
	userMap := result["params"].([]any)[0].(map[string]any)
	if userMap["l"] != "nope" {
		t.Fatalf("user map was altered: %v", userMap)
	}
}

func TestCollisionUserMapWithExtraKeys(t *testing.T) {
	message := map[string]any{"params": []any{map[string]any{placeholderKey: 0, "l": 1, "extra": true}, bytesN(T, 7)}}
	result := messageRoundtrip(t, message).(map[string]any)
	userMap := result["params"].([]any)[0].(map[string]any)
	if userMap["extra"] != true || len(userMap) != 3 {
		t.Fatalf("user map was altered: %v", userMap)
	}
}

func TestCollisionPlaceholderShapedMapInBinaryFreeMessage(t *testing.T) {
	// Without extracted binaries there is no CUIB frame, hence no placeholder
	// substitution at all.
	message := map[string]any{"params": []any{map[string]any{placeholderKey: 0, "l": 123}}}
	result := messageRoundtrip(t, message).(map[string]any)
	userMap := result["params"].([]any)[0].(map[string]any)
	if l, ok := asNonNegativeInt(userMap["l"]); !ok || l != 123 {
		t.Fatalf("user map was altered: %v", userMap)
	}
}

func TestDecodeOutOfOrderPlaceholders(t *testing.T) {
	// Go map serialization order is unspecified: placeholder 1 may appear
	// before 0 in the envelope. Segments still lie back-to-back in index
	// order. Build such a frame by hand.
	seg0 := bytesN(T, 0xaa)
	seg1 := bytesN(T+2048, 0xbb)
	envelope, err := Encode(map[string]any{
		"b": map[string]any{placeholderKey: 1, "l": T + 2048},
		"a": map[string]any{placeholderKey: 0, "l": T},
	})
	if err != nil {
		t.Fatal(err)
	}

	frame := make([]byte, 8+len(envelope)+T+(T+2048))
	copy(frame[:4], cuibMagic[:])
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(envelope)))
	copy(frame[8:], envelope)
	copy(frame[8+len(envelope):], seg0)
	copy(frame[8+len(envelope)+T:], seg1)

	result := mustDecodeMessage(t, frame).(map[string]any)
	if !bytes.Equal(asBytes(t, result["a"]), seg0) {
		t.Fatal("segment 0 mismatch")
	}
	if !bytes.Equal(asBytes(t, result["b"]), seg1) {
		t.Fatal("segment 1 mismatch")
	}
}

func TestDecodeRejectsEnvelopeLengthExceedingPayload(t *testing.T) {
	encoded := mustEncodeMessage(t, map[string]any{"params": []any{bytesN(T, 7)}})
	binary.LittleEndian.PutUint32(encoded[4:8], uint32(len(encoded)))
	_, err := DecodeMessage(encoded)
	if err == nil || !strings.Contains(err.Error(), "envelope length") {
		t.Fatalf("expected envelope length error, got %v", err)
	}
}

func TestDecodeRejectsTruncatedFrame(t *testing.T) {
	encoded := mustEncodeMessage(t, map[string]any{"params": []any{bytesN(T, 7)}})
	_, err := DecodeMessage(encoded[:len(encoded)-10])
	if err == nil || !strings.Contains(err.Error(), "invalid CUIB frame") {
		t.Fatalf("expected CUIB frame error, got %v", err)
	}
}

func TestDecodeRejectsTrailingBytes(t *testing.T) {
	encoded := mustEncodeMessage(t, map[string]any{"params": []any{bytesN(T, 7)}})
	padded := make([]byte, len(encoded)+4)
	copy(padded, encoded)
	_, err := DecodeMessage(padded)
	if err == nil || !strings.Contains(err.Error(), "expected payload size") {
		t.Fatalf("expected payload size error, got %v", err)
	}
}

func TestDecodeRejectsMissingPlaceholderIndex(t *testing.T) {
	// Envelope references segment 1 only — segment 0 is missing.
	envelope, err := Encode(map[string]any{"a": map[string]any{placeholderKey: 1, "l": 16}})
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 8+len(envelope)+16)
	copy(frame[:4], cuibMagic[:])
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(envelope)))
	copy(frame[8:], envelope)
	_, derr := DecodeMessage(frame)
	if derr == nil || !strings.Contains(derr.Error(), "missing placeholder for segment 0") {
		t.Fatalf("expected missing placeholder error, got %v", derr)
	}
}

type benchMeta struct {
	Camera string `msgpack:"camera"`
	Codec  string `msgpack:"codec"`
	Width  int    `msgpack:"width"`
	Height int    `msgpack:"height"`
}

// BenchmarkEncodePooledSmallBaseline is the pre-CUIB encode cost of a small
// envelope (reference point for the walk overhead).
func BenchmarkEncodePooledSmallBaseline(b *testing.B) {
	msg := RPCResponse{ID: "abc.123", Result: benchMeta{Camera: "front", Codec: "h264", Width: 1920, Height: 1080}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, release, err := encodePooled(msg)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
		release()
	}
}

// BenchmarkEncodeMessageSmallStructNoBinary measures the walk overhead for a
// small binary-free envelope carrying a typed struct result (early-out path).
func BenchmarkEncodeMessageSmallStructNoBinary(b *testing.B) {
	msg := RPCResponse{ID: "abc.123", Result: benchMeta{Camera: "front", Codec: "h264", Width: 1920, Height: 1080}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, release, err := encodeMessagePooled(msg)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
		release()
	}
}

// BenchmarkEncodeMessageSmallMapNoBinary: binary-free map params.
func BenchmarkEncodeMessageSmallMapNoBinary(b *testing.B) {
	msg := RPCMessage{ID: "abc.123", Method: "call", Params: []any{"snapshot", map[string]any{"quality": 80, "camera": "front"}}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, release, err := encodeMessagePooled(msg)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
		release()
	}
}

// BenchmarkEncodeMessageFrame100KB: the NVR frame hot path (struct with a
// 100KB []byte field inside the response envelope).
func BenchmarkEncodeMessageFrame100KB(b *testing.B) {
	frame := bytesN(100_000, 0x5a)
	msg := CallbackInvocation{Method: "onFrame", Args: []any{frame}}
	b.ReportAllocs()
	b.SetBytes(100_000)
	for i := 0; i < b.N; i++ {
		data, release, err := encodeMessagePooled(msg)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
		release()
	}
}

func BenchmarkDecodeMessageFrame100KB(b *testing.B) {
	frame := bytesN(100_000, 0x5a)
	encoded, err := EncodeMessage(CallbackInvocation{Method: "onFrame", Args: []any{frame}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(100_000)
	for i := 0; i < b.N; i++ {
		var inv CallbackInvocation
		if err := DecodeMessageInto(encoded, &inv); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodePlain100KB is the pre-CUIB decode cost (vmihailenco copies
// bin payloads on decode) — reference for the zero-copy gain.
func BenchmarkDecodePlain100KB(b *testing.B) {
	frame := bytesN(100_000, 0x5a)
	encoded, err := Encode(CallbackInvocation{Method: "onFrame", Args: []any{frame}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(100_000)
	for i := 0; i < b.N; i++ {
		var inv CallbackInvocation
		if err := Decode(encoded, &inv); err != nil {
			b.Fatal(err)
		}
	}
}

// Regression: sdk-style subscribers decode into concretely typed structs
// (e.g. detectionEventMessage with a nested Thumbnail []byte). The direct
// envelope decode chokes on the placeholder map in a typed []byte position;
// DecodeMessageInto must fall back to the generic-restore-reencode path.
func TestDecodeMessageIntoTypedByteFields(t *testing.T) {
	type typedSegment struct {
		Thumbnail []byte `msgpack:"thumbnail"`
		StartTime int64  `msgpack:"startTime"`
	}
	type typedEvent struct {
		ID       string         `msgpack:"id"`
		Segments []typedSegment `msgpack:"segments"`
	}
	type typedMessage struct {
		Type string     `msgpack:"type"`
		Data typedEvent `msgpack:"data"`
	}

	thumb := bytes.Repeat([]byte{0xAB}, binaryExtractThreshold+123)
	src := typedMessage{
		Type: "update",
		Data: typedEvent{
			ID:       "evt-1",
			Segments: []typedSegment{{Thumbnail: thumb, StartTime: 1783012908256}},
		},
	}

	frame := mustEncodeMessage(t, src)
	if !bytes.Equal(frame[:4], cuibMagic[:]) {
		t.Fatalf("expected a CUIB frame (binary above threshold)")
	}

	var out typedMessage
	if err := DecodeMessageInto(frame, &out); err != nil {
		t.Fatalf("typed decode failed: %v", err)
	}
	if out.Type != "update" || out.Data.ID != "evt-1" || len(out.Data.Segments) != 1 {
		t.Fatalf("typed decode mangled envelope: %+v", out)
	}
	if !bytes.Equal(out.Data.Segments[0].Thumbnail, thumb) {
		t.Fatalf("typed decode lost the extracted binary: got %d bytes", len(out.Data.Segments[0].Thumbnail))
	}
	if out.Data.Segments[0].StartTime != 1783012908256 {
		t.Fatalf("typed decode mangled sibling field: %d", out.Data.Segments[0].StartTime)
	}
}

// Small binaries stay inline — the typed fast path must not regress.
func TestDecodeMessageIntoTypedInlineBinary(t *testing.T) {
	type typedMessage struct {
		Type string `msgpack:"type"`
		Blob []byte `msgpack:"blob"`
	}
	src := typedMessage{Type: "x", Blob: bytes.Repeat([]byte{7}, 128)}
	frame := mustEncodeMessage(t, src)

	var out typedMessage
	if err := DecodeMessageInto(frame, &out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if !bytes.Equal(out.Blob, src.Blob) {
		t.Fatalf("inline binary mangled")
	}
}
