package rpc

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/tagparser/v2"
)

const binaryExtractThreshold = 16384

const (
	cuibHeaderSize    = 8 // magic + u32 LE envLen
	placeholderKey    = "__cui_bin__"
	placeholderLenKey = "l"
)

var cuibMagic = [4]byte{0x43, 0x55, 0x49, 0x42} // "CUIB"

// EncodeMessage encodes a message for the wire, moving large binaries into out-of-band segments after the msgpack envelope.
func EncodeMessage(v any) ([]byte, error) {
	transformed, segments := extractBinaries(v)
	if len(segments) == 0 {
		return Encode(v)
	}
	var buf bytes.Buffer
	if err := encodeFrame(&buf, transformed, segments); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeMessagePooled(v any) (encoded []byte, release func(), err error) {
	transformed, segments := extractBinaries(v)
	if len(segments) == 0 {
		return encodePooled(v)
	}

	buf := encodeBufPool.Get().(*bytes.Buffer)
	buf.Reset()

	release = func() {
		if buf.Cap() <= maxPooledEncodeBuf {
			encodeBufPool.Put(buf)
		}
	}
	if err := encodeFrame(buf, transformed, segments); err != nil {
		release()
		return nil, nil, err
	}
	return buf.Bytes(), release, nil
}

type wireFrame struct {
	head     *bytes.Buffer
	segments [][]byte
	size     int
}

func encodeFramePooled(v any) (wireFrame, error) {
	transformed, segments := extractBinaries(v)
	if len(segments) == 0 {
		buf, err := encodeToPooledBuf(v)
		if err != nil {
			return wireFrame{}, err
		}
		return wireFrame{head: buf, size: buf.Len()}, nil
	}

	buf := encodeBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	if err := encodeFrameHead(buf, transformed); err != nil {
		releaseEncodeBuf(buf)
		return wireFrame{}, err
	}
	size := buf.Len()
	for _, seg := range segments {
		size += len(seg)
	}
	return wireFrame{head: buf, segments: segments, size: size}, nil
}

func (f *wireFrame) joined() []byte {
	if len(f.segments) == 0 {
		return f.head.Bytes()
	}
	switch {
	case f.head.Available() >= f.size-f.head.Len():
	case f.size > maxPooledEncodeBuf:
		// never pooled, so exactly the message instead of the doubling of Grow
		exact := bytes.NewBuffer(make([]byte, f.head.Len(), f.size))
		copy(exact.Bytes(), f.head.Bytes())
		releaseEncodeBuf(f.head)
		f.head = exact
	default:
		f.head.Grow(f.size - f.head.Len())
	}
	for _, seg := range f.segments {
		f.head.Write(seg)
	}
	f.segments = nil
	return f.head.Bytes()
}

func (f *wireFrame) release() {
	releaseEncodeBuf(f.head)
}

func encodeFrame(buf *bytes.Buffer, transformed any, segments [][]byte) error {
	if err := encodeFrameHead(buf, transformed); err != nil {
		return err
	}
	total := buf.Len()
	for _, seg := range segments {
		total += len(seg)
	}
	buf.Grow(total - buf.Len())
	for _, seg := range segments {
		buf.Write(seg)
	}
	return nil
}

func encodeFrameHead(buf *bytes.Buffer, transformed any) error {
	var header [cuibHeaderSize]byte
	buf.Write(header[:])

	enc := msgpack.GetEncoder()
	enc.Reset(buf)
	err := encodeFast(enc, transformed)
	msgpack.PutEncoder(enc)
	if err != nil {
		return err
	}

	envLen := buf.Len() - cuibHeaderSize
	if int64(envLen) > math.MaxUint32 {
		return fmt.Errorf("encode message: envelope length %d exceeds u32 range", envLen)
	}
	b := buf.Bytes()
	copy(b[:4], cuibMagic[:])
	binary.LittleEndian.PutUint32(b[4:cuibHeaderSize], uint32(envLen))
	return nil
}

func extractBinaries(v any) (transformed any, segments [][]byte) {
	if !containsBinary(v) {
		return v, nil
	}
	result, changed := extractValue(v, &segments)
	if !changed {
		return v, nil
	}
	return result, segments
}

func newPlaceholder(index, length int) map[string]any {
	return map[string]any{placeholderKey: index, placeholderLenKey: length}
}

func extractValue(v any, segs *[][]byte) (any, bool) {
	switch t := v.(type) {
	case nil, bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return v, false

	case []byte:
		if len(t) >= binaryExtractThreshold {
			index := len(*segs)
			*segs = append(*segs, t)
			return newPlaceholder(index, len(t)), true
		}
		return v, false

	case map[string]any:
		var cow map[string]any
		for k, val := range t {
			nv, changed := extractValue(val, segs)
			if changed {
				if cow == nil {
					cow = make(map[string]any, len(t))
					maps.Copy(cow, t)
				}
				cow[k] = nv
			}
		}
		if cow != nil {
			return cow, true
		}
		return v, false

	case []any:
		var cow []any
		for i, val := range t {
			nv, changed := extractValue(val, segs)
			if changed {
				if cow == nil {
					cow = make([]any, len(t))
					copy(cow, t)
				}
				cow[i] = nv
			}
		}
		if cow != nil {
			return cow, true
		}
		return v, false

	default:
		return extractReflect(reflect.ValueOf(v), segs)
	}
}

func extractReflect(rv reflect.Value, segs *[][]byte) (any, bool) {
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return rv.Interface(), false
		}
		nv, changed := extractValue(rv.Elem().Interface(), segs)
		if !changed {
			return rv.Interface(), false
		}
		return nv, true

	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			if rv.Len() >= binaryExtractThreshold {
				index := len(*segs)
				*segs = append(*segs, rv.Bytes())
				return newPlaceholder(index, rv.Len()), true
			}
			return rv.Interface(), false
		}
		return extractSequence(rv, segs)

	case reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return rv.Interface(), false // byte arrays are left to msgpack
		}
		return extractSequence(rv, segs)

	case reflect.Map:
		if !typeCanContainBinary(rv.Type().Elem()) {
			return rv.Interface(), false
		}
		n := rv.Len()
		keys := make([]reflect.Value, 0, n)
		vals := make([]any, 0, n)
		changedAny := false
		iter := rv.MapRange()
		for iter.Next() {
			nv, changed := extractValue(iter.Value().Interface(), segs)
			keys = append(keys, iter.Key())
			vals = append(vals, nv)
			changedAny = changedAny || changed
		}
		if !changedAny {
			return rv.Interface(), false
		}
		if rv.Type().Key().Kind() == reflect.String {
			out := make(map[string]any, n)
			for i := range keys {
				out[keys[i].String()] = vals[i]
			}
			return out, true
		}
		out := make(map[any]any, n)
		for i := range keys {
			out[keys[i].Interface()] = vals[i]
		}
		return out, true

	case reflect.Struct:
		meta := structMetaFor(rv.Type())
		if meta.opaque || !meta.candidate {
			return rv.Interface(), false
		}
		var changedFields map[int]any
		for i := range meta.fields {
			f := &meta.fields[i]
			if !f.candidate {
				continue
			}
			fv, ok := fieldByIndexRO(rv, f.index)
			if !ok {
				continue
			}
			nv, changed := extractValue(fv.Interface(), segs)
			if changed {
				if changedFields == nil {
					changedFields = make(map[int]any)
				}
				changedFields[i] = nv
			}
		}
		if changedFields == nil {
			return rv.Interface(), false
		}
		// re-emit the struct as a map with the field names the struct encoder would use
		out := make(map[string]any, len(meta.fields))
		for i := range meta.fields {
			f := &meta.fields[i]
			if nv, ok := changedFields[i]; ok {
				out[f.name] = nv // an extracted binary is never "empty"
				continue
			}
			fv, ok := fieldByIndexRO(rv, f.index)
			if !ok {
				if !meta.hasOmitEmpty {
					out[f.name] = nil
				}
				continue
			}
			if f.omitEmpty && isEmptyValueLike(fv) {
				continue
			}
			out[f.name] = fv.Interface()
		}
		return out, true

	default:
		return rv.Interface(), false
	}
}

func extractSequence(rv reflect.Value, segs *[][]byte) (any, bool) {
	if !typeCanContainBinary(rv.Type().Elem()) {
		return rv.Interface(), false
	}
	n := rv.Len()
	var cow []any
	for i := range n {
		nv, changed := extractValue(rv.Index(i).Interface(), segs)
		if changed && cow == nil {
			cow = make([]any, n)
			for j := range i {
				cow[j] = rv.Index(j).Interface()
			}
		}
		if cow != nil {
			cow[i] = nv
		}
	}
	if cow != nil {
		return cow, true
	}
	return rv.Interface(), false
}

func containsBinary(v any) bool {
	switch t := v.(type) {
	case nil, bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return false
	case []byte:
		return len(t) >= binaryExtractThreshold
	case map[string]any:
		for _, val := range t {
			if containsBinary(val) {
				return true
			}
		}
		return false
	case []any:
		return slices.ContainsFunc(t, containsBinary)
	default:
		return valueContainsBinary(reflect.ValueOf(v))
	}
}

func valueContainsBinary(rv reflect.Value) bool {
	switch rv.Kind() {
	case reflect.Interface:
		if rv.IsNil() {
			return false
		}
		// the value an interface holds is not addressable, so this does not copy
		if rv.CanInterface() {
			return containsBinary(rv.Elem().Interface())
		}
		return valueContainsBinary(rv.Elem())

	case reflect.Pointer:
		if rv.IsNil() {
			return false
		}
		return valueContainsBinary(rv.Elem())

	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return rv.Len() >= binaryExtractThreshold
		}
		return sequenceContainsBinary(rv)

	case reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return false
		}
		return sequenceContainsBinary(rv)

	case reflect.Map:
		if !typeCanContainBinary(rv.Type().Elem()) {
			return false
		}
		// a map is a pointer in an interface, Interface() does not copy it
		if rv.Type() == mapStringAnyType && rv.CanInterface() {
			return containsBinary(rv.Interface())
		}
		var iter reflect.MapIter
		iter.Reset(rv)
		// SetIterValue refuses a map reached through an unexported field
		if !rv.CanInterface() {
			for iter.Next() {
				if valueContainsBinary(iter.Value()) {
					return true
				}
			}
			return false
		}
		elem := reflect.New(rv.Type().Elem()).Elem()
		for iter.Next() {
			elem.SetIterValue(&iter)
			if valueContainsBinary(elem) {
				return true
			}
		}
		return false

	case reflect.Struct:
		meta := structMetaFor(rv.Type())
		if meta.opaque || !meta.candidate {
			return false
		}
		for i := range meta.fields {
			f := &meta.fields[i]
			if !f.candidate {
				continue
			}
			if fv, ok := fieldByIndexRO(rv, f.index); ok && valueContainsBinary(fv) {
				return true
			}
		}
		return false

	default:
		return false
	}
}

func sequenceContainsBinary(rv reflect.Value) bool {
	if !typeCanContainBinary(rv.Type().Elem()) {
		return false
	}
	for i := range rv.Len() {
		if valueContainsBinary(rv.Index(i)) {
			return true
		}
	}
	return false
}

type structFieldMeta struct {
	name      string
	index     []int
	omitEmpty bool
	candidate bool
}

type structMeta struct {
	opaque       bool
	candidate    bool
	hasOmitEmpty bool
	fields       []structFieldMeta
}

var structMetaCache sync.Map // reflect.Type -> *structMeta

func structMetaFor(t reflect.Type) *structMeta {
	if v, ok := structMetaCache.Load(t); ok {
		return v.(*structMeta)
	}
	m := buildStructMeta(t, make(map[reflect.Type]bool))
	structMetaCache.Store(t, m)
	return m
}

func buildStructMeta(t reflect.Type, visiting map[reflect.Type]bool) *structMeta {
	meta := &structMeta{}
	if hasCustomMsgpackEncoding(t) {
		meta.opaque = true
		return meta
	}
	if visiting[t] {
		// recursive embedding, opaque instead of looping forever
		meta.opaque = true
		return meta
	}
	visiting[t] = true
	defer delete(visiting, t)

	allOmitEmpty := false
	type pendingField struct {
		f   reflect.StructField
		tag *tagparser.Tag
	}
	var pending []pendingField

	for f := range t.Fields() {
		tag := tagparser.Parse(f.Tag.Get("msgpack"))
		if tag.Name == "-" {
			continue
		}
		if f.Name == "_msgpack" {
			if tag.HasOption("as_array") || tag.HasOption("asArray") {
				meta.opaque = true
				return meta
			}
			if tag.HasOption("omitempty") {
				allOmitEmpty = true
			}
			continue
		}
		if f.PkgPath != "" && !f.Anonymous {
			continue // unexported
		}
		pending = append(pending, pendingField{f: f, tag: tag})
	}

	names := make(map[string]bool)
	for _, p := range pending {
		f, tag := p.f, p.tag

		// an embedded struct without custom encoding is inlined, shadowed names stay on the outer struct
		if f.Anonymous && !tag.HasOption("noinline") {
			et := f.Type
			for et.Kind() == reflect.Pointer {
				et = et.Elem()
			}
			if et.Kind() == reflect.Struct && !hasCustomMsgpackEncoding(et) {
				inner := buildStructMeta(et, visiting)
				if !inner.opaque {
					for _, inf := range inner.fields {
						if names[inf.name] {
							continue
						}
						names[inf.name] = true
						idx := append(append([]int{}, f.Index...), inf.index...)
						meta.fields = append(meta.fields, structFieldMeta{
							name:      inf.name,
							index:     idx,
							omitEmpty: allOmitEmpty || inf.omitEmpty,
							candidate: inf.candidate,
						})
					}
					meta.hasOmitEmpty = meta.hasOmitEmpty || inner.hasOmitEmpty || allOmitEmpty
					continue
				}
			}
		}

		name := tag.Name
		if name == "" {
			name = f.Name
		}
		if names[name] {
			continue
		}
		names[name] = true
		fieldMeta := structFieldMeta{
			name:      name,
			index:     f.Index,
			omitEmpty: allOmitEmpty || tag.HasOption("omitempty"),
			candidate: typeCanContainBinary(f.Type),
		}
		meta.fields = append(meta.fields, fieldMeta)
		meta.hasOmitEmpty = meta.hasOmitEmpty || fieldMeta.omitEmpty
	}

	for i := range meta.fields {
		if meta.fields[i].candidate {
			meta.candidate = true
			break
		}
	}
	return meta
}

func fieldByIndexRO(rv reflect.Value, index []int) (reflect.Value, bool) {
	if len(index) == 1 {
		return rv.Field(index[0]), true
	}
	for i, idx := range index {
		if i > 0 {
			if rv.Kind() == reflect.Pointer {
				if rv.IsNil() {
					return reflect.Value{}, false
				}
				rv = rv.Elem()
			}
		}
		rv = rv.Field(idx)
	}
	return rv, true
}

var (
	customEncoderIface    = reflect.TypeFor[msgpack.CustomEncoder]()
	msgpackMarshalerIface = reflect.TypeFor[msgpack.Marshaler]()
	binaryMarshalerIface  = reflect.TypeFor[encoding.BinaryMarshaler]()
	textMarshalerIface    = reflect.TypeFor[encoding.TextMarshaler]()
	timeType              = reflect.TypeFor[time.Time]()
)

func implementsMarshaler(t reflect.Type) bool {
	return t.Implements(customEncoderIface) ||
		t.Implements(msgpackMarshalerIface) ||
		t.Implements(binaryMarshalerIface) ||
		t.Implements(textMarshalerIface)
}

func hasCustomMsgpackEncoding(t reflect.Type) bool {
	if t == timeType {
		return true
	}
	if implementsMarshaler(t) {
		return true
	}
	if t.Kind() != reflect.Pointer && implementsMarshaler(reflect.PointerTo(t)) {
		return true
	}
	return false
}

var binaryCandidateCache sync.Map // reflect.Type -> bool

func typeCanContainBinary(t reflect.Type) bool {
	if v, ok := binaryCandidateCache.Load(t); ok {
		return v.(bool)
	}
	res := computeCanContainBinary(t, make(map[reflect.Type]bool))
	binaryCandidateCache.Store(t, res)
	return res
}

func computeCanContainBinary(t reflect.Type, visiting map[reflect.Type]bool) bool {
	if visiting[t] {
		return true // conservative on recursive types
	}
	switch t.Kind() {
	case reflect.Interface:
		return true
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return true
		}
		visiting[t] = true
		defer delete(visiting, t)
		return computeCanContainBinary(t.Elem(), visiting)
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return false
		}
		visiting[t] = true
		defer delete(visiting, t)
		return computeCanContainBinary(t.Elem(), visiting)
	case reflect.Map, reflect.Pointer:
		visiting[t] = true
		defer delete(visiting, t)
		return computeCanContainBinary(t.Elem(), visiting)
	case reflect.Struct:
		if hasCustomMsgpackEncoding(t) {
			return false // opaque, msgpack does not emit its fields
		}
		visiting[t] = true
		defer delete(visiting, t)
		for f := range t.Fields() {
			if f.PkgPath != "" && !f.Anonymous {
				continue
			}
			if f.Tag.Get("msgpack") == "-" {
				continue
			}
			if computeCanContainBinary(f.Type, visiting) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func nilableKind(k reflect.Kind) bool {
	switch k {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	}
	return false
}

func isEmptyValueLike(v reflect.Value) bool {
	kind := v.Kind()
	for kind == reflect.Interface {
		if v.IsNil() {
			return true
		}
		v = v.Elem()
		kind = v.Kind()
	}

	if z, ok := v.Interface().(interface{ IsZero() bool }); ok {
		return nilableKind(kind) && v.IsNil() || z.IsZero()
	}

	switch kind {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Struct:
		meta := structMetaFor(v.Type())
		if meta.opaque {
			return false
		}
		for i := range meta.fields {
			f := &meta.fields[i]
			fv, ok := fieldByIndexRO(v, f.index)
			if !ok {
				continue
			}
			if !f.omitEmpty || !isEmptyValueLike(fv) {
				return false
			}
		}
		return true
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Pointer:
		return v.IsNil()
	default:
		return false
	}
}

// DecodeMessage decodes a wire message into a generic value, framed binaries become zero-copy views into data.
func DecodeMessage(data []byte) (any, error) {
	var v any
	if err := DecodeMessageInto(data, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// DecodeMessageInto decodes a wire message into v, a non-nil pointer, framed binaries become zero-copy views into data.
func DecodeMessageInto(data []byte, v any) error {
	if len(data) < cuibHeaderSize ||
		data[0] != cuibMagic[0] || data[1] != cuibMagic[1] ||
		data[2] != cuibMagic[2] || data[3] != cuibMagic[3] {
		return Decode(data, v)
	}

	envLen := int(binary.LittleEndian.Uint32(data[4:cuibHeaderSize]))
	segmentBase := cuibHeaderSize + envLen
	if segmentBase > len(data) {
		return fmt.Errorf("invalid CUIB frame: envelope length %d exceeds payload size %d", envLen, len(data))
	}

	envelope := data[cuibHeaderSize:segmentBase]
	if err := Decode(envelope, v); err == nil {
		return restoreBinaries(v, data, segmentBase)
	}

	// a typed []byte field cannot take the placeholder map: decode generic, restore, then convert
	var generic any
	if err := Decode(envelope, &generic); err != nil {
		return err
	}
	if err := restoreBinaries(&generic, data, segmentBase); err != nil {
		return err
	}
	// the failed decode left values in v, msgpack panics decoding into a non-nil any field
	resetValue(v)
	return convertPooled(generic, v)
}

type placeholderRef struct {
	index  int
	length int
	set    func([]byte)
}

func restoreBinaries(target any, data []byte, segmentBase int) error {
	var refs []placeholderRef
	if err := collectTyped(reflect.ValueOf(target), &refs); err != nil {
		return err
	}

	offsets, lengths, err := segmentLayout(refs, data, segmentBase)
	if err != nil {
		// a typed target may leave out a field that carries a segment, check the layout against the whole envelope
		var all []placeholderRef
		var generic any
		if Decode(data[cuibHeaderSize:segmentBase], &generic) != nil || collectAny(generic, func([]byte) {}, &all) != nil {
			return err
		}
		if offsets, lengths, err = segmentLayout(all, data, segmentBase); err != nil {
			return err
		}
		for i := range refs {
			if refs[i].index >= len(lengths) || refs[i].length != lengths[refs[i].index] {
				return fmt.Errorf("invalid CUIB frame: segment %d of the target does not match the envelope", refs[i].index)
			}
		}
	}

	for i := range refs {
		start := offsets[refs[i].index]
		end := start + lengths[refs[i].index]
		refs[i].set(data[start:end:end])
	}
	return nil
}

func segmentLayout(refs []placeholderRef, data []byte, segmentBase int) (offsets, lengths []int, err error) {
	maxIndex := -1
	for i := range refs {
		maxIndex = max(maxIndex, refs[i].index)
	}

	lengths = make([]int, maxIndex+1)
	for i := range lengths {
		lengths[i] = -1
	}
	for i := range refs {
		lengths[refs[i].index] = refs[i].length
	}

	offsets = make([]int, maxIndex+1)
	offset := segmentBase
	for i, l := range lengths {
		if l < 0 {
			return nil, nil, fmt.Errorf("invalid CUIB frame: missing placeholder for segment %d", i)
		}
		if l > len(data)-offset {
			return nil, nil, fmt.Errorf("invalid CUIB frame: segment %d exceeds payload size %d", i, len(data))
		}
		offsets[i] = offset
		offset += l
	}
	if offset != len(data) {
		return nil, nil, fmt.Errorf("invalid CUIB frame: expected payload size %d, got %d", offset, len(data))
	}
	return offsets, lengths, nil
}

func placeholderInfo(m map[string]any) (index, length int, ok bool) {
	if len(m) != 2 {
		return 0, 0, false
	}
	rawIndex, found := m[placeholderKey]
	if !found {
		return 0, 0, false
	}
	rawLength, found := m[placeholderLenKey]
	if !found {
		return 0, 0, false
	}
	index, ok = asNonNegativeInt(rawIndex)
	if !ok {
		return 0, 0, false
	}
	length, ok = asNonNegativeInt(rawLength)
	if !ok {
		return 0, 0, false
	}
	return index, length, true
}

func asNonNegativeInt(v any) (int, bool) {
	var n int64
	switch t := v.(type) {
	case int:
		n = int64(t)
	case int8:
		n = int64(t)
	case int16:
		n = int64(t)
	case int32:
		n = int64(t)
	case int64:
		n = t
	case uint:
		if uint64(t) > math.MaxInt64 {
			return 0, false
		}
		n = int64(t)
	case uint8:
		n = int64(t)
	case uint16:
		n = int64(t)
	case uint32:
		n = int64(t)
	case uint64:
		if t > math.MaxInt64 {
			return 0, false
		}
		n = int64(t)
	case float32:
		f := float64(t)
		if f != math.Trunc(f) || f < 0 || f > math.MaxInt32 {
			return 0, false
		}
		n = int64(f)
	case float64:
		if t != math.Trunc(t) || t < 0 || t > math.MaxInt64 || math.IsInf(t, 0) || math.IsNaN(t) {
			return 0, false
		}
		n = int64(t)
	default:
		return 0, false
	}
	if n < 0 || n > math.MaxInt32 {
		return 0, false
	}
	return int(n), true
}

func mayHoldPlaceholder(v any) bool {
	switch v.(type) {
	case nil, bool, string, []byte,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		time.Time, msgpackrUndefined, *msgpackrUndefined:
		return false
	}
	return true
}

func collectAny(v any, setSelf func([]byte), refs *[]placeholderRef) error {
	switch t := v.(type) {
	case map[string]any:
		if index, length, ok := placeholderInfo(t); ok {
			if setSelf == nil {
				return fmt.Errorf("invalid CUIB frame: cannot restore segment %d into a non-any position", index)
			}
			*refs = append(*refs, placeholderRef{index: index, length: length, set: setSelf})
			return nil
		}
		for k, val := range t {
			if !mayHoldPlaceholder(val) {
				continue
			}
			if err := collectAny(val, func(b []byte) { t[k] = b }, refs); err != nil {
				return err
			}
		}
		return nil
	case []any:
		for i, val := range t {
			if !mayHoldPlaceholder(val) {
				continue
			}
			if err := collectAny(val, func(b []byte) { t[i] = b }, refs); err != nil {
				return err
			}
		}
		return nil
	default:
		if !mayHoldPlaceholder(v) {
			return nil
		}
		return collectTyped(reflect.ValueOf(v), refs)
	}
}

func collectTyped(rv reflect.Value, refs *[]placeholderRef) error {
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return nil
		}
		return collectTyped(rv.Elem(), refs)

	case reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		var setSelf func([]byte)
		if rv.CanSet() {
			target := rv
			setSelf = func(b []byte) { target.Set(reflect.ValueOf(b)) }
		}
		return collectAny(rv.Interface(), setSelf, refs)

	case reflect.Struct:
		t := rv.Type()
		for i := 0; i < rv.NumField(); i++ {
			if t.Field(i).PkgPath != "" {
				continue // unexported
			}
			if err := collectTyped(rv.Field(i), refs); err != nil {
				return err
			}
		}
		return nil

	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
		for i := 0; i < rv.Len(); i++ {
			if err := collectTyped(rv.Index(i), refs); err != nil {
				return err
			}
		}
		return nil

	case reflect.Map:
		if m, ok := rv.Interface().(map[string]any); ok {
			// values of a typed map[string]any are assignable in place, the map itself cannot become a []byte
			return collectAny(m, nil, refs)
		}
		mapValue := rv
		iter := rv.MapRange()
		for iter.Next() {
			mv := iter.Value()
			switch mv.Kind() {
			case reflect.Interface:
				if mv.IsNil() {
					continue
				}
				key := iter.Key()
				setSelf := func(b []byte) { mapValue.SetMapIndex(key, reflect.ValueOf(b)) }
				if err := collectAny(mv.Interface(), setSelf, refs); err != nil {
					return err
				}
			case reflect.Map, reflect.Slice, reflect.Pointer, reflect.Struct:
				if err := collectTyped(mv, refs); err != nil {
					return err
				}
			}
		}
		return nil

	default:
		return nil
	}
}
