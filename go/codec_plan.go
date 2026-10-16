package rpc

import (
	"bytes"
	"reflect"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/tagparser/v2"
)

var (
	encodePlans sync.Map // reflect.Type -> *planEntry, fn nil when the type has none
	structPlans sync.Map // reflect.Type -> *structPlan, nil when the struct has none

	isZeroerType = reflect.TypeFor[interface{ IsZero() bool }]()
	anyType      = reflect.TypeFor[any]()
	bytesType    = reflect.TypeFor[[]byte]()
	boolType     = reflect.TypeFor[bool]()
	intType      = reflect.TypeFor[int]()
	int8Type     = reflect.TypeFor[int8]()
	int16Type    = reflect.TypeFor[int16]()
	int32Type    = reflect.TypeFor[int32]()
	int64Type    = reflect.TypeFor[int64]()
	uintType     = reflect.TypeFor[uint]()
	uint8Type    = reflect.TypeFor[uint8]()
	uint16Type   = reflect.TypeFor[uint16]()
	uint32Type   = reflect.TypeFor[uint32]()
	uint64Type   = reflect.TypeFor[uint64]()
	float32Type  = reflect.TypeFor[float32]()
)

// an encoder for one type that writes the bytes vmihailenco writes for it
type encodeFunc func(enc *msgpack.Encoder, v reflect.Value) error

type planEntry struct {
	fn encodeFunc
}

type planField struct {
	key       []byte
	index     int
	omitEmpty bool
	encode    encodeFunc
	// false when vmihailenco's answer is unknown, the struct then goes to it whole
	empty func(v reflect.Value) (empty, known bool)
}

// a struct whose msgpack layout is plain: no embedding, no _msgpack options,
// no intern or alias tags and no custom encoding
type structPlan struct {
	fields       []planField
	hasOmitEmpty bool
}

// mirrors vmihailenco's getFields for the structs it accepts
func newStructPlan(t reflect.Type, building map[reflect.Type]*planEntry) *structPlan {
	if hasCustomMsgpackEncoding(t) {
		return nil
	}
	p := &structPlan{}
	names := map[string]bool{}
	for f := range t.Fields() {
		tag := tagparser.Parse(f.Tag.Get("msgpack"))
		if tag.Name == "-" {
			continue
		}
		if f.Name == "_msgpack" || f.Anonymous {
			return nil
		}
		if f.PkgPath != "" {
			continue
		}
		if tag.HasOption("intern") {
			return nil
		}
		if _, ok := tag.Options["alias"]; ok {
			return nil
		}
		name := tag.Name
		if name == "" {
			name = f.Name
		}
		if names[name] {
			return nil
		}
		names[name] = true

		var key bytes.Buffer
		enc := msgpack.NewEncoder(&key)
		if err := enc.EncodeString(name); err != nil {
			return nil
		}
		field := planField{
			key:       key.Bytes(),
			index:     f.Index[0],
			omitEmpty: tag.HasOption("omitempty"),
			encode:    fieldEncoder(f.Type, building),
		}
		if field.omitEmpty {
			field.empty = emptyCheck(f.Type, building)
			p.hasOmitEmpty = true
		}
		p.fields = append(p.fields, field)
	}
	return p
}

// addressable, as fields in a slice or behind a pointer are: some ext encoders need it
func (p *structPlan) matchesZero(t reflect.Type) bool {
	return sameAsGeneric(p.encode, reflect.New(t).Elem())
}

func (p *structPlan) encode(enc *msgpack.Encoder, v reflect.Value) error {
	var stack [32]bool
	omitted := stack[:0]
	if len(p.fields) > len(stack) {
		omitted = make([]bool, 0, len(p.fields))
	}
	count := len(p.fields)
	if p.hasOmitEmpty {
		for i := range p.fields {
			f := &p.fields[i]
			omit := false
			if f.omitEmpty {
				empty, known := f.empty(v.Field(f.index))
				if !known {
					return enc.EncodeValue(v)
				}
				omit = empty
			}
			if omit {
				count--
			}
			omitted = append(omitted, omit)
		}
	}

	if err := enc.EncodeMapLen(count); err != nil {
		return err
	}
	w := enc.Writer()
	for i := range p.fields {
		if p.hasOmitEmpty && omitted[i] {
			continue
		}
		f := &p.fields[i]
		if _, err := w.Write(f.key); err != nil {
			return err
		}
		if err := f.encode(enc, v.Field(f.index)); err != nil {
			return err
		}
	}
	return nil
}

// vmihailenco: a struct is empty when OmitEmpty leaves no field
func (p *structPlan) empty(v reflect.Value) (empty, known bool) {
	if !p.hasOmitEmpty {
		return len(p.fields) == 0, true
	}
	for i := range p.fields {
		f := &p.fields[i]
		if !f.omitEmpty {
			return false, true
		}
		empty, known := f.empty(v.Field(f.index))
		if !known {
			return false, false
		}
		if !empty {
			return false, true
		}
	}
	return true, true
}

func encodeFast(enc *msgpack.Encoder, v any) error {
	switch v.(type) {
	case nil, string, []byte, int, int64, uint, uint64, bool, float32, float64, time.Duration, time.Time:
		return enc.Encode(v)
	}
	rv := reflect.ValueOf(v)
	if fn := planFor(rv.Type()); fn != nil {
		return fn(enc, rv)
	}
	return enc.EncodeValue(rv)
}

// nil when the type has no plan; enc.EncodeValue is then what vmihailenco does.
// Two goroutines may build the same plan at once, both results are equal.
func planFor(t reflect.Type) encodeFunc {
	if e, ok := encodePlans.Load(t); ok {
		return e.(*planEntry).fn
	}
	return buildPlan(t, map[reflect.Type]*planEntry{})
}

// building holds the types on the current path, a recursive type calls its
// entry through a trampoline that reads the finished plan at encode time
func buildPlan(t reflect.Type, building map[reflect.Type]*planEntry) encodeFunc {
	if e, ok := encodePlans.Load(t); ok {
		return e.(*planEntry).fn
	}
	if e, ok := building[t]; ok {
		return func(enc *msgpack.Encoder, v reflect.Value) error {
			if e.fn != nil {
				return e.fn(enc, v)
			}
			return enc.EncodeValue(v)
		}
	}
	entry := &planEntry{}
	building[t] = entry
	defer delete(building, t)

	switch {
	case t == mapStringAnyType:
		entry.fn = encodeAnyMap
	case t.Kind() == reflect.Struct:
		if p := buildStructPlan(t, building); p != nil {
			entry.fn = p.encode
		}
	case t.Kind() == reflect.Pointer && t.Name() == "" && t.Elem().Kind() == reflect.Struct:
		if elem := buildPlan(t.Elem(), building); elem != nil && !hasCustomMsgpackEncoding(t) {
			fn := func(enc *msgpack.Encoder, v reflect.Value) error {
				if v.IsNil() {
					return enc.EncodeNil()
				}
				return elem(enc, v.Elem())
			}
			if sameAsGeneric(fn, reflect.New(t.Elem())) {
				entry.fn = fn
			}
		}
	case t.Kind() == reflect.Slice && t.Name() == "" && t.Elem().Kind() != reflect.Uint8 && t.Elem() != stringType:
		elem := fieldEncoder(t.Elem(), building)
		fn := func(enc *msgpack.Encoder, v reflect.Value) error {
			if v.IsNil() {
				return enc.EncodeNil()
			}
			n := v.Len()
			if err := enc.EncodeArrayLen(n); err != nil {
				return err
			}
			for i := range n {
				if err := elem(enc, v.Index(i)); err != nil {
					return err
				}
			}
			return nil
		}
		if sameAsGeneric(fn, reflect.MakeSlice(t, 1, 1)) {
			entry.fn = fn
		}
	}
	encodePlans.Store(t, entry)
	return entry.fn
}

// the encoder vmihailenco's getEncoder picks for t, written directly where the
// rule is plain and handed to enc.EncodeValue otherwise
func fieldEncoder(t reflect.Type, building map[reflect.Type]*planEntry) encodeFunc {
	switch t {
	case boolType:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeBool(v.Bool()) }
	case stringType:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeString(v.String()) }
	case intType:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeInt(v.Int()) }
	case int8Type:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeInt8(int8(v.Int())) }
	case int16Type:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeInt16(int16(v.Int())) }
	case int32Type:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeInt32(int32(v.Int())) }
	case int64Type:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeInt64(v.Int()) }
	case uintType:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeUint(v.Uint()) }
	case uint8Type:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeUint8(uint8(v.Uint())) }
	case uint16Type:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeUint16(uint16(v.Uint())) }
	case uint32Type:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeUint32(uint32(v.Uint())) }
	case uint64Type:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeUint64(v.Uint()) }
	case float32Type:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeFloat32(float32(v.Float())) }
	case float64Type:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeFloat64(v.Float()) }
	case bytesType:
		return func(enc *msgpack.Encoder, v reflect.Value) error { return enc.EncodeBytes(v.Bytes()) }
	case stringSliceType:
		return encodeStrings
	case anyType:
		return encodeAnyValue
	}
	if fn := buildPlan(t, building); fn != nil {
		return fn
	}
	return encodeGeneric
}

func buildStructPlan(t reflect.Type, building map[reflect.Type]*planEntry) *structPlan {
	if p, ok := structPlans.Load(t); ok {
		return p.(*structPlan)
	}
	p := newStructPlan(t, building)
	if p != nil && !p.matchesZero(t) {
		p = nil
	}
	structPlans.Store(t, p)
	return p
}

// the omitempty rule of vmihailenco's isEmptyValue for a field of type t
func emptyCheck(t reflect.Type, building map[reflect.Type]*planEntry) func(reflect.Value) (bool, bool) {
	if t.Kind() == reflect.Interface || t.Implements(isZeroerType) {
		return emptyValue
	}
	switch t.Kind() {
	case reflect.Struct:
		// a recursive struct is still being built here, its plan is read when it runs
		if _, ok := building[t]; ok {
			return emptyValue
		}
		sp := buildStructPlan(t, building)
		return func(v reflect.Value) (bool, bool) {
			if sp == nil {
				return false, false
			}
			return sp.empty(v)
		}
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return func(v reflect.Value) (bool, bool) { return v.Len() == 0, true }
	case reflect.Bool:
		return func(v reflect.Value) (bool, bool) { return !v.Bool(), true }
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(v reflect.Value) (bool, bool) { return v.Int() == 0, true }
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return func(v reflect.Value) (bool, bool) { return v.Uint() == 0, true }
	case reflect.Float32, reflect.Float64:
		return func(v reflect.Value) (bool, bool) { return v.Float() == 0, true }
	case reflect.Pointer:
		return func(v reflect.Value) (bool, bool) { return v.IsNil(), true }
	default:
		return emptyValue
	}
}

// vmihailenco's isEmptyValue for a value whose type is known only now
func emptyValue(v reflect.Value) (empty, known bool) {
	kind := v.Kind()
	for kind == reflect.Interface {
		if v.IsNil() {
			return true, true
		}
		v = v.Elem()
		kind = v.Kind()
	}
	if v.Type().Implements(isZeroerType) {
		if !v.CanInterface() {
			return false, false
		}
		if nilable(kind) && v.IsNil() {
			return true, true
		}
		return v.Interface().(interface{ IsZero() bool }).IsZero(), true
	}
	switch kind {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0, true
	case reflect.Struct:
		sp := buildStructPlan(v.Type(), map[reflect.Type]*planEntry{})
		if sp == nil {
			return false, false
		}
		return sp.empty(v)
	case reflect.Bool:
		return !v.Bool(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0, true
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0, true
	case reflect.Pointer:
		return v.IsNil(), true
	default:
		return false, true
	}
}

func nilable(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	}
	return false
}

func encodeAnyValue(enc *msgpack.Encoder, v reflect.Value) error {
	if v.IsNil() {
		return enc.EncodeNil()
	}
	elem := v.Elem()
	if fn := planFor(elem.Type()); fn != nil {
		return fn(enc, elem)
	}
	return enc.EncodeValue(elem)
}

// vmihailenco's EncodeMap: each value goes through Encode, not EncodeValue
func encodeAnyMap(enc *msgpack.Encoder, v reflect.Value) error {
	if v.IsNil() {
		return enc.EncodeNil()
	}
	if err := enc.EncodeMapLen(v.Len()); err != nil {
		return err
	}
	for key, val := range v.Interface().(map[string]any) {
		if err := enc.EncodeString(key); err != nil {
			return err
		}
		if err := encodeFast(enc, val); err != nil {
			return err
		}
	}
	return nil
}

func encodeGeneric(enc *msgpack.Encoder, v reflect.Value) error {
	return enc.EncodeValue(v)
}

func encodeStrings(enc *msgpack.Encoder, v reflect.Value) error {
	if v.IsNil() {
		return enc.EncodeNil()
	}
	n := v.Len()
	if err := enc.EncodeArrayLen(n); err != nil {
		return err
	}
	for i := range n {
		if err := enc.EncodeString(v.Index(i).String()); err != nil {
			return err
		}
	}
	return nil
}

// a registration with msgpack.Register for the type, or a rule a plan does not
// model, already shows on a zero value; a marshaler that panics on one leaves
// the type to vmihailenco
func sameAsGeneric(fn encodeFunc, v reflect.Value) (same bool) {
	defer func() {
		if recover() != nil {
			same = false
		}
	}()
	want, err := encodeToBytes(func(enc *msgpack.Encoder) error { return enc.EncodeValue(v) })
	if err != nil {
		return false
	}
	got, err := encodeToBytes(func(enc *msgpack.Encoder) error { return fn(enc, v) })
	return err == nil && bytes.Equal(want, got)
}

func encodeToBytes(write func(enc *msgpack.Encoder) error) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.GetEncoder()
	enc.Reset(&buf)
	err := write(enc)
	msgpack.PutEncoder(enc)
	return buf.Bytes(), err
}
