package rpc

import (
	"bytes"
	"encoding"
	"math"
	"reflect"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/tagparser/v2"
)

var (
	// decoders added with msgpack.Register or RegisterExtDecoder are not visible here
	customDecoderIface      = reflect.TypeFor[msgpack.CustomDecoder]()
	msgpackUnmarshalerIface = reflect.TypeFor[msgpack.Unmarshaler]()
	binaryUnmarshalerIface  = reflect.TypeFor[encoding.BinaryUnmarshaler]()
	textUnmarshalerIface    = reflect.TypeFor[encoding.TextUnmarshaler]()

	float64SliceType = reflect.TypeFor[[]float64]()
	stringSliceType  = reflect.TypeFor[[]string]()
	anySliceType     = reflect.TypeFor[[]any]()
	mapStringAnyType = reflect.TypeFor[map[string]any]()
	stringType       = reflect.TypeFor[string]()
	float64Type      = reflect.TypeFor[float64]()

	assignInfoCache sync.Map // reflect.Type -> *assignInfo
)

type assignField struct {
	index []int
	whole bool
}

type assignNamed struct {
	name  string
	index []int
}

type assignInfo struct {
	plain  bool
	fields map[string]assignField
}

func assignGeneric(dst reflect.Value, src any) bool {
	t := dst.Type()
	info := assignInfoFor(t)
	if !info.plain {
		return false
	}
	switch src.(type) {
	case nil, msgpackrUndefined, *msgpackrUndefined:
		return true
	}

	switch t.Kind() {
	case reflect.Bool:
		b, ok := src.(bool)
		if !ok {
			return false
		}
		dst.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, ok := coercedInt(src)
		if !ok {
			return false
		}
		dst.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, ok := coercedInt(src)
		if !ok {
			return false
		}
		dst.SetUint(uint64(n))
	case reflect.Float32:
		// msgpack reads a float32 only from a float32 or an integer
		var f float32
		switch x := src.(type) {
		case float32:
			f = x
			if n, ok := integralFloat32(x); ok {
				f = float32(n)
			}
		default:
			n, ok := coercedInt(src)
			if !ok {
				return false
			}
			f = float32(n)
		}
		dst.SetFloat(float64(f))
	case reflect.Float64:
		f, ok := coercedFloat64(src)
		if !ok {
			return false
		}
		dst.SetFloat(f)
	case reflect.String:
		switch s := src.(type) {
		case string:
			dst.SetString(s)
		case []byte:
			dst.SetString(string(s))
		default:
			return false
		}
	case reflect.Slice:
		return assignSlice(dst, src)
	case reflect.Array:
		return assignArray(dst, src)
	case reflect.Map:
		return assignMap(dst, src)
	case reflect.Struct:
		m, ok := src.(map[string]any)
		return ok && assignStruct(dst, info, m)
	case reflect.Pointer:
		elem := reflect.New(t.Elem())
		if !assignGeneric(elem.Elem(), src) {
			return false
		}
		dst.Set(elem)
	case reflect.Interface:
		owned, ok := ownGeneric(coerceValues(src))
		if !ok {
			return false
		}
		dst.Set(reflect.ValueOf(owned))
	default:
		return false
	}
	return true
}

func assignStruct(dst reflect.Value, info *assignInfo, m map[string]any) bool {
	for key, item := range m {
		field, ok := info.fields[key]
		if !ok {
			continue
		}
		// it overlaps the inlined fields, the result would depend on map order
		if field.whole {
			return false
		}
		var target reflect.Value
		if len(field.index) == 1 {
			target = dst.Field(field.index[0])
		} else {
			target = dst.FieldByIndex(field.index)
		}
		if !assignGeneric(target, item) {
			return false
		}
	}
	return true
}

func assignSlice(dst reflect.Value, src any) bool {
	t := dst.Type()
	if t.Elem().Kind() == reflect.Uint8 {
		switch b := src.(type) {
		case []byte:
			// copied: handlers keep attachments, a view would pin the whole message
			dst.SetBytes(bytes.Clone(b))
		case string:
			dst.SetBytes([]byte(b))
		default:
			return false
		}
		return true
	}

	items, ok := src.([]any)
	if !ok {
		return false
	}
	switch t {
	case float64SliceType:
		out := make([]float64, len(items))
		for i, item := range items {
			switch item.(type) {
			case nil, msgpackrUndefined, *msgpackrUndefined:
				continue
			}
			f, ok := coercedFloat64(item)
			if !ok {
				return false
			}
			out[i] = f
		}
		*dst.Addr().Interface().(*[]float64) = out
		return true
	case stringSliceType:
		out := make([]string, len(items))
		for i, item := range items {
			switch s := item.(type) {
			case nil, msgpackrUndefined, *msgpackrUndefined:
			case string:
				out[i] = s
			case []byte:
				out[i] = string(s)
			default:
				return false
			}
		}
		*dst.Addr().Interface().(*[]string) = out
		return true
	case anySliceType:
		owned, ok := ownGeneric(coerceValues(items))
		if !ok {
			return false
		}
		dst.Set(reflect.ValueOf(owned))
		return true
	}

	out := reflect.MakeSlice(t, len(items), len(items))
	for i, item := range items {
		if !assignGeneric(out.Index(i), item) {
			return false
		}
	}
	dst.Set(out)
	return true
}

func assignArray(dst reflect.Value, src any) bool {
	if dst.Type().Elem().Kind() == reflect.Uint8 {
		switch b := src.(type) {
		case []byte:
			if len(b) > dst.Len() {
				return false
			}
			for i, c := range b {
				dst.Index(i).SetUint(uint64(c))
			}
		case string:
			if len(b) > dst.Len() {
				return false
			}
			for i := range len(b) {
				dst.Index(i).SetUint(uint64(b[i]))
			}
		default:
			return false
		}
		return true
	}

	items, ok := src.([]any)
	if !ok || len(items) > dst.Len() {
		return false
	}
	for i, item := range items {
		if !assignGeneric(dst.Index(i), item) {
			return false
		}
	}
	return true
}

func assignMap(dst reflect.Value, src any) bool {
	m, ok := src.(map[string]any)
	if !ok {
		return false
	}
	t := dst.Type()
	keyType := t.Key()
	if keyType.Kind() != reflect.String || !assignInfoFor(keyType).plain {
		return false
	}
	if t == mapStringAnyType {
		owned, ok := ownGeneric(coerceValues(m))
		if !ok {
			return false
		}
		dst.Set(reflect.ValueOf(owned))
		return true
	}

	out := reflect.MakeMapWithSize(t, len(m))
	// SetMapIndex copies both, so one key and one value serve every entry
	keyValue := reflect.New(keyType).Elem()
	value := reflect.New(t.Elem()).Elem()
	for key, item := range m {
		value.SetZero()
		if !assignGeneric(value, item) {
			return false
		}
		keyValue.SetString(key)
		out.SetMapIndex(keyValue, value)
	}
	dst.Set(out)
	return true
}

func ownGeneric(v any) (any, bool) {
	switch x := v.(type) {
	case nil, bool, string,
		int8, int16, int32, int64,
		uint8, uint16, uint32, uint64,
		float32, float64:
		return v, true
	case []byte:
		return bytes.Clone(x), true
	case map[string]any:
		for key, item := range x {
			owned, ok := ownGeneric(item)
			if !ok {
				return nil, false
			}
			if b, isBytes := owned.([]byte); isBytes {
				x[key] = b
			}
		}
		return x, true
	case []any:
		for i, item := range x {
			owned, ok := ownGeneric(item)
			if !ok {
				return nil, false
			}
			if b, isBytes := owned.([]byte); isBytes {
				x[i] = b
			}
		}
		return x, true
	}
	return nil, false
}

func assignInfoFor(t reflect.Type) *assignInfo {
	if v, ok := assignInfoCache.Load(t); ok {
		return v.(*assignInfo)
	}
	info := &assignInfo{plain: plainDecodeType(t) && !hasCustomMsgpackDecoding(t)}
	if info.plain && t.Kind() == reflect.Struct {
		fields, _, ok := buildAssignFields(t)
		info.plain = ok
		info.fields = fields
	}
	assignInfoCache.Store(t, info)
	return info
}

func buildAssignFields(t reflect.Type) (map[string]assignField, []assignNamed, bool) {
	fields := make(map[string]assignField, t.NumField())
	list := make([]assignNamed, 0, t.NumField())
	add := func(name string, index []int) {
		fields[name] = assignField{index: index}
		list = append(list, assignNamed{name: name, index: index})
	}

	for f := range t.Fields() {
		tag := tagparser.Parse(f.Tag.Get("msgpack"))
		if tag.Name == "-" {
			continue
		}
		if f.Name == "_msgpack" && (tag.HasOption("as_array") || tag.HasOption("asArray")) {
			return nil, nil, false
		}
		if f.PkgPath != "" {
			if f.Anonymous {
				return nil, nil, false
			}
			continue
		}
		if _, alias := tag.Options["alias"]; alias || tag.HasOption("intern") {
			return nil, nil, false
		}
		name := tag.Name
		if name == "" {
			name = f.Name
		}

		if !f.Anonymous || tag.HasOption("noinline") {
			add(name, f.Index)
			continue
		}
		explicit := tag.HasOption("inline")
		switch {
		case f.Type.Kind() == reflect.Pointer:
			return nil, nil, false
		case f.Type.Kind() != reflect.Struct:
			if explicit {
				return nil, nil, false
			}
			add(name, f.Index)
			continue
		case hasCustomMsgpackEncoding(f.Type) || hasCustomMsgpackDecoding(f.Type):
			return nil, nil, false
		}

		_, inner, ok := buildAssignFields(f.Type)
		if !ok {
			return nil, nil, false
		}
		// without the inline option msgpack only inlines when no inner name is taken yet
		if !explicit && shadowsAny(fields, inner) {
			add(name, f.Index)
			continue
		}
		for _, in := range inner {
			if _, taken := fields[in.name]; taken && explicit {
				continue
			}
			add(in.name, append(append(make([]int, 0, len(f.Index)+len(in.index)), f.Index...), in.index...))
		}
		fields[name] = assignField{index: f.Index, whole: true}
	}
	return fields, list, true
}

func shadowsAny(fields map[string]assignField, inner []assignNamed) bool {
	for _, in := range inner {
		if _, taken := fields[in.name]; taken {
			return true
		}
	}
	return false
}

func hasCustomMsgpackDecoding(t reflect.Type) bool {
	if implementsUnmarshaler(t) {
		return true
	}
	return t.Kind() != reflect.Pointer && implementsUnmarshaler(reflect.PointerTo(t))
}

func implementsUnmarshaler(t reflect.Type) bool {
	return t.Implements(customDecoderIface) ||
		t.Implements(msgpackUnmarshalerIface) ||
		t.Implements(binaryUnmarshalerIface) ||
		t.Implements(textUnmarshalerIface)
}

func plainDecodeType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Interface:
		return t.NumMethod() == 0
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String,
		reflect.Slice, reflect.Array, reflect.Map, reflect.Struct, reflect.Pointer:
		return true
	}
	return false
}

func wireInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		// wraps above MaxInt64, as msgpack reads it
		return int64(n), true
	case uint:
		return int64(n), true
	}
	return 0, false
}

func coercedInt(v any) (int64, bool) {
	switch f := v.(type) {
	case float64:
		return integralFloat64(f)
	case float32:
		return integralFloat32(f)
	}
	return wireInt(v)
}

func coercedFloat64(v any) (float64, bool) {
	switch f := v.(type) {
	case float64:
		// coerceValues sent an integral float through int64, which also drops a minus zero
		if n, ok := integralFloat64(f); ok {
			return float64(n), true
		}
		return f, true
	case float32:
		if n, ok := integralFloat32(f); ok {
			return float64(n), true
		}
		return float64(f), true
	}
	return wireFloat64(v)
}

func integralFloat64(f float64) (int64, bool) {
	if f == math.Trunc(f) && !math.IsInf(f, 0) && !math.IsNaN(f) && f >= math.MinInt64 && f <= math.MaxInt64 {
		return int64(f), true
	}
	return 0, false
}

func integralFloat32(f32 float32) (int64, bool) {
	f := float64(f32)
	if f == math.Trunc(f) && !math.IsInf(f, 0) && !math.IsNaN(f) {
		return int64(f), true
	}
	return 0, false
}

func wireFloat64(v any) (float64, bool) {
	switch f := v.(type) {
	case float64:
		return f, true
	case float32:
		return float64(f), true
	}
	n, ok := wireInt(v)
	return float64(n), ok
}
