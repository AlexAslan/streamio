// Package record defines streamio's canonical, format-neutral row: the intermediate every
// cross-format conversion goes through between a decoder and an encoder.
//
// A Record is a flat []Field of tagged-union Values, not a map[string]any, so decoding a scalar
// column costs no heap allocation or interface boxing. KindMap and KindList are the two exceptions
// that nest — a Parquet-style key/value column group, and an ordered JSON array, respectively;
// everything else a decoder can't express in these Kinds is an error rather than an invented
// representation.
package record

// Kind is the physical type a Value carries. It selects which of Value's fixed fields holds the
// data; every other field is unspecified and must not be read.
type Kind uint8

const (
	// KindNull is a null/absent value. No Value field carries data.
	KindNull Kind = iota
	// KindBool reads Value.Bool.
	KindBool
	// KindInt64 reads Value.I64.
	KindInt64
	// KindFloat64 reads Value.F64.
	KindFloat64
	// KindBytes reads Value.Str, holding either a UTF-8 string or raw bytes.
	KindBytes
	// KindMap reads Value.Map, a nested Record holding one key/value column group's entries.
	KindMap
	// KindList reads Value.List, an ordered slice of nested Values holding a JSON array's elements.
	KindList
)

// String returns the Kind's name, for diagnostics.
func (k Kind) String() string {
	switch k {
	case KindNull:
		return "null"
	case KindBool:
		return "bool"
	case KindInt64:
		return "int64"
	case KindFloat64:
		return "float64"
	case KindBytes:
		return "bytes"
	case KindMap:
		return "map"
	case KindList:
		return "list"
	default:
		return "unknown"
	}
}

// Semantic tags a Value with what its physical Kind actually represents, when that isn't
// deducible from Kind alone — an Int64 that is really a day count or an epoch timestamp, say.
//
// Decoders only classify, never format: a date decodes to its raw day count, not a rendered
// string, so encoders stay free to render the same value differently and decoding stays
// allocation-free.
type Semantic uint8

const (
	// SemanticNone means the Kind fully describes the value; no reinterpretation applies.
	SemanticNone Semantic = iota
	// SemanticDate marks a KindInt64 holding a day count since the Unix epoch.
	SemanticDate
	// SemanticTimestampMillis marks a KindInt64 holding an epoch timestamp in milliseconds.
	SemanticTimestampMillis
	// SemanticTimestampMicros marks a KindInt64 holding an epoch timestamp in microseconds.
	SemanticTimestampMicros
	// SemanticTimestampNanos marks a KindInt64 holding an epoch timestamp in nanoseconds.
	SemanticTimestampNanos
	// SemanticUnsigned marks a KindInt64 whose bits are to be read as a uint64.
	SemanticUnsigned
	// SemanticFloat32 marks a KindFloat64 widened from a 32-bit float, so an encoder renders it at
	// its original precision rather than float64's.
	SemanticFloat32
)

// String returns the Semantic's name, for diagnostics.
func (s Semantic) String() string {
	switch s {
	case SemanticNone:
		return "none"
	case SemanticDate:
		return "date"
	case SemanticTimestampMillis:
		return "timestamp-millis"
	case SemanticTimestampMicros:
		return "timestamp-micros"
	case SemanticTimestampNanos:
		return "timestamp-nanos"
	case SemanticUnsigned:
		return "unsigned"
	case SemanticFloat32:
		return "float32"
	default:
		return "unknown"
	}
}

// Value is a tagged union rather than an any, so a scalar field costs no allocation or heap escape.
// Kind selects which field is live; Semantic refines how to interpret it.
//
// Str aliases the decoder's own buffer and is only valid until the owning Record is reset or
// refilled; copy it to retain it past that.
type Value struct {
	// Str is valid for KindBytes.
	Str []byte
	// Map is valid for KindMap.
	Map Record
	// List is valid for KindList, holding its elements in their original order.
	List []Value
	// I64 is valid for KindInt64.
	I64 int64
	// F64 is valid for KindFloat64 (including SemanticFloat32-tagged values).
	F64 float64
	// Kind selects which of the other fields is live.
	Kind Kind
	// Semantic refines how Kind's value is to be interpreted.
	Semantic Semantic
	// Bool is valid for KindBool.
	Bool bool
}

// Null returns a Value representing a null field.
func Null() Value {
	return Value{Kind: KindNull}
}

// Bool returns a Value holding b.
func Bool(b bool) Value {
	return Value{Kind: KindBool, Bool: b}
}

// Int64 returns a Value holding v, tagged with the given semantic (SemanticNone when v is a plain
// signed integer).
func Int64(v int64, semantic Semantic) Value {
	return Value{Kind: KindInt64, Semantic: semantic, I64: v}
}

// Float64 returns a Value holding v.
func Float64(v float64) Value {
	return Value{Kind: KindFloat64, F64: v}
}

// Float32 returns a Value holding v widened to a float64 and tagged SemanticFloat32, so an encoder
// renders it at 32-bit precision rather than at the float64 precision the widening implies.
func Float32(v float32) Value {
	return Value{Kind: KindFloat64, Semantic: SemanticFloat32, F64: float64(v)}
}

// Bytes returns a Value referencing b without copying it; see Value's doc for the lifetime this
// implies.
func Bytes(b []byte) Value {
	return Value{Kind: KindBytes, Str: b}
}

// Map returns a Value holding entries as a nested key/value group, referencing them without
// copying — the same lifetime Bytes implies, so a decoder is free to hand over scratch it refills
// for the next row.
func Map(entries Record) Value {
	return Value{Kind: KindMap, Map: entries}
}

// List returns a Value holding elements as an ordered nested sequence, referencing them without
// copying — the same lifetime Bytes implies, so a decoder is free to hand over scratch it refills
// for the next row.
func List(elements []Value) Value {
	return Value{Kind: KindList, List: elements}
}

// IsNull reports whether v carries no data.
func (v Value) IsNull() bool {
	return v.Kind == KindNull
}

// Field is one named column of a Record.
type Field struct {
	Name  string
	Value Value
}

// Record is one row: a reusable []Field buffer filled via Reset and Append rather than allocated
// fresh per row.
type Record []Field

// Reset truncates r to zero fields while keeping its capacity, so the next row reuses the same
// backing array. Use the returned Record: like append, Reset cannot update the caller's slice
// header in place.
func (r Record) Reset() Record {
	return r[:0]
}

// Append adds a named field to r and returns the grown Record, exactly as append would.
func (r Record) Append(name string, value Value) Record {
	return append(r, Field{Name: name, Value: value})
}

// Lookup returns the value of the first field named name, and whether such a field exists.
func (r Record) Lookup(name string) (Value, bool) {
	for i := range r {
		if r[i].Name == name {
			return r[i].Value, true
		}
	}
	return Value{}, false
}
