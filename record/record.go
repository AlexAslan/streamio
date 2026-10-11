// Package record defines streamio's canonical, format-neutral row: the intermediate every
// cross-format conversion goes through between a decoder and an encoder, and what a custom
// pipeline stage reads and writes.
//
// A Record is a flat []Field of tagged-union Values, not a map[string]any, so decoding a scalar
// column costs no heap allocation or interface boxing. KindMap and KindList are the two exceptions
// that nest — a Parquet-style key/value column group, and an ordered JSON array, respectively;
// everything else a decoder can't express in these Kinds is an error rather than an invented
// representation.
//
// # Lifetime
//
// A Record and everything it references (Value.Str, Value.Map, Value.List, and the field names'
// backing storage) belongs to the decoder that produced it and is reused for the next batch. Code
// that receives one — a RowFilter, a RecordTransform, a RecordConsumer — may read it freely during
// the call but must copy anything it wants to keep (string(v.Str), append([]byte(nil), v.Str...)).
// A stage may replace a field's Value or truncate the Record it is handed, but must not retain a
// slice into the Record after returning.
//
// # Stability
//
// This package is experimental: it is the first public commitment to the record model, and its API
// may change in a v0 release if building real stages shows it needs to.
package record

import (
	"strconv"

	"github.com/AlexAslan/streamio/internal/textfmt"
)

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

// AppendText appends v's canonical text to dst, rendered exactly as the converters write it, and
// reports whether v had a text to append.
//
// ok is false for a null, a zero-length string, and a KindMap or KindList (which have no scalar
// text): such a value is "empty" for hashing, comparison and profiling purposes. -0 renders as 0 so
// one number is one text, and a date or timestamp renders as ISO-8601 by its Semantic.
func (v Value) AppendText(dst []byte) ([]byte, bool) {
	switch v.Kind {
	case KindBytes:
		if len(v.Str) == 0 {
			return dst, false
		}
		return append(dst, v.Str...), true
	case KindBool:
		return strconv.AppendBool(dst, v.Bool), true
	case KindInt64:
		return appendInt(dst, v), true
	case KindFloat64:
		f, bits := v.F64, textfmt.Bits64
		if v.Semantic == SemanticFloat32 {
			bits = textfmt.Bits32
		}
		if f == 0 {
			f = 0 // folds -0 into 0: they are the same number
		}
		return textfmt.AppendFloat(dst, f, bits), true
	case KindNull, KindMap, KindList:
		return dst, false
	}
	return dst, false
}

// decimal is the radix every integer renders at.
const decimal = 10

// appendInt renders an integer the way its Semantic says it really is.
func appendInt(dst []byte, v Value) []byte {
	switch v.Semantic {
	case SemanticDate:
		return textfmt.AppendDate(dst, v.I64)
	case SemanticTimestampMillis:
		return textfmt.AppendTimestampMillis(dst, v.I64)
	case SemanticTimestampMicros:
		return textfmt.AppendTimestampMicros(dst, v.I64)
	case SemanticTimestampNanos:
		return textfmt.AppendTimestampNanos(dst, v.I64)
	case SemanticUnsigned:
		//nolint:gosec // SemanticUnsigned says the int64's bits are a uint64.
		return strconv.AppendUint(dst, uint64(v.I64), decimal)
	case SemanticNone, SemanticFloat32:
		return strconv.AppendInt(dst, v.I64, decimal)
	default:
		return strconv.AppendInt(dst, v.I64, decimal)
	}
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

// Arena is batch-lived scratch memory a pipeline stage writes its output values into.
//
// A stage that replaces a field's bytes must put the new bytes somewhere that stays valid until the
// batch is encoded. A single reused buffer does not: the next record's value would overwrite the
// previous record's. The pool hands every worker one Arena and resets it at the start of each
// batch, so everything copied into it lives exactly as long as the batch does.
type Arena struct {
	buf []byte
}

// Reset releases everything copied into a since the last Reset, keeping its capacity.
func (a *Arena) Reset() {
	a.buf = a.buf[:0]
}

// Copy copies b into the arena and returns the arena's copy.
func (a *Arena) Copy(b []byte) []byte {
	start := len(a.buf)
	a.buf = append(a.buf, b...)
	return a.buf[start:len(a.buf):len(a.buf)]
}

// Alloc returns n zeroed bytes from the arena, valid until the next Reset.
func (a *Arena) Alloc(n int) []byte {
	// Growing may move the arena's backing array. Slices handed out earlier keep pointing at the
	// old array, which is still correct, so they stay valid; only later slices use the new one.
	start := len(a.buf)
	a.buf = append(a.buf, make([]byte, n)...)
	return a.buf[start : start+n : start+n]
}
