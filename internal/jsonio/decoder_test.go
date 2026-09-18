package jsonio_test

import (
	"math"
	"strings"
	"testing"

	"github.com/AlexAslan/streamio/internal/jsonio"
	"github.com/AlexAslan/streamio/internal/record"
)

type objectDecodedField struct {
	name     string
	str      string
	i64      int64
	f64      float64
	kind     record.Kind
	semantic record.Semantic
	boolean  bool
}

func decodeOne(tb testing.TB, doc string) []objectDecodedField {
	tb.Helper()
	rec, err := jsonio.NewObjectDecoder().Decode(nil, []byte(doc))
	if err != nil {
		tb.Fatalf("Decode: %v", err)
	}
	out := make([]objectDecodedField, 0, len(rec))
	for _, f := range rec {
		out = append(out, objectDecodedField{
			name:     f.Name,
			kind:     f.Value.Kind,
			semantic: f.Value.Semantic,
			boolean:  f.Value.Bool,
			i64:      f.Value.I64,
			f64:      f.Value.F64,
			str:      string(f.Value.Str),
		})
	}
	return out
}

func TestDecode_ScalarKinds(t *testing.T) {
	tests := []struct {
		check func(objectDecodedField) bool
		name  string
		doc   string
		field string
		kind  record.Kind
	}{
		{
			name: "string", doc: `{"s":"hello"}`, field: "s", kind: record.KindBytes,
			check: func(f objectDecodedField) bool { return f.str == "hello" },
		},
		{
			name: "integer", doc: `{"n":42}`, field: "n", kind: record.KindInt64,
			check: func(f objectDecodedField) bool { return f.i64 == 42 },
		},
		{
			name: "negative integer", doc: `{"n":-9007199254740993}`, field: "n", kind: record.KindInt64,
			check: func(f objectDecodedField) bool { return f.i64 == -9007199254740993 },
		},
		{
			name: "fractional number", doc: `{"n":1.5}`, field: "n", kind: record.KindFloat64,
			check: func(f objectDecodedField) bool { return f.f64 == 1.5 },
		},
		{
			name: "exponent", doc: `{"n":1e3}`, field: "n", kind: record.KindFloat64,
			check: func(f objectDecodedField) bool { return f.f64 == 1000 },
		},
		{
			name: "huge number", doc: `{"n":1e300}`, field: "n", kind: record.KindFloat64,
			check: func(f objectDecodedField) bool { return f.f64 == 1e300 && !math.IsInf(f.f64, 0) },
		},
		{
			name: "true", doc: `{"b":true}`, field: "b", kind: record.KindBool,
			check: func(f objectDecodedField) bool { return f.boolean },
		},
		{
			name: "false", doc: `{"b":false}`, field: "b", kind: record.KindBool,
			check: func(f objectDecodedField) bool { return !f.boolean },
		},
		{
			name: "null", doc: `{"v":null}`, field: "v", kind: record.KindNull,
			check: func(objectDecodedField) bool { return true },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := decodeOne(t, tt.doc)
			if len(fields) != 1 {
				t.Fatalf("decoded %d fields, want 1", len(fields))
			}
			got := fields[0]
			if got.name != tt.field || got.kind != tt.kind || got.semantic != record.SemanticNone || !tt.check(got) {
				t.Fatalf("decoded %+v", got)
			}
		})
	}
}

func TestDecode_PreservesFieldOrder(t *testing.T) {
	fields := decodeOne(t, `{"z":1,"a":"x","m":true}`)
	got := make([]string, 0, len(fields))
	for _, f := range fields {
		got = append(got, f.name)
	}
	if strings.Join(got, ",") != "z,a,m" {
		t.Fatalf("field order = %v, want z,a,m", got)
	}
}

func TestDecode_RejectsUnsupportedDocuments(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		want string
	}{
		{name: "top-level array", doc: `[1,2]`, want: "not a JSON object"},
		{name: "top-level scalar", doc: `"just a string"`, want: "not a JSON object"},
		{name: "trailing object after close", doc: `{"a":1}{"b":2}`, want: "unexpected token"},
		{name: "trailing garbage after close", doc: `{"a":1}garbage`, want: "unexpected token"},
		{name: "malformed nested object", doc: `{"a":{"b":}}`, want: "jsontext"},
		{name: "malformed array", doc: `{"a":[1,}`, want: "jsontext"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := jsonio.NewObjectDecoder().Decode(nil, []byte(tt.doc))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Decode error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

// TestDecode_NestedObject checks a nested object decodes into record.KindMap, recursively, rather
// than being rejected.
func TestDecode_NestedObject(t *testing.T) {
	rec, err := jsonio.NewObjectDecoder().Decode(nil, []byte(`{"id":1,"attributes":{"a":"x","b":2}}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rec) != 2 {
		t.Fatalf("decoded %d fields, want 2", len(rec))
	}

	attrs, ok := rec.Lookup("attributes")
	if !ok {
		t.Fatal("decoded record has no \"attributes\" field")
	}
	if attrs.Kind != record.KindMap {
		t.Fatalf("attributes.Kind = %v, want KindMap", attrs.Kind)
	}
	if len(attrs.Map) != 2 {
		t.Fatalf("attributes has %d entries, want 2", len(attrs.Map))
	}
	if a, aOK := attrs.Map.Lookup("a"); !aOK || string(a.Str) != "x" {
		t.Errorf("attributes.a = %+v, want KindBytes \"x\"", a)
	}
	if b, bOK := attrs.Map.Lookup("b"); !bOK || b.I64 != 2 {
		t.Errorf("attributes.b = %+v, want KindInt64 2", b)
	}
}

// TestDecode_NestedArray checks an array decodes into record.KindList, preserving element order
// and each element's own Kind — including a nested object inside the array.
func TestDecode_NestedArray(t *testing.T) {
	rec, err := jsonio.NewObjectDecoder().Decode(nil, []byte(`{"tags":["a",2,{"k":"v"},null]}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	tags, ok := rec.Lookup("tags")
	if !ok {
		t.Fatal("decoded record has no \"tags\" field")
	}
	if tags.Kind != record.KindList {
		t.Fatalf("tags.Kind = %v, want KindList", tags.Kind)
	}
	if len(tags.List) != 4 {
		t.Fatalf("tags has %d elements, want 4", len(tags.List))
	}
	if tags.List[0].Kind != record.KindBytes || string(tags.List[0].Str) != "a" {
		t.Errorf("tags[0] = %+v, want KindBytes \"a\"", tags.List[0])
	}
	if tags.List[1].Kind != record.KindInt64 || tags.List[1].I64 != 2 {
		t.Errorf("tags[1] = %+v, want KindInt64 2", tags.List[1])
	}
	if tags.List[2].Kind != record.KindMap {
		t.Errorf("tags[2] = %+v, want KindMap", tags.List[2])
	} else if k, kOK := tags.List[2].Map.Lookup("k"); !kOK || string(k.Str) != "v" {
		t.Errorf("tags[2].k = %+v, want KindBytes \"v\"", k)
	}
	if tags.List[3].Kind != record.KindNull {
		t.Errorf("tags[3] = %+v, want KindNull", tags.List[3])
	}
}

// TestDecode_EmptyNestedObjectAndArray checks an empty nested object/array decodes to a zero-length
// KindMap/KindList rather than erroring.
func TestDecode_EmptyNestedObjectAndArray(t *testing.T) {
	rec, err := jsonio.NewObjectDecoder().Decode(nil, []byte(`{"m":{},"l":[]}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	m, ok := rec.Lookup("m")
	if !ok || m.Kind != record.KindMap || len(m.Map) != 0 {
		t.Errorf("m = %+v, want empty KindMap", m)
	}
	l, ok := rec.Lookup("l")
	if !ok || l.Kind != record.KindList || len(l.List) != 0 {
		t.Errorf("l = %+v, want empty KindList", l)
	}
}

// TestDecode_DeeplyNested checks recursion isn't limited to one level: an object nested inside an
// array nested inside an object.
func TestDecode_DeeplyNested(t *testing.T) {
	rec, err := jsonio.NewObjectDecoder().Decode(nil, []byte(`{"a":{"b":[{"c":1}]}}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	a, ok := rec.Lookup("a")
	if !ok || a.Kind != record.KindMap {
		t.Fatalf("a = %+v, want KindMap", a)
	}
	b, ok := a.Map.Lookup("b")
	if !ok || b.Kind != record.KindList || len(b.List) != 1 {
		t.Fatalf("a.b = %+v, want KindList of length 1", b)
	}
	if b.List[0].Kind != record.KindMap {
		t.Fatalf("a.b[0] = %+v, want KindMap", b.List[0])
	}
	if c, cOK := b.List[0].Map.Lookup("c"); !cOK || c.I64 != 1 {
		t.Errorf("a.b[0].c = %+v, want KindInt64 1", c)
	}
}
