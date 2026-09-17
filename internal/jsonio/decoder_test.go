package jsonio_test

import (
	"math"
	"streamio/internal/jsonio"
	"streamio/internal/record"
	"strings"
	"testing"
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
		{name: "nested object", doc: `{"a":{"b":1}}`, want: "nested value not supported"},
		{name: "array", doc: `{"a":[1,2]}`, want: "nested value not supported"},
		{name: "top-level array", doc: `[1,2]`, want: "not a JSON object"},
		{name: "top-level scalar", doc: `"just a string"`, want: "not a JSON object"},
		{name: "trailing object after close", doc: `{"a":1}{"b":2}`, want: "unexpected token"},
		{name: "trailing garbage after close", doc: `{"a":1}garbage`, want: "unexpected token"},
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
