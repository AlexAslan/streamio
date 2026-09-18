package jsonio_test

import (
	"math"
	"strings"
	"testing"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/jsonio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

// newEncoder builds an encoder, failing the test if the constructor does.
//
//nolint:ireturn // formatio.RecordEncoder is exactly what the constructor under test returns.
func newEncoder(tb testing.TB) formatio.RecordEncoder {
	tb.Helper()
	enc, err := jsonio.NewEncoder(options.New())
	if err != nil {
		tb.Fatalf("NewEncoder: %v", err)
	}
	return enc
}

// encodeOne encodes rec as a one-record batch and returns the single document it produced.
func encodeOne(tb testing.TB, enc formatio.RecordEncoder, rec record.Record) string {
	tb.Helper()
	docs, err := enc.EncodeBatch([]record.Record{rec})
	if err != nil {
		tb.Fatalf("EncodeBatch: %v", err)
	}
	if len(docs) != 1 {
		tb.Fatalf("EncodeBatch returned %d documents, want 1", len(docs))
	}
	return string(docs[0])
}

// TestEncode_Values covers every Kind/Semantic pair the encoder renders, including the ones whose
// whole purpose is that the decoder did not format them.
func TestEncode_Values(t *testing.T) {
	type args struct {
		name  string
		want  string
		value record.Value
	}

	tests := []args{
		{name: "null", value: record.Null(), want: `{"v":null}`},
		{name: "true", value: record.Bool(true), want: `{"v":true}`},
		{name: "false", value: record.Bool(false), want: `{"v":false}`},
		{name: "int", value: record.Int64(-42, record.SemanticNone), want: `{"v":-42}`},
		{
			name:  "unsigned reads its bits as a uint64",
			value: record.Int64(-1, record.SemanticUnsigned),
			want:  `{"v":18446744073709551615}`,
		},
		{
			name:  "date renders the day count as a calendar date",
			value: record.Int64(19723, record.SemanticDate),
			want:  `{"v":"2024-01-01"}`,
		},
		{
			name:  "millisecond timestamp keeps three fractional digits",
			value: record.Int64(1704067200123, record.SemanticTimestampMillis),
			want:  `{"v":"2024-01-01T00:00:00.123Z"}`,
		},
		{
			name:  "microsecond timestamp keeps six fractional digits",
			value: record.Int64(1704067200123456, record.SemanticTimestampMicros),
			want:  `{"v":"2024-01-01T00:00:00.123456Z"}`,
		},
		{
			name:  "nanosecond timestamp keeps nine fractional digits",
			value: record.Int64(1704067200123456789, record.SemanticTimestampNanos),
			want:  `{"v":"2024-01-01T00:00:00.123456789Z"}`,
		},
		{name: "float64", value: record.Float64(1.5), want: `{"v":1.5}`},
		{
			name:  "float32 renders at 32-bit precision",
			value: record.Float32(0.1),
			want:  `{"v":0.1}`,
		},
		{
			name:  "float64 of the same float32 does not",
			value: record.Float64(float64(float32(0.1))),
			want:  `{"v":0.10000000149011612}`,
		},
		{
			name:  "small float switches to scientific notation like encoding/json",
			value: record.Float64(1e-9),
			want:  `{"v":1e-9}`,
		},
		{name: "bytes", value: record.Bytes([]byte("hello")), want: `{"v":"hello"}`},
		{
			name:  "bytes are escaped for HTML, as encoding/json escapes them by default",
			value: record.Bytes([]byte(`a<b>c&d"e`)),
			want:  `{"v":"a\u003cb\u003ec\u0026d\"e"}`,
		},
		{
			name: "map renders as a nested object in entry order",
			value: record.Map(record.Record{}.
				Append("z", record.Bytes([]byte("1"))).
				Append("a", record.Bytes([]byte("2")))),
			want: `{"v":{"z":"1","a":"2"}}`,
		},
		{name: "empty map renders as an empty object", value: record.Map(nil), want: `{"v":{}}`},
	}

	enc := newEncoder(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeOne(t, enc, record.Record{}.Append("v", tt.value))
			if got != tt.want {
				t.Errorf("Encode = %s, want %s", got, tt.want)
			}
		})
	}
}

// TestEncode_EmptyRecord checks a record with no fields is still a well-formed object.
func TestEncode_EmptyRecord(t *testing.T) {
	if got := encodeOne(t, newEncoder(t), nil); got != `{}` {
		t.Errorf("Encode = %s, want {}", got)
	}
}

// TestEncode_PreservesFieldOrder checks fields come out in the record's own order, which is what
// makes a decode/encode round trip byte-comparable at all.
func TestEncode_PreservesFieldOrder(t *testing.T) {
	rec := record.Record{}.
		Append("z", record.Int64(1, record.SemanticNone)).
		Append("a", record.Int64(2, record.SemanticNone)).
		Append("m", record.Int64(3, record.SemanticNone))

	if got := encodeOne(t, newEncoder(t), rec); got != `{"z":1,"a":2,"m":3}` {
		t.Errorf("Encode = %s, want the record's own field order", got)
	}
}

// TestEncode_OneDocumentPerRecord checks a batch becomes one self-contained document per record —
// what makes JSON a per-record format as far as the pool is concerned — each carrying no framing
// newline of its own, since framing a stream of documents belongs to the sink.
func TestEncode_OneDocumentPerRecord(t *testing.T) {
	batch := make([]record.Record, 3)
	for i := range batch {
		batch[i] = record.Record{}.Append("i", record.Int64(int64(i), record.SemanticNone))
	}

	docs, err := newEncoder(t).EncodeBatch(batch)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	want := []string{`{"i":0}`, `{"i":1}`, `{"i":2}`}
	if len(docs) != len(want) {
		t.Fatalf("EncodeBatch returned %d documents, want %d", len(docs), len(want))
	}
	for i := range want {
		if string(docs[i]) != want[i] {
			t.Errorf("document %d = %s, want %s", i, docs[i], want[i])
		}
	}
}

// TestEncode_EmptyBatch checks an empty batch produces no documents rather than one empty one.
func TestEncode_EmptyBatch(t *testing.T) {
	docs, err := newEncoder(t).EncodeBatch(nil)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	if len(docs) != 0 {
		t.Errorf("EncodeBatch returned %d documents, want 0", len(docs))
	}
}

// TestEncode_ReusableAcrossBatches checks one encoder can encode many batches, and that a document
// it handed out earlier is not overwritten by a later one — the encoder renders through a reused
// buffer, but what it returns is the caller's to keep, since a dispatch worker may still hold it.
func TestEncode_ReusableAcrossBatches(t *testing.T) {
	enc := newEncoder(t)

	first, err := enc.EncodeBatch([]record.Record{
		record.Record{}.Append("i", record.Bytes([]byte("a much longer first document"))),
	})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	if _, err = enc.EncodeBatch([]record.Record{
		record.Record{}.Append("i", record.Int64(1, record.SemanticNone)),
	}); err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	if got := string(first[0]); got != `{"i":"a much longer first document"}` {
		t.Errorf("the first batch's document changed after a second batch: %s", got)
	}
}

// TestEncode_NonFiniteFloat checks NaN/±Inf are rejected the way encoding/json rejects them,
// rather than silently becoming a JSON string.
func TestEncode_NonFiniteFloat(t *testing.T) {
	type args struct {
		name  string
		value float64
	}

	tests := []args{
		{name: "NaN", value: math.NaN()},
		{name: "positive infinity", value: math.Inf(1)},
		{name: "negative infinity", value: math.Inf(-1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := record.Record{}.Append("v", record.Float64(tt.value))
			_, err := newEncoder(t).EncodeBatch([]record.Record{rec})
			if err == nil {
				t.Fatalf("EncodeBatch(%v) returned no error", tt.value)
			}
			if !strings.Contains(err.Error(), "non-finite") {
				t.Errorf("error %q does not mention the non-finite float", err)
			}
		})
	}
}

// TestEncode_UnsupportedKind checks an unrenderable Kind is reported rather than skipped, so
// adding a Kind without teaching the encoder about it fails loudly.
func TestEncode_UnsupportedKind(t *testing.T) {
	rec := record.Record{}.Append("v", record.Value{Kind: record.Kind(200)})

	_, err := newEncoder(t).EncodeBatch([]record.Record{rec})
	if err == nil {
		t.Fatal("EncodeBatch of an unknown Kind returned no error")
	}
	if !strings.Contains(err.Error(), "unsupported value kind") {
		t.Errorf("error %q does not name the problem", err)
	}
}
