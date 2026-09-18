package arrowio_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/AlexAslan/streamio/internal/arrowio"
	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

// newBatchEncoder builds an encoder with the default options, failing the test rather than
// returning an error nobody would check — mirrors parquetio's identical helper.
//
//nolint:ireturn // formatio.RecordEncoder is what the constructor under test returns.
func newBatchEncoder(tb testing.TB) formatio.RecordEncoder {
	tb.Helper()
	enc, err := arrowio.NewEncoder(options.New())
	if err != nil {
		tb.Fatalf("NewEncoder: %v", err)
	}
	return enc
}

// encodeOne encodes batch and returns the single document it must produce, failing on any error:
// this encoder renders a whole batch as one Arrow IPC file, so anything else is a bug.
func encodeOne(tb testing.TB, enc formatio.RecordEncoder, batch []record.Record) []byte {
	tb.Helper()
	docs, err := enc.EncodeBatch(batch)
	if err != nil {
		tb.Fatalf("EncodeBatch: %v", err)
	}
	if len(docs) != 1 {
		tb.Fatalf("EncodeBatch returned %d documents, want 1 per batch", len(docs))
	}
	return docs[0]
}

// readBackRows opens doc as an Arrow IPC file, using arrowio's own decoder — the same round trip a
// real Arrow-to-X conversion takes — and returns every row as a name -> record.Value map.
func readBackRows(tb testing.TB, doc []byte) []map[string]record.Value {
	tb.Helper()

	dec, err := arrowio.NewDecoder(options.New(), options.Source{
		Reader: bytes.NewReader(doc), Size: int64(len(doc)), Name: "readback.arrow",
	})
	if err != nil {
		tb.Fatalf("NewDecoder: %v", err)
	}
	defer dec.Close()

	rows := mustDrain(tb, dec, 16)
	out := make([]map[string]record.Value, len(rows))
	for i, row := range rows {
		out[i] = map[string]record.Value(row)
	}
	return out
}

// TestEncode_ScalarRoundTrip checks a batch covering every scalar Kind/Semantic round-trips through
// EncodeBatch and back through arrowio's own decoder unchanged.
func TestEncode_ScalarRoundTrip(t *testing.T) {
	batch := []record.Record{
		record.Record{}.
			Append("flag", record.Bool(true)).
			Append("i64", record.Int64(-9007199254740993, record.SemanticNone)).
			Append("u64", record.Int64(-1, record.SemanticUnsigned)).
			Append("day", record.Int64(19723, record.SemanticDate)).
			Append("ts", record.Int64(1704067200123, record.SemanticTimestampMillis)).
			Append("f64", record.Float64(1.5)).
			Append("f32", record.Float32(0.1)).
			Append("msg", record.Bytes([]byte(`a "quoted" message`))),
	}

	doc := encodeOne(t, newBatchEncoder(t), batch)
	rows := readBackRows(t, doc)
	if len(rows) != 1 {
		t.Fatalf("read back %d rows, want 1", len(rows))
	}

	got := rows[0]
	if !got["flag"].Bool {
		t.Errorf("flag = %+v, want true", got["flag"])
	}
	if got["i64"].I64 != -9007199254740993 {
		t.Errorf("i64 = %+v, want -9007199254740993", got["i64"])
	}
	if got["u64"].Kind != record.KindInt64 || got["u64"].Semantic != record.SemanticUnsigned || got["u64"].I64 != -1 {
		t.Errorf("u64 = %+v, want KindInt64/SemanticUnsigned bits -1", got["u64"])
	}
	if got["day"].Semantic != record.SemanticDate || got["day"].I64 != 19723 {
		t.Errorf("day = %+v, want SemanticDate 19723", got["day"])
	}
	if got["ts"].Semantic != record.SemanticTimestampMillis || got["ts"].I64 != 1704067200123 {
		t.Errorf("ts = %+v, want SemanticTimestampMillis 1704067200123", got["ts"])
	}
	if got["f64"].F64 != 1.5 {
		t.Errorf("f64 = %+v, want 1.5", got["f64"])
	}
	if got["f32"].Semantic != record.SemanticFloat32 {
		t.Errorf("f32 = %+v, want SemanticFloat32", got["f32"])
	}
	if string(got["msg"].Str) != `a "quoted" message` {
		t.Errorf("msg = %q, want %q", got["msg"].Str, `a "quoted" message`)
	}
}

// TestEncode_NullColumn checks a null value round-trips as record.KindNull, not a coerced zero
// value.
func TestEncode_NullColumn(t *testing.T) {
	batch := []record.Record{
		record.Record{}.Append("a", record.Int64(1, record.SemanticNone)),
		record.Record{}.Append("a", record.Null()),
	}

	doc := encodeOne(t, newBatchEncoder(t), batch)
	rows := readBackRows(t, doc)
	if len(rows) != 2 {
		t.Fatalf("read back %d rows, want 2", len(rows))
	}
	if rows[1]["a"].Kind != record.KindNull {
		t.Errorf("row 1 a = %+v, want KindNull", rows[1]["a"])
	}
}

// TestEncode_NestedStructAndList checks a record.KindMap field encodes to a Struct column and a
// record.KindList field encodes to a List column, and both round-trip through arrowio's own
// decoder with their original values and order intact.
func TestEncode_NestedStructAndList(t *testing.T) {
	attrs := record.Record{}.
		Append("a", record.Bytes([]byte("x"))).
		Append("b", record.Int64(2, record.SemanticNone))
	tags := []record.Value{record.Bytes([]byte("x")), record.Bytes([]byte("y")), record.Bytes([]byte("z"))}

	batch := []record.Record{
		record.Record{}.Append("attrs", record.Map(attrs)).Append("tags", record.List(tags)),
	}

	doc := encodeOne(t, newBatchEncoder(t), batch)
	rows := readBackRows(t, doc)
	if len(rows) != 1 {
		t.Fatalf("read back %d rows, want 1", len(rows))
	}

	gotAttrs := rows[0]["attrs"]
	if gotAttrs.Kind != record.KindMap {
		t.Fatalf("attrs.Kind = %v, want KindMap", gotAttrs.Kind)
	}
	if v, ok := gotAttrs.Map.Lookup("a"); !ok || string(v.Str) != "x" {
		t.Errorf("attrs.a = %+v, want KindBytes \"x\"", v)
	}
	if v, ok := gotAttrs.Map.Lookup("b"); !ok || v.I64 != 2 {
		t.Errorf("attrs.b = %+v, want KindInt64 2", v)
	}

	gotTags := rows[0]["tags"]
	if gotTags.Kind != record.KindList || len(gotTags.List) != 3 {
		t.Fatalf("tags = %+v, want KindList of length 3", gotTags)
	}
	for i, want := range []string{"x", "y", "z"} {
		if string(gotTags.List[i].Str) != want {
			t.Errorf("tags[%d] = %q, want %q", i, gotTags.List[i].Str, want)
		}
	}
}

// TestEncode_EmptyBatch checks an empty batch produces no document.
func TestEncode_EmptyBatch(t *testing.T) {
	docs, err := newBatchEncoder(t).EncodeBatch(nil)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	if len(docs) != 0 {
		t.Errorf("EncodeBatch returned %d documents, want 0", len(docs))
	}
}

// TestEncode_SchemaMismatch checks a later batch whose field names differ from the schema the
// first batch fixed is rejected rather than silently misaligned.
func TestEncode_SchemaMismatch(t *testing.T) {
	enc := newBatchEncoder(t)
	first := record.Record{}.Append("id", record.Int64(1, record.SemanticNone))
	if _, err := enc.EncodeBatch([]record.Record{first}); err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	second := record.Record{}.Append("other", record.Int64(2, record.SemanticNone))
	_, err := enc.EncodeBatch([]record.Record{second})
	if err == nil {
		t.Fatal("EncodeBatch with a mismatched field name returned no error")
	}
	if !strings.Contains(err.Error(), "does not match the derived schema") {
		t.Errorf("error %q does not name the problem", err)
	}
}

// TestEncode_EmptySchemaRejected checks a leading record with no fields is rejected rather than
// silently producing a zero-column file.
func TestEncode_EmptySchemaRejected(t *testing.T) {
	_, err := newBatchEncoder(t).EncodeBatch([]record.Record{{}})
	if err == nil {
		t.Fatal("EncodeBatch of a fieldless record returned no error")
	}
	if !strings.Contains(err.Error(), "cannot derive a schema") {
		t.Errorf("error %q does not name the problem", err)
	}
}

// TestEncode_DuplicateFieldRejected checks two fields of the same name in one record are rejected
// rather than silently displacing one another.
func TestEncode_DuplicateFieldRejected(t *testing.T) {
	rec := record.Record{}.
		Append("a", record.Int64(1, record.SemanticNone)).
		Append("a", record.Int64(2, record.SemanticNone))
	_, err := newBatchEncoder(t).EncodeBatch([]record.Record{rec})
	if err == nil {
		t.Fatal("EncodeBatch of a duplicate-field record returned no error")
	}
	if !strings.Contains(err.Error(), "duplicate field name") {
		t.Errorf("error %q does not name the problem", err)
	}
}

// TestEncode_ProducesValidArrowFile checks the encoder's output opens cleanly with the raw ipc
// package too, not just arrowio's own decoder, confirming the bytes are a genuinely well-formed
// Arrow IPC file.
func TestEncode_ProducesValidArrowFile(t *testing.T) {
	batch := []record.Record{record.Record{}.Append("id", record.Int64(1, record.SemanticNone))}
	doc := encodeOne(t, newBatchEncoder(t), batch)

	fr, err := ipc.NewFileReader(bytes.NewReader(doc))
	if err != nil {
		t.Fatalf("ipc.NewFileReader: %v", err)
	}
	defer fr.Close()

	if fr.NumRecords() != 1 {
		t.Errorf("NumRecords() = %d, want 1", fr.NumRecords())
	}
}
