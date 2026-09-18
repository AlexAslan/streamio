package csvio_test

import (
	"strings"
	"testing"

	"github.com/AlexAslan/streamio/internal/csvio"
	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

// newEncoder builds a plain (non-streaming) encoder, failing the test if the constructor does.
//
//nolint:ireturn // formatio.RecordEncoder is exactly what the constructor under test returns.
func newEncoder(tb testing.TB, delimiter rune, hasHeader bool) formatio.RecordEncoder {
	tb.Helper()
	cfg := options.New(options.WithCSVDelimiter(delimiter), options.WithCSVHasHeader(hasHeader))
	enc, err := csvio.NewEncoder(cfg)
	if err != nil {
		tb.Fatalf("NewEncoder: %v", err)
	}
	return enc
}

func row(fields ...string) record.Record {
	rec := record.Record{}
	for i := 0; i < len(fields); i += 2 {
		rec = rec.Append(fields[i], record.Bytes([]byte(fields[i+1])))
	}
	return rec
}

// TestEncode_HeaderAndRows checks the header comes from the first batch's field names, in order,
// followed by one row per record.
func TestEncode_HeaderAndRows(t *testing.T) {
	enc := newEncoder(t, ',', true)
	batch := []record.Record{
		row("id", "1", "name", "Alice"),
		row("id", "2", "name", "Bob"),
	}

	docs, err := enc.EncodeBatch(batch)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("EncodeBatch returned %d documents, want 1", len(docs))
	}

	want := "id,name\n1,Alice\n2,Bob\n"
	if got := string(docs[0]); got != want {
		t.Errorf("document = %q, want %q", got, want)
	}
}

// TestEncode_NoHeader checks no header line is written when cfg.CSV.HasHeader is false.
func TestEncode_NoHeader(t *testing.T) {
	enc := newEncoder(t, ',', false)
	docs, err := enc.EncodeBatch([]record.Record{row("id", "1")})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	want := "1\n"
	if got := string(docs[0]); got != want {
		t.Errorf("document = %q, want %q", got, want)
	}
}

// TestEncode_TSVDelimiter checks a tab delimiter is honored.
func TestEncode_TSVDelimiter(t *testing.T) {
	enc := newEncoder(t, '\t', true)
	docs, err := enc.EncodeBatch([]record.Record{row("id", "1", "name", "Alice")})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	want := "id\tname\n1\tAlice\n"
	if got := string(docs[0]); got != want {
		t.Errorf("document = %q, want %q", got, want)
	}
}

// TestEncode_QuotesFieldContainingDelimiter checks a value containing the delimiter round-trips
// through encoding/csv's own quoting, not a hand-rolled escape.
func TestEncode_QuotesFieldContainingDelimiter(t *testing.T) {
	enc := newEncoder(t, ',', false)
	docs, err := enc.EncodeBatch([]record.Record{row("name", "Doe, Jane")})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	want := "\"Doe, Jane\"\n"
	if got := string(docs[0]); got != want {
		t.Errorf("document = %q, want %q", got, want)
	}
}

// TestEncode_EmptyBatch checks an empty batch produces no documents.
func TestEncode_EmptyBatch(t *testing.T) {
	docs, err := newEncoder(t, ',', true).EncodeBatch(nil)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	if len(docs) != 0 {
		t.Errorf("EncodeBatch returned %d documents, want 0", len(docs))
	}
}

// TestEncode_SchemaMismatch checks a later batch whose field names differ from the header the
// first batch fixed is rejected rather than silently misaligned.
func TestEncode_SchemaMismatch(t *testing.T) {
	enc := newEncoder(t, ',', true)
	if _, err := enc.EncodeBatch([]record.Record{row("id", "1")}); err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	_, err := enc.EncodeBatch([]record.Record{row("other", "2")})
	if err == nil {
		t.Fatal("EncodeBatch with a mismatched field name returned no error")
	}
	if !strings.Contains(err.Error(), "does not match the derived header") {
		t.Errorf("error %q does not name the problem", err)
	}
}

// TestEncode_MapUnsupported checks a record.KindMap field is rejected, matching parquetio's
// identical scope limit.
func TestEncode_MapUnsupported(t *testing.T) {
	rec := record.Record{}.Append("m", record.Map(record.Record{}.Append("a", record.Bytes([]byte("b")))))
	_, err := newEncoder(t, ',', true).EncodeBatch([]record.Record{rec})
	if err == nil {
		t.Fatal("EncodeBatch of a map field returned no error")
	}
	if !strings.Contains(err.Error(), "not supported") {
		t.Errorf("error %q does not name the problem", err)
	}
}

// TestEncode_ListUnsupported checks a record.KindList field is rejected the same way KindMap is.
func TestEncode_ListUnsupported(t *testing.T) {
	rec := record.Record{}.Append("l", record.List([]record.Value{record.Bytes([]byte("a"))}))
	_, err := newEncoder(t, ',', true).EncodeBatch([]record.Record{rec})
	if err == nil {
		t.Fatal("EncodeBatch of a list field returned no error")
	}
	if !strings.Contains(err.Error(), "not supported") {
		t.Errorf("error %q does not name the problem", err)
	}
}

// TestEncode_SingleFileOutputSelectsStreamingEncoder checks cfg.SingleFileOutput routes NewEncoder
// to the streaming variant, mirroring parquetio.NewEncoder's identical dispatch.
func TestEncode_SingleFileOutputSelectsStreamingEncoder(t *testing.T) {
	cfg := options.New(options.WithSingleFileOutput(true))
	enc, err := csvio.NewEncoder(cfg)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if _, ok := enc.(formatio.FinalizableRecordEncoder); !ok {
		t.Fatal("NewEncoder with SingleFileOutput did not return a FinalizableRecordEncoder")
	}
}

// TestStreamingEncoder_SpansBatchesWithOneHeader checks the header is written once, ahead of the
// first batch's rows, and every later batch contributes rows alone — concatenating every
// EncodeBatch result in order is one valid CSV document.
func TestStreamingEncoder_SpansBatchesWithOneHeader(t *testing.T) {
	cfg := options.New(options.WithSingleFileOutput(true), options.WithCSVHasHeader(true))
	base, err := csvio.NewStreamingEncoder(cfg)
	if err != nil {
		t.Fatalf("NewStreamingEncoder: %v", err)
	}
	enc, ok := base.(formatio.FinalizableRecordEncoder)
	if !ok {
		t.Fatal("NewStreamingEncoder did not return a FinalizableRecordEncoder")
	}

	var out strings.Builder
	for _, batch := range [][]record.Record{
		{row("id", "1")},
		{row("id", "2")},
	} {
		docs, encErr := enc.EncodeBatch(batch)
		if encErr != nil {
			t.Fatalf("EncodeBatch: %v", encErr)
		}
		for _, d := range docs {
			out.Write(d)
		}
	}

	fin, finErr := enc.Finalize()
	if finErr != nil {
		t.Fatalf("Finalize: %v", finErr)
	}
	out.Write(fin)

	want := "id\n1\n2\n"
	if got := out.String(); got != want {
		t.Errorf("concatenated document = %q, want %q", got, want)
	}
}

// TestStreamingEncoder_EncodeAfterFinalizeFails checks a stray EncodeBatch call after Finalize is
// rejected rather than silently producing more rows past the point the caller believed the
// document was complete.
func TestStreamingEncoder_EncodeAfterFinalizeFails(t *testing.T) {
	cfg := options.New(options.WithSingleFileOutput(true))
	base, err := csvio.NewStreamingEncoder(cfg)
	if err != nil {
		t.Fatalf("NewStreamingEncoder: %v", err)
	}
	enc, ok := base.(formatio.FinalizableRecordEncoder)
	if !ok {
		t.Fatal("NewStreamingEncoder did not return a FinalizableRecordEncoder")
	}

	if _, err = enc.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	_, err = enc.EncodeBatch([]record.Record{row("id", "1")})
	if err == nil {
		t.Fatal("EncodeBatch after Finalize returned no error")
	}
}

// TestEncode_TimestampAndDateRendering checks date/timestamp semantics render identically to
// jsonio's own rendering, since csvio reuses jsonio's helpers rather than reimplementing them.
func TestEncode_TimestampAndDateRendering(t *testing.T) {
	rec := record.Record{}.
		Append("d", record.Int64(19723, record.SemanticDate)).
		Append("t", record.Int64(1704067200123, record.SemanticTimestampMillis))

	docs, err := newEncoder(t, ',', false).EncodeBatch([]record.Record{rec})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	want := "2024-01-01,2024-01-01T00:00:00.123Z\n"
	if got := string(docs[0]); got != want {
		t.Errorf("document = %q, want %q", got, want)
	}
}
