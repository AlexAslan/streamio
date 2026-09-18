package parquetio_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/parquetio"
	"github.com/AlexAslan/streamio/internal/record"
)

// newStreamingEncoder builds a streaming encoder with the default options, failing the test rather
// than returning an error nobody would check.
//
//nolint:ireturn // formatio.FinalizableRecordEncoder is what the constructor under test returns.
func newStreamingEncoder(tb testing.TB) formatio.FinalizableRecordEncoder {
	tb.Helper()

	enc, err := parquetio.NewStreamingEncoder(options.New())
	if err != nil {
		tb.Fatalf("NewStreamingEncoder: %v", err)
	}
	fin, ok := enc.(formatio.FinalizableRecordEncoder)
	if !ok {
		tb.Fatalf("NewStreamingEncoder returned %T, want a formatio.FinalizableRecordEncoder", enc)
	}
	return fin
}

// TestStreamingEncoder_MultipleBatchesFormOneFile feeds the streaming encoder more rows than one
// batch and checks the concatenation of every EncodeBatch document plus Finalize's trailing bytes
// is a single valid Parquet file containing every row, across multiple row groups.
func TestStreamingEncoder_MultipleBatchesFormOneFile(t *testing.T) {
	enc := newStreamingEncoder(t)

	first := scalarBatch()
	second := scalarBatch()

	var doc bytes.Buffer
	for _, batch := range [][]record.Record{first, second} {
		docs, err := enc.EncodeBatch(batch)
		if err != nil {
			t.Fatalf("EncodeBatch: %v", err)
		}
		for _, d := range docs {
			doc.Write(d)
		}
	}

	trailer, err := enc.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	doc.Write(trailer)

	rows := readBackColumns(t, doc.Bytes())
	wantRows := len(first) + len(second)
	if len(rows) != wantRows {
		t.Fatalf("read back %d rows, want %d", len(rows), wantRows)
	}
}

// TestStreamingEncoder_EmptyRunFinalizesToNothing checks a run with no batches produces no bytes
// at all: there is no schema to derive, and so nothing to close.
func TestStreamingEncoder_EmptyRunFinalizesToNothing(t *testing.T) {
	enc := newStreamingEncoder(t)

	trailer, err := enc.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if len(trailer) != 0 {
		t.Fatalf("Finalize on an empty run returned %d bytes, want 0", len(trailer))
	}
}

// TestStreamingEncoder_SecondBatchSchemaMismatchIsRejected checks the schema-mismatch guard
// rowBuilder shares with the batch-per-file encoder (see TestBatchEncoder_SchemaFixedByFirstBatch)
// is honored through streamingEncoder's EncodeBatch too, not just derived and then ignored.
func TestStreamingEncoder_SecondBatchSchemaMismatchIsRejected(t *testing.T) {
	enc := newStreamingEncoder(t)

	first := []record.Record{
		record.Record{}.
			Append("i", record.Int64(1, record.SemanticNone)).
			Append("msg", record.Bytes([]byte("hello"))),
	}
	if _, err := enc.EncodeBatch(first); err != nil {
		t.Fatalf("EncodeBatch(first): %v", err)
	}

	second := []record.Record{
		record.Record{}.
			Append("i", record.Float64(2.5)).
			Append("msg", record.Bytes([]byte("world"))),
	}
	_, err := enc.EncodeBatch(second)
	if err == nil {
		t.Fatal("EncodeBatch(second) succeeded, want a schema-mismatch error")
	}
	if !strings.Contains(err.Error(), "does not match the derived schema") {
		t.Errorf("EncodeBatch(second) error = %v, want one containing %q",
			err, "does not match the derived schema")
	}
}

// TestStreamingEncoder_EncodeBatchAfterFinalizeIsRejected pins the fix for a real data-loss bug:
// without this guard, calling EncodeBatch after Finalize silently accepted the rows and discarded
// them — parquet-go's WriteRows/Flush on a closed writer return a nil error with no document,
// indistinguishable from an ordinary small batch that hasn't crossed the internal flush threshold
// yet. Losing rows silently is worse than erroring, so EncodeBatch must refuse outright.
func TestStreamingEncoder_EncodeBatchAfterFinalizeIsRejected(t *testing.T) {
	enc := newStreamingEncoder(t)

	batch := []record.Record{record.Record{}.Append("i", record.Int64(1, record.SemanticNone))}
	if _, err := enc.EncodeBatch(batch); err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	if _, err := enc.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	docs, err := enc.EncodeBatch(batch)
	if err == nil {
		t.Fatalf("EncodeBatch after Finalize succeeded with %d docs, want an error", len(docs))
	}
	if !strings.Contains(err.Error(), "after Finalize") {
		t.Errorf("EncodeBatch after Finalize error = %v, want one mentioning Finalize", err)
	}
}

// maxParquetRowGroups is parquet-go's own MaxRowGroups (math.MaxInt16), duplicated here rather
// than imported since it isn't exported by name in a way this package needs elsewhere — see
// TestStreamingEncoder_TooManyRowGroupsFailsRatherThanSilentlyGrowing for why the exact value
// matters to this test.
const maxParquetRowGroups = 32_767

// TestStreamingEncoder_TooManyRowGroupsFailsRatherThanSilentlyGrowing pins a real, previously
// undocumented constraint discovered while benchmarking streaming_encoder.go at realistic scale:
// parquet-go hard-caps a single file at maxParquetRowGroups row groups, one per batch here. Past
// that, EncodeBatch fails outright with a wrapped ErrTooManyRowGroups — not a growing-but-still-
// valid file, an outright conversion failure — so a caller choosing too small a batch size for a
// very large total input can lose the whole run rather than just pay a larger footer. This is why
// streaming_encoder.go's doc comment and the CLI's --help both call out choosing a batch size that
// keeps row-group count well under this ceiling for large inputs.
func TestStreamingEncoder_TooManyRowGroupsFailsRatherThanSilentlyGrowing(t *testing.T) {
	enc := newStreamingEncoder(t)
	batch := []record.Record{record.Record{}.Append("i", record.Int64(1, record.SemanticNone))}

	for i := range maxParquetRowGroups {
		if _, err := enc.EncodeBatch(batch); err != nil {
			t.Fatalf("EncodeBatch(row group %d): %v", i, err)
		}
	}

	_, err := enc.EncodeBatch(batch)
	if err == nil {
		t.Fatalf("EncodeBatch(row group %d) succeeded, want ErrTooManyRowGroups", maxParquetRowGroups)
	}
	if !strings.Contains(err.Error(), "row groups has been reached") {
		t.Errorf("EncodeBatch(row group %d) error = %v, want one containing %q",
			maxParquetRowGroups, err, "row groups has been reached")
	}
}
