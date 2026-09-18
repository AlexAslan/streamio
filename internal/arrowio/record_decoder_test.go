package arrowio_test

import (
	"context"
	"errors"
	"io"
	"os"
	"streamio/internal/arrowio"
	"streamio/internal/formatio"
	"streamio/internal/record"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// materialised is a decoded record copied out of the decoder's reusable buffers, so a whole file's
// worth of rows can be compared after the fact — mirrors parquetio's identical test helper.
type materialised map[string]record.Value

func materialise(rec record.Record) materialised {
	out := make(materialised, len(rec))
	for _, f := range rec {
		out[f.Name] = copyValue(f.Value)
	}
	return out
}

// copyValue returns v with every buffer/nested structure it references replaced by a private copy.
func copyValue(v record.Value) record.Value {
	if v.Str != nil {
		v.Str = append([]byte(nil), v.Str...)
	}
	if v.Kind == record.KindMap {
		entries := make(record.Record, 0, len(v.Map))
		for _, e := range v.Map {
			entries = entries.Append(e.Name, copyValue(e.Value))
		}
		v.Map = entries
	}
	if v.Kind == record.KindList {
		elements := make([]record.Value, len(v.List))
		for i, e := range v.List {
			elements[i] = copyValue(e)
		}
		v.List = elements
	}
	return v
}

// drainDecoder reads dec to exhaustion with a batch of batchSize and returns every row it yielded.
func drainDecoder(dec formatio.RecordDecoder, batchSize int) ([]materialised, error) {
	var rows []materialised
	batch := make([]record.Record, batchSize)
	for {
		n, err := dec.DecodeNext(context.Background(), batch)
		for i := range batch[:n] {
			rows = append(rows, materialise(batch[i]))
		}
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return rows, err
		}
	}
}

// mustDrain is drainDecoder for the sequential tests, failing on any decode error.
func mustDrain(tb testing.TB, dec formatio.RecordDecoder, batchSize int) []materialised {
	tb.Helper()
	rows, err := drainDecoder(dec, batchSize)
	if err != nil {
		tb.Fatalf("DecodeNext: %v", err)
	}
	return rows
}

// TestNewDecoder_DecodesScalarColumns checks a basic id/label file round-trips through the decoder
// with the right Kinds.
func TestNewDecoder_DecodesScalarColumns(t *testing.T) {
	path := writeArrowFile(t, 3, 1)

	dec, err := arrowio.NewDecoder(newConfig(8, 1), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer func() {
		if cerr := dec.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	}()

	rows := mustDrain(t, dec, 8)
	if len(rows) != 3 {
		t.Fatalf("decoded %d rows, want 3", len(rows))
	}
	for i, row := range rows {
		id, ok := row["id"]
		if !ok || id.Kind != record.KindInt64 || id.I64 != int64(i) {
			t.Errorf("row %d id = %+v, want KindInt64 %d", i, id, i)
		}
		label, ok := row["label"]
		if !ok || label.Kind != record.KindBytes {
			t.Errorf("row %d label = %+v, want KindBytes", i, label)
		}
	}
}

// TestNewDecoder_EmptyFile checks a schema-only file (no record batches) decodes to zero rows
// rather than erroring.
func TestNewDecoder_EmptyFile(t *testing.T) {
	path := writeEmptyArrowFile(t)

	dec, err := arrowio.NewDecoder(newConfig(8, 1), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer dec.Close()

	rows := mustDrain(t, dec, 8)
	if len(rows) != 0 {
		t.Errorf("decoded %d rows from an empty file, want 0", len(rows))
	}
}

// TestNewDecoder_BatchesAndRecordBatches checks the decoder suspends and resumes correctly across
// both an Arrow record-batch boundary and streamio's own dispatch-batch boundary, yielding every
// row exactly once either way — mirrors parquetio's identical coverage of row-group vs. dispatch
// batch boundaries.
func TestNewDecoder_BatchesAndRecordBatches(t *testing.T) {
	const rows = 97
	path := writeArrowFile(t, rows, 5)

	dec, err := arrowio.NewDecoder(newConfig(11, 1), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer dec.Close()

	got := mustDrain(t, dec, 11)
	if len(got) != rows {
		t.Fatalf("decoded %d rows, want %d", len(got), rows)
	}
	seen := make(map[int64]bool, rows)
	for _, row := range got {
		id := row["id"].I64
		if seen[id] {
			t.Fatalf("row id %d decoded more than once", id)
		}
		seen[id] = true
	}
}

// TestNewDecoder_SplitDividesRecordBatches checks Split's decoders together cover every row exactly
// once, mirroring parquetio's identical coverage for row groups.
func TestNewDecoder_SplitDividesRecordBatches(t *testing.T) {
	const rows = 40
	path := writeArrowFile(t, rows, 4)

	dec, err := arrowio.NewDecoder(newConfig(8, 4), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	splittable, ok := dec.(formatio.SplittableRecordDecoder)
	if !ok {
		t.Fatal("decoder does not implement formatio.SplittableRecordDecoder")
	}

	decoders := splittable.Split(4)
	if len(decoders) != 4 {
		t.Fatalf("Split(4) returned %d decoders, want 4", len(decoders))
	}

	seen := make(map[int64]bool, rows)
	for _, d := range decoders {
		got, drainErr := drainDecoder(d, 8)
		if drainErr != nil {
			t.Fatalf("DecodeNext: %v", drainErr)
		}
		for _, row := range got {
			id := row["id"].I64
			if seen[id] {
				t.Fatalf("row id %d decoded by more than one Split sibling", id)
			}
			seen[id] = true
		}
	}
	if len(seen) != rows {
		t.Fatalf("decoded %d distinct rows across every Split sibling, want %d", len(seen), rows)
	}
	if closeErr := decoders[0].Close(); closeErr != nil {
		t.Errorf("Close: %v", closeErr)
	}
}

// nestedItemType is the "items" list's element type: a small key/value struct, used so the list
// column exercises a List of Struct, not just a list of scalars.
func nestedItemType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "k", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	)
}

// writeNestedArrowFile writes one row with a Struct "attrs" column ({a: "x", b: 2}) and a
// List-of-Struct "items" column ([{k: "k1", v: 9}]), and returns its path.
func writeNestedArrowFile(tb testing.TB) string {
	tb.Helper()
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "attrs", Type: arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
		{Name: "items", Type: arrow.ListOf(nestedItemType()), Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	structBld := rb.Field(0).(*array.StructBuilder)
	structBld.Append(true)
	structBld.FieldBuilder(0).(*array.StringBuilder).Append("x")
	structBld.FieldBuilder(1).(*array.Int64Builder).Append(2)

	listBld := rb.Field(1).(*array.ListBuilder)
	listBld.Append(true)
	itemBld := listBld.ValueBuilder().(*array.StructBuilder)
	itemBld.Append(true)
	itemBld.FieldBuilder(0).(*array.StringBuilder).Append("k1")
	itemBld.FieldBuilder(1).(*array.Int64Builder).Append(9)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	path := tb.TempDir() + "/nested.arrow"
	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	w, err := ipc.NewFileWriter(f, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	if err != nil {
		tb.Fatalf("NewFileWriter: %v", err)
	}
	if err = w.Write(rec); err != nil {
		tb.Fatalf("Write: %v", err)
	}
	if err = w.Close(); err != nil {
		tb.Fatalf("Close writer: %v", err)
	}
	if err = f.Close(); err != nil {
		tb.Fatalf("close file: %v", err)
	}
	return path
}

// TestNewDecoder_NestedStructAndList checks a Struct column decodes to record.KindMap and a List
// column decodes to record.KindList, preserving element order and nested values, including a List
// of Struct — one level of real nesting, not just a flat schema.
func TestNewDecoder_NestedStructAndList(t *testing.T) {
	path := writeNestedArrowFile(t)

	dec, err := arrowio.NewDecoder(newConfig(8, 1), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer dec.Close()

	rows := mustDrain(t, dec, 8)
	if len(rows) != 1 {
		t.Fatalf("decoded %d rows, want 1", len(rows))
	}

	attrs := rows[0]["attrs"]
	if attrs.Kind != record.KindMap {
		t.Fatalf("attrs.Kind = %v, want KindMap", attrs.Kind)
	}
	if v, ok := attrs.Map.Lookup("a"); !ok || string(v.Str) != "x" {
		t.Errorf("attrs.a = %+v, want KindBytes \"x\"", v)
	}
	if v, ok := attrs.Map.Lookup("b"); !ok || v.I64 != 2 {
		t.Errorf("attrs.b = %+v, want KindInt64 2", v)
	}

	items := rows[0]["items"]
	if items.Kind != record.KindList || len(items.List) != 1 {
		t.Fatalf("items = %+v, want KindList of length 1", items)
	}
	if items.List[0].Kind != record.KindMap {
		t.Fatalf("items[0] = %+v, want KindMap", items.List[0])
	}
	if v, ok := items.List[0].Map.Lookup("k"); !ok || string(v.Str) != "k1" {
		t.Errorf("items[0].k = %+v, want KindBytes \"k1\"", v)
	}
	if v, ok := items.List[0].Map.Lookup("v"); !ok || v.I64 != 9 {
		t.Errorf("items[0].v = %+v, want KindInt64 9", v)
	}
}

// TestNewDecoder_ContextCancellation checks a cancelled context stops decoding rather than running
// the file to completion.
func TestNewDecoder_ContextCancellation(t *testing.T) {
	path := writeArrowFile(t, 200, 5)

	dec, err := arrowio.NewDecoder(newConfig(8, 1), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer dec.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch := make([]record.Record, 8)
	if _, decErr := dec.DecodeNext(ctx, batch); !errors.Is(decErr, context.Canceled) {
		t.Fatalf("DecodeNext error = %v, want context.Canceled", decErr)
	}
}

// TestNewDecoder_EmptyBatch checks the documented "up to len(batch)" contract holds for the
// degenerate case, without consuming input.
func TestNewDecoder_EmptyBatch(t *testing.T) {
	path := writeArrowFile(t, 5, 1)

	dec, err := arrowio.NewDecoder(newConfig(8, 1), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer dec.Close()

	n, err := dec.DecodeNext(context.Background(), nil)
	if n != 0 || err != nil {
		t.Fatalf("DecodeNext(nil) = (%d, %v), want (0, nil)", n, err)
	}
}
