package parquetio_test

import (
	"bytes"
	"strings"
	"testing"

	parquetgo "github.com/AlexAslan/parquet-go"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/parquetio"
	"github.com/AlexAslan/streamio/internal/record"
)

// newBatchEncoder builds an encoder with the default options, failing the test rather than returning
// an error nobody would check.
//
//nolint:ireturn // formatio.RecordEncoder is what the constructor under test returns.
func newBatchEncoder(tb testing.TB) formatio.RecordEncoder {
	tb.Helper()

	enc, err := parquetio.NewEncoder(options.New())
	if err != nil {
		tb.Fatalf("NewEncoder: %v", err)
	}
	return enc
}

// readBackColumns opens doc as a Parquet file and returns every row as a name → parquetgo.Value map,
// so assertions name the column rather than depending on the schema's own (name-sorted) order.
func readBackColumns(tb testing.TB, doc []byte) []map[string]parquetgo.Value {
	tb.Helper()

	file, err := parquetgo.OpenFile(bytes.NewReader(doc), int64(len(doc)))
	if err != nil {
		tb.Fatalf("OpenFile: %v", err)
	}

	names := make([]string, 0, len(file.Schema().Fields()))
	for _, field := range file.Schema().Fields() {
		names = append(names, field.Name())
	}

	rows := make([]map[string]parquetgo.Value, 0, file.NumRows())
	for _, rowGroup := range file.RowGroups() {
		reader := parquetgo.NewGenericRowGroupReader[any](rowGroup)
		buf := make([]parquetgo.Row, rowGroup.NumRows())
		n, readErr := reader.ReadRows(buf)
		if closeErr := reader.Close(); closeErr != nil {
			tb.Fatalf("close row group reader: %v", closeErr)
		}
		if n == 0 && readErr != nil {
			tb.Fatalf("ReadRows: %v", readErr)
		}

		for _, row := range buf[:n] {
			values := make(map[string]parquetgo.Value, len(names))
			for i, value := range row {
				if i < len(names) {
					values[names[i]] = value
				}
			}
			rows = append(rows, values)
		}
	}
	return rows
}

// encodeOne encodes batch and returns the single document it must produce, failing on any error:
// this encoder renders a whole batch as one Parquet file, so anything else is a bug.
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

// scalarBatch is one batch covering every column shape the encoder derives a distinct Parquet node
// for, plus a fully-null second row so the optional-column handling is exercised alongside it.
func scalarBatch() []record.Record {
	typed := record.Record{}.
		Append("flag", record.Bool(true)).
		Append("i64", record.Int64(-9007199254740993, record.SemanticNone)).
		Append("u64", record.Int64(-1, record.SemanticUnsigned)). // bits of math.MaxUint64
		Append("day", record.Int64(19723, record.SemanticDate)).
		Append("ts", record.Int64(1704067200123, record.SemanticTimestampMillis)).
		Append("f64", record.Float64(1.5)).
		Append("f32", record.Float32(0.1)).
		Append("msg", record.Bytes([]byte(`a "quoted" message`)))

	nulls := record.Record{}
	for _, field := range typed {
		nulls = nulls.Append(field.Name, record.Null())
	}

	return []record.Record{typed, nulls}
}

// TestBatchEncoder_RoundTrip checks a batch of canonical records becomes one standalone Parquet
// file whose rows read back as the values that went in, at the physical type each Semantic implies.
func TestBatchEncoder_RoundTrip(t *testing.T) {
	doc := encodeOne(t, newBatchEncoder(t), scalarBatch())

	rows := readBackColumns(t, doc)
	if len(rows) != 2 {
		t.Fatalf("read back %d rows, want 2", len(rows))
	}

	got := rows[0]
	if !got["flag"].Boolean() {
		t.Errorf("flag = false, want true")
	}
	if v := got["i64"].Int64(); v != -9007199254740993 {
		t.Errorf("i64 = %d, want -9007199254740993", v)
	}
	if v := got["u64"].Uint64(); v != 18446744073709551615 {
		t.Errorf("u64 = %d, want 18446744073709551615", v)
	}
	if v := got["day"].Int32(); v != 19723 {
		t.Errorf("day = %d, want 19723", v)
	}
	if v := got["ts"].Int64(); v != 1704067200123 {
		t.Errorf("ts = %d, want 1704067200123", v)
	}
	if v := got["f64"].Double(); v != 1.5 {
		t.Errorf("f64 = %v, want 1.5", v)
	}
	if v := got["f32"].Float(); v != 0.1 {
		t.Errorf("f32 = %v, want 0.1", v)
	}
	if v := string(got["msg"].ByteArray()); v != `a "quoted" message` {
		t.Errorf("msg = %q, want %q", v, `a "quoted" message`)
	}

	for name, value := range rows[1] {
		if !value.IsNull() {
			t.Errorf("row 1 column %q = %v, want null", name, value)
		}
	}
}

// TestBatchEncoder_SchemaAnnotations checks the logical types land on the schema rather than on the
// values: a date is an int32 DATE column and a timestamp keeps the unit its Semantic carried, which
// is the whole reason decoders classify instead of formatting.
func TestBatchEncoder_SchemaAnnotations(t *testing.T) {
	batch := []record.Record{
		record.Record{}.
			Append("day", record.Int64(19723, record.SemanticDate)).
			Append("ms", record.Int64(1, record.SemanticTimestampMillis)).
			Append("us", record.Int64(2, record.SemanticTimestampMicros)).
			Append("ns", record.Int64(3, record.SemanticTimestampNanos)).
			Append("u64", record.Int64(4, record.SemanticUnsigned)),
	}

	doc := encodeOne(t, newBatchEncoder(t), batch)
	file, err := parquetgo.OpenFile(bytes.NewReader(doc), int64(len(doc)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	schema := file.Schema().String()
	for _, want := range []string{
		"optional int32 day (DATE)",
		"unit=MILLIS",
		"unit=MICROS",
		"unit=NANOS",
		"INT(64,false)",
	} {
		if !strings.Contains(schema, want) {
			t.Errorf("schema is missing %q:\n%s", want, schema)
		}
	}
}

// TestBatchEncoder_SchemaFixedByFirstBatch checks a second batch is written under the schema the
// first one derived, and that a record which doesn't fit it is an error rather than a silent
// coercion — a Parquet file has one schema, so there is nowhere for a divergent record to go.
func TestBatchEncoder_SchemaFixedByFirstBatch(t *testing.T) {
	type args struct {
		name    string
		wantErr string
		second  []record.Record
	}

	base := []record.Record{
		record.Record{}.
			Append("i", record.Int64(1, record.SemanticNone)).
			Append("msg", record.Bytes([]byte("hello"))),
	}

	tests := []args{
		{
			name: "same shape is accepted",
			second: []record.Record{
				record.Record{}.
					Append("i", record.Int64(2, record.SemanticNone)).
					Append("msg", record.Bytes([]byte("world"))),
			},
		},
		{
			name: "null for a typed column is accepted",
			second: []record.Record{
				record.Record{}.
					Append("i", record.Null()).
					Append("msg", record.Null()),
			},
		},
		{
			name: "changed kind is rejected",
			second: []record.Record{
				record.Record{}.
					Append("i", record.Float64(2.5)).
					Append("msg", record.Bytes([]byte("world"))),
			},
			wantErr: "does not match the derived schema",
		},
		{
			name: "changed semantic is rejected",
			second: []record.Record{
				record.Record{}.
					Append("i", record.Int64(2, record.SemanticTimestampMillis)).
					Append("msg", record.Bytes([]byte("world"))),
			},
			wantErr: "does not match the derived schema",
		},
		{
			name: "extra field is rejected",
			second: []record.Record{
				record.Record{}.
					Append("i", record.Int64(2, record.SemanticNone)).
					Append("msg", record.Bytes([]byte("world"))).
					Append("extra", record.Bool(true)),
			},
			wantErr: "has 3 fields, the schema has 2",
		},
		{
			name: "reordered fields are rejected",
			second: []record.Record{
				record.Record{}.
					Append("msg", record.Bytes([]byte("world"))).
					Append("i", record.Int64(2, record.SemanticNone)),
			},
			wantErr: "does not match the derived schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := newBatchEncoder(t)
			encodeOne(t, enc, base)

			docs, err := enc.EncodeBatch(tt.second)
			if tt.wantErr == "" {
				assertSecondBatchAccepted(t, docs, err)
				return
			}

			if err == nil {
				t.Fatalf("EncodeBatch succeeded, want an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("EncodeBatch error = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}

// assertSecondBatchAccepted checks a second batch the fixed schema accepts came back as one
// readable single-row document.
func assertSecondBatchAccepted(tb testing.TB, docs [][]byte, err error) {
	tb.Helper()

	if err != nil {
		tb.Fatalf("EncodeBatch: %v", err)
	}
	if len(docs) != 1 {
		tb.Fatalf("second batch produced %d documents, want 1", len(docs))
	}
	if rows := readBackColumns(tb, docs[0]); len(rows) != 1 {
		tb.Fatalf("second batch read back %d rows, want 1", len(rows))
	}
}

// TestBatchEncoder_Rejects covers the inputs the encoder refuses outright, each of which would
// otherwise produce a corrupt or silently wrong file. KindMap and KindList are the documented scope
// limit: either has to fail loudly rather than panic or write a column it guessed at.
func TestBatchEncoder_Rejects(t *testing.T) {
	type args struct {
		name    string
		wantErr string
		batch   []record.Record
	}

	tests := []args{
		{
			name: "map field",
			batch: []record.Record{
				record.Record{}.
					Append("attrs", record.Map(record.Record{}.Append("k", record.Bytes([]byte("v"))))),
			},
			wantErr: "not supported",
		},
		{
			name: "list field",
			batch: []record.Record{
				record.Record{}.Append("tags", record.List([]record.Value{record.Bytes([]byte("a"))})),
			},
			wantErr: "not supported",
		},
		{
			name:    "record with no fields",
			batch:   []record.Record{{}},
			wantErr: "cannot derive a schema from a record with no fields",
		},
		{
			name: "duplicate field name",
			batch: []record.Record{
				record.Record{}.
					Append("i", record.Int64(1, record.SemanticNone)).
					Append("i", record.Int64(2, record.SemanticNone)),
			},
			wantErr: "duplicate field name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newBatchEncoder(t).EncodeBatch(tt.batch)
			if err == nil {
				t.Fatalf("EncodeBatch succeeded, want an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("EncodeBatch error = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}

// TestBatchEncoder_TypesColumnFromFirstNonNullInBatch checks a column whose leading record happens
// to be null is still typed from a later record in the same batch. A JSON null carries no type, so
// looking only at the first record would leave a perfectly well-typed column untyped — which is the
// common case for an NDJSON file whose first line has an absent optional field.
func TestBatchEncoder_TypesColumnFromFirstNonNullInBatch(t *testing.T) {
	batch := []record.Record{
		record.Record{}.Append("x", record.Null()),
		record.Record{}.Append("x", record.Int64(42, record.SemanticNone)),
	}

	rows := readBackColumns(t, encodeOne(t, newBatchEncoder(t), batch))
	if len(rows) != 2 {
		t.Fatalf("read back %d rows, want 2", len(rows))
	}
	if !rows[0]["x"].IsNull() {
		t.Errorf("row 0 x = %v, want null", rows[0]["x"])
	}
	if got := rows[1]["x"].Int64(); got != 42 {
		t.Errorf("row 1 x = %d, want 42", got)
	}
}

// TestBatchEncoder_UntypedColumnTakesStrings pins what happens to a column with no non-null value
// anywhere in the first batch: it becomes a string column, which then accepts nulls and strings and
// rejects anything else rather than guessing at a coercion.
func TestBatchEncoder_UntypedColumnTakesStrings(t *testing.T) {
	enc := newBatchEncoder(t)
	doc := encodeOne(t, enc, []record.Record{record.Record{}.Append("x", record.Null())})

	rows := readBackColumns(t, doc)
	if len(rows) != 1 || !rows[0]["x"].IsNull() {
		t.Fatalf("read back %v, want one null row", rows)
	}

	accepted, err := enc.EncodeBatch(
		[]record.Record{record.Record{}.Append("x", record.Bytes([]byte("later")))})
	if err != nil {
		t.Fatalf("EncodeBatch rejected a string for an untyped column: %v", err)
	}
	if got := string(readBackColumns(t, accepted[0])[0]["x"].ByteArray()); got != "later" {
		t.Errorf("x = %q, want %q", got, "later")
	}

	_, err = enc.EncodeBatch(
		[]record.Record{record.Record{}.Append("x", record.Int64(1, record.SemanticNone))})
	if err == nil {
		t.Fatal("EncodeBatch accepted an integer for a column that had settled on strings")
	}
	if !strings.Contains(err.Error(), "does not match the derived schema") {
		t.Errorf("EncodeBatch error = %v, want a schema mismatch", err)
	}
}

// TestBatchEncoder_EmptyBatchWritesNothing checks an empty batch produces no document at all: a
// zero-row Parquet file is not a document any consumer of this stream has a use for, and the pool
// relies on this to skip dispatching one.
func TestBatchEncoder_EmptyBatchWritesNothing(t *testing.T) {
	docs, err := newBatchEncoder(t).EncodeBatch(nil)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	if len(docs) != 0 {
		t.Errorf("EncodeBatch produced %d documents for an empty batch, want 0", len(docs))
	}
}

// TestBatchEncoder_WriterReuseAcrossBatches checks the reused writer and buffer really are reset per
// batch: each document must be independently openable and hold only its own rows, not an
// accumulation of every batch seen so far. It also checks a document handed out earlier still reads
// back as itself once later batches have overwritten the buffer it was rendered in — a dispatch
// worker may still be holding it.
func TestBatchEncoder_WriterReuseAcrossBatches(t *testing.T) {
	enc := newBatchEncoder(t)
	docs := make([][]byte, 0, 3)

	for batchIdx := range 3 {
		batch := make([]record.Record, batchIdx+1)
		for i := range batch {
			batch[i] = record.Record{}.Append("i", record.Int64(int64(batchIdx*10+i), record.SemanticNone))
		}
		docs = append(docs, encodeOne(t, enc, batch))
	}

	for batchIdx, doc := range docs {
		rows := readBackColumns(t, doc)
		if len(rows) != batchIdx+1 {
			t.Fatalf("batch %d read back %d rows, want %d", batchIdx, len(rows), batchIdx+1)
		}
		for i, row := range rows {
			if got, want := row["i"].Int64(), int64(batchIdx*10+i); got != want {
				t.Errorf("batch %d row %d = %d, want %d", batchIdx, i, got, want)
			}
		}
	}
}
