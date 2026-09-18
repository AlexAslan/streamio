package parquetio_test

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/parquetio"
	"github.com/AlexAslan/streamio/internal/record"
)

// richRow covers every classification branch scalarValue has: each physical parquet kind, and each
// logical-type annotation that changes how a value must be read back.
type richRow struct {
	Attributes map[string]string `parquet:"attributes"`
	Name       string            `parquet:"name"`
	TSMillis   int64             `parquet:"ts_millis,timestamp"`
	TSMicros   int64             `parquet:"ts_micros,timestamp(microsecond)"`
	TSNanos    int64             `parquet:"ts_nanos,timestamp(nanosecond)"`
	U64        uint64            `parquet:"u64,uint(64)"`
	I64        int64             `parquet:"i64"`
	F64        float64           `parquet:"f64"`
	Day        int32             `parquet:"day,date"`
	U32        uint32            `parquet:"u32,uint(32)"`
	I32        int32             `parquet:"i32"`
	F32        float32           `parquet:"f32"`
	Flag       bool              `parquet:"flag"`
}

// optionalRow has a nullable column, so a decoded row can carry a genuine null.
type optionalRow struct {
	Extra *int64 `parquet:"extra,optional"`
	Name  string `parquet:"name"`
}

// materialised is a decoded record copied out of the decoder's reusable buffers, so a whole file's
// worth of rows can be compared after the fact. Records alias the decoder's scratch and its page
// buffers, which the next DecodeNext call reuses.
type materialised map[string]record.Value

// materialise deep-copies rec, including any nested map entries and every []byte payload.
func materialise(rec record.Record) materialised {
	out := make(materialised, len(rec))
	for _, f := range rec {
		out[f.Name] = copyValue(f.Value)
	}
	return out
}

// copyValue returns v with every buffer it references replaced by a private copy.
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
	return v
}

// drainDecoder reads dec to exhaustion with a batch of batchSize and returns every row it yielded.
// It reports errors rather than failing the test itself, so concurrent callers don't call Fatalf
// off the test goroutine.
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

// newRichParquet writes a one-row richRow fixture and returns its path.
func newRichParquet(tb testing.TB, row richRow) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "rich.parquet")
	writeParquetRows(tb, path, []richRow{row})
	return path
}

// TestNewDecoder_ClassifiesColumns pins the whole point of the record path: a decoder classifies
// and never formats, so a date stays a raw day count and a timestamp stays a raw epoch integer
// carrying the unit it was stored in.
func TestNewDecoder_ClassifiesColumns(t *testing.T) {
	row := richRow{
		Name:     "hello",
		Day:      19723,
		TSMillis: 1704067200000,
		TSMicros: 1704067200000000,
		TSNanos:  1704067200000000123,
		U32:      4294967295,
		U64:      18446744073709551615,
		I32:      -7,
		I64:      -9007199254740993,
		F32:      0.1,
		F64:      1.5,
		Flag:     true,
	}
	path := newRichParquet(t, row)

	dec, err := parquetio.NewDecoder(newConfig(8, 1), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer func() {
		if cerr := dec.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	}()

	rows := mustDrain(t, dec, 8)
	if len(rows) != 1 {
		t.Fatalf("decoded %d rows, want 1", len(rows))
	}
	got := rows[0]

	type args struct {
		check    func(record.Value) bool
		name     string
		field    string
		wantKind record.Kind
		wantSem  record.Semantic
	}

	tests := []args{
		{
			name: "string stays bytes", field: "name",
			wantKind: record.KindBytes, wantSem: record.SemanticNone,
			check: func(v record.Value) bool { return string(v.Str) == "hello" },
		},
		{
			name: "date keeps the raw day count", field: "day",
			wantKind: record.KindInt64, wantSem: record.SemanticDate,
			check: func(v record.Value) bool { return v.I64 == 19723 },
		},
		{
			name: "millisecond timestamp keeps its unit", field: "ts_millis",
			wantKind: record.KindInt64, wantSem: record.SemanticTimestampMillis,
			check: func(v record.Value) bool { return v.I64 == row.TSMillis },
		},
		{
			name: "microsecond timestamp keeps its unit", field: "ts_micros",
			wantKind: record.KindInt64, wantSem: record.SemanticTimestampMicros,
			check: func(v record.Value) bool { return v.I64 == row.TSMicros },
		},
		{
			name: "nanosecond timestamp keeps its unit", field: "ts_nanos",
			wantKind: record.KindInt64, wantSem: record.SemanticTimestampNanos,
			check: func(v record.Value) bool { return v.I64 == row.TSNanos },
		},
		{
			name: "uint32 is tagged unsigned", field: "u32",
			wantKind: record.KindInt64, wantSem: record.SemanticUnsigned,
			check: func(v record.Value) bool { return uint64(v.I64) == uint64(row.U32) },
		},
		{
			name: "uint64 keeps its bit pattern", field: "u64",
			wantKind: record.KindInt64, wantSem: record.SemanticUnsigned,
			check: func(v record.Value) bool { return uint64(v.I64) == row.U64 },
		},
		{
			name: "signed int32 is a plain int64", field: "i32",
			wantKind: record.KindInt64, wantSem: record.SemanticNone,
			check: func(v record.Value) bool { return v.I64 == int64(row.I32) },
		},
		{
			name: "signed int64 is a plain int64", field: "i64",
			wantKind: record.KindInt64, wantSem: record.SemanticNone,
			check: func(v record.Value) bool { return v.I64 == row.I64 },
		},
		{
			name: "float32 is tagged with its precision", field: "f32",
			wantKind: record.KindFloat64, wantSem: record.SemanticFloat32,
			check: func(v record.Value) bool { return v.F64 == float64(row.F32) },
		},
		{
			name: "float64 carries no semantic", field: "f64",
			wantKind: record.KindFloat64, wantSem: record.SemanticNone,
			check: func(v record.Value) bool { return v.F64 == row.F64 },
		},
		{
			name: "bool", field: "flag",
			wantKind: record.KindBool, wantSem: record.SemanticNone,
			check: func(v record.Value) bool { return v.Bool },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, ok := got[tt.field]
			if !ok {
				t.Fatalf("field %q missing from decoded record %v", tt.field, got)
			}
			if v.Kind != tt.wantKind {
				t.Errorf("Kind = %v, want %v", v.Kind, tt.wantKind)
			}
			if v.Semantic != tt.wantSem {
				t.Errorf("Semantic = %v, want %v", v.Semantic, tt.wantSem)
			}
			if !tt.check(v) {
				t.Errorf("payload check failed for %+v", v)
			}
		})
	}
}

// TestNewDecoder_MapColumns checks Map(String,String) reconstruction: entries land in one nested
// KindMap field, and an empty map produces no field at all rather than an empty object nothing was
// stored in.
func TestNewDecoder_MapColumns(t *testing.T) {
	type args struct {
		attrs      map[string]string
		name       string
		wantSize   int
		wantMapped bool
	}

	tests := []args{
		{
			name:       "populated map",
			attrs:      map[string]string{"service": "checkout", "region": "eu"},
			wantMapped: true,
			wantSize:   2,
		},
		{name: "empty map omits the field", attrs: map[string]string{}, wantMapped: false},
		{name: "nil map omits the field", attrs: nil, wantMapped: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "maps.parquet")
			writeParquetRows(t, path, []testRowWithMap{{Name: "row", Attributes: tt.attrs}})

			dec, err := parquetio.NewDecoder(newConfig(8, 1), openSource(t, path))
			if err != nil {
				t.Fatalf("NewDecoder: %v", err)
			}
			defer func() {
				if cerr := dec.Close(); cerr != nil {
					t.Errorf("Close: %v", cerr)
				}
			}()

			rows := mustDrain(t, dec, 8)
			if len(rows) != 1 {
				t.Fatalf("decoded %d rows, want 1", len(rows))
			}

			v, ok := rows[0]["attributes"]
			if ok != tt.wantMapped {
				t.Fatalf("attributes present = %v, want %v (record %v)", ok, tt.wantMapped, rows[0])
			}
			if tt.wantMapped {
				assertMapValue(t, v, tt.attrs, tt.wantSize)
			}
		})
	}
}

// assertMapValue checks v is a KindMap holding exactly the entries want describes.
func assertMapValue(tb testing.TB, v record.Value, want map[string]string, wantSize int) {
	tb.Helper()

	if v.Kind != record.KindMap {
		tb.Fatalf("Kind = %v, want %v", v.Kind, record.KindMap)
	}
	if len(v.Map) != wantSize {
		tb.Fatalf("map has %d entries, want %d", len(v.Map), wantSize)
	}
	for _, entry := range v.Map {
		wantVal, found := want[entry.Name]
		if !found {
			tb.Errorf("unexpected map key %q", entry.Name)
			continue
		}
		if string(entry.Value.Str) != wantVal {
			tb.Errorf("map[%q] = %q, want %q", entry.Name, entry.Value.Str, wantVal)
		}
	}
}

// TestNewDecoder_NullScalar checks an absent optional value decodes as a null Value rather than as
// a zero-valued one, which would be indistinguishable from a real zero downstream.
func TestNewDecoder_NullScalar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optional.parquet")
	present := int64(7)
	writeParquetRows(t, path, []optionalRow{{Name: "a", Extra: nil}, {Name: "b", Extra: &present}})

	dec, err := parquetio.NewDecoder(newConfig(8, 1), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer func() {
		if cerr := dec.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	}()

	rows := mustDrain(t, dec, 8)
	if len(rows) != 2 {
		t.Fatalf("decoded %d rows, want 2", len(rows))
	}
	if v := rows[0]["extra"]; !v.IsNull() {
		t.Errorf("row 0 extra = %+v, want null", v)
	}
	if v := rows[1]["extra"]; v.IsNull() || v.I64 != present {
		t.Errorf("row 1 extra = %+v, want %d", v, present)
	}
}

// TestNewDecoder_BatchesAndRowGroups checks the decoder suspends and resumes correctly across both
// boundaries it has to cross — a batch smaller than a row group, and a row group smaller than a
// batch — yielding every row exactly once either way.
func TestNewDecoder_BatchesAndRowGroups(t *testing.T) {
	type args struct {
		name      string
		rows      int
		rowGroup  int
		batchSize int
	}

	tests := []args{
		{name: "batch smaller than row group", rows: 50, rowGroup: 25, batchSize: 7},
		{name: "batch larger than row group", rows: 50, rowGroup: 5, batchSize: 32},
		{name: "batch equal to row group", rows: 40, rowGroup: 10, batchSize: 10},
		{name: "single row group", rows: 13, rowGroup: 13, batchSize: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeParquetWithRowGroups(t, tt.rows, tt.rowGroup)

			dec, err := parquetio.NewDecoder(newConfig(tt.batchSize, 1), openSource(t, path))
			if err != nil {
				t.Fatalf("NewDecoder: %v", err)
			}
			defer func() {
				if cerr := dec.Close(); cerr != nil {
					t.Errorf("Close: %v", cerr)
				}
			}()

			assertRowIDs(t, mustDrain(t, dec, tt.batchSize), tt.rows)
		})
	}
}

// assertRowIDs checks decoded holds exactly the ids 0..want-1, each once.
func assertRowIDs(tb testing.TB, decoded []materialised, want int) {
	tb.Helper()

	if len(decoded) != want {
		tb.Fatalf("decoded %d rows, want %d", len(decoded), want)
	}

	seen := make(map[int64]bool, want)
	for _, row := range decoded {
		id := row["id"].I64
		if seen[id] {
			tb.Fatalf("row id %d decoded twice", id)
		}
		seen[id] = true
	}
	for i := range int64(want) {
		if !seen[i] {
			tb.Errorf("row id %d never decoded", i)
		}
	}
}

// TestNewDecoder_SplitDividesRowGroups checks Split's siblings share one open file and one
// row-group queue: run concurrently, they cover every row exactly once between them.
func TestNewDecoder_SplitDividesRowGroups(t *testing.T) {
	const (
		rows     = 200
		rowGroup = 10
		workers  = 4
	)
	path := writeParquetWithRowGroups(t, rows, rowGroup)

	dec, err := parquetio.NewDecoder(newConfig(16, workers), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer func() {
		if cerr := dec.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	}()

	splittable, ok := dec.(formatio.SplittableRecordDecoder)
	if !ok {
		t.Fatal("parquetio.NewDecoder's decoder does not implement formatio.SplittableRecordDecoder")
	}
	decoders := splittable.Split(workers)
	if len(decoders) != workers {
		t.Fatalf("Split(%d) returned %d decoders, want %d", workers, len(decoders), workers)
	}

	var (
		mu   sync.Mutex
		seen = make(map[int64]int, rows)
		errs []error
		wg   sync.WaitGroup
	)
	for _, d := range decoders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decoded, derr := drainDecoder(d, 16)
			mu.Lock()
			defer mu.Unlock()
			if derr != nil {
				errs = append(errs, derr)
				return
			}
			for _, row := range decoded {
				seen[row["id"].I64]++
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("decode errors: %v", errors.Join(errs...))
	}
	if len(seen) != rows {
		t.Fatalf("saw %d distinct rows, want %d", len(seen), rows)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("row id %d decoded %d times, want 1", id, n)
		}
	}
}

// TestNewDecoder_SplitCapsAtRowGroupCount checks Split doesn't hand out decoders that could only
// find the queue empty, while still always returning at least the receiver.
func TestNewDecoder_SplitCapsAtRowGroupCount(t *testing.T) {
	path := writeParquetWithRowGroups(t, 6, 3) // two row groups

	dec, err := parquetio.NewDecoder(newConfig(8, 8), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer func() {
		if cerr := dec.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	}()

	splittable, ok := dec.(formatio.SplittableRecordDecoder)
	if !ok {
		t.Fatal("parquetio.NewDecoder's decoder does not implement formatio.SplittableRecordDecoder")
	}
	if got := len(splittable.Split(8)); got != 2 {
		t.Errorf("Split(8) returned %d decoders, want 2 (one per row group)", got)
	}
}

// TestNewDecoder_ContextCancellation checks a cancelled context stops the decoder instead of
// draining the file.
func TestNewDecoder_ContextCancellation(t *testing.T) {
	path := writeParquetWithRowGroups(t, 100, 10)

	dec, err := parquetio.NewDecoder(newConfig(8, 1), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer func() {
		if cerr := dec.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch := make([]record.Record, 8)
	if _, err = dec.DecodeNext(ctx, batch); !errors.Is(err, context.Canceled) {
		t.Fatalf("DecodeNext error = %v, want context.Canceled", err)
	}
}

// TestNewDecoder_EmptyBatch checks the documented "up to len(batch)" contract holds for the
// degenerate case, without consuming input.
func TestNewDecoder_EmptyBatch(t *testing.T) {
	path := writeParquetWithRowGroups(t, 10, 5)

	dec, err := parquetio.NewDecoder(newConfig(8, 1), openSource(t, path))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	defer func() {
		if cerr := dec.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	}()

	n, err := dec.DecodeNext(context.Background(), nil)
	if n != 0 || err != nil {
		t.Fatalf("DecodeNext(nil) = (%d, %v), want (0, nil)", n, err)
	}
	if got := len(mustDrain(t, dec, 8)); got != 10 {
		t.Errorf("decoded %d rows after an empty batch, want 10", got)
	}
}
