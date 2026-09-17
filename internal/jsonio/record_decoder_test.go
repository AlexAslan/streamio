package jsonio_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"streamio/internal/formatio"
	"streamio/internal/jsonio"
	"streamio/internal/options"
	"streamio/internal/record"
	"strings"
	"sync"
	"testing"
)

// decodedField is one field of a decoded line, flattened to something a table can state literally.
type decodedField struct {
	name     string
	str      string
	i64      int64
	f64      float64
	kind     record.Kind
	semantic record.Semantic
	boolean  bool
}

// flatten converts a record to decodedFields, copying every payload out of the decoder's buffers.
func flatten(rec record.Record) []decodedField {
	out := make([]decodedField, 0, len(rec))
	for _, f := range rec {
		out = append(out, decodedField{
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

// drainRecordDecoder reads dec to exhaustion and returns every line it yielded. It reports errors
// rather than failing the test itself, so concurrent callers don't call Fatalf off the test
// goroutine.
func drainRecordDecoder(dec formatio.RecordDecoder, batchSize int) ([][]decodedField, error) {
	var lines [][]decodedField
	batch := make([]record.Record, batchSize)
	for {
		n, err := dec.DecodeNext(context.Background(), batch)
		for i := range batch[:n] {
			lines = append(lines, flatten(batch[i]))
		}
		if errors.Is(err, io.EOF) {
			return lines, nil
		}
		if err != nil {
			return lines, err
		}
	}
}

// mustDrainRecords is drainRecordDecoder for the sequential tests, failing on any decode error.
func mustDrainRecords(tb testing.TB, dec formatio.RecordDecoder, batchSize int) [][]decodedField {
	tb.Helper()
	lines, err := drainRecordDecoder(dec, batchSize)
	if err != nil {
		tb.Fatalf("DecodeNext: %v", err)
	}
	return lines
}

// openRecordDecoder opens a decoder over path and registers its Close.
//
//nolint:ireturn // formatio.RecordDecoder is exactly what the constructor under test returns.
func openRecordDecoder(tb testing.TB, path string, cfg options.Config) formatio.RecordDecoder {
	tb.Helper()
	dec, err := jsonio.NewDecoder(cfg, path)
	if err != nil {
		tb.Fatalf("NewDecoder: %v", err)
	}
	tb.Cleanup(func() {
		if cerr := dec.Close(); cerr != nil {
			tb.Errorf("Close: %v", cerr)
		}
	})
	return dec
}

// TestNewDecoder_ScalarKinds checks each JSON scalar lands in the Value field its Kind selects,
// including the int-before-float rule for numbers.
func TestNewDecoder_ScalarKinds(t *testing.T) {
	type args struct {
		check func(decodedField) bool
		name  string
		line  string
		field string
		kind  record.Kind
	}

	tests := []args{
		{
			name: "string", line: `{"s":"hello"}`, field: "s", kind: record.KindBytes,
			check: func(f decodedField) bool { return f.str == "hello" },
		},
		{
			name: "integer", line: `{"n":42}`, field: "n", kind: record.KindInt64,
			check: func(f decodedField) bool { return f.i64 == 42 },
		},
		{
			name: "negative integer", line: `{"n":-9007199254740993}`, field: "n", kind: record.KindInt64,
			check: func(f decodedField) bool { return f.i64 == -9007199254740993 },
		},
		{
			name: "fractional number falls back to float", line: `{"n":1.5}`, field: "n", kind: record.KindFloat64,
			check: func(f decodedField) bool { return f.f64 == 1.5 },
		},
		{
			name: "exponent falls back to float", line: `{"n":1e3}`, field: "n", kind: record.KindFloat64,
			check: func(f decodedField) bool { return f.f64 == 1000 },
		},
		{
			name: "number past int64 falls back to float", line: `{"n":1e300}`, field: "n", kind: record.KindFloat64,
			check: func(f decodedField) bool { return f.f64 == 1e300 && !math.IsInf(f.f64, 0) },
		},
		{
			name: "true", line: `{"b":true}`, field: "b", kind: record.KindBool,
			check: func(f decodedField) bool { return f.boolean },
		},
		{
			name: "false", line: `{"b":false}`, field: "b", kind: record.KindBool,
			check: func(f decodedField) bool { return !f.boolean },
		},
		{
			name: "null", line: `{"v":null}`, field: "v", kind: record.KindNull,
			check: func(decodedField) bool { return true },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeNdjsonFile(t, []string{tt.line})
			dec := openRecordDecoder(t, path, newConfig(1, 0, 0))

			lines := mustDrainRecords(t, dec, 8)
			if len(lines) != 1 || len(lines[0]) != 1 {
				t.Fatalf("decoded %v, want exactly one line of one field", lines)
			}
			got := lines[0][0]
			if got.name != tt.field {
				t.Errorf("field name = %q, want %q", got.name, tt.field)
			}
			if got.kind != tt.kind {
				t.Errorf("Kind = %v, want %v", got.kind, tt.kind)
			}
			if got.semantic != record.SemanticNone {
				t.Errorf("Semantic = %v, want none — NDJSON carries no logical types", got.semantic)
			}
			if !tt.check(got) {
				t.Errorf("payload check failed for %+v", got)
			}
		})
	}
}

// TestNewDecoder_PreservesFieldOrder checks a record keeps the line's own field order, which is
// what lets a JSON round trip come back byte-identical.
func TestNewDecoder_PreservesFieldOrder(t *testing.T) {
	path := writeNdjsonFile(t, []string{`{"z":1,"a":"x","m":true}`})
	dec := openRecordDecoder(t, path, newConfig(1, 0, 0))

	lines := mustDrainRecords(t, dec, 8)
	if len(lines) != 1 {
		t.Fatalf("decoded %d lines, want 1", len(lines))
	}

	want := []string{"z", "a", "m"}
	got := make([]string, 0, len(lines[0]))
	for _, f := range lines[0] {
		got = append(got, f.name)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("field order = %v, want %v", got, want)
	}
}

// TestNewDecoder_RejectsUnsupportedLines pins the documented scope: flat objects of scalars, with
// a specific error naming the offending field for anything else, rather than a silent
// reinterpretation.
func TestNewDecoder_RejectsUnsupportedLines(t *testing.T) {
	type args struct {
		name     string
		line     string
		wantText string
	}

	tests := []args{
		{name: "nested object", line: `{"a":{"b":1}}`, wantText: `nested value not supported`},
		{name: "array", line: `{"a":[1,2]}`, wantText: `nested value not supported`},
		{name: "top-level array", line: `[1,2]`, wantText: `not a JSON object`},
		{name: "top-level scalar", line: `"just a string"`, wantText: `not a JSON object`},
		{name: "malformed", line: `{"a":}`, wantText: `jsontext`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeNdjsonFile(t, []string{tt.line})
			dec := openRecordDecoder(t, path, newConfig(1, 0, 0))

			_, err := drainRecordDecoder(dec, 8)
			if err == nil {
				t.Fatalf("decoding %s returned no error", tt.line)
			}
			if !strings.Contains(err.Error(), tt.wantText) {
				t.Errorf("error %q does not contain %q", err, tt.wantText)
			}
		})
	}
}

// TestNewDecoder_NestedErrorNamesTheField checks the nested-value error says which field it was,
// which is the whole point of rejecting rather than skipping.
func TestNewDecoder_NestedErrorNamesTheField(t *testing.T) {
	path := writeNdjsonFile(t, []string{`{"id":1,"attributes":{"a":"b"}}`})
	dec := openRecordDecoder(t, path, newConfig(1, 0, 0))

	_, err := drainRecordDecoder(dec, 8)
	if err == nil {
		t.Fatal("decoding a nested object returned no error")
	}
	if !strings.Contains(err.Error(), `"attributes"`) {
		t.Errorf("error %q does not name the offending field", err)
	}
}

// TestNewDecoder_BatchesAndChunks checks the decoder suspends and resumes across both boundaries
// it has to cross — a batch smaller than a chunk, and a chunk smaller than a batch — yielding
// every line exactly once either way, including lines straddling a chunk boundary.
func TestNewDecoder_BatchesAndChunks(t *testing.T) {
	type args struct {
		name      string
		lines     int
		chunkSize int
		batchSize int
	}

	tests := []args{
		{name: "single chunk", lines: 50, chunkSize: 1 << 20, batchSize: 8},
		{name: "many small chunks", lines: 200, chunkSize: 16, batchSize: 8},
		{name: "batch larger than chunk", lines: 200, chunkSize: 16, batchSize: 256},
		{name: "chunk of one byte", lines: 40, chunkSize: 1, batchSize: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeNdjsonFile(t, genLines(tt.lines))
			dec := openRecordDecoder(t, path, newConfig(1, tt.chunkSize, tt.chunkSize))

			assertLineIndexes(t, mustDrainRecords(t, dec, tt.batchSize), tt.lines)
		})
	}
}

// assertLineIndexes checks decoded holds exactly the genLines indexes 0..want-1, each once.
func assertLineIndexes(tb testing.TB, decoded [][]decodedField, want int) {
	tb.Helper()

	if len(decoded) != want {
		tb.Fatalf("decoded %d lines, want %d", len(decoded), want)
	}

	seen := make(map[int64]bool, want)
	for _, fields := range decoded {
		if len(fields) != 1 || fields[0].name != "i" {
			tb.Fatalf("unexpected decoded line %+v", fields)
		}
		if seen[fields[0].i64] {
			tb.Fatalf("line i=%d decoded twice", fields[0].i64)
		}
		seen[fields[0].i64] = true
	}
	for i := range int64(want) {
		if !seen[i] {
			tb.Errorf("line i=%d never decoded", i)
		}
	}
}

// TestNewDecoder_SplitCapsAtChunkCount checks Split doesn't hand out more decoders than the file
// has chunks: a chunk is the smallest claimable unit, so a decoder beyond that count would only
// ever find the shared queue empty. A tiny file (one chunk) requesting many workers should get
// back exactly one decoder, mirroring parquetio.recordDecoder.Split's row-group cap.
func TestNewDecoder_SplitCapsAtChunkCount(t *testing.T) {
	const workers = 8
	path := writeNdjsonFile(t, genLines(2))
	dec := openRecordDecoder(t, path, newConfig(workers, 1<<20, 4096))

	splittable, ok := dec.(formatio.SplittableRecordDecoder)
	if !ok {
		t.Fatal("jsonio.NewDecoder's decoder does not implement formatio.SplittableRecordDecoder")
	}
	decoders := splittable.Split(workers)
	if len(decoders) != 1 {
		t.Fatalf("Split(%d) on a one-chunk file returned %d decoders, want 1", workers, len(decoders))
	}
}

// TestNewDecoder_SplitDividesChunks checks Split's siblings share one open file and one chunk
// queue: run concurrently, they cover every line exactly once between them.
func TestNewDecoder_SplitDividesChunks(t *testing.T) {
	const (
		lines   = 500
		workers = 4
	)
	path := writeNdjsonFile(t, genLines(lines))
	dec := openRecordDecoder(t, path, newConfig(workers, 32, 32))

	splittable, ok := dec.(formatio.SplittableRecordDecoder)
	if !ok {
		t.Fatal("jsonio.NewDecoder's decoder does not implement formatio.SplittableRecordDecoder")
	}
	decoders := splittable.Split(workers)
	if len(decoders) != workers {
		t.Fatalf("Split(%d) returned %d decoders, want %d", workers, len(decoders), workers)
	}

	var (
		mu   sync.Mutex
		seen = make(map[int64]int, lines)
		errs []error
		wg   sync.WaitGroup
	)
	for _, d := range decoders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decoded, derr := drainRecordDecoder(d, 16)
			mu.Lock()
			defer mu.Unlock()
			if derr != nil {
				errs = append(errs, derr)
				return
			}
			for _, fields := range decoded {
				seen[fields[0].i64]++
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("decode errors: %v", errors.Join(errs...))
	}
	if len(seen) != lines {
		t.Fatalf("saw %d distinct lines, want %d", len(seen), lines)
	}
	for i, n := range seen {
		if n != 1 {
			t.Errorf("line i=%d decoded %d times, want 1", i, n)
		}
	}
}

// TestNewDecoder_EmptyFile checks an empty input reports EOF immediately rather than blocking or
// yielding a phantom record.
func TestNewDecoder_EmptyFile(t *testing.T) {
	path := writeTemp(t, "")
	dec := openRecordDecoder(t, path, newConfig(1, 0, 0))

	batch := make([]record.Record, 8)
	n, err := dec.DecodeNext(context.Background(), batch)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("DecodeNext = (%d, %v), want (0, io.EOF)", n, err)
	}
}

// TestNewDecoder_SkipsBlankLines checks blank lines are skipped rather than failing to parse,
// matching the raw path's own line filtering.
func TestNewDecoder_SkipsBlankLines(t *testing.T) {
	path := writeTemp(t, "{\"i\":0}\n\n\n{\"i\":1}\n\n")
	dec := openRecordDecoder(t, path, newConfig(1, 0, 0))

	if got := len(mustDrainRecords(t, dec, 8)); got != 2 {
		t.Errorf("decoded %d lines, want 2", got)
	}
}

// TestNewDecoder_ContextCancellation checks a cancelled context stops the decoder instead of
// draining the file.
func TestNewDecoder_ContextCancellation(t *testing.T) {
	path := writeNdjsonFile(t, genLines(1000))
	dec := openRecordDecoder(t, path, newConfig(1, 64, 64))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch := make([]record.Record, 8)
	if _, err := dec.DecodeNext(ctx, batch); !errors.Is(err, context.Canceled) {
		t.Fatalf("DecodeNext error = %v, want context.Canceled", err)
	}
}

// TestNewDecoder_MissingFile checks the constructor surfaces an open failure rather than returning
// a decoder that fails later.
func TestNewDecoder_MissingFile(t *testing.T) {
	_, err := jsonio.NewDecoder(newConfig(1, 0, 0), fmt.Sprintf("%s/absent.ndjson", t.TempDir()))
	if err == nil {
		t.Fatal("NewDecoder on a missing file returned no error")
	}
}
