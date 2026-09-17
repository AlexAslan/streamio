package csvio_test

import (
	"context"
	"errors"
	"io"
	"strconv"
	"streamio/internal/csvio"
	"streamio/internal/formatio"
	"streamio/internal/options"
	"streamio/internal/record"
	"strings"
	"sync"
	"testing"
)

// openRecordDecoder opens a decoder over path and registers its Close.
//
//nolint:ireturn // formatio.RecordDecoder is exactly what the constructor under test returns.
func openRecordDecoder(tb testing.TB, path string, cfg options.Config) formatio.RecordDecoder {
	tb.Helper()
	dec, err := csvio.NewDecoder(cfg, path)
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

// drainRecordDecoder reads dec to exhaustion and returns each row as a name->string map.
func drainRecordDecoder(dec formatio.RecordDecoder, batchSize int) ([]map[string]string, error) {
	var rows []map[string]string
	batch := make([]record.Record, batchSize)
	for {
		n, err := dec.DecodeNext(context.Background(), batch)
		for i := range batch[:n] {
			row := make(map[string]string, len(batch[i]))
			for _, f := range batch[i] {
				row[f.Name] = string(f.Value.Str)
			}
			rows = append(rows, row)
		}
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return rows, err
		}
	}
}

func mustDrainRecords(tb testing.TB, dec formatio.RecordDecoder, batchSize int) []map[string]string {
	tb.Helper()
	rows, err := drainRecordDecoder(dec, batchSize)
	if err != nil {
		tb.Fatalf("DecodeNext: %v", err)
	}
	return rows
}

// TestNewDecoder_HeaderNamesColumns checks header row values become field names and every field
// decodes as a plain string (record.KindBytes), never inferred as a number or bool.
func TestNewDecoder_HeaderNamesColumns(t *testing.T) {
	path := writeCSVFile(t, "id,name,active\n1,Alice,true\n2,Bob,false\n")
	dec := openRecordDecoder(t, path, newConfig(1, 0, ',', true))

	rows := mustDrainRecords(t, dec, 8)
	want := []map[string]string{
		{"id": "1", "name": "Alice", "active": "true"},
		{"id": "2", "name": "Bob", "active": "false"},
	}
	if len(rows) != len(want) {
		t.Fatalf("decoded %d rows, want %d", len(rows), len(want))
	}
	for i := range want {
		for k, v := range want[i] {
			if rows[i][k] != v {
				t.Errorf("row %d field %q = %q, want %q", i, k, rows[i][k], v)
			}
		}
	}
}

// TestNewDecoder_ScalarsDecodeAsBytes checks a value that looks numeric or boolean still decodes
// as record.KindBytes: CSV has no type system of its own, so nothing is inferred.
func TestNewDecoder_ScalarsDecodeAsBytes(t *testing.T) {
	path := writeCSVFile(t, "n,b\n42,true\n")
	dec := openRecordDecoder(t, path, newConfig(1, 0, ',', true))

	batch := make([]record.Record, 1)
	n, err := dec.DecodeNext(context.Background(), batch)
	if n != 1 {
		t.Fatalf("DecodeNext returned %d rows, want 1 (err=%v)", n, err)
	}
	for _, f := range batch[0] {
		if f.Value.Kind != record.KindBytes {
			t.Errorf("field %q decoded as Kind %v, want KindBytes", f.Name, f.Value.Kind)
		}
	}
}

// TestNewDecoder_NoHeaderSynthesizesColumnNames checks a headerless file gets "col0".."colN" names
// derived from the first row's own field count.
func TestNewDecoder_NoHeaderSynthesizesColumnNames(t *testing.T) {
	path := writeCSVFile(t, "1,Alice,true\n2,Bob,false\n")
	dec := openRecordDecoder(t, path, newConfig(1, 0, ',', false))

	rows := mustDrainRecords(t, dec, 8)
	if len(rows) != 2 {
		t.Fatalf("decoded %d rows, want 2", len(rows))
	}
	want := []string{"col0", "col1", "col2"}
	for _, name := range want {
		if _, ok := rows[0][name]; !ok {
			t.Errorf("row 0 has no field %q; got %v", name, rows[0])
		}
	}
	if rows[0]["col1"] != "Alice" || rows[1]["col1"] != "Bob" {
		t.Errorf("synthesized column values wrong: %v", rows)
	}
}

// TestNewDecoder_TSVDelimiter checks a tab delimiter is honored.
func TestNewDecoder_TSVDelimiter(t *testing.T) {
	path := writeCSVFile(t, "id\tname\n1\tAlice\n")
	dec := openRecordDecoder(t, path, newConfig(1, 0, '\t', true))

	rows := mustDrainRecords(t, dec, 8)
	if len(rows) != 1 || rows[0]["name"] != "Alice" {
		t.Fatalf("decoded %v, want one row with name=Alice", rows)
	}
}

// TestNewDecoder_EmptyFile checks a file with no rows at all reports EOF immediately.
func TestNewDecoder_EmptyFile(t *testing.T) {
	path := writeCSVFile(t, "")
	dec := openRecordDecoder(t, path, newConfig(1, 0, ',', false))

	batch := make([]record.Record, 8)
	n, err := dec.DecodeNext(context.Background(), batch)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("DecodeNext = (%d, %v), want (0, io.EOF)", n, err)
	}
}

// TestNewDecoder_HeaderOnlyFile checks a header with no data rows decodes zero records rather than
// erroring.
func TestNewDecoder_HeaderOnlyFile(t *testing.T) {
	path := writeCSVFile(t, "id,name\n")
	dec := openRecordDecoder(t, path, newConfig(1, 0, ',', true))

	rows := mustDrainRecords(t, dec, 8)
	if len(rows) != 0 {
		t.Fatalf("decoded %d rows, want 0", len(rows))
	}
}

// TestNewDecoder_FieldCountMismatch checks a row with a different field count than the header
// reports an error rather than silently truncating or padding.
func TestNewDecoder_FieldCountMismatch(t *testing.T) {
	path := writeCSVFile(t, "a,b,c\n1,2\n")
	dec := openRecordDecoder(t, path, newConfig(1, 0, ',', true))

	_, err := drainRecordDecoder(dec, 8)
	if err == nil {
		t.Fatal("decoding a short row returned no error")
	}
}

// TestNewDecoder_QuotedFieldWithDelimiter checks a quoted field containing the delimiter decodes
// as one field, per RFC 4180 quoting rules that encoding/csv already implements.
func TestNewDecoder_QuotedFieldWithDelimiter(t *testing.T) {
	path := writeCSVFile(t, "name,note\n\"Doe, Jane\",hello\n")
	dec := openRecordDecoder(t, path, newConfig(1, 0, ',', true))

	rows := mustDrainRecords(t, dec, 8)
	if len(rows) != 1 || rows[0]["name"] != "Doe, Jane" {
		t.Fatalf("decoded %v, want one row with name=\"Doe, Jane\"", rows)
	}
}

// TestNewDecoder_BatchesAndChunks checks the decoder suspends and resumes across both boundaries,
// yielding every row exactly once, mirroring jsonio's identical NDJSON test.
func TestNewDecoder_BatchesAndChunks(t *testing.T) {
	type args struct {
		name      string
		rows      int
		chunkSize int
		batchSize int
	}

	tests := []args{
		{name: "single chunk", rows: 50, chunkSize: 1 << 20, batchSize: 8},
		{name: "many small chunks", rows: 200, chunkSize: 16, batchSize: 8},
		{name: "batch larger than chunk", rows: 200, chunkSize: 16, batchSize: 256},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeCSVFile(t, genCSVRows(tt.rows))
			dec := openRecordDecoder(t, path, newConfig(1, tt.chunkSize, ',', true))

			rows := mustDrainRecords(t, dec, tt.batchSize)
			assertRowIndexes(t, rows, tt.rows)
		})
	}
}

// genCSVRows returns a header plus n rows of the form "i" holding the row's own index.
func genCSVRows(n int) string {
	var b strings.Builder
	b.WriteString("i\n")
	for i := range n {
		b.WriteString(strconv.Itoa(i))
		b.WriteByte('\n')
	}
	return b.String()
}

// assertRowIndexes checks decoded holds exactly the genCSVRows indexes 0..want-1, each once.
func assertRowIndexes(tb testing.TB, decoded []map[string]string, want int) {
	tb.Helper()

	if len(decoded) != want {
		tb.Fatalf("decoded %d rows, want %d", len(decoded), want)
	}

	seen := make(map[string]bool, want)
	for _, row := range decoded {
		i, ok := row["i"]
		if !ok {
			tb.Fatalf("row missing field %q: %v", "i", row)
		}
		if seen[i] {
			tb.Fatalf("row i=%s decoded twice", i)
		}
		seen[i] = true
	}
	for i := range want {
		if !seen[strconv.Itoa(i)] {
			tb.Errorf("row i=%d never decoded", i)
		}
	}
}

// TestNewDecoder_SplitDividesChunks checks Split's siblings share one open file and one chunk
// queue: run concurrently, they cover every row exactly once between them.
func TestNewDecoder_SplitDividesChunks(t *testing.T) {
	const (
		rows    = 500
		workers = 4
	)
	path := writeCSVFile(t, genCSVRows(rows))
	dec := openRecordDecoder(t, path, newConfig(workers, 32, ',', true))

	splittable, ok := dec.(formatio.SplittableRecordDecoder)
	if !ok {
		t.Fatal("csvio.NewDecoder's decoder does not implement formatio.SplittableRecordDecoder")
	}
	decoders := splittable.Split(workers)

	var (
		mu   sync.Mutex
		seen = make(map[string]int, rows)
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
			for _, row := range decoded {
				seen[row["i"]]++
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
	for i, n := range seen {
		if n != 1 {
			t.Errorf("row i=%s decoded %d times, want 1", i, n)
		}
	}
}

// TestNewDecoder_ContextCancellation checks a cancelled context stops the decoder instead of
// draining the file.
func TestNewDecoder_ContextCancellation(t *testing.T) {
	path := writeCSVFile(t, genCSVRows(1000))
	dec := openRecordDecoder(t, path, newConfig(1, 64, ',', true))

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
	_, err := csvio.NewDecoder(newConfig(1, 0, ',', false), t.TempDir()+"/absent.csv")
	if err == nil {
		t.Fatal("NewDecoder on a missing file returned no error")
	}
}

// TestNewDecoder_QuotedNewlineSingleWorker checks a quoted field containing a literal newline
// decodes as one field on a single-worker run. This is a correctness fix, not just a performance
// one: the predecessor of this decoder split chunks into physical lines before parsing each one as
// a CSV row, which misreads a literal newline inside a quoted field as a row terminator regardless
// of worker count — a bug this rewrite fixes by driving encoding/csv directly off the chunk's own
// byte stream instead. With exactly one worker there is exactly one chunk, starting at byte 0 (or
// just past the header), so no boundary resolution runs at all and encoding/csv's own
// quoted-newline handling sees the whole file uninterrupted.
//
// This is deliberately single-worker: chunk-boundary resolution for a multi-worker run still
// cannot tell a byte offset landing inside an arbitrarily large quoted field from one that isn't
// (see sharedState.openChunkReader's doc comment for why that is not fixable by a local scan) —
// that remains a known, documented limitation of running CSV/TSV decode with more than one worker
// against a file containing quoted newlines. See TestNewDecoder_QuotedNewlineAtChunkBoundaryErrors
// below for what that limitation actually does when triggered: it errors, it does not corrupt data.
func TestNewDecoder_QuotedNewlineSingleWorker(t *testing.T) {
	content := "id,note\n" +
		"1,a\n" +
		"2,\"line one\nline two\"\n" +
		"3,c\n"
	path := writeCSVFile(t, content)

	dec := openRecordDecoder(t, path, newConfig(1, 0, ',', true))
	rows := mustDrainRecords(t, dec, 8)

	if len(rows) != 3 {
		t.Fatalf("decoded %d rows, want 3: %v", len(rows), rows)
	}
	if want := "line one\nline two"; rows[1]["note"] != want {
		t.Errorf("row 1 note = %q, want %q", rows[1]["note"], want)
	}
	if rows[0]["note"] != "a" || rows[2]["note"] != "c" {
		t.Errorf("surrounding rows corrupted: %v", rows)
	}
}

// TestNewDecoder_QuotedNewlineAtChunkBoundaryErrors pins the actual, verified behavior of the
// known multi-worker/quoted-newline limitation referenced above: when a chunk boundary lands
// inside a quoted field spanning a literal newline, encoding/csv reports a parse error for that
// chunk rather than silently returning misaligned or corrupted rows. chunkSize=15 deterministically
// puts chunk 1's start (byte 23, computed as len(header)+chunkSize) inside row 1's quoted field
// (bytes [16,38)) regardless of goroutine scheduling, since chunk boundaries are a pure function of
// chunkSize and chunk index, not of which worker claims which chunk.
//
// This does not fix the limitation — it verifies the failure mode is the safe one (an explicit
// error, matching this repo's errors-over-coercion convention) rather than the dangerous one
// (silent data corruption), which was previously documented as a known gap but never actually
// confirmed either way.
func TestNewDecoder_QuotedNewlineAtChunkBoundaryErrors(t *testing.T) {
	const chunkSize = 15
	const workers = 2

	content := "id,note\n" +
		"0,short\n" +
		"1,\"line-one\nline-two\"\n" +
		"2,short-again\n"
	path := writeCSVFile(t, content)

	dec := openRecordDecoder(t, path, newConfig(workers, chunkSize, ',', true))
	splittable, ok := dec.(formatio.SplittableRecordDecoder)
	if !ok {
		t.Fatal("decoder does not implement SplittableRecordDecoder")
	}
	decoders := splittable.Split(workers)

	var (
		mu   sync.Mutex
		rows []map[string]string
		errs []error
		wg   sync.WaitGroup
	)
	for _, d := range decoders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decoded, derr := drainRecordDecoder(d, 8)
			mu.Lock()
			defer mu.Unlock()
			if derr != nil {
				errs = append(errs, derr)
			}
			rows = append(rows, decoded...)
		}()
	}
	wg.Wait()

	if len(errs) == 0 {
		t.Fatalf("want a decode error when a chunk boundary lands inside a quoted newline, got none; "+
			"rows decoded: %v (if this now succeeds, the limitation may have been fixed — update this "+
			"test and the doc comments it verifies rather than deleting it)", rows)
	}

	// The safety property under test: whatever rows DID decode must be genuine, unmodified rows
	// from the file, never a corrupted hybrid — a decode error for the affected chunk is fine, but
	// a row with a value that isn't one of the three real rows' values would mean silent corruption
	// slipped past the error.
	want := map[string]bool{"short": true, "short-again": true, "line-one\nline-two": true}
	for _, r := range rows {
		if !want[r["note"]] {
			t.Errorf("silent corruption: decoded row with unexpected note %q: %v", r["note"], r)
		}
	}
}
