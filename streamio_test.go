package streamio_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	parquetgo "github.com/AlexAslan/parquet-go"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/AlexAslan/streamio"
)

// testParquetRow is a minimal flat struct used to create test parquet fixtures.
type testParquetRow struct {
	Value string `parquet:"value"`
}

// createArrowFile writes a single-row Arrow IPC file at path, one string column.
func createArrowFile(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create arrow file: %v", err)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	alloc := memory.NewGoAllocator()
	w, err := ipc.NewFileWriter(f, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}

	rb := array.NewRecordBuilder(alloc, schema)
	rb.Field(0).(*array.StringBuilder).Append("hello")
	rec := rb.NewRecordBatch()
	defer rec.Release()

	if err = w.Write(rec); err != nil {
		t.Fatalf("write arrow record: %v", err)
	}
	if err = w.Close(); err != nil {
		t.Fatalf("close arrow writer: %v", err)
	}
	if err = f.Close(); err != nil {
		t.Fatalf("close arrow file: %v", err)
	}
}

// createParquetFile writes a single-row parquet file at path.
func createParquetFile(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create parquet file: %v", err)
	}
	w := parquetgo.NewGenericWriter[testParquetRow](f)
	if _, err = w.Write([]testParquetRow{{Value: "hello"}}); err != nil {
		t.Fatalf("write parquet rows: %v", err)
	}
	if err = w.Close(); err != nil {
		t.Fatalf("close parquet writer: %v", err)
	}
	if err = f.Close(); err != nil {
		t.Fatalf("close parquet file: %v", err)
	}
}

// createNDJSONFile writes a minimal NDJSON file at path.
func createNDJSONFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("{\"a\":1}\n"), 0o600); err != nil {
		t.Fatalf("write ndjson file: %v", err)
	}
}

// createNDJSONFileWithLines writes n distinct NDJSON lines at path.
func createNDJSONFileWithLines(t *testing.T, path string, n int) {
	t.Helper()
	buf := make([]byte, 0, n*16)
	for i := range n {
		buf = append(buf, []byte(fmt.Sprintf(`{"i":%d}`+"\n", i))...)
	}
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("write ndjson file: %v", err)
	}
}

// collectingSink returns a sink safe for concurrent workers that appends every document to a
// slice, plus a function to retrieve what was collected afterward.
func collectingSink() (streamio.DocumentHandler, func() [][]byte) {
	var (
		mu   sync.Mutex
		docs [][]byte
	)
	sink := func(_ context.Context, doc []byte) error {
		mu.Lock()
		docs = append(docs, doc)
		mu.Unlock()
		return nil
	}
	return sink, func() [][]byte { return docs }
}

// TestProcess_DispatchesParquet verifies that Process routes .parquet and .PARQUET extensions
// (case-insensitive) to the Parquet reader.
func TestProcess_DispatchesParquet(t *testing.T) {
	cases := []struct {
		name string
		ext  string
	}{
		{name: "lowercase extension", ext: ".parquet"},
		{name: "uppercase extension", ext: ".PARQUET"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "data"+tc.ext)
			createParquetFile(t, path)
			sink, collected := collectingSink()

			if _, err := streamio.ProcessFile(context.Background(), path, sink); err != nil {
				t.Fatalf("Process: %v", err)
			}
			docs := collected()
			if len(docs) != 1 || len(docs[0]) == 0 {
				t.Fatalf("got %d docs, want 1 non-empty document", len(docs))
			}
		})
	}
}

// TestProcess_DispatchesArrow verifies that Process routes .arrow extensions to the Arrow reader,
// through the generic record path — arrowio has no raw source, so this exercises decode+re-encode
// rather than raw passthrough, unlike TestProcess_DispatchesParquet's equivalent.
func TestProcess_DispatchesArrow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.arrow")
	createArrowFile(t, path)
	sink, collected := collectingSink()

	if _, err := streamio.ProcessFile(context.Background(), path, sink); err != nil {
		t.Fatalf("Process: %v", err)
	}
	docs := collected()
	if len(docs) != 1 || len(docs[0]) == 0 {
		t.Fatalf("got %d docs, want 1 non-empty document", len(docs))
	}
}

// TestProcess_DispatchesNDJSON verifies that Process routes .ndjson and unknown extensions to
// the NDJSON reader.
func TestProcess_DispatchesNDJSON(t *testing.T) {
	cases := []struct {
		name string
		ext  string
	}{
		{name: "ndjson extension", ext: ".ndjson"},
		{name: "unknown extension", ext: ".json"},
		{name: "no extension", ext: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "data"+tc.ext)
			createNDJSONFile(t, path)
			sink, collected := collectingSink()

			_, err := streamio.ProcessFile(context.Background(), path, sink, streamio.WithReadBufferSize(4096))
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			docs := collected()
			if len(docs) != 1 || string(docs[0]) != `{"a":1}` {
				t.Fatalf("got %v, want [%q]", docs, `{"a":1}`)
			}
		})
	}
}

// TestProcess_ParallelWorkers verifies that Process honors WithParallelWorkers for a non-Parquet
// file, decoding every line across multiple workers.
func TestProcess_ParallelWorkers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	createNDJSONFileWithLines(t, path, 100)
	sink, collected := collectingSink()

	result, err := streamio.ProcessFile(context.Background(), path, sink,
		streamio.WithParallelWorkers(4),
		streamio.WithChunkSize(64),
	)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(collected()) != 100 || result.Stats.RowsRead != 100 {
		t.Errorf("got %d documents, want 100", result.Stats.RowsRead)
	}
}

// TestProcessFile_MissingFile checks ProcessFile surfaces the os.Open failure for a nonexistent
// path, rather than returning a Result that looks like a successful empty run. This is the one
// place that failure can now surface from: NewRawSource/NewDecoder no longer open the file
// themselves (see options.Source), so the open happens in ProcessFile before any format package is
// even reached.
func TestProcessFile_MissingFile(t *testing.T) {
	_, err := streamio.ProcessFile(
		context.Background(),
		filepath.Join(t.TempDir(), "absent.ndjson"),
		func(context.Context, []byte) error { return nil },
	)
	if err == nil {
		t.Fatal("ProcessFile on a missing file returned no error")
	}
}

// TestProcessFile_MaxRowErrorsZeroFailsImmediately checks the default MaxRowErrors of 0 stops the
// whole run on the first malformed line, exactly as streamio behaved before WithMaxRowErrors
// existed.
//
// Output is forced to CSV, not JSON: NDJSON-to-JSON is a native-format match, which ProcessFile
// takes as raw passthrough (see ProcessReaderAt's doc) — no record decoder, and so no row-error
// handling, is ever in play on that path. WithMaxRowErrors only affects the generic decode/encode
// route, which a genuine format conversion (here, to CSV) actually exercises.
func TestProcessFile_MaxRowErrorsZeroFailsImmediately(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.ndjson")
	if err := os.WriteFile(path, []byte("{\"a\":1}\n{\"a\":}\n{\"a\":3}\n"), 0o600); err != nil {
		t.Fatalf("write ndjson file: %v", err)
	}
	sink, collected := collectingSink()

	_, err := streamio.ProcessFile(context.Background(), path, sink, streamio.WithOutputFormat(streamio.FormatCSV))
	if err == nil {
		t.Fatal("ProcessFile on a malformed line returned no error")
	}
	// The one row read successfully before the malformed one is still dispatched as its own
	// document: DecodeNext's contract returns whatever it filled (n) alongside the error, and the
	// pool encodes+dispatches that partial batch before propagating the error — pre-existing
	// behavior this option doesn't change, only what a positive MaxRowErrors does differently.
	if len(collected()) != 1 {
		t.Errorf("dispatched %d documents before failing, want 1 (the row read before the bad one)",
			len(collected()))
	}
}

// TestProcessFile_MaxRowErrorsAllowsSkipping checks a positive MaxRowErrors drops a malformed line
// under the limit and continues, dispatching every good line, counting the drop in
// Stats.RowsSkipped, and calling onSkip once. See
// TestProcessFile_MaxRowErrorsZeroFailsImmediately's doc for why output is forced to CSV.
func TestProcessFile_MaxRowErrorsAllowsSkipping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.ndjson")
	if err := os.WriteFile(path, []byte("{\"a\":1}\n{\"a\":}\n{\"a\":3}\n"), 0o600); err != nil {
		t.Fatalf("write ndjson file: %v", err)
	}
	sink, collected := collectingSink()

	var (
		mu       sync.Mutex
		skipped  []error
		skipDone bool
	)
	onSkip := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		skipped = append(skipped, err)
		skipDone = true
	}

	result, err := streamio.ProcessFile(context.Background(), path, sink,
		streamio.WithOutputFormat(streamio.FormatCSV),
		streamio.WithMaxRowErrors(1, onSkip))
	if err != nil {
		t.Fatalf("ProcessFile: %v", err)
	}

	docs := collected()
	if len(docs) != 1 {
		t.Fatalf("dispatched %d documents, want 1 (one CSV document holding both good rows)", len(docs))
	}
	if got := string(docs[0]); !strings.Contains(got, "1") || !strings.Contains(got, "3") {
		t.Errorf("dispatched document = %q, want it to contain both good rows' values", got)
	}
	if result.Stats.RowsSkipped != 1 {
		t.Errorf("Stats.RowsSkipped = %d, want 1", result.Stats.RowsSkipped)
	}
	mu.Lock()
	defer mu.Unlock()
	if !skipDone || len(skipped) != 1 {
		t.Errorf("onSkip called %d times, want exactly 1", len(skipped))
	}
}

// TestProcessFile_MaxRowErrorsExceeded checks the run still fails once the number of malformed
// lines exceeds a positive MaxRowErrors, and that the returned error unwraps to a
// *streamio.TooManyRowErrorsError wrapping every collected row error, including the one that
// exceeded the limit — not just a count.
func TestProcessFile_MaxRowErrorsExceeded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.ndjson")
	if err := os.WriteFile(path, []byte("{\"a\":}\n{\"a\":}\n{\"a\":3}\n"), 0o600); err != nil {
		t.Fatalf("write ndjson file: %v", err)
	}
	sink, _ := collectingSink()

	result, err := streamio.ProcessFile(context.Background(), path, sink,
		streamio.WithOutputFormat(streamio.FormatCSV),
		streamio.WithMaxRowErrors(1, nil))
	if err == nil {
		t.Fatal("ProcessFile with 2 malformed lines and MaxRowErrors 1 returned no error")
	}

	var tooMany *streamio.TooManyRowErrorsError
	if !errors.As(err, &tooMany) {
		t.Fatalf("ProcessFile error = %v, want it to unwrap to *streamio.TooManyRowErrorsError", err)
	}
	if tooMany.Limit != 1 {
		t.Errorf("TooManyRowErrorsError.Limit = %d, want 1", tooMany.Limit)
	}
	if len(tooMany.Errors) != 2 {
		t.Errorf("TooManyRowErrorsError.Errors has %d entries, want 2 (both malformed lines, "+
			"including the one that exceeded the limit)", len(tooMany.Errors))
	}
	if !errors.Is(err, streamio.ErrTooManyRowErrors) {
		t.Error("ProcessFile error does not match streamio.ErrTooManyRowErrors via errors.Is")
	}
	if result.Stats.RowsSkipped != 2 {
		t.Errorf("Stats.RowsSkipped = %d, want 2 — the row that finally exceeds the limit is still "+
			"itself skipped and counted, per WithMaxRowErrors' documented contract", result.Stats.RowsSkipped)
	}
}

// TestProcessFile_MaxRowErrorsIsGlobalAcrossWorkers checks MaxRowErrors is a single limit shared
// by every decode worker, not a separate allowance per worker: with --workers 4 and MaxRowErrors 1,
// malformed rows spread across different workers' chunks must still trip the same shared limit
// once the total (not any one worker's own count) exceeds it.
func TestProcessFile_MaxRowErrorsIsGlobalAcrossWorkers(t *testing.T) {
	const chunkSize = 64
	// Four chunks, each containing exactly one malformed line among otherwise-valid ones, so with
	// 4 workers each worker's own chunk sees exactly one bad row — a per-worker limit of 1 would
	// let every single one through, while a shared limit of 1 must still fail on the second one
	// encountered across all workers combined.
	var buf strings.Builder
	for chunk := range 4 {
		buf.WriteString(`{"a":}` + "\n")
		for i := range 3 {
			fmt.Fprintf(&buf, `{"a":%d}`+"\n", chunk*10+i)
		}
	}
	path := filepath.Join(t.TempDir(), "data.ndjson")
	if err := os.WriteFile(path, []byte(buf.String()), 0o600); err != nil {
		t.Fatalf("write ndjson file: %v", err)
	}
	sink, _ := collectingSink()

	_, err := streamio.ProcessFile(context.Background(), path, sink,
		streamio.WithOutputFormat(streamio.FormatCSV),
		streamio.WithParallelWorkers(4),
		streamio.WithChunkSize(chunkSize),
		streamio.WithMaxRowErrors(1, nil))
	if err == nil {
		t.Fatal("ProcessFile with 4 malformed lines spread across 4 workers and MaxRowErrors 1 " +
			"returned no error — the limit must be shared across workers, not per-worker")
	}
	var tooMany *streamio.TooManyRowErrorsError
	if !errors.As(err, &tooMany) {
		t.Fatalf("ProcessFile error = %v, want it to unwrap to *streamio.TooManyRowErrorsError", err)
	}
}

func TestProcessFile_InvalidTransformReturnsError(t *testing.T) {
	_, err := streamio.ProcessFile(
		context.Background(),
		filepath.Join(t.TempDir(), "missing.parquet"),
		func(context.Context, []byte) error { return nil },
		streamio.WithTransforms(streamio.RenamePath("attributes.service", "service")),
	)
	if err == nil || !strings.Contains(err.Error(), "changes path depth") {
		t.Fatalf("ProcessFile error = %v, want invalid transform path error", err)
	}
}
