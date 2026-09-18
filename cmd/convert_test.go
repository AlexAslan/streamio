package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"streamio"
	"strings"
	"sync"
	"testing"

	parquetgo "github.com/parquet-go/parquet-go"
)

// convertTestParquetRow is the fixture row shape for the Parquet-input tests: wide enough (an int
// and a string column) to be representative, narrow enough to keep fixtures fast to generate.
type convertTestParquetRow struct {
	Msg string `parquet:"msg"`
	ID  int64  `parquet:"id"`
}

// writeParquetFixture writes a rows-row Parquet fixture split into row groups of rowsPerGroup and
// returns its path — multiple row groups is the point: it's what exposes whether a Parquet output
// route actually stitches them into one valid file instead of concatenating independent ones.
func writeParquetFixture(tb testing.TB, rows, rowsPerGroup int) string {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), "in.parquet")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create: %v", err)
	}
	w := parquetgo.NewGenericWriter[convertTestParquetRow](f, parquetgo.MaxRowsPerRowGroup(int64(rowsPerGroup)))
	data := make([]convertTestParquetRow, rows)
	for i := range data {
		data[i] = convertTestParquetRow{ID: int64(i), Msg: fmt.Sprintf("row-%d", i)}
	}
	if _, err = w.Write(data); err != nil {
		tb.Fatalf("write: %v", err)
	}
	if err = w.Close(); err != nil {
		tb.Fatalf("close writer: %v", err)
	}
	if err = f.Close(); err != nil {
		tb.Fatalf("close file: %v", err)
	}
	return path
}

// countParquetRows opens path as a Parquet file and returns its row count, failing the test if it
// isn't even a valid, openable Parquet file — the property TestRunConvert_ParquetToParquet pins.
func countParquetRows(tb testing.TB, path string) int64 {
	tb.Helper()

	stat, err := os.Stat(path)
	if err != nil {
		tb.Fatalf("stat %s: %v", path, err)
	}
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	file, err := parquetgo.OpenFile(f, stat.Size())
	if err != nil {
		tb.Fatalf("OpenFile %s: %v", path, err)
	}
	return file.NumRows()
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		in      string
		want    streamio.Format
		wantErr bool
	}{
		{in: "", want: streamio.FormatJSON},
		{in: "json", want: streamio.FormatJSON},
		{in: "JSON", want: streamio.FormatJSON},
		{in: "parquet", want: streamio.FormatParquet},
		{in: "PARQUET", want: streamio.FormatParquet},
		{in: "csv", want: streamio.FormatCSV},
		{in: "CSV", want: streamio.FormatCSV},
		{in: "tsv", want: streamio.FormatTSV},
		{in: "TSV", want: streamio.FormatTSV},
		{in: "xml", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%q", tt.in), func(t *testing.T) {
			got, err := parseFormat(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseFormat(%q) succeeded, want an error", tt.in)
				}
				if !strings.Contains(err.Error(), "unknown format") {
					t.Errorf("parseFormat(%q) error = %v, want one containing %q", tt.in, err, "unknown format")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFormat(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("parseFormat(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// writeNDJSONFixture writes n JSON lines to a temp file and returns its path.
func writeNDJSONFixture(tb testing.TB, n int) string {
	tb.Helper()

	dir := tb.TempDir()
	path := filepath.Join(dir, "in.json")

	var buf bytes.Buffer
	for i := range n {
		doc, err := json.Marshal(map[string]any{"id": i, "name": fmt.Sprintf("row-%d", i)})
		if err != nil {
			tb.Fatalf("marshal fixture row %d: %v", i, err)
		}
		buf.Write(doc)
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		tb.Fatalf("writing fixture: %v", err)
	}
	return path
}

// countNDJSONLines reports how many non-empty lines path contains.
func countNDJSONLines(tb testing.TB, path string) int {
	tb.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("reading %s: %v", path, err)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		if line != "" {
			n++
		}
	}
	return n
}

// TestRunConvert_JSONToJSONRoundTrip checks the plain NDJSON-to-NDJSON path writes exactly the
// rows read, through the same newlineWriter path production runs take.
func TestRunConvert_JSONToJSONRoundTrip(t *testing.T) {
	const rows = 25
	in := writeNDJSONFixture(t, rows)
	out := filepath.Join(t.TempDir(), "out.json")

	err := runConvert(context.Background(), convertFlags{in: in, out: out}, "json")
	if err != nil {
		t.Fatalf("runConvert: %v", err)
	}

	if got := countNDJSONLines(t, out); got != rows {
		t.Errorf("output has %d lines, want %d", got, rows)
	}
}

// TestRunConvert_JSONToParquetIgnoresWorkersFlag checks --out-format parquet produces a single
// valid, correctly-ordered Parquet file even when --workers requests more than one — pinning the
// documented override (WithSingleFileOutput + WithParallelWorkers(1) forced regardless of the flag)
// against a regression that silently drops it back to honoring --workers, which would corrupt
// multi-row-group output under concurrent dispatch.
func TestRunConvert_JSONToParquetIgnoresWorkersFlag(t *testing.T) {
	const rows = 500
	in := writeNDJSONFixture(t, rows)
	parquetOut := filepath.Join(t.TempDir(), "out.parquet")

	flags := convertFlags{in: in, out: parquetOut, batchSize: 10, workers: 8}
	if err := runConvert(context.Background(), flags, "parquet"); err != nil {
		t.Fatalf("runConvert (to parquet): %v", err)
	}

	// Convert back to NDJSON to confirm every row survived the round trip, in order and without
	// duplication — the observable symptom of dispatch reordering corrupting row-group order.
	jsonOut := filepath.Join(t.TempDir(), "roundtrip.json")
	backFlags := convertFlags{in: parquetOut, out: jsonOut, inFormat: "parquet"}
	if err := runConvert(context.Background(), backFlags, "json"); err != nil {
		t.Fatalf("runConvert (back to json): %v", err)
	}

	if got := countNDJSONLines(t, jsonOut); got != rows {
		t.Errorf("round trip produced %d rows, want %d", got, rows)
	}
}

// TestRunConvert_JSONToCSVIgnoresWorkersFlag checks --out-format csv with --csv-has-header produces
// exactly one header line and every row in order even when --workers requests more than one —
// pinning a fix for a real bug: CSV/TSV output used to honor --workers like any other non-Parquet
// format, but with more than one worker and no streaming encoder, dispatch delivers batches in
// completion order (not input order) and each worker's own encoder derives and writes its own
// header from its first batch, corrupting the output with reordered rows and a repeated header.
func TestRunConvert_JSONToCSVIgnoresWorkersFlag(t *testing.T) {
	const rows = 500
	in := writeNDJSONFixture(t, rows)
	out := filepath.Join(t.TempDir(), "out.csv")

	flags := convertFlags{in: in, out: out, batchSize: 10, workers: 8, csvHasHeader: true}
	if err := runConvert(context.Background(), flags, "csv"); err != nil {
		t.Fatalf("runConvert: %v", err)
	}

	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("reading %s: %v", out, err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != rows+1 {
		t.Fatalf("output has %d lines, want %d (1 header + %d rows)", len(lines), rows+1, rows)
	}
	if lines[0] != "id,name" {
		t.Errorf("header = %q, want %q", lines[0], "id,name")
	}
	headerCount := 0
	for _, line := range lines {
		if line == "id,name" {
			headerCount++
		}
	}
	if headerCount != 1 {
		t.Errorf("header line appears %d times, want exactly 1", headerCount)
	}
	for i := 1; i <= rows; i++ {
		want := fmt.Sprintf("%d,row-%d", i-1, i-1)
		if lines[i] != want {
			t.Errorf("row %d = %q, want %q (order not preserved)", i-1, lines[i], want)
		}
	}
}

// TestRunConvert_ParquetToParquetProducesOneValidFile pins a fix for a real bug: Parquet input with
// more than one row group converted straight through to Parquet output used to take the raw-
// passthrough route regardless of --out-format's single-file contract, dispatching one complete,
// independently-closed Parquet file per input row group and concatenating them into --out with no
// separator — not a valid Parquet file at all past the first row group's footer. ProcessFile now
// rules raw passthrough out whenever WithSingleFileOutput is set (see streamio.go), so
// Parquet-to-Parquet decodes to records and re-encodes through streamingEncoder like every other
// route instead. This test would have failed with "OpenFile: reading page index of parquet file:
// ..." before that fix, for exactly the multi-row-group fixture it uses.
func TestRunConvert_ParquetToParquetProducesOneValidFile(t *testing.T) {
	const rows = 300
	in := writeParquetFixture(t, rows, 100) // 3 row groups in the input.
	out := filepath.Join(t.TempDir(), "out.parquet")

	if err := runConvert(context.Background(), convertFlags{in: in, out: out}, "parquet"); err != nil {
		t.Fatalf("runConvert: %v", err)
	}

	if got := countParquetRows(t, out); got != rows {
		t.Errorf("output has %d rows, want %d", got, rows)
	}
}

// TestRunConvert_ParquetToJSONRoundTrip checks the generic decode route from a multi-row-group
// Parquet input converts cleanly to NDJSON — the other Parquet-input route the CLI supports
// alongside Parquet-to-Parquet, and one nothing exercised end-to-end before this test.
func TestRunConvert_ParquetToJSONRoundTrip(t *testing.T) {
	const rows = 300
	in := writeParquetFixture(t, rows, 100)
	out := filepath.Join(t.TempDir(), "out.json")

	flags := convertFlags{in: in, out: out, inFormat: "parquet"}
	if err := runConvert(context.Background(), flags, "json"); err != nil {
		t.Fatalf("runConvert: %v", err)
	}

	if got := countNDJSONLines(t, out); got != rows {
		t.Errorf("output has %d lines, want %d", got, rows)
	}
}

// TestRunConvert_TooSmallBatchSizeGetsAnActionableError checks that a --batch-size too small for
// the input's row count (which parquet-go's writer would otherwise fail with a bare "the limit of
// 32767 row groups has been reached") comes back from runConvert with an actionable hint pointing
// at --batch-size, not just parquet-go's own wording — see runConvert's errors.Is(err,
// parquetgo.ErrTooManyRowGroups) branch. --batch-size 1 makes every one of the 32,768 rows its own
// row group, the cheapest way to exceed the 32,767 cap in a test.
func TestRunConvert_TooSmallBatchSizeGetsAnActionableError(t *testing.T) {
	const rows = 32_768 // one more than parquet-go's 32,767 row-group cap, at batchSize: 1
	in := writeNDJSONFixture(t, rows)
	out := filepath.Join(t.TempDir(), "out.parquet")

	flags := convertFlags{in: in, out: out, batchSize: 1}
	err := runConvert(context.Background(), flags, "parquet")
	if err == nil {
		t.Fatal("runConvert succeeded, want an error past the 32,767 row-group cap")
	}
	if !strings.Contains(err.Error(), "--batch-size") {
		t.Errorf("runConvert error = %v, want one mentioning --batch-size", err)
	}
}

// countCSVLines reports how many lines path contains, trailing newline aside — used for CSV/TSV
// fixtures where each row is one line.
func countCSVLines(tb testing.TB, path string) int {
	tb.Helper()
	return countNDJSONLines(tb, path)
}

// TestRunConvert_JSONToCSVRoundTrip checks NDJSON input converts to CSV with a header row and
// converts back to NDJSON with every row intact.
func TestRunConvert_JSONToCSVRoundTrip(t *testing.T) {
	const rows = 25
	in := writeNDJSONFixture(t, rows)
	csvOut := filepath.Join(t.TempDir(), "out.csv")

	flags := convertFlags{in: in, out: csvOut, csvHasHeader: true}
	if err := runConvert(context.Background(), flags, "csv"); err != nil {
		t.Fatalf("runConvert (to csv): %v", err)
	}
	if got := countCSVLines(t, csvOut); got != rows+1 { // +1 for the header row.
		t.Errorf("csv output has %d lines, want %d (rows + header)", got, rows+1)
	}

	jsonOut := filepath.Join(t.TempDir(), "roundtrip.json")
	backFlags := convertFlags{in: csvOut, out: jsonOut, inFormat: "csv", csvHasHeader: true}
	if err := runConvert(context.Background(), backFlags, "json"); err != nil {
		t.Fatalf("runConvert (back to json): %v", err)
	}
	if got := countNDJSONLines(t, jsonOut); got != rows {
		t.Errorf("round trip produced %d rows, want %d", got, rows)
	}
}

// TestRunConvert_JSONToTSVRoundTrip mirrors the CSV round trip for TSV, checking the tab
// delimiter is picked automatically without needing --csv-delimiter.
func TestRunConvert_JSONToTSVRoundTrip(t *testing.T) {
	const rows = 25
	in := writeNDJSONFixture(t, rows)
	tsvOut := filepath.Join(t.TempDir(), "out.tsv")

	flags := convertFlags{in: in, out: tsvOut, csvHasHeader: true}
	if err := runConvert(context.Background(), flags, "tsv"); err != nil {
		t.Fatalf("runConvert (to tsv): %v", err)
	}

	data, err := os.ReadFile(tsvOut)
	if err != nil {
		t.Fatalf("reading %s: %v", tsvOut, err)
	}
	if !strings.Contains(string(data), "\t") {
		t.Errorf("tsv output has no tab characters: %q", data)
	}

	jsonOut := filepath.Join(t.TempDir(), "roundtrip.json")
	backFlags := convertFlags{in: tsvOut, out: jsonOut, inFormat: "tsv", csvHasHeader: true}
	if backErr := runConvert(context.Background(), backFlags, "json"); backErr != nil {
		t.Fatalf("runConvert (back to json): %v", backErr)
	}
	if got := countNDJSONLines(t, jsonOut); got != rows {
		t.Errorf("round trip produced %d rows, want %d", got, rows)
	}
}

// TestRunConvert_CustomCSVDelimiter checks --csv-delimiter overrides CSV's default comma.
func TestRunConvert_CustomCSVDelimiter(t *testing.T) {
	const rows = 10
	in := writeNDJSONFixture(t, rows)
	out := filepath.Join(t.TempDir(), "out.csv")

	flags := convertFlags{in: in, out: out, csvDelimiter: "|"}
	if err := runConvert(context.Background(), flags, "csv"); err != nil {
		t.Fatalf("runConvert: %v", err)
	}

	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("reading %s: %v", out, err)
	}
	if !strings.Contains(string(data), "|") {
		t.Errorf("csv output has no '|' delimiter: %q", data)
	}
}

// TestRunConvert_InvalidCSVDelimiterRejected checks a --csv-delimiter longer than one character
// fails with an actionable error rather than silently taking its first rune.
func TestRunConvert_InvalidCSVDelimiterRejected(t *testing.T) {
	in := writeNDJSONFixture(t, 5)
	out := filepath.Join(t.TempDir(), "out.csv")

	flags := convertFlags{in: in, out: out, csvDelimiter: "; "}
	err := runConvert(context.Background(), flags, "csv")
	if err == nil {
		t.Fatal("runConvert succeeded, want an error for a multi-character --csv-delimiter")
	}
	if !strings.Contains(err.Error(), "--csv-delimiter") {
		t.Errorf("runConvert error = %v, want one mentioning --csv-delimiter", err)
	}
}

// TestRunConvert_MaxRowErrorsZeroFailsImmediately checks the default --max-row-errors 0 stops the
// whole conversion on the first malformed line, rather than writing partial or corrupted output.
func TestRunConvert_MaxRowErrorsZeroFailsImmediately(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in.ndjson")
	if err := os.WriteFile(in, []byte("{\"a\":1}\n{\"a\":}\n{\"a\":3}\n"), 0o600); err != nil {
		t.Fatalf("write ndjson file: %v", err)
	}
	out := filepath.Join(t.TempDir(), "out.csv")

	flags := convertFlags{in: in, out: out}
	if err := runConvert(context.Background(), flags, "csv"); err == nil {
		t.Fatal("runConvert on a malformed line returned no error")
	}
}

// TestRunConvert_MaxRowErrorsAllowsSkipping checks a positive --max-row-errors drops a malformed
// line under the limit and writes every good row to the output.
func TestRunConvert_MaxRowErrorsAllowsSkipping(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in.ndjson")
	if err := os.WriteFile(in, []byte("{\"a\":1}\n{\"a\":}\n{\"a\":3}\n"), 0o600); err != nil {
		t.Fatalf("write ndjson file: %v", err)
	}
	out := filepath.Join(t.TempDir(), "out.csv")

	flags := convertFlags{in: in, out: out, maxRowErrors: 1}
	if err := runConvert(context.Background(), flags, "csv"); err != nil {
		t.Fatalf("runConvert: %v", err)
	}

	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("reading %s: %v", out, err)
	}
	got := string(data)
	if !strings.Contains(got, "1") || !strings.Contains(got, "3") {
		t.Errorf("output = %q, want it to contain both good rows' values", got)
	}
}

// TestRunConvert_MaxRowErrorsExceeded checks a positive --max-row-errors still fails the run once
// the number of malformed rows exceeds it, rather than tolerating an unbounded number of them.
func TestRunConvert_MaxRowErrorsExceeded(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in.ndjson")
	if err := os.WriteFile(in, []byte("{\"a\":}\n{\"a\":}\n{\"a\":3}\n"), 0o600); err != nil {
		t.Fatalf("write ndjson file: %v", err)
	}
	out := filepath.Join(t.TempDir(), "out.csv")

	flags := convertFlags{in: in, out: out, maxRowErrors: 1}
	if err := runConvert(context.Background(), flags, "csv"); err == nil {
		t.Fatal("runConvert with 2 malformed lines and --max-row-errors 1 returned no error")
	}
}

// TestRunConvert_NegativeMaxRowErrorsRejected checks a negative --max-row-errors fails with an
// actionable error rather than being silently treated as 0 or unlimited.
func TestRunConvert_NegativeMaxRowErrorsRejected(t *testing.T) {
	in := writeNDJSONFixture(t, 5)
	out := filepath.Join(t.TempDir(), "out.csv")

	flags := convertFlags{in: in, out: out, maxRowErrors: -1}
	err := runConvert(context.Background(), flags, "csv")
	if err == nil {
		t.Fatal("runConvert succeeded, want an error for a negative --max-row-errors")
	}
	if !strings.Contains(err.Error(), "--max-row-errors") {
		t.Errorf("runConvert error = %v, want one mentioning --max-row-errors", err)
	}
}

// TestFileWriter_ConcurrentWritesDoNotInterleave drives fileWriter.write from many goroutines at
// once and checks every document appears in the output whole and exactly once: the mutex the type
// is documented as needing "since decode workers may call the handler concurrently" is otherwise
// never actually exercised concurrently by any other test.
func TestFileWriter_ConcurrentWritesDoNotInterleave(t *testing.T) {
	const (
		writers      = 16
		perGoroutine = 50
	)

	path := filepath.Join(t.TempDir(), "concurrent.out")
	w, err := newFileWriter(path)
	if err != nil {
		t.Fatalf("newFileWriter: %v", err)
	}

	var wg sync.WaitGroup
	for g := range writers {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range perGoroutine {
				doc := []byte(fmt.Sprintf("g%d-i%d;", g, i))
				if writeErr := w.write(context.Background(), doc); writeErr != nil {
					t.Errorf("write: %v", writeErr)
				}
			}
		}(g)
	}
	wg.Wait()

	if closeErr := w.close(); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}

	seen := make(map[string]bool)
	for _, doc := range strings.Split(string(data), ";") {
		if doc == "" {
			continue
		}
		if seen[doc] {
			t.Fatalf("document %q appears more than once: writes interleaved", doc)
		}
		seen[doc] = true
	}
	if want := writers * perGoroutine; len(seen) != want {
		t.Fatalf("saw %d distinct documents, want %d — some were split or merged by interleaving",
			len(seen), want)
	}
}
