package parquetio_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"streamio/internal/options"
	"streamio/internal/parquetio"
	"streamio/internal/pool"
	"sync"
	"sync/atomic"
	"testing"

	parquetgo "github.com/parquet-go/parquet-go"
)

// errRawTestSink is the static sentinel the sink-failure test returns, per the repo's
// wrapped-static-error convention.
var errRawTestSink = errors.New("raw test sink failed")

// rawTestRow exercises the column shapes the raw path has to carry through verbatim: a string, an
// int, and a Map(String,String), which is the one parquet-go reconstructs from a nested
// key_value group rather than a flat leaf.
type rawTestRow struct {
	Attributes map[string]string `parquet:"attributes"`
	Label      string            `parquet:"label"`
	ID         int64             `parquet:"id"`
}

// writeRawTestFile writes n rows split into row groups of rgSize rows, and returns the path plus
// the rows written.
func writeRawTestFile(tb testing.TB, n, rgSize int) (string, []rawTestRow) {
	tb.Helper()

	rows := make([]rawTestRow, n)
	for i := range rows {
		rows[i] = rawTestRow{
			ID:         int64(i),
			Label:      fmt.Sprintf("label-%d", i),
			Attributes: map[string]string{"k": fmt.Sprintf("v%d", i)},
		}
	}

	path := filepath.Join(tb.TempDir(), "raw.parquet")
	writeParquetRows(tb, path, rows, parquetgo.MaxRowsPerRowGroup(int64(rgSize)))
	return path, rows
}

// runRawSource runs path through parquetio.NewRawSource with the given worker count and returns every
// dispatched document alongside the Result.
func runRawSource(tb testing.TB, path string, workers int) ([][]byte, options.Result) {
	tb.Helper()

	cfg := options.New(
		options.WithParallelWorkers(workers),
		options.WithOutputFormat(options.FormatParquet),
	)

	src, err := parquetio.NewRawSource(cfg, path)
	if err != nil {
		tb.Fatalf("NewRawSource: %v", err)
	}
	defer func() {
		if closeErr := src.Close(); closeErr != nil {
			tb.Errorf("Close: %v", closeErr)
		}
	}()

	var (
		mu   sync.Mutex
		docs [][]byte
	)
	sink := func(_ context.Context, doc []byte) error {
		mu.Lock()
		defer mu.Unlock()
		docs = append(docs, doc)
		return nil
	}

	result, err := pool.RunRaw(context.Background(), cfg, src, sink)
	if err != nil {
		tb.Fatalf("RunRaw: %v", err)
	}
	return docs, result
}

// readBackRows reopens a dispatched document as a standalone Parquet file and returns its rows.
// This is the assertion that matters: the raw path is only correct if each document is a complete,
// independently-openable Parquet file, not a headerless fragment.
func readBackRows(tb testing.TB, doc []byte) []rawTestRow {
	tb.Helper()

	pf, err := parquetgo.OpenFile(bytes.NewReader(doc), int64(len(doc)))
	if err != nil {
		tb.Fatalf("OpenFile on dispatched document: %v", err)
	}
	if got := len(pf.RowGroups()); got != 1 {
		tb.Fatalf("dispatched document has %d row groups, want exactly 1", got)
	}

	r := parquetgo.NewGenericReader[rawTestRow](pf)
	defer r.Close()

	rows := make([]rawTestRow, pf.NumRows())
	n, err := r.Read(rows)
	if err != nil && !errors.Is(err, io.EOF) {
		tb.Fatalf("reading back dispatched document: %v", err)
	}
	if int64(n) != pf.NumRows() {
		tb.Fatalf("read %d rows back, want %d", n, pf.NumRows())
	}
	return rows
}

// TestRawSource_RoundTrip is the core correctness claim of the raw-passthrough path: every
// dispatched document reopens as a valid single-row-group Parquet file, and the union of their
// rows is exactly the source file's rows.
func TestRawSource_RoundTrip(t *testing.T) {
	type args struct {
		name     string
		rows     int
		rowGroup int
		workers  int
	}

	tests := []args{
		{name: "single row group, single worker", rows: 25, rowGroup: 25, workers: 1},
		{name: "many row groups, single worker", rows: 100, rowGroup: 10, workers: 1},
		{name: "many row groups, parallel workers", rows: 100, rowGroup: 10, workers: 4},
		{name: "more workers than row groups", rows: 6, rowGroup: 3, workers: 8},
		{name: "one row per row group", rows: 5, rowGroup: 1, workers: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, want := writeRawTestFile(t, tt.rows, tt.rowGroup)
			docs, result := runRawSource(t, path, tt.workers)

			if result.Stats.RowsRead != int64(tt.rows) {
				t.Errorf("Stats.RowsRead = %d, want %d", result.Stats.RowsRead, tt.rows)
			}

			assertRowsMatch(t, readBackAll(t, docs), want)
		})
	}
}

// TestRawSource_OneDocumentPerRowGroup pins the dispatch unit: a row group, not a row. Getting this
// wrong is what makes the pool's dispatch counter disagree with Stats.RowsRead.
func TestRawSource_OneDocumentPerRowGroup(t *testing.T) {
	const (
		rows     = 40
		rowGroup = 10
	)
	path, _ := writeRawTestFile(t, rows, rowGroup)

	docs, result := runRawSource(t, path, 2)

	if want := rows / rowGroup; len(docs) != want {
		t.Errorf("dispatched %d documents, want %d (one per row group)", len(docs), want)
	}
	if result.Stats.RowsRead != rows {
		t.Errorf("Stats.RowsRead = %d, want %d — Count must report rows, not row groups", result.Stats.RowsRead, rows)
	}
}

// TestRawSource_MissingFile checks the constructor surfaces an open failure instead of returning a
// source that fails later, mid-run.
func TestRawSource_MissingFile(t *testing.T) {
	_, err := parquetio.NewRawSource(options.New(), filepath.Join(t.TempDir(), "does-not-exist.parquet"))
	if err == nil {
		t.Fatal("NewRawSource on a missing file returned no error")
	}
}

// TestRawSource_ContextCancellation checks a cancelled context stops the source rather than running
// the file to completion.
func TestRawSource_ContextCancellation(t *testing.T) {
	path, _ := writeRawTestFile(t, 200, 5)

	cfg := options.New(
		options.WithParallelWorkers(2),
		options.WithOutputFormat(options.FormatParquet),
	)
	src, err := parquetio.NewRawSource(cfg, path)
	if err != nil {
		t.Fatalf("NewRawSource: %v", err)
	}
	defer src.Close()

	ctx, cancel := context.WithCancel(context.Background())
	// Atomic: options.DocumentHandler is documented as callable concurrently, and cfg.Workers here is 2.
	var seen atomic.Int64
	sink := func(context.Context, []byte) error {
		if seen.Add(1) == 2 {
			cancel()
		}
		return nil
	}

	if _, err = pool.RunRaw(ctx, cfg, src, sink); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunRaw error = %v, want context.Canceled", err)
	}
}

// TestRawSource_SinkErrorPropagates checks a sink failure surfaces rather than being swallowed.
func TestRawSource_SinkErrorPropagates(t *testing.T) {
	path, _ := writeRawTestFile(t, 20, 5)

	cfg := options.New(
		options.WithParallelWorkers(1),
		options.WithOutputFormat(options.FormatParquet),
	)
	src, err := parquetio.NewRawSource(cfg, path)
	if err != nil {
		t.Fatalf("NewRawSource: %v", err)
	}
	defer src.Close()

	sink := func(context.Context, []byte) error { return errRawTestSink }
	if _, err = pool.RunRaw(context.Background(), cfg, src, sink); !errors.Is(err, errRawTestSink) {
		t.Fatalf("RunRaw error = %v, want errRawTestSink", err)
	}
}

// readBackAll reopens every dispatched document and indexes the rows it finds by ID, failing tb if
// any row shows up in more than one document.
func readBackAll(tb testing.TB, docs [][]byte) map[int64]rawTestRow {
	tb.Helper()

	got := make(map[int64]rawTestRow)
	for _, doc := range docs {
		for _, row := range readBackRows(tb, doc) {
			if _, dup := got[row.ID]; dup {
				tb.Fatalf("row id %d dispatched more than once", row.ID)
			}
			got[row.ID] = row
		}
	}
	return got
}

// assertRowsMatch checks got holds exactly want, comparing by ID since dispatch order across
// workers isn't guaranteed.
func assertRowsMatch(tb testing.TB, got map[int64]rawTestRow, want []rawTestRow) {
	tb.Helper()

	if len(got) != len(want) {
		tb.Fatalf("read back %d rows, want %d", len(got), len(want))
	}
	for _, w := range want {
		g, ok := got[w.ID]
		if !ok {
			tb.Fatalf("row id %d missing from the dispatched documents", w.ID)
		}
		if g.Label != w.Label {
			tb.Errorf("row %d: Label = %q, want %q", w.ID, g.Label, w.Label)
		}
		if g.Attributes["k"] != w.Attributes["k"] {
			tb.Errorf("row %d: Attributes[k] = %q, want %q", w.ID, g.Attributes["k"], w.Attributes["k"])
		}
	}
}
