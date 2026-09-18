package streamio_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/AlexAslan/streamio"
)

// routingRow is the fixture struct for the format-routing tests.
type routingRow struct {
	Label string `parquet:"label"`
	ID    int64  `parquet:"id"`
}

// writeRoutingParquet writes n rows split into row groups of rgSize rows and returns the path.
func writeRoutingParquet(tb testing.TB, n, rgSize int) string {
	tb.Helper()

	rows := make([]routingRow, n)
	for i := range rows {
		rows[i] = routingRow{ID: int64(i), Label: fmt.Sprintf("label-%d", i)}
	}

	path := filepath.Join(tb.TempDir(), "routing.parquet")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create: %v", err)
	}
	w := parquetgo.NewGenericWriter[routingRow](f, parquetgo.MaxRowsPerRowGroup(int64(rgSize)))
	if _, err = w.Write(rows); err != nil {
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

// countingSink counts documents and records the first one seen.
func countingSink() (streamio.DocumentHandler, *atomic.Int64, *atomic.Pointer[[]byte]) {
	var (
		count atomic.Int64
		first atomic.Pointer[[]byte]
	)
	sink := func(_ context.Context, doc []byte) error {
		count.Add(1)
		if count.Load() == 1 {
			cp := make([]byte, len(doc))
			copy(cp, doc)
			first.Store(&cp)
		}
		return nil
	}
	return sink, &count, &first
}

// TestProcessFile_ParquetToParquetUsesRawPassthrough checks that asking for the input's own format
// selects raw passthrough: documents come out as standalone Parquet files, one per row group, while
// Result stats distinguish rows read from dispatched documents.
func TestProcessFile_ParquetToParquetUsesRawPassthrough(t *testing.T) {
	const (
		rows     = 30
		rowGroup = 10
	)
	path := writeRoutingParquet(t, rows, rowGroup)
	sink, count, first := countingSink()

	result, err := streamio.ProcessFile(
		context.Background(), path, sink,
		streamio.WithOutputFormat(streamio.FormatParquet),
		streamio.WithParallelWorkers(1),
	)
	if err != nil {
		t.Fatalf("ProcessFile: %v", err)
	}

	if got := count.Load(); got != rows/rowGroup {
		t.Errorf("dispatched %d documents, want %d (one per row group)", got, rows/rowGroup)
	}
	if result.Stats.RowsRead != rows {
		t.Errorf("Stats.RowsRead = %d, want %d", result.Stats.RowsRead, rows)
	}
	if result.Stats.RowsRead != rows {
		t.Errorf("RowsRead = %d, want %d", result.Stats.RowsRead, rows)
	}
	if result.Stats.DocumentsDispatched != rows/rowGroup {
		t.Errorf("DocumentsDispatched = %d, want %d row-group documents",
			result.Stats.DocumentsDispatched, rows/rowGroup)
	}

	doc := *first.Load()
	if !bytes.HasPrefix(doc, []byte("PAR1")) || !bytes.HasSuffix(doc, []byte("PAR1")) {
		t.Fatalf("dispatched document is not a standalone Parquet file (len %d)", len(doc))
	}
	pf, err := parquetgo.OpenFile(bytes.NewReader(doc), int64(len(doc)))
	if err != nil {
		t.Fatalf("reopening dispatched document: %v", err)
	}
	if pf.NumRows() != rowGroup {
		t.Errorf("dispatched document has %d rows, want %d", pf.NumRows(), rowGroup)
	}
}

func TestProcessFile_ExplicitInputFormatOverridesExtension(t *testing.T) {
	const (
		rows     = 8
		rowGroup = 4
	)
	path := writeRoutingParquet(t, rows, rowGroup)
	extensionlessPath := filepath.Join(filepath.Dir(path), "routing")
	if err := os.Rename(path, extensionlessPath); err != nil {
		t.Fatalf("rename fixture: %v", err)
	}

	sink, count, first := countingSink()
	result, err := streamio.ProcessFile(
		context.Background(), extensionlessPath, sink,
		streamio.WithInputFormat(streamio.FormatParquet),
		streamio.WithOutputFormat(streamio.FormatParquet),
		streamio.WithParallelWorkers(1),
	)
	if err != nil {
		t.Fatalf("ProcessFile: %v", err)
	}
	if result.Stats.RowsRead != rows || result.Stats.DocumentsDispatched != rows/rowGroup {
		t.Fatalf("Stats = %+v, want %d rows and %d documents",
			result.Stats, rows, rows/rowGroup)
	}
	if count.Load() != rows/rowGroup {
		t.Fatalf("dispatched %d documents, want %d", count.Load(), rows/rowGroup)
	}
	doc := *first.Load()
	if !bytes.HasPrefix(doc, []byte("PAR1")) || !bytes.HasSuffix(doc, []byte("PAR1")) {
		t.Fatalf("first document is not a standalone Parquet file")
	}
}

// TestProcessFile_ParquetToJSONDispatchesOneObjectPerRow checks Parquet→JSON is routed to the
// generic record path and shaped the way JSON output always has been: one document per row, each a
// JSON object, whether the format is requested explicitly or left to the default. What the values
// look like is TestProcessFile_ParquetToJSON's business; this is about routing and framing.
func TestProcessFile_ParquetToJSONDispatchesOneObjectPerRow(t *testing.T) {
	type args struct {
		name string
		opts []streamio.Option
	}

	tests := []args{
		{name: "default format", opts: nil},
		{name: "explicit json", opts: []streamio.Option{streamio.WithOutputFormat(streamio.FormatJSON)}},
	}

	const rows = 12
	path := writeRoutingParquet(t, rows, 4)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink, count, first := countingSink()
			opts := append([]streamio.Option{streamio.WithParallelWorkers(1)}, tt.opts...)

			result, err := streamio.ProcessFile(context.Background(), path, sink, opts...)
			if err != nil {
				t.Fatalf("ProcessFile: %v", err)
			}
			if count.Load() != rows || result.Stats.RowsRead != rows {
				t.Errorf("dispatched %d documents (Stats.RowsRead %d), want %d each",
					count.Load(), result.Stats.RowsRead, rows)
			}
			if doc := *first.Load(); !bytes.HasPrefix(doc, []byte("{")) {
				t.Errorf("first document = %q, want a JSON object", doc)
			}
		})
	}
}

// TestProcessFile_NDJSONToJSONUnchanged checks NDJSON's native-format match still yields exactly
// today's line passthrough — one document per line, byte-identical to the source line.
func TestProcessFile_NDJSONToJSONUnchanged(t *testing.T) {
	type args struct {
		name string
		opts []streamio.Option
	}

	tests := []args{
		{name: "default format", opts: nil},
		{name: "explicit json", opts: []streamio.Option{streamio.WithOutputFormat(streamio.FormatJSON)}},
	}

	const lines = 25
	path := filepath.Join(t.TempDir(), "routing.ndjson")
	createNDJSONFileWithLines(t, path, lines)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink, count, first := countingSink()
			opts := append([]streamio.Option{streamio.WithParallelWorkers(1)}, tt.opts...)

			result, err := streamio.ProcessFile(context.Background(), path, sink, opts...)
			if err != nil {
				t.Fatalf("ProcessFile: %v", err)
			}
			if count.Load() != lines || result.Stats.RowsRead != lines {
				t.Errorf("dispatched %d documents (Stats.RowsRead %d), want %d each",
					count.Load(), result.Stats.RowsRead, lines)
			}
			if doc := string(*first.Load()); doc != `{"i":0}` {
				t.Errorf("first document = %q, want %q", doc, `{"i":0}`)
			}
		})
	}
}

// TestProcessFile_UnsupportedFormatPair checks an unregistered conversion reports the documented
// error rather than panicking or silently falling back to some other format.
func TestProcessFile_UnsupportedFormatPair(t *testing.T) {
	type args struct {
		name     string
		makeFile func(tb testing.TB) string
		format   streamio.Format
		wantIn   string
		wantOut  string
	}

	// NDJSON → Parquet used to belong here; it is a real conversion now, covered end to end by
	// TestProcessFile_NDJSONToParquet. What is left unsupported is a format with no registry entry
	// at all, which is what this test is really about.
	tests := []args{
		{
			name:     "parquet to an unknown format",
			makeFile: writeRoutingParquetSmall,
			format:   streamio.Format("xml"),
			wantIn:   "parquet",
			wantOut:  "xml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.makeFile(t)
			sink := func(context.Context, []byte) error { return nil }

			_, err := streamio.ProcessFile(
				context.Background(), path, sink,
				streamio.WithOutputFormat(tt.format),
			)
			if !errors.Is(err, streamio.ErrNoConversionPath) {
				t.Fatalf("error = %v, want it to wrap ErrNoConversionPath", err)
			}
			msg := err.Error()
			for _, want := range []string{tt.wantIn, tt.wantOut} {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q does not name the format %q", msg, want)
				}
			}
		})
	}
}

// writeRoutingParquetSmall writes the tiny Parquet fixture the unsupported-pair test needs; it
// exists as a named helper so the table can reference it without a wrapper closure.
func writeRoutingParquetSmall(tb testing.TB) string {
	tb.Helper()
	return writeRoutingParquet(tb, 4, 2)
}
