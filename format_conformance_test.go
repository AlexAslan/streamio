package streamio_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/AlexAslan/streamio"
)

// conformanceFormat is one entry in the table every registered format is checked against: how to
// write a minimal valid file and a malformed one for that format, and the file extension that
// makes ProcessFile detect it.
type conformanceFormat struct {
	writeRows  func(tb testing.TB, n int) string
	writeEmpty func(tb testing.TB) string
	writeBad   func(tb testing.TB) string
	name       string
	format     streamio.Format
}

// conformanceFormats lists every format wired into streamio's registry. A format added to
// streamio.go's formatSupportFor without a matching entry here is not caught automatically — this
// table is maintained by hand, deliberately, so a new format's author has to think about these
// same edge cases rather than inherit untested ones.
func conformanceFormats() []conformanceFormat {
	return []conformanceFormat{
		{
			name:   "json",
			format: streamio.FormatJSON,
			writeRows: func(tb testing.TB, n int) string {
				tb.Helper()
				return writeConformanceFile(tb, ".ndjson", genConformanceNDJSON(n))
			},
			writeEmpty: func(tb testing.TB) string {
				tb.Helper()
				return writeConformanceFile(tb, ".ndjson", "")
			},
			writeBad: func(tb testing.TB) string {
				tb.Helper()
				return writeConformanceFile(tb, ".ndjson", "{not json}\n")
			},
		},
		{
			name:   "csv",
			format: streamio.FormatCSV,
			writeRows: func(tb testing.TB, n int) string {
				tb.Helper()
				return writeConformanceFile(tb, ".csv", genConformanceCSVRows(n))
			},
			writeEmpty: func(tb testing.TB) string {
				tb.Helper()
				return writeConformanceFile(tb, ".csv", "")
			},
			writeBad: func(tb testing.TB) string {
				tb.Helper()
				return writeConformanceFile(tb, ".csv", "1,2,3\n1,2\n") // wrong field count
			},
		},
		{
			name:   "tsv",
			format: streamio.FormatTSV,
			writeRows: func(tb testing.TB, n int) string {
				tb.Helper()
				return writeConformanceFile(tb, ".tsv", genConformanceTSVRows(n))
			},
			writeEmpty: func(tb testing.TB) string {
				tb.Helper()
				return writeConformanceFile(tb, ".tsv", "")
			},
			writeBad: func(tb testing.TB) string {
				tb.Helper()
				return writeConformanceFile(tb, ".tsv", "1\t2\t3\n1\t2\n") // wrong field count
			},
		},
	}
}

func writeConformanceFile(tb testing.TB, ext, content string) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "data"+ext)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		tb.Fatalf("write %s: %v", ext, err)
	}
	return path
}

func genConformanceNDJSON(n int) string {
	var b strings.Builder
	for i := range n {
		b.WriteString(`{"i":`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString("}\n")
	}
	return b.String()
}

func genConformanceCSVRows(n int) string {
	var b strings.Builder
	for i := range n {
		b.WriteString(strconv.Itoa(i))
		b.WriteByte('\n')
	}
	return b.String()
}

func genConformanceTSVRows(n int) string {
	return genConformanceCSVRows(n) // single-column rows render identically either delimiter
}

// collectConformanceDocs returns a sink safe for concurrent workers plus a function retrieving
// what it collected.
func collectConformanceDocs() (streamio.DocumentHandler, func() int) {
	var (
		mu    sync.Mutex
		count int
	)
	sink := func(context.Context, []byte) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	}
	return sink, func() int { return count }
}

// TestConformance_EmptyFile checks every registered format decodes an empty input to zero
// documents rather than erroring or blocking, run through the format's own native raw-passthrough
// route.
func TestConformance_EmptyFile(t *testing.T) {
	for _, f := range conformanceFormats() {
		t.Run(f.name, func(t *testing.T) {
			path := f.writeEmpty(t)
			sink, count := collectConformanceDocs()

			result, err := streamio.ProcessFile(context.Background(), path, sink)
			if err != nil {
				t.Fatalf("ProcessFile: %v", err)
			}
			if got := count(); got != 0 {
				t.Errorf("dispatched %d documents, want 0", got)
			}
			if result.Stats.RowsRead != 0 {
				t.Errorf("RowsRead = %d, want 0", result.Stats.RowsRead)
			}
		})
	}
}

// TestConformance_SingleRow checks every registered format round-trips exactly one row.
func TestConformance_SingleRow(t *testing.T) {
	for _, f := range conformanceFormats() {
		t.Run(f.name, func(t *testing.T) {
			path := f.writeRows(t, 1)
			sink, count := collectConformanceDocs()

			if _, err := streamio.ProcessFile(context.Background(), path, sink); err != nil {
				t.Fatalf("ProcessFile: %v", err)
			}
			if got := count(); got != 1 {
				t.Errorf("dispatched %d documents, want 1", got)
			}
		})
	}
}

// TestConformance_MultiRowSpansChunks checks every registered format's raw-passthrough route
// splits a file with more rows than fit in one worker's chunk without losing or duplicating any,
// exercising the same byte-range splitting jsonio.chunkLines and csvio.chunkLines both implement.
func TestConformance_MultiRowSpansChunks(t *testing.T) {
	const rows = 500

	for _, f := range conformanceFormats() {
		t.Run(f.name, func(t *testing.T) {
			path := f.writeRows(t, rows)
			sink, count := collectConformanceDocs()

			result, err := streamio.ProcessFile(context.Background(), path, sink,
				streamio.WithParallelWorkers(4), streamio.WithChunkSize(37))
			if err != nil {
				t.Fatalf("ProcessFile: %v", err)
			}
			if got := count(); got != rows {
				t.Errorf("dispatched %d documents, want %d", got, rows)
			}
			if result.Stats.RowsRead != rows {
				t.Errorf("RowsRead = %d, want %d", result.Stats.RowsRead, rows)
			}
		})
	}
}

// TestConformance_MalformedInputFails checks every registered format reports an error for input
// its own decoder cannot parse, rather than silently succeeding on a partial or garbage read. Run
// through the generic record path (output forced to a different format) so a decoder is actually
// invoked instead of a raw-passthrough copy that would never look at the bytes' structure.
func TestConformance_MalformedInputFails(t *testing.T) {
	for _, f := range conformanceFormats() {
		t.Run(f.name, func(t *testing.T) {
			path := f.writeBad(t)
			sink, _ := collectConformanceDocs()

			out := streamio.FormatJSON
			if f.format == streamio.FormatJSON {
				out = streamio.FormatCSV
			}

			_, err := streamio.ProcessFile(context.Background(), path, sink,
				streamio.WithInputFormat(f.format), streamio.WithOutputFormat(out))
			if err == nil {
				t.Fatalf("ProcessFile on malformed %s input returned no error", f.name)
			}
		})
	}
}

// TestConformance_ExtensionDetection checks each format's own file extension is detected without
// an explicit WithInputFormat, and that requesting the same format back as output takes the
// raw-passthrough route (verified indirectly: it succeeds and dispatches the right document count,
// since only the raw route is reachable with no record decoder/encoder pair needed).
func TestConformance_ExtensionDetection(t *testing.T) {
	for _, f := range conformanceFormats() {
		t.Run(f.name, func(t *testing.T) {
			path := f.writeRows(t, 3)
			sink, count := collectConformanceDocs()

			if _, err := streamio.ProcessFile(context.Background(), path, sink); err != nil {
				t.Fatalf("ProcessFile: %v", err)
			}
			if got := count(); got != 3 {
				t.Errorf("dispatched %d documents, want 3", got)
			}
		})
	}
}
