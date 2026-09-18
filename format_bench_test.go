package streamio_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/AlexAslan/streamio"
)

// benchRow is deliberately wider than the one-field fixtures elsewhere in this package: the cost
// raw passthrough avoids is per-value JSON encoding, which only shows up with enough columns to
// encode.
type benchRow struct {
	Attributes map[string]string `parquet:"attributes"`
	Label      string            `parquet:"label"`
	Host       string            `parquet:"host"`
	Message    string            `parquet:"message"`
	ID         int64             `parquet:"id"`
	Timestamp  int64             `parquet:"timestamp"`
	Score      float64           `parquet:"score"`
}

// narrowRow is the opposite extreme: scalar-only, no strings or maps to encode, so it isolates
// per-value token-writing overhead from string/map reconstruction cost.
type narrowRow struct {
	ID     int64   `parquet:"id"`
	Score  float64 `parquet:"score"`
	Active bool    `parquet:"active"`
}

// mapHeavyRow exercises the map-reconstruction path (mapKeys/mapVals zipping in
// parquet/record_decoder.go), the single most expensive part of the JSON-decode route — the case
// raw passthrough benefits from most.
type mapHeavyRow struct {
	Attributes map[string]string `parquet:"attributes"`
	ID         int64             `parquet:"id"`
}

// benchFixture sizes the default Parquet fixture the format benchmarks read.
const (
	benchRows         = 20_000
	benchRowsPerGroup = 2_000
)

// writeFormatBenchParquet writes a benchRow fixture with the given row and row-group sizing and
// returns its path.
func writeFormatBenchParquet(b *testing.B, rows, rowsPerGroup int) string {
	b.Helper()

	data := make([]benchRow, rows)
	for i := range data {
		data[i] = benchRow{
			ID:         int64(i),
			Timestamp:  1704067200000 + int64(i),
			Score:      float64(i) * 1.5,
			Label:      fmt.Sprintf("label-%d", i),
			Host:       fmt.Sprintf("host-%d.example.internal", i%128),
			Message:    fmt.Sprintf("a reasonably representative log line for record %d", i),
			Attributes: map[string]string{"service": "checkout", "region": "eu-west-1"},
		}
	}
	return writeGenericParquet(b, "bench-wide.parquet", data, rowsPerGroup)
}

// writeNarrowBenchParquet writes a narrowRow (scalar-only) fixture and returns its path.
func writeNarrowBenchParquet(b *testing.B, rows, rowsPerGroup int) string {
	b.Helper()

	data := make([]narrowRow, rows)
	for i := range data {
		data[i] = narrowRow{ID: int64(i), Score: float64(i) * 1.5, Active: i%2 == 0}
	}
	return writeGenericParquet(b, "bench-narrow.parquet", data, rowsPerGroup)
}

// writeMapHeavyBenchParquet writes a mapHeavyRow fixture with entriesPerMap keys per row and
// returns its path.
func writeMapHeavyBenchParquet(b *testing.B, rows, rowsPerGroup, entriesPerMap int) string {
	b.Helper()

	data := make([]mapHeavyRow, rows)
	for i := range data {
		attrs := make(map[string]string, entriesPerMap)
		for j := range entriesPerMap {
			attrs[fmt.Sprintf("key%d", j)] = fmt.Sprintf("val-%d-%d", i, j)
		}
		data[i] = mapHeavyRow{ID: int64(i), Attributes: attrs}
	}
	return writeGenericParquet(b, "bench-map-heavy.parquet", data, rowsPerGroup)
}

// writeGenericParquet writes rows to a fresh file under b.TempDir(), split into row groups of
// rowsPerGroup, and returns the file's path.
func writeGenericParquet[T any](b *testing.B, name string, rows []T, rowsPerGroup int) string {
	b.Helper()

	path := filepath.Join(b.TempDir(), name)
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("create: %v", err)
	}
	w := parquetgo.NewGenericWriter[T](f, parquetgo.MaxRowsPerRowGroup(int64(rowsPerGroup)))
	if _, err = w.Write(rows); err != nil {
		b.Fatalf("write: %v", err)
	}
	if err = w.Close(); err != nil {
		b.Fatalf("close writer: %v", err)
	}
	if err = f.Close(); err != nil {
		b.Fatalf("close file: %v", err)
	}
	return path
}

// runFormatBench drains path once per b.N iteration via ProcessFile with the given format and
// worker count, asserting the row count came back right, and reports ns/row.
func runFormatBench(b *testing.B, path string, format streamio.Format, workers, wantRows int) {
	b.Helper()

	ctx := context.Background()
	sink := func(context.Context, []byte) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		result, err := streamio.ProcessFile(
			ctx, path, sink,
			streamio.WithOutputFormat(format),
			streamio.WithParallelWorkers(workers),
		)
		if err != nil {
			b.Fatalf("ProcessFile: %v", err)
		}
		if result.Stats.RowsRead != int64(wantRows) {
			b.Fatalf("Stats.RowsRead = %d, want %d", result.Stats.RowsRead, wantRows)
		}
	}

	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*wantRows), "ns/row")
}

// BenchmarkProcessFile_ParquetFormats quantifies what raw passthrough buys over the JSON-decode
// route — which is the generic record path, the only way Parquet→JSON is produced — on the same
// file, across schema shapes with very different JSON-encoding cost:
// "narrow" (scalar-only, cheapest possible JSON encode), "wide" (strings + a small map, the
// original fixture), and "map-heavy" (a large map, the most expensive reconstruction path in
// parquet/record_decoder.go's mapKeys/mapVals zipping). Compare ns/row and B/op per shape — raw
// passthrough's win should grow with encoding cost, since it skips that cost entirely regardless
// of shape.
func BenchmarkProcessFile_ParquetFormats(b *testing.B) {
	type shape struct {
		name string
		path string
		rows int
	}

	shapes := []shape{
		{name: "narrow", path: writeNarrowBenchParquet(b, benchRows, benchRowsPerGroup), rows: benchRows},
		{name: "wide", path: writeFormatBenchParquet(b, benchRows, benchRowsPerGroup), rows: benchRows},
		{name: "map-heavy", path: writeMapHeavyBenchParquet(b, 5_000, 500, 20), rows: 5_000},
	}

	formats := []streamio.Format{streamio.FormatParquet, streamio.FormatJSON}
	formatLabel := map[streamio.Format]string{
		streamio.FormatParquet: "parquet-raw",
		streamio.FormatJSON:    "json-decode",
	}

	for _, s := range shapes {
		b.Run(s.name, func(b *testing.B) {
			for _, f := range formats {
				b.Run(formatLabel[f], func(b *testing.B) {
					runFormatBench(b, s.path, f, 1, s.rows)
				})
			}
		})
	}
}

// BenchmarkProcessFile_ParquetFormats_RowGroupSize checks whether raw passthrough's per-row cost
// depends on row-group granularity: each dispatched item is a whole row group re-framed as a
// standalone file, so very small row groups mean more, smaller writer invocations, while the
// JSON-decode route's per-row cost shouldn't move at all since it never sees row-group
// boundaries. Coarser row groups should make raw passthrough's ns/row roughly flat or improve
// slightly; the JSON-decode column should stay flat throughout, as a control.
func BenchmarkProcessFile_ParquetFormats_RowGroupSize(b *testing.B) {
	const rows = 20_000

	rowsPerGroup := []int{500, 2_000, 10_000, rows}
	formats := []streamio.Format{streamio.FormatParquet, streamio.FormatJSON}
	formatLabel := map[streamio.Format]string{
		streamio.FormatParquet: "parquet-raw",
		streamio.FormatJSON:    "json-decode",
	}

	for _, rpg := range rowsPerGroup {
		path := writeFormatBenchParquet(b, rows, rpg)
		b.Run(fmt.Sprintf("rowGroupSize=%d", rpg), func(b *testing.B) {
			for _, f := range formats {
				b.Run(formatLabel[f], func(b *testing.B) {
					runFormatBench(b, path, f, 1, rows)
				})
			}
		})
	}
}

// BenchmarkProcessFile_ParquetFormats_Workers measures how each route parallelizes across
// row-group decode workers. Raw passthrough is a verbatim compressed-byte copy
// (I/O/memcpy-bound), while JSON-decode does real per-value CPU work and allocates a document per
// row; the two are expected to scale differently with added workers.
func BenchmarkProcessFile_ParquetFormats_Workers(b *testing.B) {
	const rows = 100_000
	const rowsPerGroup = 6_250 // 16 row groups, enough to keep every worker count below busy.

	path := writeFormatBenchParquet(b, rows, rowsPerGroup)

	workerCounts := []int{1, 2, 4, 8, runtime.NumCPU()}
	seen := map[int]bool{}
	unique := workerCounts[:0]
	for _, n := range workerCounts {
		if !seen[n] {
			seen[n] = true
			unique = append(unique, n)
		}
	}

	formats := []streamio.Format{streamio.FormatParquet, streamio.FormatJSON}
	formatLabel := map[streamio.Format]string{
		streamio.FormatParquet: "parquet-raw",
		streamio.FormatJSON:    "json-decode",
	}

	for _, f := range formats {
		b.Run(formatLabel[f], func(b *testing.B) {
			for _, workers := range unique {
				b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
					runFormatBench(b, path, f, workers, rows)
				})
			}
		})
	}
}

// BenchmarkProcessFile_NDJSON guards against ProcessFile's own routing switch (inputFormat
// detection, then the raw-versus-generic branch in ProcessFile) picking up overhead that
// would affect every format, including NDJSON's. This is deliberately a single case, not a
// doc-size/worker table — jsonio's own test package already has dedicated benchmarks
// (BenchmarkNDJSONProcess, BenchmarkNDJSONProcessParallel) covering that scaling; this one exists
// to catch a regression in the shared dispatch layer above it, which those can't see since they
// drive NewRawSource and pool.RunRaw directly, bypassing ProcessFile's routing entirely.
func BenchmarkProcessFile_NDJSON(b *testing.B) {
	const rows = 20_000

	path := filepath.Join(b.TempDir(), "bench.ndjson")
	var buf strings.Builder
	for i := range rows {
		fmt.Fprintf(&buf, `{"id":%d,"msg":"a reasonably representative log line"}`+"\n", i)
	}
	if err := os.WriteFile(path, []byte(buf.String()), 0o600); err != nil {
		b.Fatalf("write ndjson: %v", err)
	}

	runFormatBench(b, path, streamio.FormatJSON, 1, rows)
}
