package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// runConvertBenchCase is one BenchmarkRunConvert/BenchmarkRunConvert_LargeScale case: an input
// file already on disk, the --out-format to convert it to, and any extra flags that format needs.
type runConvertBenchCase struct {
	label        string
	in           string
	inFormat     string
	outFormat    string
	outExt       string
	batchSize    int
	csvHasHeader bool
}

// runConvertBench runs one case's conversion b.N times, each to its own fresh --out path, and
// reports ns/row for totalRows.
func runConvertBench(b *testing.B, tc runConvertBenchCase, dir string, totalRows int) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		out := filepath.Join(dir, fmt.Sprintf("out-%s-%d%s", tc.label, i, tc.outExt))
		flags := convertFlags{
			in: tc.in, out: out, inFormat: tc.inFormat, batchSize: tc.batchSize,
			csvHasHeader: tc.csvHasHeader,
		}
		if err := runConvert(context.Background(), flags, tc.outFormat); err != nil {
			b.Fatalf("runConvert: %v", err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*totalRows), "ns/row")
}

// BenchmarkRunConvert measures the CLI's actual entry point end-to-end — flag-to-option wiring in
// runConvert plus real file I/O through fileWriter/newlineWriter — for every (input format, output
// format) pair the CLI supports, since nothing before this benchmarked anything beyond the
// library's ProcessFile in isolation. Each fixture is written once outside the timed loop; every
// iteration writes a fresh --out file since fileWriter truncates on create.
//
// parquet-to-parquet is the raw-passthrough-turned-generic-path case fixed alongside this
// benchmark (see streamio.go's WithSingleFileOutput routing rule and
// TestRunConvert_ParquetToParquetProducesOneValidFile): before that fix it silently produced a
// corrupted output file for any multi-row-group input, so this benchmark existing at all — and
// passing — is itself a regression guard, not just a throughput number.
func BenchmarkRunConvert(b *testing.B) {
	const rows = 20_000
	const rowsPerGroup = 2_000

	jsonIn := writeNDJSONFixture(b, rows)
	parquetIn := writeParquetFixture(b, rows, rowsPerGroup)
	dir := b.TempDir()
	csvIn := filepath.Join(dir, "in.csv")
	csvFlags := convertFlags{in: jsonIn, out: csvIn, csvHasHeader: true}
	if err := runConvert(context.Background(), csvFlags, "csv"); err != nil {
		b.Fatalf("preparing csv fixture: %v", err)
	}
	arrowIn := filepath.Join(dir, "in.arrow")
	arrowFlags := convertFlags{in: jsonIn, out: arrowIn}
	if err := runConvert(context.Background(), arrowFlags, "arrow"); err != nil {
		b.Fatalf("preparing arrow fixture: %v", err)
	}

	cases := []runConvertBenchCase{
		{label: "json-to-json", in: jsonIn, outFormat: "json", outExt: ".json"},
		{label: "json-to-parquet", in: jsonIn, outFormat: "parquet", outExt: ".parquet"},
		{label: "parquet-to-parquet", in: parquetIn, inFormat: "parquet", outFormat: "parquet", outExt: ".parquet"},
		{label: "parquet-to-json", in: parquetIn, inFormat: "parquet", outFormat: "json", outExt: ".json"},
		{label: "json-to-csv", in: jsonIn, outFormat: "csv", outExt: ".csv", csvHasHeader: true},
		{label: "csv-to-json", in: csvIn, inFormat: "csv", outFormat: "json", outExt: ".json", csvHasHeader: true},
		{label: "json-to-tsv", in: jsonIn, outFormat: "tsv", outExt: ".tsv", csvHasHeader: true},
		{label: "json-to-arrow", in: jsonIn, outFormat: "arrow", outExt: ".arrow"},
		{label: "arrow-to-json", in: arrowIn, inFormat: "arrow", outFormat: "json", outExt: ".json"},
	}

	for _, tc := range cases {
		b.Run(tc.label, func(b *testing.B) {
			runConvertBench(b, tc, dir, rows)
		})
	}
}

// BenchmarkRunConvert_LargeScale runs the same end-to-end conversions as BenchmarkRunConvert at
// 100,000,000 rows — the "does this actually work at the scale a 1GB+ single-file conversion
// needs" size — kept as its own benchmark rather than a case in BenchmarkRunConvert so routine use
// of that one (e.g. from an editor's "run benchmark" action) doesn't unexpectedly take upward of a
// minute. Run explicitly with -bench=BenchmarkRunConvert_LargeScale.
//
// Every Parquet-output case passes an explicit --batch-size: the library's default (512) would
// produce roughly 195,000 row groups for 100,000,000 rows, five times over parquet-go's
// 32,767-row-group cap (see streaming_encoder.go and
// TestRunConvert_TooSmallBatchSizeGetsAnActionableError) — this benchmark measures realistic
// large-scale throughput, not that failure mode, so it uses a batch size that keeps row-group
// count sane for this input size instead. The Parquet *input* fixture itself is written with a
// matching row-group size for the same reason.
func BenchmarkRunConvert_LargeScale(b *testing.B) {
	const rows = 100_000_000
	const parquetBatchSize = 10_000 // 10,000 row groups for 100,000,000 rows; well under the cap.

	jsonIn := writeNDJSONFixture(b, rows)
	parquetIn := writeParquetFixture(b, rows, parquetBatchSize)
	dir := b.TempDir()
	csvIn := filepath.Join(dir, "in.csv")
	csvFlags := convertFlags{in: jsonIn, out: csvIn, csvHasHeader: true}
	if err := runConvert(context.Background(), csvFlags, "csv"); err != nil {
		b.Fatalf("preparing csv fixture: %v", err)
	}
	arrowIn := filepath.Join(dir, "in.arrow")
	arrowFixtureFlags := convertFlags{in: jsonIn, out: arrowIn, batchSize: parquetBatchSize}
	if err := runConvert(context.Background(), arrowFixtureFlags, "arrow"); err != nil {
		b.Fatalf("preparing arrow fixture: %v", err)
	}

	cases := []runConvertBenchCase{
		{label: "json-to-json", in: jsonIn, outFormat: "json", outExt: ".json"},
		{
			label: "json-to-parquet", in: jsonIn, outFormat: "parquet", outExt: ".parquet",
			batchSize: parquetBatchSize,
		},
		{
			label: "parquet-to-parquet", in: parquetIn, inFormat: "parquet", outFormat: "parquet",
			outExt: ".parquet", batchSize: parquetBatchSize,
		},
		{label: "parquet-to-json", in: parquetIn, inFormat: "parquet", outFormat: "json", outExt: ".json"},
		{label: "json-to-csv", in: jsonIn, outFormat: "csv", outExt: ".csv", csvHasHeader: true},
		{label: "csv-to-json", in: csvIn, inFormat: "csv", outFormat: "json", outExt: ".json", csvHasHeader: true},
		{label: "json-to-tsv", in: jsonIn, outFormat: "tsv", outExt: ".tsv", csvHasHeader: true},
		{
			label: "json-to-arrow", in: jsonIn, outFormat: "arrow", outExt: ".arrow",
			batchSize: parquetBatchSize,
		},
		{label: "arrow-to-json", in: arrowIn, inFormat: "arrow", outFormat: "json", outExt: ".json"},
	}

	for _, tc := range cases {
		b.Run(tc.label, func(b *testing.B) {
			runConvertBench(b, tc, dir, rows)
		})
	}
}
