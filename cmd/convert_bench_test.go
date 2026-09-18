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

// benchFormat is one format benchAllPairCases's matrix covers.
type benchFormat struct {
	name         string
	ext          string
	csvHasHeader bool
}

// benchFormats lists every format the matrix covers, in a fixed order so benchmark names are
// stable across runs.
func benchFormats() []benchFormat {
	return []benchFormat{
		{name: "json", ext: ".json"},
		{name: "parquet", ext: ".parquet"},
		{name: "csv", ext: ".csv", csvHasHeader: true},
		{name: "tsv", ext: ".tsv", csvHasHeader: true},
		{name: "arrow", ext: ".arrow"},
	}
}

// buildBenchFixtures converts jsonIn to every format in benchFormats once, in dir, using batchSize
// for every Parquet/Arrow fixture (0 leaves it at the library default). It returns each format's
// fixture path keyed by name, reused as the input for every in-> pair below rather than
// reconverting from JSON per case.
func buildBenchFixtures(b *testing.B, jsonIn, dir string, batchSize int) map[string]string {
	b.Helper()

	paths := make(map[string]string, len(benchFormats()))
	for _, f := range benchFormats() {
		if f.name == "json" {
			paths["json"] = jsonIn
			continue
		}
		path := filepath.Join(dir, "fixture"+f.ext)
		flags := convertFlags{in: jsonIn, out: path, csvHasHeader: f.csvHasHeader, batchSize: batchSize}
		if err := runConvert(context.Background(), flags, f.name); err != nil {
			b.Fatalf("preparing %s fixture: %v", f.name, err)
		}
		paths[f.name] = path
	}
	return paths
}

// benchAllPairCases builds one runConvertBenchCase per in->out pair across every format in
// benchFormats (5*5 = 25 cases, including same-format passthrough/self-conversion), given each
// format's fixture path from buildBenchFixtures. batchSize is applied to every case whose output
// is Parquet or Arrow, the two formats whose writer needs one to stay under Parquet's row-group cap
// at large scale (0 leaves it at the library default, used by BenchmarkRunConvert).
func benchAllPairCases(fixtures map[string]string, batchSize int) []runConvertBenchCase {
	formats := benchFormats()
	cases := make([]runConvertBenchCase, 0, len(formats)*len(formats))
	for _, in := range formats {
		for _, out := range formats {
			tc := runConvertBenchCase{
				label:        in.name + "-to-" + out.name,
				in:           fixtures[in.name],
				inFormat:     in.name,
				outFormat:    out.name,
				outExt:       out.ext,
				csvHasHeader: in.csvHasHeader || out.csvHasHeader,
			}
			if out.name == "parquet" || out.name == "arrow" {
				tc.batchSize = batchSize
			}
			cases = append(cases, tc)
		}
	}
	return cases
}

// BenchmarkRunConvert measures the CLI's actual entry point end-to-end — flag-to-option wiring in
// runConvert plus real file I/O through fileWriter/newlineWriter — for every (input format, output
// format) pair the CLI supports (5*5 = 25, including same-format passthrough/self-conversion),
// since nothing before this benchmarked anything beyond the library's ProcessFile in isolation.
// Each fixture is written once outside the timed loop; every iteration writes a fresh --out file
// since fileWriter truncates on create.
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
	dir := b.TempDir()

	// The Parquet fixture needs its own row-group size, distinct from the batch size every other
	// fixture and every benchmark case uses, so it's built directly rather than through
	// buildBenchFixtures's single batchSize parameter.
	fixtures := buildBenchFixtures(b, jsonIn, dir, 0)
	fixtures["parquet"] = writeParquetFixture(b, rows, rowsPerGroup)

	for _, tc := range benchAllPairCases(fixtures, 0) {
		b.Run(tc.label, func(b *testing.B) {
			runConvertBench(b, tc, dir, rows)
		})
	}
}

// BenchmarkRunConvert_LargeScale runs the same end-to-end conversions as BenchmarkRunConvert at
// 10,000,000 rows — the "does this actually work at a large-input scale" size — kept as its own
// benchmark rather than a case in BenchmarkRunConvert so routine use of that one (e.g. from an
// editor's "run benchmark" action) doesn't unexpectedly take upward of a minute. Run explicitly
// with -bench=BenchmarkRunConvert_LargeScale.
//
// Every Parquet- or Arrow-output case passes an explicit --batch-size: the library's default (512)
// would produce roughly 19,531 row groups/record batches for 10,000,000 rows, uncomfortably close
// to parquet-go's 32,767-row-group cap (see streaming_encoder.go and
// TestRunConvert_TooSmallBatchSizeGetsAnActionableError) — this benchmark measures realistic
// large-scale throughput, not that failure mode, so it uses a batch size that keeps row-group/
// record-batch count well under the cap instead. The Parquet and Arrow *input* fixtures themselves
// are written with a matching batch size for the same reason.
func BenchmarkRunConvert_LargeScale(b *testing.B) {
	const rows = 10_000_000
	const parquetBatchSize = 1_000 // 10,000 row groups/record batches for 10,000,000 rows.

	jsonIn := writeNDJSONFixture(b, rows)
	dir := b.TempDir()

	fixtures := buildBenchFixtures(b, jsonIn, dir, parquetBatchSize)
	fixtures["parquet"] = writeParquetFixture(b, rows, parquetBatchSize)

	for _, tc := range benchAllPairCases(fixtures, parquetBatchSize) {
		b.Run(tc.label, func(b *testing.B) {
			runConvertBench(b, tc, dir, rows)
		})
	}
}
