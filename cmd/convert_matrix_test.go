package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
)

// matrixRow is the fixture row shape for TestConvertMatrix_AllFormatPairs. Both fields are strings,
// deliberately not writeNDJSONFixture's int id: CSV/TSV has no type system of its own (every
// decoded field is record.KindBytes, see README's csvio notes), so a numeric field round-tripped
// through a CSV/TSV leg comes back as a JSON string, not a JSON number, which would make every
// CSV/TSV pair fail on a type mismatch that has nothing to do with row content actually surviving.
// Keeping both fields string-typed here tests exactly what every format is meant to preserve:
// field names, field order, and field values, without comparing across a legitimate type-fidelity
// difference this repo already documents and accepts.
type matrixRow struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// writeMatrixFixture writes n JSON lines with string id/name fields and returns its path.
func writeMatrixFixture(tb testing.TB, n int) string {
	tb.Helper()

	dir := tb.TempDir()
	path := filepath.Join(dir, "in.json")

	var buf bytes.Buffer
	for i := range n {
		doc, err := json.Marshal(matrixRow{ID: strconv.Itoa(i), Name: fmt.Sprintf("row-%d", i)})
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

// readNDJSONRows reads path as NDJSON and returns every row, failing the test on any malformed
// line — the counterpart to writeNDJSONFixture, used to verify content survived a round trip, not
// just row count.
func readNDJSONRows(tb testing.TB, path string) []matrixRow {
	tb.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("reading %s: %v", path, err)
	}

	var rows []matrixRow
	dec := json.NewDecoder(bytes.NewReader(data))
	for dec.More() {
		var row matrixRow
		if decErr := dec.Decode(&row); decErr != nil {
			tb.Fatalf("decoding row from %s: %v", path, decErr)
		}
		rows = append(rows, row)
	}
	return rows
}

// matrixFormat is one format the matrix exercises.
type matrixFormat struct {
	name         string
	ext          string
	csvHasHeader bool
}

// matrixFormats lists every format the matrix exercises, in a fixed order so subtest names are
// stable across runs.
func matrixFormats() []matrixFormat {
	return []matrixFormat{
		{name: "json", ext: ".json"},
		{name: "parquet", ext: ".parquet"},
		{name: "csv", ext: ".csv", csvHasHeader: true},
		{name: "tsv", ext: ".tsv", csvHasHeader: true},
		{name: "arrow", ext: ".arrow"},
	}
}

// sortedByID returns rows sorted by ID, since CSV/TSV/Arrow output under single-worker forcing
// preserves input order but the comparison should not be sensitive to that implementation detail
// changing for a format that doesn't guarantee it.
func sortedByID(rows []matrixRow) []matrixRow {
	out := make([]matrixRow, len(rows))
	copy(out, rows)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// TestConvertMatrix_AllFormatPairs converts an NDJSON fixture to every supported format (the
// "spoke" leg), then converts each of those spoke files to every other supported format (the "hub"
// leg), and finally back to NDJSON, checking every one of the resulting 5*5 = 25 in->out pairs
// (including same-format pairs, which exercise each format's own passthrough/self-conversion path)
// round-trips every row's id and name unchanged.
//
// This closes the coverage gap a prior audit found: every existing round-trip test in this file
// converts through JSON on at least one side, so no pair of two non-JSON formats (e.g.
// csv->parquet, parquet->arrow) had ever been directly exercised, even though they're expected to
// work identically via the generic record/formatio decode/re-encode path.
func TestConvertMatrix_AllFormatPairs(t *testing.T) {
	const rows = 30

	jsonIn := writeMatrixFixture(t, rows)
	want := sortedByID(readNDJSONRows(t, jsonIn))

	formats := matrixFormats()

	// spokes[name] is jsonIn converted to that format once, reused as the input for every "in ->
	// out" pair below rather than re-converting from JSON per pair.
	spokes := make(map[string]string, len(formats))
	for _, f := range formats {
		path := filepath.Join(t.TempDir(), "spoke"+f.ext)
		flags := convertFlags{in: jsonIn, out: path, csvHasHeader: f.csvHasHeader}
		if err := runConvert(context.Background(), flags, f.name); err != nil {
			t.Fatalf("preparing %s spoke: %v", f.name, err)
		}
		spokes[f.name] = path
	}

	for _, in := range formats {
		for _, out := range formats {
			t.Run(in.name+"->"+out.name, func(t *testing.T) {
				checkMatrixPair(t, spokes[in.name], in, out, want)
			})
		}
	}
}

// checkMatrixPair converts spoke (already in in's format) to out's format and back to NDJSON,
// checking the result matches want row-for-row after sorting by ID.
func checkMatrixPair(t *testing.T, spoke string, in, out matrixFormat, want []matrixRow) {
	t.Helper()

	// csvHasHeader is a single flag applying to whichever side (in or out) is CSV/TSV — every
	// CSV/TSV fixture here is written with a header, so it must be set whenever either leg touches
	// a CSV-family format, or the header line gets misread as a data row (or, on write, silently
	// omitted).
	mid := filepath.Join(t.TempDir(), "mid"+out.ext)
	midFlags := convertFlags{
		in: spoke, out: mid, inFormat: in.name, csvHasHeader: in.csvHasHeader || out.csvHasHeader,
	}
	if err := runConvert(context.Background(), midFlags, out.name); err != nil {
		t.Fatalf("runConvert (%s -> %s): %v", in.name, out.name, err)
	}

	back := filepath.Join(t.TempDir(), "back.json")
	backFlags := convertFlags{in: mid, out: back, inFormat: out.name, csvHasHeader: out.csvHasHeader}
	if err := runConvert(context.Background(), backFlags, "json"); err != nil {
		t.Fatalf("runConvert (%s -> json): %v", out.name, err)
	}

	got := sortedByID(readNDJSONRows(t, back))
	if len(got) != len(want) {
		t.Fatalf("%s -> %s: got %d rows, want %d", in.name, out.name, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s -> %s: row %d = %+v, want %+v", in.name, out.name, i, got[i], want[i])
		}
	}
}
