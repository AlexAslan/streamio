package streamio_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	parquetgo "github.com/AlexAslan/parquet-go"

	"github.com/AlexAslan/streamio"
)

// ndjsonRow is one source line's values, kept as a named type so the expectation and the read-back
// assertion compare the same shape rather than a bag of parallel slices.
type ndjsonRow struct {
	msg string
	opt string
	f   float64
	i   int64
	// optionalSet says whether the "opt" field carried a string rather than null on this line.
	optionalSet bool
	flag        bool
}

// writeConversionNDJSON writes n lines whose fields are identical in name and order on every line —
// which is what the Parquet batch encoder requires, since a Parquet file has one schema — while
// varying the values, including a column that is null on some lines and a string on others.
func writeConversionNDJSON(tb testing.TB, n int) (string, []ndjsonRow) {
	tb.Helper()

	rows := make([]ndjsonRow, n)
	var buf bytes.Buffer
	for i := range rows {
		row := ndjsonRow{
			i:           int64(i) - 5,
			msg:         fmt.Sprintf(`line %d with "quotes" & <html>`, i),
			flag:        i%2 == 0,
			f:           float64(i) + 0.25,
			optionalSet: i%3 != 0,
		}
		// Left empty when absent, so the expectation is exactly what reading the column back yields.
		if row.optionalSet {
			row.opt = fmt.Sprintf("opt-%d", i)
		}
		rows[i] = row

		optional := "null"
		if row.optionalSet {
			optional = fmt.Sprintf("%q", row.opt)
		}
		fmt.Fprintf(&buf, "{\"i\":%d,\"msg\":%q,\"flag\":%t,\"f\":%v,\"opt\":%s}\n",
			row.i, row.msg, row.flag, row.f, optional)
	}

	path := filepath.Join(tb.TempDir(), "convert.ndjson")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		tb.Fatalf("write ndjson: %v", err)
	}
	return path, rows
}

// readParquetDocument opens one dispatched document as a standalone Parquet file and returns its
// rows keyed by the "i" column, which is unique per source line. Keying by a value rather than by
// position is what lets the same assertion cover the concurrent case, where dispatch order isn't
// the file's line order.
func readParquetDocument(tb testing.TB, doc []byte, into map[int64]ndjsonRow) {
	tb.Helper()

	file, err := parquetgo.OpenFile(bytes.NewReader(doc), int64(len(doc)))
	if err != nil {
		tb.Fatalf("OpenFile: %v", err)
	}

	columns := make(map[string]int, len(file.Schema().Fields()))
	for idx, field := range file.Schema().Fields() {
		columns[field.Name()] = idx
	}
	for _, name := range []string{"i", "msg", "flag", "f", "opt"} {
		if _, ok := columns[name]; !ok {
			tb.Fatalf("document is missing column %q; schema:\n%s", name, file.Schema())
		}
	}

	for _, rowGroup := range file.RowGroups() {
		reader := parquetgo.NewGenericRowGroupReader[any](rowGroup)
		buf := make([]parquetgo.Row, rowGroup.NumRows())
		n, readErr := reader.ReadRows(buf)
		if closeErr := reader.Close(); closeErr != nil {
			tb.Fatalf("close row group reader: %v", closeErr)
		}
		if n == 0 && readErr != nil {
			tb.Fatalf("ReadRows: %v", readErr)
		}

		for _, row := range buf[:n] {
			opt := row[columns["opt"]]
			got := ndjsonRow{
				i:           row[columns["i"]].Int64(),
				msg:         string(row[columns["msg"]].ByteArray()),
				flag:        row[columns["flag"]].Boolean(),
				f:           row[columns["f"]].Double(),
				optionalSet: !opt.IsNull(),
				opt:         string(opt.ByteArray()),
			}
			if _, duplicate := into[got.i]; duplicate {
				tb.Fatalf("row i=%d appeared in more than one document", got.i)
			}
			into[got.i] = got
		}
	}
}

// TestProcessFile_NDJSONToParquet is the end-to-end proof that the generic record path is wired
// through the production entry point: ProcessFile against an NDJSON file asking for Parquet output
// must dispatch real, independently-openable Parquet documents whose rows are the source lines.
//
// It goes through ProcessFile rather than calling pool.RunRecords directly precisely because the
// gap this closes was a registry one — the pieces existed and worked, but no caller could reach
// them.
func TestProcessFile_NDJSONToParquet(t *testing.T) {
	type args struct {
		name    string
		lines   int
		batch   int
		workers int
	}

	tests := []args{
		{name: "one document", lines: 20, batch: 512, workers: 1},
		{name: "several documents", lines: 20, batch: 6, workers: 1},
		{name: "batch of one", lines: 5, batch: 1, workers: 1},
		{name: "concurrent", lines: 200, batch: 7, workers: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, want := writeConversionNDJSON(t, tt.lines)

			sink, collected := collectingSink()
			result, err := streamio.ProcessFile(context.Background(), path, sink,
				streamio.WithOutputFormat(streamio.FormatParquet),
				streamio.WithBatchSize(tt.batch),
				streamio.WithParallelWorkers(tt.workers),
			)
			if err != nil {
				t.Fatalf("ProcessFile: %v", err)
			}

			// Count is rows, not dispatched documents — the same contract raw passthrough honours,
			// where one dispatched item is a whole row group.
			if result.Stats.RowsRead != int64(tt.lines) {
				t.Errorf("Stats.RowsRead = %d, want %d rows", result.Stats.RowsRead, tt.lines)
			}

			docs := collected()
			if len(docs) == 0 {
				t.Fatal("ProcessFile dispatched no documents")
			}
			wantDocs := (tt.lines + tt.batch - 1) / tt.batch
			if tt.workers == 1 && len(docs) != wantDocs {
				t.Errorf("dispatched %d documents, want %d", len(docs), wantDocs)
			}

			got := make(map[int64]ndjsonRow, tt.lines)
			for _, doc := range docs {
				readParquetDocument(t, doc, got)
			}
			assertRowsMatch(t, got, want)
		})
	}
}

// assertRowsMatch checks every source line came back exactly once, with every value intact.
func assertRowsMatch(tb testing.TB, got map[int64]ndjsonRow, want []ndjsonRow) {
	tb.Helper()

	if len(got) != len(want) {
		tb.Fatalf("read back %d rows, want %d", len(got), len(want))
	}
	for _, wantRow := range want {
		gotRow, ok := got[wantRow.i]
		if !ok {
			tb.Errorf("row i=%d is missing from the output", wantRow.i)
			continue
		}
		if gotRow != wantRow {
			tb.Errorf("row i=%d = %+v, want %+v", wantRow.i, gotRow, wantRow)
		}
	}
}

// TestProcessFile_NDJSONToParquetRejectsRaggedLines checks the conversion fails loudly when the
// source lines don't share one shape. A Parquet file has exactly one schema, so there is no honest
// way to write a line with different fields than the one that fixed it — and silently dropping or
// coercing the difference would only surface in whatever a datastore ends up storing.
func TestProcessFile_NDJSONToParquetRejectsRaggedLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ragged.ndjson")
	content := "{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"c\":\"y\"}\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write ndjson: %v", err)
	}

	sink, _ := collectingSink()
	_, err := streamio.ProcessFile(context.Background(), path, sink,
		streamio.WithOutputFormat(streamio.FormatParquet),
		streamio.WithParallelWorkers(1),
	)
	if err == nil {
		t.Fatal("ProcessFile accepted lines with different field sets")
	}
	if !strings.Contains(err.Error(), "does not match the derived schema") {
		t.Errorf("ProcessFile error = %v, want a schema mismatch", err)
	}
}
