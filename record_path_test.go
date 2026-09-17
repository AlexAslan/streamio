package streamio_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"streamio"
	"streamio/internal/formatio"
	"streamio/internal/jsonio"
	"streamio/internal/options"
	"streamio/internal/pool"
	"strings"
	"sync"
	"testing"
	"time"

	parquetgo "github.com/parquet-go/parquet-go"
)

// identityRow deliberately carries one column of every shape whose JSON rendering could go wrong on
// its own: a map, each timestamp precision, a date, both signednesses, both float widths, a nullable
// column, and the two field names the decoder special-cases.
type identityRow struct {
	Attributes    map[string]string `parquet:"attributes"`
	Tags          map[string]string `parquet:"tags"`
	Optional      *string           `parquet:"optional,optional"`
	Message       string            `parquet:"message"`
	Timestamp     int64             `parquet:"Timestamp"`
	TimestampTime int64             `parquet:"TimestampTime"`
	TSMillis      int64             `parquet:"ts_millis,timestamp"`
	TSMicros      int64             `parquet:"ts_micros,timestamp(microsecond)"`
	TSNanos       int64             `parquet:"ts_nanos,timestamp(nanosecond)"`
	U64           uint64            `parquet:"u64,uint(64)"`
	I64           int64             `parquet:"i64"`
	F64           float64           `parquet:"f64"`
	Day           int32             `parquet:"day,date"`
	U32           uint32            `parquet:"u32,uint(32)"`
	I32           int32             `parquet:"i32"`
	F32           float32           `parquet:"f32"`
	Flag          bool              `parquet:"flag"`
}

// writeIdentityParquet writes n identityRows, split into row groups of rgSize, and returns the
// path. Every row varies the values that formatting decisions hinge on — tiny floats, huge
// unsigned integers, empty maps — rather than repeating one representative row.
func writeIdentityParquet(tb testing.TB, n, rgSize int) string {
	tb.Helper()

	rows := make([]identityRow, n)
	for i := range rows {
		optional := fmt.Sprintf("opt-%d", i)
		row := identityRow{
			Message:       fmt.Sprintf(`a message with "quotes" & <html> #%d`, i),
			Timestamp:     1704067200000 + int64(i),
			TimestampTime: 999,
			Day:           19723 + int32(i),
			TSMillis:      1704067200123 + int64(i),
			TSMicros:      1704067200123456 + int64(i),
			TSNanos:       1704067200123456789 + int64(i),
			U32:           4294967295 - uint32(i),
			U64:           18446744073709551615 - uint64(i),
			I32:           -7 - int32(i),
			I64:           -9007199254740993 - int64(i),
			F32:           float32(i) * 0.1,
			F64:           float64(i) * 1.0000000000001,
			Flag:          i%2 == 0,
			Attributes:    map[string]string{"service": "checkout", "region": "eu-west-1"},
		}
		// Every third row leaves both maps empty and the optional column absent, so the tests cover
		// field omission as well as field rendering.
		if i%3 != 0 {
			row.Tags = map[string]string{"env": "prod"}
			row.Optional = &optional
		}
		// A float that trips encoding/json's fixed-to-scientific switch in both directions.
		switch i % 4 {
		case 1:
			row.F64 = 1e-9
		case 2:
			row.F64 = 1e22
		case 3:
			row.F64 = 0
		}
		rows[i] = row
	}

	path := filepath.Join(tb.TempDir(), "identity.parquet")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create: %v", err)
	}
	w := parquetgo.NewGenericWriter[identityRow](f, parquetgo.MaxRowsPerRowGroup(int64(rgSize)))
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

// orderedSink collects documents in dispatch order. Used with a single worker, that is the file's
// own row order.
func orderedSink() (streamio.DocumentHandler, func() []string) {
	var (
		mu   sync.Mutex
		docs []string
	)
	sink := func(_ context.Context, doc []byte) error {
		mu.Lock()
		defer mu.Unlock()
		docs = append(docs, string(doc))
		return nil
	}
	return sink, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return docs
	}
}

// runGenericRecordPath drives the generic decode→encode route explicitly, for a pair ProcessFile
// itself routes elsewhere: NDJSON→JSON is a native-format match and so always takes raw
// passthrough, leaving jsonio.NewDecoder with no way in through ProcessFile. Calling
// the pieces the registry would have wired together is how that pair gets exercised.
func runGenericRecordPath(
	tb testing.TB,
	newDecoder func(options.Config, string) (formatio.RecordDecoder, error),
	path string,
	sink streamio.DocumentHandler,
	opts ...streamio.Option,
) options.Result {
	tb.Helper()

	cfg := options.New(opts...)
	dec, err := newDecoder(cfg, path)
	if err != nil {
		tb.Fatalf("NewDecoder: %v", err)
	}
	defer func() {
		if cerr := dec.Close(); cerr != nil {
			tb.Errorf("Close: %v", cerr)
		}
	}()

	newEncoder := func() (formatio.RecordEncoder, error) { return jsonio.NewEncoder(cfg) }

	result, err := pool.RunRecords(context.Background(), cfg, dec, newEncoder, nil, options.DocumentHandler(sink))
	if err != nil {
		tb.Fatalf("RunRecords: %v", err)
	}
	return result
}

// Layouts the JSON encoder renders a date and each timestamp precision with. Spelled out here
// rather than imported from jsonio so the test pins what the bytes look like, instead of agreeing
// with the code under test by construction.
const (
	testDateLayout   = "2006-01-02"
	testMillisLayout = "2006-01-02T15:04:05.000Z"
	testMicrosLayout = "2006-01-02T15:04:05.000000Z"
	testNanosLayout  = "2006-01-02T15:04:05.000000000Z"
)

// TestProcessFile_ParquetToJSON is the end-to-end bar for Parquet→JSON through the production entry
// point: every value shape a Parquet file can carry has to come back out as the JSON its type says
// it is. It covers the pair across row-group and batch sizings, since decoding suspends and resumes
// mid-row-group whenever a batch fills.
func TestProcessFile_ParquetToJSON(t *testing.T) {
	type args struct {
		name     string
		rows     int
		rowGroup int
		batch    int
	}

	tests := []args{
		{name: "single row group", rows: 12, rowGroup: 12, batch: 512},
		{name: "many row groups", rows: 12, rowGroup: 4, batch: 512},
		{name: "batch smaller than row group", rows: 12, rowGroup: 12, batch: 5},
		{name: "batch larger than row group", rows: 12, rowGroup: 3, batch: 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeIdentityParquet(t, tt.rows, tt.rowGroup)

			// One worker, so dispatch order is the file's own row order and document i is row i.
			sink, collected := orderedSink()
			result, err := streamio.ProcessFile(context.Background(), path, sink,
				streamio.WithParallelWorkers(1), streamio.WithBatchSize(tt.batch))
			if err != nil {
				t.Fatalf("ProcessFile: %v", err)
			}
			if result.Stats.RowsRead != int64(tt.rows) {
				t.Errorf("Stats.RowsRead = %d, want %d", result.Stats.RowsRead, tt.rows)
			}

			docs := collected()
			if len(docs) != tt.rows {
				t.Fatalf("dispatched %d documents, want one per row (%d)", len(docs), tt.rows)
			}
			for i, doc := range docs {
				assertIdentityRowJSON(t, i, doc)
			}
		})
	}
}

func TestProcessFile_ParquetToJSON_AppliesConfiguredPathTransforms(t *testing.T) {
	path := writeIdentityParquet(t, 1, 1)
	sink, collected := orderedSink()

	result, err := streamio.ProcessFile(context.Background(), path, sink,
		streamio.WithParallelWorkers(1),
		streamio.WithTransforms(
			streamio.RenamePath("Timestamp", "@timestamp"),
			streamio.DropPath("TimestampTime"),
		),
	)
	if err != nil {
		t.Fatalf("ProcessFile: %v", err)
	}
	if result.Stats.RowsRead != 1 || result.Stats.DocumentsDispatched != 1 {
		t.Fatalf("Stats = %+v, want 1 row read and 1 document dispatched", result.Stats)
	}

	docs := collected()
	if len(docs) != 1 {
		t.Fatalf("dispatched %d documents, want 1", len(docs))
	}
	got := decodeDocument(t, docs[0])
	if _, ok := got["@timestamp"]; !ok {
		t.Fatalf("transformed document is missing @timestamp: %s", docs[0])
	}
	for _, field := range []string{"Timestamp", "TimestampTime"} {
		if _, ok := got[field]; ok {
			t.Fatalf("field %q should not appear after transform: %s", field, docs[0])
		}
	}
}

// TestProcessFile_ParquetToJSONConcurrently checks fanning the same conversion out across workers
// yields the same documents, just in no particular order.
func TestProcessFile_ParquetToJSONConcurrently(t *testing.T) {
	const (
		rows     = 120
		rowGroup = 10
		workers  = 4
	)
	path := writeIdentityParquet(t, rows, rowGroup)

	sink, collected := orderedSink()
	result, err := streamio.ProcessFile(context.Background(), path, sink,
		streamio.WithParallelWorkers(workers), streamio.WithBatchSize(7))
	if err != nil {
		t.Fatalf("ProcessFile: %v", err)
	}
	if result.Stats.RowsRead != rows {
		t.Errorf("Stats.RowsRead = %d, want %d", result.Stats.RowsRead, rows)
	}

	docs := collected()
	if len(docs) != rows {
		t.Fatalf("dispatched %d documents, want %d", len(docs), rows)
	}

	// Every row has a distinct "i64", so matching each document back to the row it came from is
	// what makes the assertion order-independent.
	seen := make(map[int]bool, rows)
	for _, doc := range docs {
		i := identityRowIndex(t, doc)
		if seen[i] {
			t.Fatalf("row %d was dispatched more than once", i)
		}
		seen[i] = true
		assertIdentityRowJSON(t, i, doc)
	}
	for i := range rows {
		if !seen[i] {
			t.Errorf("row %d was never dispatched", i)
		}
	}
}

// decodeDocument parses one dispatched document, keeping numbers as json.Number so a uint64 too
// large for a float64 still compares exactly.
func decodeDocument(tb testing.TB, doc string) map[string]any {
	tb.Helper()

	dec := json.NewDecoder(strings.NewReader(doc))
	dec.UseNumber()

	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		tb.Fatalf("decoding %s: %v", doc, err)
	}
	return m
}

// numberField returns field name's value as the raw JSON number text it was written as, which is
// what lets a uint64 beyond float64's exact range be compared at all.
func numberField(tb testing.TB, doc map[string]any, name string) string {
	tb.Helper()

	number, ok := doc[name].(json.Number)
	if !ok {
		tb.Fatalf("field %q = %#v, want a JSON number", name, doc[name])
	}
	return string(number)
}

// identityRowIndex recovers which writeIdentityParquet row a document came from, via the one field
// that is unique per row and cheap to invert.
func identityRowIndex(tb testing.TB, doc string) int {
	tb.Helper()

	i64, err := strconv.ParseInt(numberField(tb, decodeDocument(tb, doc), "i64"), 10, 64)
	if err != nil {
		tb.Fatalf("parsing i64 out of %s: %v", doc, err)
	}
	return int(-9007199254740993 - i64)
}

// assertIdentityRowJSON checks one document holds exactly what writeIdentityParquet's row i carried,
// rendered as the JSON its Parquet type implies.
func assertIdentityRowJSON(tb testing.TB, i int, doc string) {
	tb.Helper()

	got := decodeDocument(tb, doc)

	// A Parquet column annotated as a DATE or a TIMESTAMP renders as a string at exactly the
	// precision its unit carries, not as the raw integer and not widened to nanoseconds.
	want := map[string]any{
		"Timestamp":     json.Number(strconv.FormatInt(1704067200000+int64(i), 10)),
		"TimestampTime": json.Number("999"),
		"message":       fmt.Sprintf(`a message with "quotes" & <html> #%d`, i),
		"day":           time.Unix(int64(19723+i)*86400, 0).UTC().Format(testDateLayout),
		"ts_millis":     time.UnixMilli(1704067200123 + int64(i)).UTC().Format(testMillisLayout),
		"ts_micros":     time.UnixMicro(1704067200123456 + int64(i)).UTC().Format(testMicrosLayout),
		"ts_nanos":      time.Unix(1704067200, 123456789+int64(i)).UTC().Format(testNanosLayout),
		// An unsigned column keeps its full range rather than wrapping into a negative signed value.
		"u32":  json.Number(strconv.FormatUint(uint64(4294967295-uint32(i)), 10)),
		"u64":  json.Number(strconv.FormatUint(18446744073709551615-uint64(i), 10)),
		"i32":  json.Number(strconv.FormatInt(int64(-7-int32(i)), 10)),
		"i64":  json.Number(strconv.FormatInt(-9007199254740993-int64(i), 10)),
		"f32":  json.Number(strconv.FormatFloat(float64(float32(i)*0.1), 'f', -1, 32)),
		"flag": i%2 == 0,
		"attributes": map[string]any{
			"service": "checkout",
			"region":  "eu-west-1",
		},
	}

	// The rows that carry them; the rest leave the map empty and the optional column null.
	if i%3 != 0 {
		want["tags"] = map[string]any{"env": "prod"}
		want["optional"] = fmt.Sprintf("opt-%d", i)
	} else {
		want["optional"] = nil
	}

	for name, wantValue := range want {
		if gotValue, ok := got[name]; !ok {
			tb.Errorf("row %d: field %q is missing from %s", i, name, doc)
		} else if !reflect.DeepEqual(gotValue, wantValue) {
			tb.Errorf("row %d: field %q = %#v, want %#v", i, name, gotValue, wantValue)
		}
	}

	if _, ok := got["@timestamp"]; ok {
		tb.Errorf("row %d: field %q should not appear without a configured transform in %s",
			i, "@timestamp", doc)
	}
	if _, ok := got["tags"]; ok && i%3 == 0 {
		tb.Errorf("row %d: an empty map column should be omitted, got %s", i, doc)
	}

	// A float64 is rendered as encoding/json would render it, which is not strconv's 'g' format;
	// json.Number keeps the bytes, so this compares what was actually written.
	wantF64 := []string{"0", "1e-9", "1e+22", "0"}[i%4]
	if i%4 == 0 {
		wantF64 = strconv.FormatFloat(float64(i)*1.0000000000001, 'f', -1, 64)
	}
	if gotF64 := numberField(tb, got, "f64"); gotF64 != wantF64 {
		tb.Errorf("row %d: f64 = %s, want %s", i, gotF64, wantF64)
	}
}

// TestGenericPath_NDJSONRoundTrip checks NDJSON's decoder and the JSON encoder compose back into
// the lines they came from, for lines already written in the encoder's own canonical form.
func TestGenericPath_NDJSONRoundTrip(t *testing.T) {
	lines := []string{
		`{"i":0,"msg":"hello","ok":true,"missing":null}`,
		`{"i":1,"msg":"quotes \" and backslash \\","ok":false,"f":1.5}`,
		`{"z":-9007199254740993,"a":"out of order on purpose"}`,
		`{}`,
	}

	path := filepath.Join(t.TempDir(), "roundtrip.ndjson")
	var buf []byte
	for _, l := range lines {
		buf = append(buf, l...)
		buf = append(buf, '\n')
	}
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("write ndjson: %v", err)
	}

	sink, collected := orderedSink()
	result := runGenericRecordPath(t, jsonio.NewDecoder, path, sink, streamio.WithParallelWorkers(1))

	if result.Stats.RowsRead != int64(len(lines)) {
		t.Errorf("Stats.RowsRead = %d, want %d", result.Stats.RowsRead, len(lines))
	}
	got := collected()
	if len(got) != len(lines) {
		t.Fatalf("round trip produced %d documents, want %d", len(got), len(lines))
	}
	for i := range lines {
		if got[i] != lines[i] {
			t.Errorf("line %d round-tripped to %s, want %s", i, got[i], lines[i])
		}
	}
}
