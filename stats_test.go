package streamio_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio"
)

// statsNDJSON has an empty string and a null, so both flavors of "empty" have to be handled the
// same way in every format. (A missing attribute can't appear here: the CSV, Parquet and Arrow
// encoders used to build the other fixtures reject schema drift.)
const statsNDJSON = `{"id":1,"name":"ann","country":"US"}
{"id":2,"name":"bob","country":"US"}
{"id":3,"name":"","country":"GR"}
{"id":4,"name":"dan","country":null}
{"id":5,"name":"eve","country":null}
`

// convertFixture writes NDJSON-derived content in format f to a new file and returns its path.
func convertFixture(t *testing.T, f streamio.Format, ext string) string {
	t.Helper()

	in := filepath.Join(t.TempDir(), "in.ndjson")
	require.NoError(t, os.WriteFile(in, []byte(statsNDJSON), 0o600))
	out := filepath.Join(t.TempDir(), "out."+ext)

	var buf bytes.Buffer
	var mu sync.Mutex
	_, err := streamio.ProcessFile(context.Background(), in,
		func(_ context.Context, doc []byte) error {
			mu.Lock()
			defer mu.Unlock()
			buf.Write(doc)
			if f == streamio.FormatJSON {
				buf.WriteByte('\n')
			}
			return nil
		},
		streamio.WithOutputFormat(f),
		streamio.WithSingleFileOutput(true),
		streamio.WithParallelWorkers(1),
		streamio.WithCSVHasHeader(true))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(out, buf.Bytes(), 0o600))
	return out
}

func byName(p streamio.Profile) map[string]streamio.ColumnStats {
	m := make(map[string]streamio.ColumnStats, len(p.Columns))
	for _, c := range p.Columns {
		m[c.Name] = c
	}
	return m
}

func assertStatsFixtureProfile(t *testing.T, p streamio.Profile) {
	t.Helper()

	assert.Equal(t, int64(5), p.Records)
	cols := byName(p)
	require.Len(t, cols, 3)

	assert.Equal(t, int64(5), cols["id"].NonEmpty)
	assert.InDelta(t, 100.0, cols["id"].Pct, 1e-9)
	assert.Equal(t, int64(5), cols["id"].Distinct)

	// name: "" is empty.
	assert.Equal(t, int64(4), cols["name"].NonEmpty)
	assert.InDelta(t, 80.0, cols["name"].Pct, 1e-9)

	// country: null is empty, and US is the top value.
	country := cols["country"]
	assert.Equal(t, int64(3), country.NonEmpty)
	assert.InDelta(t, 60.0, country.Pct, 1e-9)
	assert.Equal(t, int64(2), country.Distinct)
	assert.True(t, country.DistinctExact)
	require.Len(t, country.TopValues, 2)
	assert.Equal(t, streamio.ValueCount{Value: "US", Count: 2, Pct: 40.0}, country.TopValues[0])
	assert.Equal(t, streamio.ValueCount{Value: "GR", Count: 1, Pct: 20.0}, country.TopValues[1])
}

func TestProfileFile_SameAcrossFormats(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		format streamio.Format
		ext    string
	}{
		"ndjson":  {streamio.FormatJSON, "ndjson"},
		"csv":     {streamio.FormatCSV, "csv"},
		"tsv":     {streamio.FormatTSV, "tsv"},
		"parquet": {streamio.FormatParquet, "parquet"},
		"arrow":   {streamio.FormatArrow, "arrow"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := convertFixture(t, tc.format, tc.ext)
			var opts []streamio.Option
			if tc.format == streamio.FormatCSV || tc.format == streamio.FormatTSV {
				opts = append(opts, streamio.WithCSVHasHeader(true))
			}

			p, err := streamio.ProfileFile(context.Background(), path, opts...)
			require.NoError(t, err)
			assertStatsFixtureProfile(t, p)
		})
	}
}

func TestProfileFile_WorkerCountDoesNotChangeResult(t *testing.T) {
	t.Parallel()

	var sb strings.Builder
	for i := range 5000 {
		sb.WriteString(`{"id":`)
		sb.WriteString(strings.Repeat("1", i%7+1))
		sb.WriteString(`,"bucket":"b`)
		sb.WriteString(strings.Repeat("x", i%3))
		sb.WriteString("\"}\n")
	}
	path := filepath.Join(t.TempDir(), "big.ndjson")
	require.NoError(t, os.WriteFile(path, []byte(sb.String()), 0o600))

	one, err := streamio.ProfileFile(context.Background(), path,
		streamio.WithParallelWorkers(1), streamio.WithChunkSize(4096))
	require.NoError(t, err)
	many, err := streamio.ProfileFile(context.Background(), path,
		streamio.WithParallelWorkers(8), streamio.WithChunkSize(4096))
	require.NoError(t, err)

	assert.Equal(t, int64(5000), one.Records)
	assert.Equal(t, one.Records, many.Records)
	assert.Equal(t, byName(one), byName(many))
}

func TestProfileFile_MaxDistinctMarksInexact(t *testing.T) {
	t.Parallel()

	var sb strings.Builder
	for i := range 100 {
		sb.WriteString(`{"k":"v`)
		sb.WriteString(strings.Repeat("a", i))
		sb.WriteString("\"}\n")
	}
	path := filepath.Join(t.TempDir(), "hc.ndjson")
	require.NoError(t, os.WriteFile(path, []byte(sb.String()), 0o600))

	p, err := streamio.ProfileFile(context.Background(), path,
		streamio.WithProfileMaxDistinct(10), streamio.WithParallelWorkers(1))
	require.NoError(t, err)

	k := byName(p)["k"]
	assert.Equal(t, int64(100), k.NonEmpty, "non-empty is exact even past the distinct cap")
	assert.Equal(t, int64(10), k.Distinct)
	assert.False(t, k.DistinctExact)
}

func TestProfileFile_TopNAndColumns(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "in.ndjson")
	require.NoError(t, os.WriteFile(path, []byte(statsNDJSON), 0o600))

	p, err := streamio.ProfileFile(context.Background(), path,
		streamio.WithProfileTopN(1), streamio.WithProfileColumns("country"))
	require.NoError(t, err)

	require.Len(t, p.Columns, 1)
	assert.Equal(t, "country", p.Columns[0].Name)
	assert.Len(t, p.Columns[0].TopValues, 1)
	assert.Equal(t, "US", p.Columns[0].TopValues[0].Value)
}

func TestProfileFile_SkippedRowsAreNotCounted(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "bad.ndjson")
	data := "{\"a\":1}\nnot json\n{\"a\":2}\n"
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))

	p, err := streamio.ProfileFile(context.Background(), path,
		streamio.WithMaxRowErrors(5, nil), streamio.WithParallelWorkers(1))
	require.NoError(t, err)

	assert.Equal(t, int64(2), p.Records)
	assert.Equal(t, int64(1), p.RowsSkipped)
}

func TestProfileFile_EmptyFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "empty.ndjson")
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	p, err := streamio.ProfileFile(context.Background(), path)
	require.NoError(t, err)
	assert.Zero(t, p.Records)
	assert.Empty(t, p.Columns)
}

func TestProfileFile_MissingAttributeIsEmpty(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sparse.ndjson")
	data := "{\"a\":1,\"b\":\"x\"}\n{\"a\":2}\n{\"a\":3,\"b\":\"x\"}\n{\"a\":4}\n"
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))

	p, err := streamio.ProfileFile(context.Background(), path, streamio.WithParallelWorkers(1))
	require.NoError(t, err)

	assert.Equal(t, int64(4), p.Records)
	b := byName(p)["b"]
	assert.Equal(t, int64(2), b.NonEmpty)
	assert.InDelta(t, 50.0, b.Pct, 1e-9)
}
