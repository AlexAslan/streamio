package streamio_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio"
	"github.com/AlexAslan/streamio/record"
)

// whereRows is 1,000 records: id, age (10..79), country cycling through four values, and a name
// that is empty for every tenth row.
func whereRows() string {
	var sb strings.Builder
	countries := []string{"US", "GR", "DE", "JP"}
	for i := range 1000 {
		name := fmt.Sprintf("user%d", i)
		if i%10 == 0 {
			name = ""
		}
		fmt.Fprintf(&sb, "{\"id\":%d,\"age\":%d,\"country\":%q,\"name\":%q}\n", i, 10+i%70, countries[i%4], name)
	}
	return sb.String()
}

// idCollector gathers the "id" of every record that reaches it, safely across workers.
type idCollector struct {
	ids []string
	mu  sync.Mutex
}

func (c *idCollector) Consume(batch []record.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, rec := range batch {
		if v, ok := rec.Lookup("id"); ok {
			text, _ := v.AppendText(nil)
			c.ids = append(c.ids, string(text))
		}
	}
	return nil
}

// whereIDs returns the ids that survive expression, via a consumer so the result is the record set
// itself rather than a rendering.
func whereIDs(t *testing.T, path, expression string, opts ...streamio.Option) []string {
	t.Helper()

	c := &idCollector{}
	_, err := streamio.ConsumeFile(context.Background(), path,
		func() (streamio.RecordConsumer, error) { return c, nil },
		append([]streamio.Option{streamio.WithWhere(expression), streamio.WithParallelWorkers(1)}, opts...)...)
	require.NoError(t, err)
	sort.Strings(c.ids)
	return c.ids
}

func TestWhere_FiltersRows(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "rows.ndjson", []byte(whereRows()))

	var want []string
	for i := range 1000 {
		age := 10 + i%70
		country := []string{"US", "GR", "DE", "JP"}[i%4]
		if age >= 18 && (country == "US" || country == "GR") && i%10 != 0 {
			want = append(want, strconv.Itoa(i))
		}
	}
	sort.Strings(want)

	got := whereIDs(t, path, `age >= 18 and country in ('US', 'GR') and name is not empty`)
	assert.Equal(t, want, got)
}

func TestWhere_SameResultAtAnyWorkerCountAndAcrossFormats(t *testing.T) {
	t.Parallel()

	src := writeTemp(t, "rows.ndjson", []byte(whereRows()))
	expression := `age > 40 and (country = 'JP' or name starts_with 'user9')`

	want := whereIDs(t, src, expression)
	require.NotEmpty(t, want)

	assert.Equal(t, want, whereIDs(t, src, expression,
		streamio.WithParallelWorkers(8), streamio.WithChunkSize(8*1024), streamio.WithBatchSize(16)))

	convert := func(f streamio.Format, ext string) string {
		var buf []byte
		var mu sync.Mutex
		_, err := streamio.ProcessFile(context.Background(), src,
			func(_ context.Context, doc []byte) error {
				mu.Lock()
				buf = append(buf, doc...)
				mu.Unlock()
				return nil
			},
			streamio.WithOutputFormat(f), streamio.WithSingleFileOutput(true),
			streamio.WithParallelWorkers(1), streamio.WithCSVHasHeader(true))
		require.NoError(t, err)
		return writeTemp(t, "rows."+ext, buf)
	}

	// CSV has every value as text, Parquet and Arrow carry types: the same rows must match.
	assert.Equal(t, want, whereIDs(t, convert(streamio.FormatCSV, "csv"), expression, streamio.WithCSVHasHeader(true)))
	assert.Equal(t, want, whereIDs(t, convert(streamio.FormatParquet, "parquet"), expression))
	assert.Equal(t, want, whereIDs(t, convert(streamio.FormatArrow, "arrow"), expression))
}

func TestWhere_AppliesToSameFormatAndStats(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "rows.ndjson", []byte(whereRows()))

	docs, res := collectDocs(t, path, streamio.WithWhere(`country = 'GR'`))
	assert.Len(t, docs, 250)
	assert.Equal(t, int64(1000), res.Stats.RowsRead)
	assert.Equal(t, int64(750), res.Stats.RowsFiltered)

	p, err := streamio.ProfileFile(context.Background(), path, streamio.WithWhere(`country = 'GR'`))
	require.NoError(t, err)
	assert.Equal(t, int64(250), p.Records)
}

func TestWhere_CountsTypeMissesAndStrictFails(t *testing.T) {
	t.Parallel()

	data := "{\"id\":1,\"age\":\"unknown\"}\n{\"id\":2,\"age\":30}\n{\"id\":3,\"age\":\"n/a\"}\n"
	path := writeTemp(t, "messy.ndjson", []byte(data))

	_, res := collectDocs(t, path, streamio.WithWhere(`age > 18`))
	assert.Equal(t, int64(2), res.Stats.PredicateMisses)
	assert.Equal(t, int64(1), res.Stats.DocumentsDispatched)

	_, err := streamio.ProcessFile(context.Background(), path,
		func(context.Context, []byte) error { return nil }, streamio.WithWhereStrict(`age > 18`))
	require.Error(t, err)
	assert.ErrorContains(t, err, `"age"`)
}

func TestWhere_InvalidExpressionIsAnOptionError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "x.ndjson")
	require.NoError(t, os.WriteFile(path, []byte("{}\n"), 0o600))

	_, err := streamio.ProcessFile(context.Background(), path,
		func(context.Context, []byte) error { return nil }, streamio.WithWhere(`age >`))
	require.ErrorIs(t, err, streamio.ErrInvalidWhere)
	assert.ErrorContains(t, err, "column")
}

// TestWhere_ComposesWithCustomFilters pins that WithWhere is an ordinary row filter in the chain.
func TestWhere_ComposesWithCustomFilters(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "rows.ndjson", []byte(whereRows()))
	even := func() (streamio.RowFilter, error) {
		return rowFilterFunc(func(rec record.Record) bool {
			v, _ := rec.Lookup("id")
			return v.I64%2 == 0
		}), nil
	}

	got := whereIDs(t, path, `country = 'US'`, streamio.WithRowFilter(even))
	for _, id := range got {
		var n int
		_, _ = fmt.Sscan(id, &n)
		assert.Zero(t, n%2)
		assert.Zero(t, n%4, "US is every fourth row starting at 0")
	}
	assert.NotEmpty(t, got)
}

type rowFilterFunc func(record.Record) bool

func (f rowFilterFunc) Keep(rec record.Record) (bool, error) { return f(rec), nil }
