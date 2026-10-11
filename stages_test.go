package streamio_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio"
	"github.com/AlexAslan/streamio/record"
)

var errNoKey = errors.New("no key configured")

// idFilter keeps records whose "id" field passes keep. It is built only from the public API, as an
// outside module would.
type idFilter struct {
	keep func(id string) bool
}

func (f idFilter) Keep(rec record.Record) (bool, error) {
	v, ok := rec.Lookup("id")
	if !ok {
		return false, nil
	}
	text, ok := v.AppendText(nil)
	if !ok {
		return false, nil
	}
	return f.keep(string(text)), nil
}

// upperName rewrites the "name" field to upper case, writing the result into the batch arena.
type upperName struct{}

func (upperName) Apply(rec record.Record, arena *record.Arena) (record.Record, error) {
	for i := range rec {
		if rec[i].Name != "name" || rec[i].Value.Kind != record.KindBytes {
			continue
		}
		out := arena.Copy(rec[i].Value.Str)
		for j, b := range out {
			if b >= 'a' && b <= 'z' {
				out[j] = b - 'a' + 'A'
			}
		}
		rec[i].Value = record.Bytes(out)
	}
	return rec, nil
}

func writeNDJSONRows(t *testing.T, n int) string {
	t.Helper()

	var sb strings.Builder
	for i := range n {
		fmt.Fprintf(&sb, "{\"id\":%d,\"name\":\"row%d\"}\n", i, i)
	}
	path := filepath.Join(t.TempDir(), "rows.ndjson")
	require.NoError(t, os.WriteFile(path, []byte(sb.String()), 0o600))
	return path
}

func collectDocs(t *testing.T, path string, opts ...streamio.Option) ([]string, streamio.Result) {
	t.Helper()

	var mu sync.Mutex
	var docs []string
	res, err := streamio.ProcessFile(context.Background(), path, func(_ context.Context, doc []byte) error {
		mu.Lock()
		defer mu.Unlock()
		docs = append(docs, string(doc))
		return nil
	}, opts...)
	require.NoError(t, err)
	sort.Strings(docs)
	return docs, res
}

func TestRowFilter_DropsRowsAndCountsThem(t *testing.T) {
	t.Parallel()

	path := writeNDJSONRows(t, 1000)
	even := func() (streamio.RowFilter, error) {
		return idFilter{keep: func(id string) bool { return id[len(id)-1]%2 == '0'%2 }}, nil
	}

	// Same-format output: also proves a filter rules raw passthrough out.
	docs, res := collectDocs(t, path, streamio.WithRowFilter(even), streamio.WithBatchSize(64))

	assert.Len(t, docs, 500)
	assert.Equal(t, int64(1000), res.Stats.RowsRead)
	assert.Equal(t, int64(500), res.Stats.RowsFiltered)
	assert.Equal(t, int64(500), res.Stats.DocumentsDispatched)
}

func TestRowFilter_WorkerCountDoesNotChangeTheKeptSet(t *testing.T) {
	t.Parallel()

	path := writeNDJSONRows(t, 5000)
	third := func() (streamio.RowFilter, error) {
		return idFilter{keep: func(id string) bool { return strings.HasSuffix(id, "3") }}, nil
	}

	one, _ := collectDocs(t, path, streamio.WithRowFilter(third), streamio.WithParallelWorkers(1))
	many, _ := collectDocs(t, path, streamio.WithRowFilter(third),
		streamio.WithParallelWorkers(8), streamio.WithChunkSize(8*1024), streamio.WithBatchSize(32))

	assert.Equal(t, one, many)
	assert.Len(t, one, 500)
}

func TestRecordTransform_RunsBeforeEncodingAndAfterFilters(t *testing.T) {
	t.Parallel()

	path := writeNDJSONRows(t, 10)
	only3 := func() (streamio.RowFilter, error) {
		return idFilter{keep: func(id string) bool { return id == "3" }}, nil
	}
	upper := func() (streamio.RecordTransform, error) { return upperName{}, nil }

	docs, _ := collectDocs(t, path,
		streamio.WithRecordTransform(upper), streamio.WithRowFilter(only3), streamio.WithParallelWorkers(1))

	require.Len(t, docs, 1)
	assert.Contains(t, docs[0], `"ROW3"`)
}

func TestStages_FactoryRunsOncePerWorker(t *testing.T) {
	t.Parallel()

	path := writeNDJSONRows(t, 4000)
	var built atomic.Int64
	newFilter := func() (streamio.RowFilter, error) {
		built.Add(1)
		return idFilter{keep: func(string) bool { return true }}, nil
	}

	_, _ = collectDocs(t, path, streamio.WithRowFilter(newFilter),
		streamio.WithParallelWorkers(4), streamio.WithChunkSize(8*1024))

	got := built.Load()
	assert.GreaterOrEqual(t, got, int64(1))
	assert.LessOrEqual(t, got, int64(4), "one instance per decode worker, never per record or batch")
}

func TestStages_FactoryErrorFailsTheRun(t *testing.T) {
	t.Parallel()

	path := writeNDJSONRows(t, 10)
	boom := func() (streamio.RowFilter, error) { return nil, errNoKey }

	_, err := streamio.ProcessFile(context.Background(), path,
		func(context.Context, []byte) error { return nil }, streamio.WithRowFilter(boom))
	require.ErrorContains(t, err, "no key configured")
}

// sumConsumer totals the "id" field of every record it sees.
type sumConsumer struct {
	sum  int64
	rows int64
}

func (c *sumConsumer) Consume(batch []record.Record) error {
	for _, rec := range batch {
		c.rows++
		if v, ok := rec.Lookup("id"); ok && v.Kind == record.KindInt64 {
			c.sum += v.I64
		}
	}
	return nil
}

func TestConsumeFile_MergesPerWorkerConsumers(t *testing.T) {
	t.Parallel()

	path := writeNDJSONRows(t, 3000)
	var (
		mu        sync.Mutex
		consumers []*sumConsumer
	)
	res, err := streamio.ConsumeFile(context.Background(), path,
		func() (streamio.RecordConsumer, error) {
			c := &sumConsumer{}
			mu.Lock()
			consumers = append(consumers, c)
			mu.Unlock()
			return c, nil
		},
		streamio.WithParallelWorkers(4), streamio.WithChunkSize(8*1024))
	require.NoError(t, err)

	var sum, rows int64
	for _, c := range consumers {
		sum += c.sum
		rows += c.rows
	}
	assert.Equal(t, int64(3000), rows)
	assert.Equal(t, int64(2999*3000/2), sum)
	assert.Equal(t, int64(3000), res.Stats.RowsRead)
	assert.Zero(t, res.Stats.DocumentsDispatched)
}

func TestConsumeFile_SeesFilteredAndCompressedInput(t *testing.T) {
	t.Parallel()

	var sb strings.Builder
	for i := range 100 {
		fmt.Fprintf(&sb, "{\"id\":%d}\n", i)
	}
	path := writeTemp(t, "data.gz", gzipBytes(t, sb.String()))

	c := &sumConsumer{}
	_, err := streamio.ConsumeFile(context.Background(), path,
		func() (streamio.RecordConsumer, error) { return c, nil },
		streamio.WithParallelWorkers(1),
		streamio.WithRowFilter(func() (streamio.RowFilter, error) {
			return idFilter{keep: func(id string) bool { return len(id) == 1 }}, nil
		}))
	require.NoError(t, err)
	assert.Equal(t, int64(10), c.rows, "only single-digit ids pass the filter")
}

func TestStages_FilteringAllocatesNothingOncePerRecord(t *testing.T) {
	path := writeNDJSONRows(t, 2000)
	allocs := func(opts ...streamio.Option) float64 {
		return testing.AllocsPerRun(5, func() {
			_, err := streamio.ConsumeFile(context.Background(), path,
				func() (streamio.RecordConsumer, error) { return &sumConsumer{}, nil },
				append([]streamio.Option{streamio.WithParallelWorkers(1)}, opts...)...)
			require.NoError(t, err)
		})
	}

	base := allocs()
	filtered := allocs(streamio.WithRowFilter(func() (streamio.RowFilter, error) {
		return idFilter{keep: func(id string) bool { return id[len(id)-1] != '7' }}, nil
	}))

	// The filter itself allocates for the id text via AppendText(nil) in this test helper only, so
	// the bound is per-row slack, not zero; the pool's compaction must add none of its own.
	assert.Less(t, filtered-base, float64(2*2000+64))
}

// scratchMasker is the trap the arena exists to prevent: it renders every value into one reused
// buffer and points the field at it.
type scratchMasker struct {
	scratch []byte
}

func (m *scratchMasker) Apply(rec record.Record, arena *record.Arena) (record.Record, error) {
	for i := range rec {
		if rec[i].Name != "name" {
			continue
		}
		m.scratch = append(m.scratch[:0], "tok-"...)
		m.scratch = append(m.scratch, rec[i].Value.Str...)
		// Copying into the arena is what makes this correct; assigning m.scratch directly would
		// leave every record in the batch pointing at the last token.
		rec[i].Value = record.Bytes(arena.Copy(m.scratch))
	}
	return rec, nil
}

func TestRecordTransform_ArenaKeepsEveryRecordsOutputDistinct(t *testing.T) {
	t.Parallel()

	path := writeNDJSONRows(t, 50)
	docs, _ := collectDocs(t, path,
		streamio.WithRecordTransform(func() (streamio.RecordTransform, error) { return &scratchMasker{}, nil }),
		streamio.WithBatchSize(50), streamio.WithParallelWorkers(1))

	require.Len(t, docs, 50)
	seen := map[string]bool{}
	for _, d := range docs {
		seen[d] = true
	}
	assert.Len(t, seen, 50, "each record keeps its own token")
}
