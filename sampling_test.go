package streamio_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	parquetgo "github.com/AlexAslan/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio"
	"github.com/AlexAslan/streamio/record"
)

// keptIDs returns the sorted "id" values of every record that reaches a consumer under opts, using
// the mutex-guarded idCollector from where_test.go so it is safe at any worker count.
func keptIDs(t *testing.T, path string, opts ...streamio.Option) []string {
	t.Helper()

	c := &idCollector{}
	_, err := streamio.ConsumeFile(context.Background(), path,
		func() (streamio.RecordConsumer, error) { return c, nil },
		append([]streamio.Option{streamio.WithParallelWorkers(1)}, opts...)...)
	require.NoError(t, err)

	sort.Strings(c.ids)
	return c.ids
}

func TestSampling_SameKeptSetForAnyWorkerCount(t *testing.T) {
	t.Parallel()

	path := writeNDJSONRows(t, 20_000)
	spec := streamio.SamplingSpec{Seed: "s", Key: []string{"id"}, Rate: 0.1}

	got := map[int]int{}
	for _, workers := range []int{1, 2, 8} {
		var mu sync.Mutex
		var ids []string
		res, err := streamio.ProcessFile(context.Background(), path,
			func(_ context.Context, doc []byte) error {
				mu.Lock()
				ids = append(ids, string(doc))
				mu.Unlock()
				return nil
			},
			streamio.WithSampling(spec),
			streamio.WithParallelWorkers(workers), streamio.WithChunkSize(16*1024), streamio.WithBatchSize(100))
		require.NoError(t, err)
		sort.Strings(ids)
		got[workers] = len(ids)
		assert.Equal(t, int64(20_000), res.Stats.RowsRead)
		assert.Equal(t, int64(20_000-len(ids)), res.Stats.RowsFiltered)

		if workers > 1 {
			assert.Equal(t, got[1], got[workers], "workers=%d", workers)
		}
	}
	assert.InDelta(t, 2000, got[1], 250)
}

// TestSampling_SameEntitiesAcrossFormats is the property that makes subsets joinable: one key and
// seed selects the same entities from NDJSON, CSV, TSV, Parquet and Arrow copies of the data.
func TestSampling_SameEntitiesAcrossFormats(t *testing.T) {
	t.Parallel()

	var sb strings.Builder
	for i := range 2000 {
		fmt.Fprintf(&sb, "{\"id\":%d,\"name\":\"u%d\",\"score\":%d.5}\n", i, i, i%10)
	}
	src := writeTemp(t, "in.ndjson", []byte(sb.String()))

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
		return writeTemp(t, "in."+ext, buf)
	}

	spec := streamio.SamplingSpec{Seed: "same", Key: []string{"id"}, Rate: 0.2}
	csvOpt := streamio.WithCSVHasHeader(true)

	want := keptIDs(t, src, streamio.WithSampling(spec))
	require.NotEmpty(t, want)

	assert.Equal(t, want, keptIDs(t, convert(streamio.FormatCSV, "csv"), streamio.WithSampling(spec), csvOpt))
	assert.Equal(t, want, keptIDs(t, convert(streamio.FormatTSV, "tsv"), streamio.WithSampling(spec), csvOpt))
	assert.Equal(t, want, keptIDs(t, convert(streamio.FormatParquet, "parquet"), streamio.WithSampling(spec)))
	assert.Equal(t, want, keptIDs(t, convert(streamio.FormatArrow, "arrow"), streamio.WithSampling(spec)))

	// With no key the whole row is hashed: still the same entities across formats.
	rowSpec := streamio.SamplingSpec{Seed: "same", Rate: 0.2}
	rowWant := keptIDs(t, src, streamio.WithSampling(rowSpec))
	assert.Equal(t, rowWant, keptIDs(t, convert(streamio.FormatParquet, "parquet"), streamio.WithSampling(rowSpec)))
	assert.Equal(t, rowWant,
		keptIDs(t, convert(streamio.FormatCSV, "csv"), streamio.WithSampling(rowSpec), csvOpt))
}

func TestSampling_SameKeyKeepsEntitiesTogetherAcrossFiles(t *testing.T) {
	t.Parallel()

	var users, orders strings.Builder
	for i := range 500 {
		fmt.Fprintf(&users, "{\"id\":%d,\"name\":\"u%d\"}\n", i, i)
		for j := range 3 {
			fmt.Fprintf(&orders, "{\"id\":%d,\"user_id\":%d,\"total\":%d}\n", i*10+j, i, j)
		}
	}
	usersPath := writeTemp(t, "users.ndjson", []byte(users.String()))
	ordersPath := writeTemp(t, "orders.ndjson", []byte(orders.String()))

	keptUsers := keptIDs(t, usersPath, streamio.WithSampling(
		streamio.SamplingSpec{Seed: "s", Key: []string{"id"}, Rate: 0.3}))

	// Orders keyed by user_id must keep exactly the orders of the kept users.
	var orderUsers []string
	c := &fieldCollector{field: "user_id", out: &orderUsers}
	_, err := streamio.ConsumeFile(context.Background(), ordersPath,
		func() (streamio.RecordConsumer, error) { return c, nil },
		streamio.WithParallelWorkers(1),
		streamio.WithSampling(streamio.SamplingSpec{Seed: "s", Key: []string{"user_id"}, Rate: 0.3}))
	require.NoError(t, err)

	distinct := map[string]int{}
	for _, u := range orderUsers {
		distinct[u]++
	}
	got := make([]string, 0, len(distinct))
	for u, n := range distinct {
		assert.Equal(t, 3, n, "user %s keeps all three orders or none", u)
		got = append(got, u)
	}
	sort.Strings(got)
	assert.Equal(t, keptUsers, got)
}

type fieldCollector struct {
	out   *[]string
	field string
}

func (c *fieldCollector) Consume(batch []record.Record) error {
	for _, rec := range batch {
		if v, ok := rec.Lookup(c.field); ok {
			text, _ := v.AppendText(nil)
			*c.out = append(*c.out, string(text))
		}
	}
	return nil
}

func TestSampling_WeightsRecoverTheOriginalCount(t *testing.T) {
	t.Parallel()

	var sb strings.Builder
	levels := []string{"ERROR", "WARN", "INFO"}
	total := map[string]int{}
	for i := range 30_000 {
		lv := levels[i%3]
		total[lv]++
		fmt.Fprintf(&sb, "{\"id\":%d,\"level\":%q}\n", i, lv)
	}
	path := writeTemp(t, "logs.ndjson", []byte(sb.String()))

	var mu sync.Mutex
	var weighted float64
	var kept int
	byLevel := map[string]int{}
	_, err := streamio.ProcessFile(context.Background(), path,
		func(_ context.Context, doc []byte) error {
			var d struct {
				Level  string  `json:"level"`
				Weight float64 `json:"w"`
			}
			if err := json.Unmarshal(doc, &d); err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			weighted += d.Weight
			kept++
			byLevel[d.Level]++
			return nil
		},
		streamio.WithSampling(streamio.SamplingSpec{
			Seed: "s", Key: []string{"id"}, StratifyBy: "level", WeightColumn: "w",
			StratumRates: map[string]float64{"ERROR": 1, "WARN": 0.1}, DefaultRate: 0.02,
		}))
	require.NoError(t, err)

	assert.Equal(t, total["ERROR"], byLevel["ERROR"], "every error is kept")
	assert.Less(t, kept, 30_000/2)
	assert.InDelta(t, 30_000, weighted, 30_000*0.08, "sum of weights estimates the original row count")
}

func TestSampling_AppliesToSameFormatConversionAndStats(t *testing.T) {
	t.Parallel()

	path := writeNDJSONRows(t, 5000)
	spec := streamio.SamplingSpec{Seed: "s", Key: []string{"id"}, Rate: 0.1}

	// NDJSON -> NDJSON would be raw passthrough; sampling must still apply.
	docs, res := collectDocs(t, path, streamio.WithSampling(spec))
	assert.Less(t, len(docs), 1000)
	assert.Equal(t, int64(5000), res.Stats.RowsRead)

	p, err := streamio.ProfileFile(context.Background(), path, streamio.WithSampling(spec))
	require.NoError(t, err)
	assert.Equal(t, int64(len(docs)), p.Records, "stats profiles the sample")
}

func TestSampling_InvalidSpecIsReportedAsAnOptionError(t *testing.T) {
	t.Parallel()

	path := writeNDJSONRows(t, 10)
	for name, spec := range map[string]streamio.SamplingSpec{
		"empty":        {},
		"rate too big": {Rate: 2},
		"two modes":    {Rate: 0.5, RangeHi: 0.5},
		"bad range":    {RangeLo: 0.9, RangeHi: 0.2},
	} {
		_, err := streamio.ProcessFile(context.Background(), path,
			func(context.Context, []byte) error { return nil }, streamio.WithSampling(spec))
		require.Error(t, err, name)
		assert.ErrorContains(t, err, "invalid sampling", name)
	}
}

func TestSampling_ParquetInputKeepsTypedKey(t *testing.T) {
	t.Parallel()

	type row struct {
		Name string `parquet:"name"`
		ID   int64  `parquet:"id"`
	}
	path := filepath.Join(t.TempDir(), "typed.parquet")
	f, err := os.Create(path)
	require.NoError(t, err)
	w := parquetgo.NewGenericWriter[row](f)
	rows := make([]row, 3000)
	for i := range rows {
		rows[i] = row{ID: int64(i), Name: fmt.Sprintf("n%d", i)}
	}
	_, err = w.Write(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	spec := streamio.SamplingSpec{Seed: "s", Key: []string{"id"}, Rate: 0.5}
	fromParquet := keptIDs(t, path, streamio.WithSampling(spec))

	var sb strings.Builder
	for i := range rows {
		fmt.Fprintf(&sb, "{\"id\":%d}\n", i)
	}
	fromJSON := keptIDs(t, writeTemp(t, "ids.ndjson", []byte(sb.String())), streamio.WithSampling(spec))
	assert.Equal(t, fromJSON, fromParquet)
}

// sparseNDJSON has an id on even rows and none on odd rows.
func sparseNDJSON(n int) string {
	var sb strings.Builder
	for i := range n {
		if i%2 == 0 {
			fmt.Fprintf(&sb, "{\"n\":%d,\"id\":%d}\n", i, i)
		} else {
			fmt.Fprintf(&sb, "{\"n\":%d}\n", i)
		}
	}
	return sb.String()
}

func TestSampling_RowsWithoutAKeyAreSampledIndependently(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "sparse.ndjson", []byte(sparseNDJSON(4000)))

	for _, seed := range []string{"a", "b", "c", "d"} {
		var mu sync.Mutex
		unkeyed := 0
		res, err := streamio.ProcessFile(context.Background(), path,
			func(_ context.Context, doc []byte) error {
				if !strings.Contains(string(doc), `"id"`) {
					mu.Lock()
					unkeyed++
					mu.Unlock()
				}
				return nil
			},
			streamio.WithSampling(streamio.SamplingSpec{Seed: seed, Key: []string{"id"}, Rate: 0.5}))
		require.NoError(t, err)

		assert.Equal(t, int64(2000), res.Stats.RowsMissingKey)
		assert.InDelta(t, 1000, unkeyed, 150, "seed %s: about half of the unkeyed rows, not none or all", seed)
	}
}

func TestSampling_MissingKeyPolicies(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "sparse.ndjson", []byte(sparseNDJSON(2000)))
	count := func(policy streamio.MissingKeyPolicy) (int, error) {
		var mu sync.Mutex
		unkeyed := 0
		_, err := streamio.ProcessFile(context.Background(), path,
			func(_ context.Context, doc []byte) error {
				if !strings.Contains(string(doc), `"id"`) {
					mu.Lock()
					unkeyed++
					mu.Unlock()
				}
				return nil
			},
			streamio.WithSampling(streamio.SamplingSpec{
				Seed: "p", Key: []string{"id"}, Rate: 0.1, OnMissingKey: policy,
			}))
		return unkeyed, err
	}

	keep, err := count(streamio.MissingKeyKeep)
	require.NoError(t, err)
	assert.Equal(t, 1000, keep)

	drop, err := count(streamio.MissingKeyDrop)
	require.NoError(t, err)
	assert.Zero(t, drop)

	_, err = count(streamio.MissingKeyError)
	require.ErrorIs(t, err, streamio.ErrMissingKey)

	_, err = count("bogus")
	require.Error(t, err)
}

func TestSampling_StatsReportsMissingKeys(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "sparse.ndjson", []byte(sparseNDJSON(1000)))
	p, err := streamio.ProfileFile(context.Background(), path,
		streamio.WithSampling(streamio.SamplingSpec{Seed: "s", Key: []string{"id"}, Rate: 0.5}))
	require.NoError(t, err)
	assert.Equal(t, int64(500), p.RowsMissingKey)
}

// TestSampling_ComposesWithWhere pins that the predicate runs first, so a sample is taken from the
// records that pass it and never contains one that does not.
func TestSampling_ComposesWithWhere(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "rows.ndjson", []byte(whereRows()))
	spec := streamio.SamplingSpec{Seed: "w", Key: []string{"id"}, Rate: 0.5}

	all := whereIDs(t, path, `country = 'US'`)
	sampled := whereIDs(t, path, `country = 'US'`, streamio.WithSampling(spec))

	assert.Less(t, len(sampled), len(all))
	assert.NotEmpty(t, sampled)
	assert.Subset(t, all, sampled)
}
