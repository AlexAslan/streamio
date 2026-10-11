package profile_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio/internal/profile"
	"github.com/AlexAslan/streamio/internal/record"
)

func feed(t *testing.T, set *profile.Set, recs ...record.Record) {
	t.Helper()

	c, err := set.NewCollector()
	require.NoError(t, err)
	docs, err := c.EncodeBatch(recs)
	require.NoError(t, err)
	assert.Empty(t, docs, "a collector never emits documents")
}

func cols(p profile.Profile) map[string]profile.Column {
	m := make(map[string]profile.Column, len(p.Columns))
	for _, c := range p.Columns {
		m[c.Name] = c
	}
	return m
}

func names(p profile.Profile) []string {
	out := make([]string, 0, len(p.Columns))
	for _, c := range p.Columns {
		out = append(out, c.Name)
	}
	return out
}

func str(s string) record.Value { return record.Bytes([]byte(s)) }

func num(n int64) record.Value { return record.Int64(n, record.SemanticNone) }

func TestCollector_ScalarKinds(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	feed(t, set,
		record.Record{}.
			Append("s", str("x")).
			Append("i", num(-5)).
			Append("u", record.Int64(-1, record.SemanticUnsigned)).
			Append("f", record.Float64(1.5)).
			Append("f32", record.Float32(0.1)).
			Append("b", record.Bool(true)),
	)

	got := cols(set.Merge())
	assert.Equal(t, "x", got["s"].TopValues[0].Value)
	assert.Equal(t, "-5", got["i"].TopValues[0].Value)
	assert.Equal(t, "18446744073709551615", got["u"].TopValues[0].Value)
	assert.Equal(t, "1.5", got["f"].TopValues[0].Value)
	assert.Equal(t, "0.1", got["f32"].TopValues[0].Value)
	assert.Equal(t, "true", got["b"].TopValues[0].Value)
}

// TestCollector_RendersLikeTheConverters pins that a value is one key however it is typed, using
// the same formatting the converters write.
func TestCollector_RendersLikeTheConverters(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	feed(t, set,
		record.Record{}.
			Append("f", record.Float64(1_000_000)).
			Append("neg0", record.Float64(negZero())).
			Append("date", record.Int64(19723, record.SemanticDate)).
			Append("ms", record.Int64(1_704_067_200_000, record.SemanticTimestampMillis)).
			Append("us", record.Int64(1_704_067_200_000_000, record.SemanticTimestampMicros)).
			Append("ns", record.Int64(1_704_067_200_000_000_000, record.SemanticTimestampNanos)),
		record.Record{}.
			Append("f", record.Float64(1_000_000)).
			Append("neg0", record.Float64(0)),
	)

	got := cols(set.Merge())
	assert.Equal(t, int64(1), got["f"].Distinct)
	assert.Equal(t, "1000000", got["f"].TopValues[0].Value)
	assert.Equal(t, int64(1), got["neg0"].Distinct, "-0 and 0 are the same number")
	assert.Equal(t, "2024-01-01", got["date"].TopValues[0].Value)
	assert.Equal(t, "2024-01-01T00:00:00.000Z", got["ms"].TopValues[0].Value)
	assert.Equal(t, "2024-01-01T00:00:00.000000Z", got["us"].TopValues[0].Value)
	assert.Equal(t, "2024-01-01T00:00:00.000000000Z", got["ns"].TopValues[0].Value)
}

func negZero() float64 {
	z := 0.0
	return -z
}

func TestCollector_EmptyValues(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	feed(t, set,
		record.Record{}.Append("a", record.Null()).Append("b", record.Bytes(nil)),
		record.Record{}.Append("a", str(" ")).Append("b", record.Bytes([]byte{})),
	)

	p := set.Merge()
	assert.Equal(t, int64(2), p.Records)
	got := cols(p)

	// Whitespace is not empty.
	assert.Equal(t, int64(1), got["a"].NonEmpty)

	// A column empty in every record is still reported, at 0%.
	require.Contains(t, got, "b")
	assert.Zero(t, got["b"].NonEmpty)
	assert.Zero(t, got["b"].Pct)
	assert.Empty(t, got["b"].TopValues)
	assert.Equal(t, []string{"a", "b"}, names(p))
}

func TestCollector_NestedMapsAndLists(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	feed(t, set,
		record.Record{}.
			Append("attrs", record.Map(record.Record{}.
				Append("k", str("v")).
				Append("inner", record.Map(record.Record{}.Append("deep", num(7)))))).
			Append("tags", record.List([]record.Value{str("a")})).
			Append("none", record.List(nil)).
			Append("empty", record.Map(record.Record{})),
	)

	got := cols(set.Merge())
	assert.Equal(t, int64(1), got["attrs.k"].NonEmpty)
	assert.Equal(t, int64(1), got["attrs.inner.deep"].NonEmpty)
	assert.Equal(t, int64(1), got["tags"].NonEmpty)
	assert.Equal(t, "[list]", got["tags"].TopValues[0].Value)
	assert.Zero(t, got["none"].NonEmpty)
	require.Contains(t, got, "empty", "an empty map still names an attribute")
	assert.Zero(t, got["empty"].NonEmpty)
}

// TestCollector_RepeatedAttributeCountsOncePerRecord guards non-empty never exceeding records.
func TestCollector_RepeatedAttributeCountsOncePerRecord(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	feed(t, set,
		// Duplicate header, as in a CSV with two columns both named a.
		record.Record{}.Append("a", str("1")).Append("a", str("2")),
		record.Record{}.Append("a", str("3")).Append("a", str("4")),
		// A nested path that aliases a top-level dotted name.
		record.Record{}.
			Append("x", record.Map(record.Record{}.Append("y", num(1)))).
			Append("x.y", num(2)),
	)

	p := set.Merge()
	got := cols(p)
	assert.Equal(t, int64(2), got["a"].NonEmpty)
	assert.Equal(t, int64(1), got["x.y"].NonEmpty)
	for _, c := range p.Columns {
		assert.LessOrEqual(t, c.NonEmpty, p.Records, c.Name)
		assert.LessOrEqual(t, c.Pct, 100.0, c.Name)
	}
}

func TestCollector_EmptyNamesDoNotCollide(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	feed(t, set, record.Record{}.
		Append("", record.Map(record.Record{}.Append("b", num(1)))).
		Append("b", num(2)))

	got := cols(set.Merge())
	assert.Equal(t, int64(1), got[".b"].NonEmpty)
	assert.Equal(t, int64(1), got["b"].NonEmpty)
}

func TestCollector_ColumnFilter(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{Columns: []string{"attrs", "id"}})
	feed(t, set, record.Record{}.
		Append("id", num(1)).
		Append("name", str("x")).
		Append("attrs", record.Map(record.Record{}.Append("k", str("v")).Append("j", str("w")))))

	assert.Equal(t, []string{"id", "attrs.k", "attrs.j"}, names(set.Merge()))
}

func TestSet_MergesCollectors(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{TopN: 2})
	a := record.Record{}.Append("c", str("x"))
	b := record.Record{}.Append("c", str("y"))
	feed(t, set, a, a, b)
	feed(t, set, b, b, a, record.Record{}.Append("c", str("z")))

	p := set.Merge()
	assert.Equal(t, int64(7), p.Records)

	c := cols(p)["c"]
	assert.Equal(t, int64(7), c.NonEmpty)
	assert.Equal(t, int64(3), c.Distinct)
	assert.True(t, c.DistinctExact)
	require.Len(t, c.TopValues, 2)
	// x and y tie at 3; ties break by value.
	assert.Equal(t, "x", c.TopValues[0].Value)
	assert.Equal(t, "y", c.TopValues[1].Value)
	assert.Zero(t, c.TopValues[0].MaxError)
	assert.InDelta(t, 3.0*100/7, c.TopValues[0].Pct, 1e-9)
}

func TestSet_TopNSelection(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{TopN: 3})
	recs := make([]record.Record, 0, 50*51/2)
	for v := range 50 {
		for range v + 1 { // value i occurs i+1 times
			recs = append(recs, record.Record{}.Append("c", str(fmt.Sprintf("v%02d", v))))
		}
	}
	feed(t, set, recs...)

	c := cols(set.Merge())["c"]
	require.Len(t, c.TopValues, 3)
	assert.Equal(t, "v49", c.TopValues[0].Value)
	assert.Equal(t, int64(50), c.TopValues[0].Count)
	assert.Equal(t, "v48", c.TopValues[1].Value)
	assert.Equal(t, "v47", c.TopValues[2].Value)
}

// TestCollector_KeepsFrequentValuesPastTheLimit is the case first-come admission got wrong: many
// rare values arrive first, then one dominant one.
func TestCollector_KeepsFrequentValuesPastTheLimit(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{MaxDistinct: 20, TopN: 3})
	recs := make([]record.Record, 0, 5200)
	for i := range 200 {
		recs = append(recs, record.Record{}.Append("c", str(fmt.Sprintf("rare%03d", i))))
	}
	for range 5000 {
		recs = append(recs, record.Record{}.Append("c", str("HOT")))
	}
	feed(t, set, recs...)

	p := set.Merge()
	c := cols(p)["c"]

	assert.Equal(t, int64(5200), c.NonEmpty, "non-empty stays exact")
	assert.False(t, c.DistinctExact)
	require.NotEmpty(t, c.TopValues)
	hot := c.TopValues[0]
	assert.Equal(t, "HOT", hot.Value)
	assert.InDelta(t, 5000, hot.Count, float64(hot.MaxError)+1)
	assert.Positive(t, hot.MaxError)
	assert.GreaterOrEqual(t, hot.Count, int64(5000), "Space-Saving never under-counts a tracked value")
	assert.LessOrEqual(t, hot.Count-hot.MaxError, int64(5000))
}

func TestCollector_ExactUpToTheLimit(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{MaxDistinct: 5})
	recs := make([]record.Record, 0, 5)
	for i := range 5 {
		recs = append(recs, record.Record{}.Append("c", str(fmt.Sprintf("v%d", i))))
	}
	feed(t, set, recs...)

	c := cols(set.Merge())["c"]
	assert.True(t, c.DistinctExact)
	assert.Equal(t, int64(5), c.Distinct)
}

// TestSet_MergeBoundsEstimatesAcrossWorkers checks that a value one worker stopped tracking widens
// the merged error rather than being silently treated as absent.
func TestSet_MergeBoundsEstimatesAcrossWorkers(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{MaxDistinct: 4, TopN: 2})

	w1 := make([]record.Record, 0, 100)
	for range 100 {
		w1 = append(w1, record.Record{}.Append("c", str("hot")))
	}
	feed(t, set, w1...)

	// The second worker sees "hot" too, but buried under enough other values to evict it.
	w2 := make([]record.Record, 0, 40)
	for i := range 40 {
		v := fmt.Sprintf("o%d", i%8)
		w2 = append(w2, record.Record{}.Append("c", str(v)))
	}
	feed(t, set, w2...)

	c := cols(set.Merge())["c"]
	require.False(t, c.DistinctExact)
	require.NotEmpty(t, c.TopValues)
	assert.Equal(t, "hot", c.TopValues[0].Value)
	assert.Positive(t, c.TopValues[0].MaxError, "the worker that went approximate may have missed hot")
}

func TestSet_ColumnCapAppliesToTheMergedSet(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	for w := range 3 {
		rec := record.Record{}
		for i := range profile.MaxColumns {
			rec = rec.Append(fmt.Sprintf("w%d_c%d", w, i), num(1))
		}
		feed(t, set, rec)
	}

	p := set.Merge()
	assert.Len(t, p.Columns, profile.MaxColumns)
	assert.True(t, p.ColumnsTruncated)
}

func TestSet_ColumnOrderIsFirstSeen(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	feed(t, set,
		// a is empty in the first record, so order must come from the record, not from fill.
		record.Record{}.Append("a", str("")).Append("b", str("x")).Append("c", str("y")),
		record.Record{}.Append("a", str("1")).Append("b", str("x")).Append("c", str("y")),
	)
	assert.Equal(t, []string{"a", "b", "c"}, names(set.Merge()))
}

// TestCollector_SteadyStateAllocatesNothing pins the no-allocation promise for every value kind
// once the distinct values have been seen.
func TestCollector_SteadyStateAllocatesNothing(t *testing.T) {
	batch := []record.Record{
		record.Record{}.
			Append("s", str("alpha")).
			Append("empty", str("")).
			Append("null", record.Null()).
			Append("i", num(7)).
			Append("u", record.Int64(9, record.SemanticUnsigned)).
			Append("f", record.Float64(2.5)).
			Append("f32", record.Float32(0.25)).
			Append("b", record.Bool(true)).
			Append("d", record.Int64(19723, record.SemanticDate)).
			Append("ts", record.Int64(1_704_067_200_000, record.SemanticTimestampMillis)).
			Append("l", record.List([]record.Value{str("a")})).
			Append("m", record.Map(record.Record{}.Append("k", str("v")))),
	}

	set := profile.NewSet(profile.Config{})
	c, err := set.NewCollector()
	require.NoError(t, err)
	_, _ = c.EncodeBatch(batch) // warm: first sightings allocate their keys

	allocs := testing.AllocsPerRun(100, func() { _, _ = c.EncodeBatch(batch) })
	assert.Zero(t, allocs)
}

// TestCollector_ApproximateHitsAllocateNothing covers the Space-Saving path: counting a value
// already tracked must not allocate either.
func TestCollector_ApproximateHitsAllocateNothing(t *testing.T) {
	set := profile.NewSet(profile.Config{MaxDistinct: 4})
	c, err := set.NewCollector()
	require.NoError(t, err)

	warm := make([]record.Record, 0, 20)
	for i := range 20 {
		warm = append(warm, record.Record{}.Append("c", str(fmt.Sprintf("v%d", i))))
	}
	_, _ = c.EncodeBatch(warm)

	// After the warm-up the tracked values are some subset; repeating the last one is a hit.
	hit := []record.Record{record.Record{}.Append("c", str("v19"))}
	allocs := testing.AllocsPerRun(100, func() { _, _ = c.EncodeBatch(hit) })
	assert.Zero(t, allocs)
}

func BenchmarkCollector_WarmDistinctSet(b *testing.B) {
	set := profile.NewSet(profile.Config{})
	c, err := set.NewCollector()
	require.NoError(b, err)

	batch := make([]record.Record, 512)
	vals := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta")}
	for i := range batch {
		batch[i] = record.Record{}.
			Append("id", num(int64(i%64))).
			Append("name", record.Bytes(vals[i%len(vals)])).
			Append("score", record.Float64(float64(i%8)))
	}
	// Warm the distinct sets so only steady-state cost is measured.
	_, _ = c.EncodeBatch(batch)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = c.EncodeBatch(batch)
	}
}

func BenchmarkMerge_HighCardinality(b *testing.B) {
	for range b.N {
		b.StopTimer()
		set := profile.NewSet(profile.Config{MaxDistinct: 1 << 30, TopN: 10})
		c, err := set.NewCollector()
		require.NoError(b, err)

		batch := make([]record.Record, 0, 4096)
		for i := range 100_000 {
			batch = append(batch, record.Record{}.Append("c", str(fmt.Sprintf("value-%d", i))))
			if len(batch) == cap(batch) {
				_, _ = c.EncodeBatch(batch)
				batch = batch[:0]
			}
		}
		_, _ = c.EncodeBatch(batch)

		b.StartTimer()
		_ = set.Merge()
	}
}
