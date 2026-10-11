package profile_test

import (
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

func TestCollector_ScalarKinds(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	feed(t, set,
		record.Record{}.
			Append("s", record.Bytes([]byte("x"))).
			Append("i", record.Int64(-5, record.SemanticNone)).
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

func TestCollector_EmptyValues(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	feed(t, set,
		record.Record{}.Append("a", record.Null()).Append("b", record.Bytes(nil)),
		record.Record{}.Append("a", record.Bytes([]byte(" "))).Append("b", record.Bytes([]byte{})),
	)

	p := set.Merge()
	assert.Equal(t, int64(2), p.Records)
	got := cols(p)
	// Whitespace is not empty.
	assert.Equal(t, int64(1), got["a"].NonEmpty)
	assert.Zero(t, got["b"].NonEmpty)
	assert.Empty(t, got["b"].TopValues)
}

func TestCollector_NestedMapsAndLists(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	feed(t, set,
		record.Record{}.
			Append("attrs", record.Map(record.Record{}.
				Append("k", record.Bytes([]byte("v"))).
				Append("inner", record.Map(record.Record{}.Append("deep", record.Int64(7, record.SemanticNone)))))).
			Append("tags", record.List([]record.Value{record.Bytes([]byte("a"))})).
			Append("none", record.List(nil)),
	)

	got := cols(set.Merge())
	assert.Equal(t, int64(1), got["attrs.k"].NonEmpty)
	assert.Equal(t, int64(1), got["attrs.inner.deep"].NonEmpty)
	assert.Equal(t, int64(1), got["tags"].NonEmpty)
	assert.Zero(t, got["none"].NonEmpty)
}

func TestSet_MergesCollectors(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{TopN: 2})
	a := record.Record{}.Append("c", record.Bytes([]byte("x")))
	b := record.Record{}.Append("c", record.Bytes([]byte("y")))
	feed(t, set, a, a, b)
	feed(t, set, b, b, a, record.Record{}.Append("c", record.Bytes([]byte("z"))))

	p := set.Merge()
	assert.Equal(t, int64(7), p.Records)

	c := cols(p)["c"]
	assert.Equal(t, int64(7), c.NonEmpty)
	assert.Equal(t, int64(3), c.Distinct)
	require.Len(t, c.TopValues, 2)
	// x and y tie at 3; ties break by value.
	assert.Equal(t, "x", c.TopValues[0].Value)
	assert.Equal(t, "y", c.TopValues[1].Value)
	assert.InDelta(t, 3.0*100/7, c.TopValues[0].Pct, 1e-9)
}

func TestSet_ColumnCap(t *testing.T) {
	t.Parallel()

	set := profile.NewSet(profile.Config{})
	rec := record.Record{}
	for i := range profile.MaxColumns + 10 {
		name := string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+i/676))
		rec = rec.Append(name, record.Int64(1, record.SemanticNone))
	}
	feed(t, set, rec)

	p := set.Merge()
	assert.Len(t, p.Columns, profile.MaxColumns)
	assert.True(t, p.ColumnsTruncated)
}

func BenchmarkCollector_WarmDistinctSet(b *testing.B) {
	set := profile.NewSet(profile.Config{})
	c, err := set.NewCollector()
	require.NoError(b, err)

	batch := make([]record.Record, 512)
	vals := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta")}
	for i := range batch {
		batch[i] = record.Record{}.
			Append("id", record.Int64(int64(i%64), record.SemanticNone)).
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
