package sample_test

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/sample"
	"github.com/AlexAslan/streamio/record"
)

func str(s string) record.Value { return record.Bytes([]byte(s)) }

func rowFor(i int) record.Record {
	return record.Record{}.
		Append("id", record.Int64(int64(i), record.SemanticNone)).
		Append("name", str(fmt.Sprintf("user-%d", i))).
		Append("tier", str([]string{"free", "pro", "team"}[i%3]))
}

// filterFor builds one worker's filter (and transform, when a weight column is configured).
//
//nolint:ireturn // The interfaces are what the pool hands a worker, which is what is under test.
func filterFor(t *testing.T, spec sample.Spec) (options.RowFilter, options.RecordTransform) {
	t.Helper()

	opt, err := sample.Option(spec)
	require.NoError(t, err)
	var cfg options.Config
	opt(&cfg)

	f, err := cfg.RowFilters[0]()
	require.NoError(t, err)
	var tr options.RecordTransform
	if len(cfg.RecordTransforms) > 0 {
		tr, err = cfg.RecordTransforms[0]()
		require.NoError(t, err)
	}
	return f, tr
}

func keptSet(t *testing.T, spec sample.Spec, n int) map[int]bool {
	t.Helper()

	f, _ := filterFor(t, spec)
	kept := make(map[int]bool)
	for i := range n {
		ok, err := f.Keep(rowFor(i))
		require.NoError(t, err)
		if ok {
			kept[i] = true
		}
	}
	return kept
}

func rate(seed string, r float64, key ...string) sample.Spec {
	return sample.Spec{Seed: seed, Key: key, Rate: r, HasRate: true}
}

func TestSpecValidate(t *testing.T) {
	t.Parallel()

	for name, spec := range map[string]sample.Spec{
		"no mode":          {},
		"zero rate":        {HasRate: true},
		"rate above one":   {HasRate: true, Rate: 1.5},
		"two modes":        {HasRate: true, Rate: 0.5, HasRange: true, RangeLo: 0, RangeHi: 0.5},
		"empty range":      {HasRange: true, RangeLo: 0.5, RangeHi: 0.5},
		"reversed range":   {HasRange: true, RangeLo: 0.8, RangeHi: 0.2},
		"range past one":   {HasRange: true, RangeLo: 0, RangeHi: 1.2},
		"strata no column": {HasStrata: true, DefaultRate: 0.1},
		"strata no rates":  {HasStrata: true, StratifyBy: "tier"},
		"bad stratum rate": {HasStrata: true, StratifyBy: "tier", StratumRates: map[string]float64{"a": 2}},
		"empty key column": {HasRate: true, Rate: 0.5, Key: []string{"id", ""}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, spec.Validate())
		})
	}

	require.NoError(t, rate("s", 1).Validate())
	require.NoError(t, sample.Spec{HasRange: true, RangeLo: 0, RangeHi: 1}.Validate())
}

func TestRate_KeepsRoughlyThatFraction(t *testing.T) {
	t.Parallel()

	const n = 100_000
	for _, r := range []float64{0.01, 0.1, 0.5, 0.9} {
		for _, seed := range []string{"a", "b", "c"} {
			kept := len(keptSet(t, rate(seed, r, "id"), n))

			want := r * n
			sigma := math.Sqrt(n * r * (1 - r))
			assert.InDelta(t, want, float64(kept), 5*sigma, "rate %v seed %s", r, seed)
		}
	}
}

func TestRate_IsDeterministicAndSeedDependent(t *testing.T) {
	t.Parallel()

	a := keptSet(t, rate("seed-1", 0.2, "id"), 5000)
	b := keptSet(t, rate("seed-1", 0.2, "id"), 5000)
	c := keptSet(t, rate("seed-2", 0.2, "id"), 5000)

	assert.Equal(t, a, b)
	assert.NotEqual(t, a, c, "a different seed picks different records")
}

func TestRate_LowerRateIsASubsetOfHigher(t *testing.T) {
	t.Parallel()

	low := keptSet(t, rate("s", 0.05, "id"), 20_000)
	high := keptSet(t, rate("s", 0.25, "id"), 20_000)

	for i := range low {
		assert.True(t, high[i], "row %d kept at 5%% but not 25%%", i)
	}
	assert.Greater(t, len(high), len(low))
}

func TestRate_OneKeepsEverything(t *testing.T) {
	t.Parallel()

	assert.Len(t, keptSet(t, rate("s", 1, "id"), 1000), 1000)
}

func TestRanges_PartitionTheKeys(t *testing.T) {
	t.Parallel()

	const n = 20_000
	ranges := [][2]float64{{0, 0.7}, {0.7, 0.9}, {0.9, 1}}
	seen := make(map[int]int)
	for _, rg := range ranges {
		spec := sample.Spec{Seed: "s", Key: []string{"id"}, HasRange: true, RangeLo: rg[0], RangeHi: rg[1]}
		for i := range keptSet(t, spec, n) {
			seen[i]++
		}
	}

	assert.Len(t, seen, n, "every row lands in some split")
	for i, c := range seen {
		assert.Equal(t, 1, c, "row %d is in %d splits", i, c)
	}
}

func TestKey_SameKeySameDecisionAcrossRecords(t *testing.T) {
	t.Parallel()

	f, _ := filterFor(t, rate("s", 0.5, "id"))
	for i := range 2000 {
		a := record.Record{}.Append("id", record.Int64(int64(i), record.SemanticNone)).Append("v", str("x"))
		b := record.Record{}.Append("v", str("completely different")).Append("id", str(strconv.Itoa(i)))

		ka, _ := f.Keep(a)
		kb, _ := f.Keep(b)
		require.Equal(t, ka, kb, "key %d: number and text spelling, other columns, and order must not matter", i)
	}
}

func TestRowHash_IgnoresColumnOrderAndEmptiness(t *testing.T) {
	t.Parallel()

	f, _ := filterFor(t, rate("s", 0.5))
	for i := range 2000 {
		a := record.Record{}.Append("id", record.Int64(int64(i), record.SemanticNone)).Append("name", str("n"))
		b := record.Record{}.
			Append("name", str("n")).
			Append("extra", record.Null()).
			Append("blank", str("")).
			Append("id", str(strconv.Itoa(i)))

		ka, _ := f.Keep(a)
		kb, _ := f.Keep(b)
		require.Equal(t, ka, kb, "row %d", i)
	}
}

func TestRowHash_NestedMapsContributeDottedPaths(t *testing.T) {
	t.Parallel()

	f, _ := filterFor(t, rate("s", 0.5))
	differs := 0
	for i := range 500 {
		nested := record.Record{}.Append("attrs", record.Map(record.Record{}.Append("k", str(strconv.Itoa(i)))))
		flat := record.Record{}.Append("attrs.k", str(strconv.Itoa(i)))
		other := record.Record{}.Append("attrs", record.Map(record.Record{}.Append("k", str(strconv.Itoa(i+1)))))

		kn, _ := f.Keep(nested)
		kf, _ := f.Keep(flat)
		ko, _ := f.Keep(other)
		require.Equal(t, kn, kf, "attrs.k as a map entry or as a dotted column is the same field")
		if kn != ko {
			differs++
		}
	}
	assert.Positive(t, differs, "a different nested value should change decisions")
}

func TestStrata_PerValueRates(t *testing.T) {
	t.Parallel()

	spec := sample.Spec{
		Seed: "s", Key: []string{"id"}, HasStrata: true, StratifyBy: "tier",
		StratumRates: map[string]float64{"pro": 1, "free": 0.1}, DefaultRate: 0.5,
	}
	const n = 30_000
	kept := keptSet(t, spec, n)

	counts := map[string][2]int{}
	for i := range n {
		tier := []string{"free", "pro", "team"}[i%3]
		c := counts[tier]
		c[0]++
		if kept[i] {
			c[1]++
		}
		counts[tier] = c
	}

	assert.Equal(t, counts["pro"][0], counts["pro"][1], "pro is kept in full")
	assert.InDelta(t, 0.1*float64(counts["free"][0]), float64(counts["free"][1]), 150)
	assert.InDelta(t, 0.5*float64(counts["team"][0]), float64(counts["team"][1]), 250, "unlisted uses the default")
}

func TestWeight_IsInverseOfTheKeptRate(t *testing.T) {
	t.Parallel()

	spec := sample.Spec{
		Seed: "s", Key: []string{"id"}, HasStrata: true, StratifyBy: "tier",
		StratumRates: map[string]float64{"pro": 1, "free": 0.1}, DefaultRate: 0.5, WeightColumn: "w",
	}
	_, tr := filterFor(t, spec)
	require.NotNil(t, tr)

	var arena record.Arena
	for i, want := range []float64{10, 1, 2} { // free, pro, team
		rec, err := tr.Apply(rowFor(i), &arena)
		require.NoError(t, err)
		v, ok := rec.Lookup("w")
		require.True(t, ok)
		assert.InDelta(t, want, v.F64, 1e-9)
	}

	// A weight column that already exists is an error, not a silent duplicate.
	dup := rowFor(0).Append("w", record.Float64(1))
	_, err := tr.Apply(dup, &arena)
	require.Error(t, err)
}

func TestWeight_ForRateAndRange(t *testing.T) {
	t.Parallel()

	var arena record.Arena

	_, tr := filterFor(t, sample.Spec{Seed: "s", Rate: 0.25, HasRate: true, WeightColumn: "w"})
	rec, err := tr.Apply(rowFor(1), &arena)
	require.NoError(t, err)
	v, _ := rec.Lookup("w")
	assert.InDelta(t, 4, v.F64, 1e-9)

	_, tr = filterFor(t, sample.Spec{Seed: "s", HasRange: true, RangeLo: 0.5, RangeHi: 1, WeightColumn: "w"})
	rec, err = tr.Apply(rowFor(1), &arena)
	require.NoError(t, err)
	v, _ = rec.Lookup("w")
	assert.InDelta(t, 2, v.F64, 1e-9)
}

func TestFilter_AllocatesNothing(t *testing.T) {
	for name, spec := range map[string]sample.Spec{
		"key":    rate("s", 0.3, "id", "tier"),
		"row":    rate("s", 0.3),
		"nested": rate("s", 0.3, "attrs.k"),
		"strata": {Seed: "s", Key: []string{"id"}, HasStrata: true, StratifyBy: "tier", DefaultRate: 0.2},
	} {
		t.Run(name, func(t *testing.T) {
			f, _ := filterFor(t, spec)
			rec := rowFor(7).Append("attrs", record.Map(record.Record{}.Append("k", str("v"))))
			_, _ = f.Keep(rec) // warm the scratch

			allocs := testing.AllocsPerRun(1000, func() { _, _ = f.Keep(rec) })
			assert.Zero(t, allocs)
		})
	}
}

func BenchmarkFilter_Keep(b *testing.B) {
	for name, spec := range map[string]sample.Spec{
		"key": {Seed: "s", Key: []string{"id"}, Rate: 0.1, HasRate: true},
		"row": {Seed: "s", Rate: 0.1, HasRate: true},
	} {
		b.Run(name, func(b *testing.B) {
			opt, err := sample.Option(spec)
			require.NoError(b, err)
			var cfg options.Config
			opt(&cfg)
			f, err := cfg.RowFilters[0]()
			require.NoError(b, err)

			rec := rowFor(12345)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = f.Keep(rec)
			}
		})
	}
}
