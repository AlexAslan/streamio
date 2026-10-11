//nolint:testpackage // Exercises the unexported bottomK and threshold helpers directly.
package sample

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/record"
)

func TestBottomK_MatchesBruteForce(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(1, 2))
	for _, k := range []int{1, 5, 100, 1000} {
		bk := newBottomK(k)
		var all []uint64
		for range 20_000 {
			// A small value space forces duplicates, which must count once.
			h := rng.Uint64() % 7919 * 1_000_003
			all = append(all, h)
			bk.add(h)
		}

		slices.Sort(all)
		all = slices.Compact(all)

		got, ok := bk.kth()
		require.True(t, ok, "k=%d", k)
		assert.Equal(t, all[k-1], got, "k=%d", k)
	}
}

func TestBottomK_FewerThanKDistinct(t *testing.T) {
	t.Parallel()

	bk := newBottomK(10)
	for i := range 100 {
		bk.add(uint64(i % 4))
	}
	_, ok := bk.kth()
	assert.False(t, ok)
}

func TestBottomK_MergeEqualsOneBottomK(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(3, 4))
	whole := newBottomK(50)
	parts := []*bottomK{newBottomK(50), newBottomK(50), newBottomK(50)}
	for i := range 30_000 {
		h := rng.Uint64()
		whole.add(h)
		parts[i%3].add(h)
	}

	merged := newBottomK(50)
	for _, p := range parts {
		merged.merge(p)
	}

	want, _ := whole.kth()
	got, ok := merged.kth()
	require.True(t, ok)
	assert.Equal(t, want, got)
}

func TestThresholdHelpers(t *testing.T) {
	t.Parallel()

	assert.True(t, boundAbove(^uint64(0)).all)
	assert.True(t, boundAbove(10).keeps(10))
	assert.False(t, boundAbove(10).keeps(11))
	assert.InDelta(t, 0.5, threshold{bound: 1 << 63}.fraction(), 1e-12)
	assert.InDelta(t, 1.0, threshold{all: true}.fraction(), 1e-12)
	assert.True(t, maxThreshold(threshold{bound: 5}, threshold{all: true}).all)
	assert.Equal(t, uint64(9), maxThreshold(threshold{bound: 5}, threshold{bound: 9}).bound)
}

// runPrepared applies spec to a fresh config, runs its preparer over recs as one worker would, and
// returns the filter and (when configured) weight transform for the main run.
//
//nolint:ireturn // The pool hands a worker these interfaces, which is what is under test.
func runPrepared(
	t *testing.T,
	spec Spec,
	recs []record.Record,
) (options.RowFilter, options.RecordTransform) {
	t.Helper()

	opt, err := Option(spec)
	require.NoError(t, err)
	var cfg options.Config
	opt(&cfg)

	for _, slot := range cfg.Preparers {
		err = slot.Preparer.Prepare(context.Background(), func(newConsumer func() (options.RecordConsumer, error)) error {
			c, cerr := newConsumer()
			if cerr != nil {
				return cerr
			}
			return c.Consume(recs)
		})
		require.NoError(t, err)
	}

	f, err := cfg.RowFilters[0]()
	require.NoError(t, err)
	var tr options.RecordTransform
	if len(cfg.RecordTransforms) > 0 {
		tr, err = cfg.RecordTransforms[0]()
		require.NoError(t, err)
	}
	return f, tr
}

func idRows(n int) []record.Record {
	recs := make([]record.Record, n)
	for i := range recs {
		recs[i] = record.Record{}.Append("id", record.Int64(int64(i), record.SemanticNone))
	}
	return recs
}

func TestCount_KeepsExactlyNDistinctKeys(t *testing.T) {
	t.Parallel()

	recs := idRows(20_000)
	for _, n := range []int{1, 7, 500, 5000} {
		f, _ := runPrepared(t, Spec{Seed: "s", Key: []string{"id"}, HasCount: true, Count: n}, recs)

		kept := 0
		for _, r := range recs {
			ok, err := f.Keep(r)
			require.NoError(t, err)
			if ok {
				kept++
			}
		}
		assert.Equal(t, n, kept, "count %d", n)
	}
}

func TestCount_FewerKeysThanRequestedKeepsAll(t *testing.T) {
	t.Parallel()

	recs := idRows(30)
	f, _ := runPrepared(t, Spec{Seed: "s", Key: []string{"id"}, HasCount: true, Count: 100}, recs)
	for _, r := range recs {
		ok, _ := f.Keep(r)
		assert.True(t, ok)
	}
}

func TestCount_DuplicateKeysCountOnce(t *testing.T) {
	t.Parallel()

	// 100 distinct keys, each repeated 50 times: asking for 10 keys keeps 10*50 rows.
	recs := make([]record.Record, 0, 100*50)
	for i := range 100 {
		for range 50 {
			recs = append(recs, record.Record{}.Append("id", record.Int64(int64(i), record.SemanticNone)))
		}
	}
	f, _ := runPrepared(t, Spec{Seed: "s", Key: []string{"id"}, HasCount: true, Count: 10}, recs)

	kept := 0
	for _, r := range recs {
		if ok, _ := f.Keep(r); ok {
			kept++
		}
	}
	assert.Equal(t, 10*50, kept)
}

func TestCount_WeightEstimatesTheInverseFraction(t *testing.T) {
	t.Parallel()

	recs := idRows(10_000)
	_, tr := runPrepared(t,
		Spec{Seed: "s", Key: []string{"id"}, HasCount: true, Count: 1000, WeightColumn: "w"}, recs)

	var arena record.Arena
	out, err := tr.Apply(recs[0], &arena)
	require.NoError(t, err)
	w, _ := out.Lookup("w")
	assert.InDelta(t, 10, w.F64, 1.5, "1000 of 10000 keys is a weight near 10")
}

func TestUnpreparedFilterErrorsInsteadOfSamplingWrong(t *testing.T) {
	t.Parallel()

	opt, err := Option(Spec{Seed: "s", HasCount: true, Count: 5})
	require.NoError(t, err)
	var cfg options.Config
	opt(&cfg)

	f, err := cfg.RowFilters[0]()
	require.NoError(t, err)
	_, err = f.Keep(idRows(1)[0])
	require.ErrorIs(t, err, errNotPrepared)
}

func tierRows() []record.Record {
	recs := make([]record.Record, 0, 20_043)
	add := func(tier string, n int) {
		for i := range n {
			recs = append(recs, record.Record{}.
				Append("id", str(tier+"-"+strconv.Itoa(i))).
				Append("tier", str(tier)))
		}
	}
	add("common", 20_000)
	add("rare", 40)
	add("tiny", 3)
	return recs
}

func TestMinPerStratum_RareStrataSurviveALowRate(t *testing.T) {
	t.Parallel()

	recs := tierRows()
	spec := Spec{
		Seed: "s", Key: []string{"id"}, HasStrata: true, StratifyBy: "tier",
		DefaultRate: 0.001, MinPerStratum: 25, MaxStrata: 100,
	}
	f, _ := runPrepared(t, spec, recs)

	kept := map[string]int{}
	for _, r := range recs {
		if ok, _ := f.Keep(r); ok {
			v, _ := r.Lookup("tier")
			kept[string(v.Str)]++
		}
	}

	assert.GreaterOrEqual(t, kept["common"], 25, "a big stratum still keeps its minimum")
	assert.Less(t, kept["common"], 200, "but not much more than its rate gives")
	assert.Equal(t, 25, kept["rare"], "a rare stratum keeps exactly the minimum")
	assert.Equal(t, 3, kept["tiny"], "a stratum smaller than the minimum is kept whole")
}

func TestMinPerStratum_ExplicitRatesAreAFloor(t *testing.T) {
	t.Parallel()

	recs := tierRows()
	spec := Spec{
		Seed: "s", Key: []string{"id"}, HasStrata: true, StratifyBy: "tier",
		StratumRates: map[string]float64{"common": 0.5}, DefaultRate: 0.001, MinPerStratum: 5,
	}
	f, _ := runPrepared(t, spec, recs)

	kept := 0
	for _, r := range recs {
		v, _ := r.Lookup("tier")
		if string(v.Str) != "common" {
			continue
		}
		if ok, _ := f.Keep(r); ok {
			kept++
		}
	}
	assert.InDelta(t, 10_000, kept, 400)
}

func TestMinPerStratum_WeightsReflectTheEffectiveRate(t *testing.T) {
	t.Parallel()

	recs := tierRows()
	spec := Spec{
		Seed: "s", Key: []string{"id"}, HasStrata: true, StratifyBy: "tier",
		DefaultRate: 0.001, MinPerStratum: 25, WeightColumn: "w",
	}
	_, tr := runPrepared(t, spec, recs)

	var arena record.Arena
	weightOf := func(tier string) float64 {
		for _, r := range recs {
			v, _ := r.Lookup("tier")
			if string(v.Str) == tier {
				out, err := tr.Apply(r, &arena)
				require.NoError(t, err)
				w, _ := out.Lookup("w")
				return w.F64
			}
		}
		return 0
	}

	assert.InDelta(t, 1, weightOf("tiny"), 1e-9, "a stratum kept whole weighs 1")
	assert.Greater(t, weightOf("common"), 100.0)
}

func TestSpecValidate_CountAndMin(t *testing.T) {
	t.Parallel()

	for name, spec := range map[string]Spec{
		"count zero":       {HasCount: true},
		"count with min":   {HasCount: true, Count: 5, MinPerStratum: 2},
		"negative min":     {HasStrata: true, StratifyBy: "t", DefaultRate: 0.1, MinPerStratum: -1},
		"min over the cap": {HasStrata: true, StratifyBy: "t", DefaultRate: 0.1, MinPerStratum: 1 << 20, MaxStrata: 1 << 20},
		"count and rate":   {HasCount: true, Count: 5, HasRate: true, Rate: 0.5},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, spec.Validate())
		})
	}
	require.NoError(t, Spec{HasCount: true, Count: 5}.Validate())
}

func BenchmarkBottomK_Add(b *testing.B) {
	rng := rand.New(rand.NewPCG(5, 6))
	hashes := make([]uint64, 1<<16)
	for i := range hashes {
		hashes[i] = rng.Uint64()
	}

	for _, k := range []int{100, 100_000} {
		b.Run(fmt.Sprintf("k=%d", k), func(b *testing.B) {
			bk := newBottomK(k)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				bk.add(hashes[i&(len(hashes)-1)])
			}
		})
	}
}

func str(s string) record.Value { return record.Bytes([]byte(s)) }
