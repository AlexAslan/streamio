package streamio

import (
	"fmt"

	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/sample"
)

// SamplingSpec describes which records a run keeps. Set exactly one of Rate, RangeHi, Count, or
// StratifyBy; the choice is a pure function of Seed and the record's key.
//
//nolint:govet // Fields are grouped by meaning, with their docs, rather than packed by size.
type SamplingSpec struct {
	// StratumRates gives each StratifyBy value its own keep rate; values not listed use
	// DefaultRate.
	StratumRates map[string]float64
	// Seed makes the choice reproducible: the same seed and key always select the same records.
	Seed string
	// StratifyBy names the column whose value picks a record's stratum. Dotted paths address
	// nested map entries.
	StratifyBy string
	// WeightColumn, when non-empty, adds a float column of 1/rate to every kept record, so a
	// sample taken at unequal rates can be re-weighted.
	WeightColumn string
	// Key lists the columns whose values decide a record's fate, so every record sharing a key is
	// kept or dropped together. Empty hashes the whole record, independent of column order.
	Key []string
	// Rate keeps a record when its hash fraction is below Rate, in (0, 1].
	Rate float64
	// RangeLo and RangeHi keep records whose hash fraction is in [RangeLo, RangeHi). Disjoint
	// ranges over one key give non-overlapping splits. Setting RangeHi selects range mode.
	RangeLo, RangeHi float64
	// DefaultRate is the keep rate in strata mode for values not in StratumRates.
	DefaultRate float64
	// OnMissingKey says what happens to a record with no value for any of its Key columns. The
	// default, MissingKeyRow, samples it independently by its whole content; hashing the missing
	// key would give every such record one hash and keep or drop them all together.
	OnMissingKey MissingKeyPolicy
	// Count keeps exactly this many distinct keys: the ones with the smallest hashes, or all of
	// them when there are fewer. It reads the input twice.
	Count int
	// MinPerStratum keeps at least this many distinct keys of every stratum in strata mode,
	// whatever the stratum's rate, so rare values are not lost. It reads the input twice.
	MinPerStratum int
	// MaxStrata bounds how many distinct stratum values MinPerStratum tracks; the rest use the
	// default rate. Zero means 10,000.
	MaxStrata int
}

// MissingKeyPolicy says what happens to a record with no value for any of its sampling key
// columns.
type MissingKeyPolicy = sample.MissingKeyPolicy

const (
	// MissingKeyRow samples the record independently, by hashing its whole content. This is the
	// default.
	MissingKeyRow = sample.MissingKeyRow
	// MissingKeyKeep keeps the record, outside the sample's rate.
	MissingKeyKeep = sample.MissingKeyKeep
	// MissingKeyDrop drops the record.
	MissingKeyDrop = sample.MissingKeyDrop
	// MissingKeyError fails the run, for pipelines that require every record to be keyed.
	MissingKeyError = sample.MissingKeyError
)

// ErrMissingKey is returned under MissingKeyError for a record with no value for its key columns.
var ErrMissingKey = sample.ErrMissingKey

// WithSampling keeps a deterministic subset of the input's records.
//
// A record's fate depends only on the seed and its key, so the same data yields the same sample for
// any worker count, input format or row order, and a lower rate keeps a subset of what a higher one
// keeps. Sampling runs on the decoded input record, before any transform. It makes a same-format
// conversion decode and re-encode instead of streaming raw bytes.
//
// Count and MinPerStratum need to see the data before deciding, so those runs read the input twice.
func WithSampling(spec SamplingSpec) Option {
	opt, err := sample.Option(spec.internal())
	if err != nil {
		return func(c *options.Config) {
			if c.OptionErr == nil {
				c.OptionErr = fmt.Errorf("streamio: invalid sampling: %w", err)
			}
		}
	}
	return opt
}

func (s SamplingSpec) internal() sample.Spec {
	return sample.Spec{
		StratumRates:  s.StratumRates,
		Seed:          s.Seed,
		StratifyBy:    s.StratifyBy,
		WeightColumn:  s.WeightColumn,
		Key:           s.Key,
		Rate:          s.Rate,
		RangeLo:       s.RangeLo,
		RangeHi:       s.RangeHi,
		DefaultRate:   s.DefaultRate,
		Count:         s.Count,
		MinPerStratum: s.MinPerStratum,
		MaxStrata:     s.MaxStrata,
		HasRate:       s.Rate > 0,
		HasRange:      s.RangeHi > 0,
		HasStrata:     s.StratifyBy != "",
		OnMissingKey:  s.OnMissingKey,
		HasCount:      s.Count > 0,
	}
}
