// Package sample decides, deterministically, which records a run keeps.
//
// Every decision is a pure function of a seed and a key: a record's hash fraction h in [0, 1) is
// compared with a threshold. Nothing is shared between workers and nothing depends on row order,
// worker count or input format, so the same data always yields the same kept set, and a lower rate
// keeps a subset of what a higher one keeps.
package sample

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/zeebo/xxh3"

	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/record"
)

// DefaultWeightColumn is the column name the CLI writes weights to.
const DefaultWeightColumn = "_sample_weight"

// MissingKeyPolicy says what happens to a record that has no value for any of its key columns.
//
// Hashing the key of such records would give them all one hash, so they would be kept or dropped
// together, which can swing a sample by thousands of rows.
type MissingKeyPolicy string

const (
	// MissingKeyRow samples the record independently, by hashing its whole content. It is the
	// default: a record with no key has nothing to join on, so independence costs nothing.
	MissingKeyRow MissingKeyPolicy = "row"
	// MissingKeyKeep keeps the record, outside the sample's rate.
	MissingKeyKeep MissingKeyPolicy = "keep"
	// MissingKeyDrop drops the record.
	MissingKeyDrop MissingKeyPolicy = "drop"
	// MissingKeyError fails the run, for pipelines that require every record to be keyed.
	MissingKeyError MissingKeyPolicy = "error"
)

// Spec describes a sampling run. Exactly one mode applies: Rate, Range, or Strata.
//
//nolint:govet // Fields are grouped by meaning, with their docs, rather than packed by size.
type Spec struct {
	// StratumRates gives each stratum value its own keep rate; values not listed use DefaultRate.
	StratumRates map[string]float64
	// Seed makes the choice reproducible; the same seed and key always select the same records.
	Seed string
	// StratifyBy is the column whose value picks a record's stratum, for strata mode. Dotted paths
	// address nested map entries.
	StratifyBy string
	// WeightColumn, when non-empty, adds a float column holding 1/rate for every kept record, so
	// analytics can re-weight a sample taken at unequal rates.
	WeightColumn string
	// Key lists the columns whose values are hashed. Empty means the whole record is hashed
	// independent of column order.
	Key []string
	// Rate keeps a record when its hash fraction is below Rate, in (0, 1].
	Rate float64
	// RangeLo and RangeHi keep a record when its hash fraction is in [RangeLo, RangeHi). Disjoint
	// ranges over one key give non-overlapping splits.
	RangeLo, RangeHi float64
	// DefaultRate is strata mode's rate for values not in StratumRates.
	DefaultRate float64
	// OnMissingKey says what happens to a record whose key columns are all missing, null or empty.
	// The zero value is MissingKeyRow.
	OnMissingKey MissingKeyPolicy
	// Count keeps exactly this many distinct keys, the ones with the smallest hashes. It needs a
	// first pass over the input.
	Count int
	// MinPerStratum keeps at least this many distinct keys of every stratum in strata mode,
	// whatever the stratum's rate. It needs a first pass over the input.
	MinPerStratum int
	// MaxStrata bounds how many distinct stratum values a first pass tracks; the rest share the
	// default rate. Zero means DefaultMaxStrata.
	MaxStrata int
	// HasRate, HasRange and HasStrata say which mode is selected, so a zero rate or range is an
	// error instead of being mistaken for "unset".
	HasRate, HasRange, HasStrata, HasCount bool
}

var (
	errNoMode       = errors.New("sampling needs a rate, a range, or strata")
	errManyModes    = errors.New("sampling takes one of rate, range, or strata")
	errBadRate      = errors.New("sampling rate must be in (0, 1]")
	errBadRange     = errors.New("sampling range must satisfy 0 <= lo < hi <= 1")
	errNoStratifyBy = errors.New("strata sampling needs a column to stratify by")
	errNoStrata     = errors.New("strata sampling needs stratum rates or a default rate")
	errKeyWithEmpty = errors.New("sampling key has an empty column name")
	errBadPolicy    = errors.New("unknown missing-key policy: want row, keep, drop, or error")
	// ErrMissingKey is returned under MissingKeyError for a record with no value for its key.
	ErrMissingKey = errors.New("sampling key is missing")

	errBadCount         = errors.New("sampling count must be at least 1")
	errBadMin           = errors.New("min per stratum must not be negative")
	errMinWithoutStrata = errors.New("min per stratum needs strata mode")
	errTooManyHashes    = errors.New("min per stratum would track too many hashes; lower it or the strata limit")
	errNotPrepared      = errors.New("sampling was not prepared: its first pass did not run")
)

// Validate reports a problem that would make the spec ambiguous or unusable.
func (s Spec) Validate() error {
	modes := 0
	for _, on := range []bool{s.HasRate, s.HasRange, s.HasStrata, s.HasCount} {
		if on {
			modes++
		}
	}
	switch {
	case modes == 0:
		return errNoMode
	case modes > 1:
		return errManyModes
	}

	for _, k := range s.Key {
		if k == "" {
			return errKeyWithEmpty
		}
	}
	switch s.OnMissingKey {
	case "", MissingKeyRow, MissingKeyKeep, MissingKeyDrop, MissingKeyError:
	default:
		return fmt.Errorf("%w: got %q", errBadPolicy, string(s.OnMissingKey))
	}

	switch {
	case s.HasRate:
		return s.validateRate()
	case s.HasRange:
		return s.validateRange()
	case s.HasCount:
		return s.validateCount()
	default:
		return s.validateStrata()
	}
}

func (s Spec) validateCount() error {
	if s.Count < 1 {
		return fmt.Errorf("%w: got %d", errBadCount, s.Count)
	}
	if s.MinPerStratum != 0 {
		return errMinWithoutStrata
	}
	return nil
}

func (s Spec) validateRate() error {
	if !(s.Rate > 0 && s.Rate <= 1) {
		return fmt.Errorf("%w: got %v", errBadRate, s.Rate)
	}
	return nil
}

func (s Spec) validateRange() error {
	if !(s.RangeLo >= 0 && s.RangeLo < s.RangeHi && s.RangeHi <= 1) {
		return fmt.Errorf("%w: got %v:%v", errBadRange, s.RangeLo, s.RangeHi)
	}
	return nil
}

func (s Spec) validateStrata() error {
	if s.StratifyBy == "" {
		return errNoStratifyBy
	}
	if s.MinPerStratum < 0 {
		return fmt.Errorf("%w: got %d", errBadMin, s.MinPerStratum)
	}
	if s.MinPerStratum > 0 {
		if cells := int64(s.maxStrata()) * int64(s.MinPerStratum); cells > maxTrackedHashes {
			return fmt.Errorf("%w: %d strata x %d is %d", errTooManyHashes, s.maxStrata(), s.MinPerStratum, cells)
		}
	}
	if len(s.StratumRates) == 0 && s.DefaultRate == 0 {
		return errNoStrata
	}
	for v, r := range s.StratumRates {
		if r < 0 || r > 1 {
			return fmt.Errorf("%w: stratum %q has %v", errBadRate, v, r)
		}
	}
	if s.DefaultRate < 0 || s.DefaultRate > 1 {
		return fmt.Errorf("%w: default has %v", errBadRate, s.DefaultRate)
	}
	return nil
}

// DefaultMaxStrata is how many distinct stratum values a first pass tracks when Spec.MaxStrata is
// zero.
const DefaultMaxStrata = 10_000

// maxTrackedHashes bounds the hashes a first pass may hold across all strata (about 16 bytes each).
const maxTrackedHashes = 20_000_000

func (s Spec) maxStrata() int {
	if s.MaxStrata > 0 {
		return s.MaxStrata
	}
	return DefaultMaxStrata
}

// hashBits is the width of the hash a fraction is scaled to.
const hashBits = 64

// splitmix64's finalizer constants.
const (
	mixShift1 = 30
	mixShift2 = 27
	mixShift3 = 31
	mixMul1   = 0xbf58476d1ce4e5b9
	mixMul2   = 0x94d049bb133111eb
)

// threshold converts a rate into the largest hash value kept, as a half-open bound: a record is
// kept when h < bound, and bound == 0 together with all == false means keep nothing.
type threshold struct {
	bound uint64
	all   bool
}

func rateThreshold(rate float64) threshold {
	if rate >= 1 {
		return threshold{all: true}
	}
	if rate <= 0 {
		return threshold{}
	}
	// rate * 2^64, which fits in a uint64 for rate < 1.
	return threshold{bound: uint64(math.Ldexp(rate, hashBits))}
}

func (t threshold) keeps(h uint64) bool {
	return t.all || h < t.bound
}

// compiled is a Spec resolved once, shared read-only by every worker's filter.
type compiled struct {
	strata       map[string]threshold
	weights      map[string]float64
	weightCol    string
	onMissing    MissingKeyPolicy
	stratifyPath []string
	keyPaths     [][]string
	keyNames     []string
	hi           threshold
	rate         threshold
	lo           threshold
	defaultT     threshold
	countT       threshold
	defaultW     float64
	seed         uint64
	weight       float64
	count        int
	minPer       int
	maxStrata    int
	mode         mode
	// prepared is set once a first pass has resolved the count or strata thresholds.
	prepared bool
}

type mode uint8

const (
	modeRate mode = iota
	modeRange
	modeStrata
	modeCount
)

func seedOf(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

func weightOf(rate float64) float64 {
	if rate <= 0 {
		return 0
	}
	return 1 / rate
}

func compile(s Spec) (*compiled, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	c := &compiled{seed: seedOf(s.Seed), weightCol: s.WeightColumn, onMissing: s.OnMissingKey}
	if c.onMissing == "" {
		c.onMissing = MissingKeyRow
	}
	for _, k := range s.Key {
		c.keyPaths = append(c.keyPaths, strings.Split(k, "."))
		c.keyNames = append(c.keyNames, k)
	}
	switch {
	case s.HasRate:
		c.mode = modeRate
		c.rate = rateThreshold(s.Rate)
		c.weight = weightOf(s.Rate)
	case s.HasRange:
		c.mode = modeRange
		c.lo = threshold{bound: uint64(math.Ldexp(s.RangeLo, hashBits))}
		c.hi = rateThreshold(s.RangeHi)
		c.weight = weightOf(s.RangeHi - s.RangeLo)
	case s.HasCount:
		c.mode = modeCount
		c.count = s.Count
	case s.HasStrata:
		c.mode = modeStrata
		c.minPer = s.MinPerStratum
		c.maxStrata = s.maxStrata()
		c.stratifyPath = strings.Split(s.StratifyBy, ".")
		c.strata = make(map[string]threshold, len(s.StratumRates))
		c.weights = make(map[string]float64, len(s.StratumRates))
		for v, r := range s.StratumRates {
			c.strata[v] = rateThreshold(r)
			c.weights[v] = weightOf(r)
		}
		c.defaultT = rateThreshold(s.DefaultRate)
		c.defaultW = weightOf(s.DefaultRate)
	}
	// Rate, range, and plain strata decide from the spec alone; count and min-per-stratum wait for a
	// first pass to resolve them.
	c.prepared = c.mode != modeCount && c.minPer == 0
	return c, nil
}

// Option returns the pipeline options that apply spec: a row filter, a transform that adds the
// weight column when one is requested, and, for the modes that need a first pass, the preparer that
// runs it. It reports an invalid spec immediately.
func Option(s Spec) (options.Option, error) {
	if _, err := compile(s); err != nil {
		return nil, err
	}
	return func(cfg *options.Config) {
		// Compiled per application: a first pass resolves thresholds on it, and one Option value may
		// be applied to several runs.
		c, err := compile(s)
		if err != nil {
			return
		}
		if !c.prepared {
			cfg.Preparers = append(cfg.Preparers, options.PreparerSlot{
				Preparer:    &preparer{c: c},
				FilterIndex: len(cfg.RowFilters),
			})
		}
		cfg.RowFilters = append(cfg.RowFilters, func() (options.RowFilter, error) {
			return &filter{c: c}, nil
		})
		if c.weightCol != "" {
			cfg.RecordTransforms = append(cfg.RecordTransforms, func() (options.RecordTransform, error) {
				return &weighter{c: c}, nil
			})
		}
	}, nil
}

// filter is one worker's sampler; scratch is reused across records.
type filter struct {
	c       *compiled
	scratch []byte
	// missing counts records that had no value for any key column.
	missing int64
}

// MissingKeys reports how many records had no value for any of their key columns, so the run's
// Stats can surface a key that is not always present.
func (f *filter) MissingKeys() int64 { return f.missing }

// Keep reports whether rec is in the sample.
func (f *filter) Keep(rec record.Record) (bool, error) {
	if !f.c.prepared {
		return false, errNotPrepared
	}
	h, missing := f.hashOf(rec)
	if missing {
		f.missing++
		switch f.c.onMissing {
		case MissingKeyKeep:
			return true, nil
		case MissingKeyDrop:
			return false, nil
		case MissingKeyError:
			return false, fmt.Errorf("%w: every key column is empty (%s)", ErrMissingKey, strings.Join(f.c.keyNames, ", "))
		case MissingKeyRow:
			h = f.rowHash(rec)
		}
	}
	switch f.c.mode {
	case modeRate:
		return f.c.rate.keeps(h), nil
	case modeRange:
		return h >= f.c.lo.bound && f.c.hi.keeps(h), nil
	case modeStrata:
		t, _ := f.c.stratumThreshold(rec, &f.scratch)
		return t.keeps(h), nil
	case modeCount:
		return f.c.countT.keeps(h), nil
	}
	return false, nil
}

// keyMissing reports whether a keyed run's record has no value for any key column.
func (c *compiled) keyMissing(rec record.Record, scratch *[]byte) bool {
	if len(c.keyPaths) == 0 {
		return false
	}
	for _, path := range c.keyPaths {
		v, ok := lookupPath(rec, path)
		if !ok {
			continue
		}
		var text bool
		*scratch, text = v.AppendText((*scratch)[:0])
		if text {
			return false
		}
	}
	return true
}

// stratumThreshold returns the keep threshold and weight for rec's stratum.
func (c *compiled) stratumThreshold(rec record.Record, scratch *[]byte) (threshold, float64) {
	text := c.stratumText(rec, scratch)
	if t, found := c.strata[string(text)]; found {
		return t, c.weights[string(text)]
	}
	return c.defaultT, c.defaultW
}

// lookupPath finds a possibly nested field without allocating.
func lookupPath(rec record.Record, path []string) (record.Value, bool) {
	for i, seg := range path {
		var next record.Value
		found := false
		for j := range rec {
			if rec[j].Name == seg {
				next, found = rec[j].Value, true
				break
			}
		}
		if !found {
			return record.Value{}, false
		}
		if i == len(path)-1 {
			return next, true
		}
		if next.Kind != record.KindMap {
			return record.Value{}, false
		}
		rec = next.Map
	}
	return record.Value{}, false
}

// Separators keep ("ab","c") from hashing like ("a","bc").
const (
	sepField   = 0x1f
	sepMissing = 0x00
)

// hashOf returns the record's 64-bit sampling hash. missing is true when the record has a key
// configured but none of its key columns holds a value; the hash is then of no use for keying.
func (f *filter) hashOf(rec record.Record) (uint64, bool) {
	if len(f.c.keyPaths) > 0 {
		return f.keyHash(rec)
	}
	return f.rowHash(rec), false
}

// keyHash hashes the configured key columns' text, in the configured order. A column that is
// missing, null or empty contributes a marker, so a record with only some of its key columns is
// still keyed by the rest. missing is true when every key column was empty.
func (f *filter) keyHash(rec record.Record) (uint64, bool) {
	buf := f.scratch[:0]
	present := false
	for _, path := range f.c.keyPaths {
		buf = append(buf, sepField)
		v, ok := lookupPath(rec, path)
		if !ok {
			buf = append(buf, sepMissing)
			continue
		}
		var text bool
		buf, text = v.AppendText(buf)
		if text {
			present = true
		} else {
			buf = append(buf, sepMissing)
		}
	}
	f.scratch = buf
	return xxh3.HashSeed(buf, f.c.seed), !present
}

// rowHash sums a per-field hash over every non-empty field, so the result does not depend on
// column order, and null, empty and missing are the same. Nested maps contribute dotted paths.
func (f *filter) rowHash(rec record.Record) uint64 {
	var sum uint64
	f.scratch = f.scratch[:0]
	sum = f.addFields(rec, sum, 0)
	return mix(sum)
}

func (f *filter) addFields(rec record.Record, sum uint64, prefix int) uint64 {
	for i := range rec {
		fld := &rec[i]

		f.scratch = f.scratch[:prefix]
		f.scratch = append(f.scratch, fld.Name...)

		switch fld.Value.Kind {
		case record.KindMap:
			f.scratch = append(f.scratch, '.')
			sum = f.addFields(fld.Value.Map, sum, len(f.scratch))
			continue
		case record.KindList:
			if len(fld.Value.List) == 0 {
				continue
			}
			f.scratch = append(f.scratch, sepMissing, '[', 'l', 'i', 's', 't', ']')
		case record.KindNull, record.KindBool, record.KindInt64, record.KindFloat64, record.KindBytes:
			f.scratch = append(f.scratch, sepMissing)
			var ok bool
			f.scratch, ok = fld.Value.AppendText(f.scratch)
			if !ok {
				continue
			}
		}
		sum += xxh3.HashSeed(f.scratch, f.c.seed)
	}
	return sum
}

// mix is the splitmix64 finalizer: it spreads the bits of a sum of hashes evenly again.
func mix(x uint64) uint64 {
	x ^= x >> mixShift1
	x *= mixMul1
	x ^= x >> mixShift2
	x *= mixMul2
	x ^= x >> mixShift3
	return x
}

// weighter adds the sampling weight column to kept records.
type weighter struct {
	c       *compiled
	scratch []byte
}

var errWeightColumnExists = errors.New("sampling weight column already exists in the record")

// Apply appends the record's weight, 1/rate of the rate it was kept at.
func (w *weighter) Apply(rec record.Record, _ *record.Arena) (record.Record, error) {
	for i := range rec {
		if rec[i].Name == w.c.weightCol {
			return rec, fmt.Errorf("%w: %q", errWeightColumnExists, w.c.weightCol)
		}
	}

	weight := w.c.weight
	if w.c.onMissing == MissingKeyKeep && w.c.keyMissing(rec, &w.scratch) {
		// Kept regardless of the rate, so it stands for itself.
		return rec.Append(w.c.weightCol, record.Float64(1)), nil
	}
	if w.c.mode == modeStrata {
		_, weight = w.c.stratumThreshold(rec, &w.scratch)
	}
	return rec.Append(w.c.weightCol, record.Float64(weight)), nil
}
