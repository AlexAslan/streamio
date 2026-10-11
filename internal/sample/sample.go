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

// Spec describes a sampling run. Exactly one mode applies: Rate, Range, or Strata.
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
	// HasRate, HasRange and HasStrata say which mode is selected, so a zero rate or range is an
	// error instead of being mistaken for "unset".
	HasRate, HasRange, HasStrata bool
}

var (
	errNoMode       = errors.New("sampling needs a rate, a range, or strata")
	errManyModes    = errors.New("sampling takes one of rate, range, or strata")
	errBadRate      = errors.New("sampling rate must be in (0, 1]")
	errBadRange     = errors.New("sampling range must satisfy 0 <= lo < hi <= 1")
	errNoStratifyBy = errors.New("strata sampling needs a column to stratify by")
	errNoStrata     = errors.New("strata sampling needs stratum rates or a default rate")
	errKeyWithEmpty = errors.New("sampling key has an empty column name")
)

// Validate reports a problem that would make the spec ambiguous or unusable.
func (s Spec) Validate() error {
	modes := 0
	for _, on := range []bool{s.HasRate, s.HasRange, s.HasStrata} {
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

	switch {
	case s.HasRate:
		return s.validateRate()
	case s.HasRange:
		return s.validateRange()
	default:
		return s.validateStrata()
	}
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
	stratifyPath []string
	keyPaths     [][]string
	hi           threshold
	rate         threshold
	lo           threshold
	defaultT     threshold
	defaultW     float64
	seed         uint64
	weight       float64
	mode         mode
}

type mode uint8

const (
	modeRate mode = iota
	modeRange
	modeStrata
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

	c := &compiled{seed: seedOf(s.Seed), weightCol: s.WeightColumn}
	for _, k := range s.Key {
		c.keyPaths = append(c.keyPaths, strings.Split(k, "."))
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
	case s.HasStrata:
		c.mode = modeStrata
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
	return c, nil
}

// Option returns the pipeline options that apply spec: a row filter and, when a weight column is
// requested, a transform that adds it. It reports an invalid spec immediately.
func Option(s Spec) (options.Option, error) {
	c, err := compile(s)
	if err != nil {
		return nil, err
	}
	return func(cfg *options.Config) {
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
}

// Keep reports whether rec is in the sample.
func (f *filter) Keep(rec record.Record) (bool, error) {
	h := f.hash(rec)
	switch f.c.mode {
	case modeRate:
		return f.c.rate.keeps(h), nil
	case modeRange:
		return h >= f.c.lo.bound && f.c.hi.keeps(h), nil
	case modeStrata:
		t, _ := f.c.stratumThreshold(rec, &f.scratch)
		return t.keeps(h), nil
	}
	return false, nil
}

// stratumThreshold returns the keep threshold and weight for rec's stratum.
func (c *compiled) stratumThreshold(rec record.Record, scratch *[]byte) (threshold, float64) {
	v, ok := lookupPath(rec, c.stratifyPath)
	if ok {
		var text []byte
		text, ok = v.AppendText((*scratch)[:0])
		*scratch = text
		if ok {
			if t, found := c.strata[string(text)]; found {
				return t, c.weights[string(text)]
			}
		}
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

// hash returns the record's 64-bit sampling hash.
func (f *filter) hash(rec record.Record) uint64 {
	if len(f.c.keyPaths) > 0 {
		return f.keyHash(rec)
	}
	return f.rowHash(rec)
}

// keyHash hashes the configured key columns' text, in the configured order. A missing, null or
// empty column hashes the same way, which keeps formats with and without nulls aligned.
func (f *filter) keyHash(rec record.Record) uint64 {
	buf := f.scratch[:0]
	for _, path := range f.c.keyPaths {
		buf = append(buf, sepField)
		v, ok := lookupPath(rec, path)
		if !ok {
			buf = append(buf, sepMissing)
			continue
		}
		var text bool
		buf, text = v.AppendText(buf)
		if !text {
			buf = append(buf, sepMissing)
		}
	}
	f.scratch = buf
	return xxh3.HashSeed(buf, f.c.seed)
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
	if w.c.mode == modeStrata {
		_, weight = w.c.stratumThreshold(rec, &w.scratch)
	}
	return rec.Append(w.c.weightCol, record.Float64(weight)), nil
}
