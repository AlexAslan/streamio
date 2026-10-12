package main

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/AlexAslan/streamio"
	"github.com/AlexAslan/streamio/internal/sample"
)

const (
	percentScale   = 100
	rangeParts     = 2
	defaultStratum = "default"
)

var (
	errBadFraction     = errors.New("want a fraction in (0, 1] such as 0.01, or a percentage such as 1%")
	errBadRangeFlag    = errors.New("--sample-range wants lo:hi, each a fraction or percentage")
	errBadStratumRate  = errors.New("--stratum-rate wants VALUE=RATE pairs such as ERROR=100%,default=1%")
	errSampleFlagsMode = errors.New("--sample, --sample-range, --sample-n and --stratify select one sampling mode")
	errSampleNeedsMode = errors.New(
		"--sample-key, --sample-seed and --sample-weight need --sample, --sample-range or --stratify")
	errStratumNeedsCol  = errors.New("--stratum-rate needs --stratify")
	errMinNeedsStratify = errors.New("--min-per-stratum and --max-strata need --stratify")
)

// sampleFlags holds the sampling flags convert and stats share.
type sampleFlags struct {
	rate        string
	rng         string
	seed        string
	stratifyBy  string
	stratumRate string
	onMissing   string
	key         []string
	count       int
	minPer      int
	maxStrata   int
	weight      bool
}

func (f sampleFlags) any() bool {
	return f.rate != "" || f.rng != "" || f.stratifyBy != "" || f.stratumRate != "" ||
		len(f.key) > 0 || f.seed != "" || f.weight || f.onMissing != "" || f.count != 0 || f.minPer != 0 ||
		f.maxStrata != 0
}

// registerSampleFlags adds the sampling flags to cmd, bound to f.
func registerSampleFlags(cmd *cobra.Command, f *sampleFlags) {
	fl := cmd.Flags()
	fl.StringVar(&f.rate, "sample", "",
		"keep a deterministic fraction of records: 0.01 or 1% (the choice depends only on seed and key)")
	fl.IntVar(&f.count, "sample-n", 0,
		"keep exactly this many distinct keys, the ones with the smallest hashes (reads the input twice)")
	fl.IntVar(&f.minPer, "min-per-stratum", 0,
		"with --stratify, keep at least this many distinct keys of every stratum (reads the input twice)")
	fl.IntVar(&f.maxStrata, "max-strata", 0,
		"distinct stratum values --min-per-stratum tracks; the rest use the default rate (0 = 10000)")
	fl.StringVar(&f.rng, "sample-range", "",
		"keep records whose hash fraction is in [lo, hi), e.g. 0.8:1.0, for disjoint train/test splits")
	fl.StringSliceVar(&f.key, "sample-key", nil,
		"columns whose values decide a record's fate, so equal keys are kept together (default: whole record)")
	fl.StringVar(&f.seed, "sample-seed", "", "seed making sampling reproducible; set it explicitly")
	fl.StringVar(&f.stratifyBy, "stratify", "", "column whose value picks a stratum, used with --stratum-rate")
	fl.StringVar(&f.stratumRate, "stratum-rate", "",
		"per-stratum keep rates, e.g. ERROR=100%,WARN=10%,default=1% (values not listed use default)")
	fl.StringVar(&f.onMissing, "sample-key-missing", "",
		"what to do with a record whose --sample-key columns are all empty: row (default, sample it "+
			"independently by content), keep, drop, or error")
	fl.BoolVar(&f.weight, "sample-weight", false,
		"add a "+sample.DefaultWeightColumn+" column of 1/rate so a sample can be re-weighted")
}

// parseFraction reads "0.01" or "1%" as a fraction.
func parseFraction(s string) (float64, error) {
	s = strings.TrimSpace(s)
	scale := 1.0
	if strings.HasSuffix(s, "%") {
		s = strings.TrimSuffix(s, "%")
		scale = percentScale
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, fmt.Errorf("%w: got %q", errBadFraction, s)
	}
	return v / scale, nil
}

func parseRange(s string) (float64, float64, error) {
	parts := strings.Split(s, ":")
	if len(parts) != rangeParts {
		return 0, 0, fmt.Errorf("%w: got %q", errBadRangeFlag, s)
	}
	lo, err := parseFraction(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %w", errBadRangeFlag, err)
	}
	hi, err := parseFraction(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %w", errBadRangeFlag, err)
	}
	return lo, hi, nil
}

// parseStratumRates reads "A=10%,B=0.5,default=1%" into per-value rates and the default rate.
func parseStratumRates(s string) (map[string]float64, float64, error) {
	rates := make(map[string]float64)
	var def float64
	for _, pair := range strings.Split(s, ",") {
		name, val, ok := strings.Cut(pair, "=")
		name = strings.TrimSpace(name)
		if !ok || name == "" {
			return nil, 0, fmt.Errorf("%w: got %q", errBadStratumRate, pair)
		}
		r, err := parseFraction(val)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: %q: %w", errBadStratumRate, pair, err)
		}
		if name == defaultStratum {
			def = r
			continue
		}
		rates[name] = r
	}
	return rates, def, nil
}

// sampleOptions turns the flags into library options, or nil when none were given.
func sampleOptions(f sampleFlags) ([]streamio.Option, error) {
	if !f.any() {
		return nil, nil
	}
	if err := f.checkModes(); err != nil {
		return nil, err
	}

	spec := streamio.SamplingSpec{
		Seed: f.seed, Key: f.key, Count: f.count, MinPerStratum: f.minPer, MaxStrata: f.maxStrata,
		OnMissingKey: streamio.MissingKeyPolicy(f.onMissing),
	}
	if f.weight {
		spec.WeightColumn = sample.DefaultWeightColumn
	}
	if err := f.fillSelection(&spec); err != nil {
		return nil, err
	}

	return []streamio.Option{streamio.WithSampling(spec)}, nil
}

// checkModes rejects flag combinations that name no mode, several, or a modifier without its mode.
func (f sampleFlags) checkModes() error {
	// --sample beside --stratify is strata mode's base rate, not a second mode.
	rateIsMode := f.rate != "" && f.stratifyBy == ""

	modes := 0
	for _, on := range []bool{rateIsMode, f.rng != "", f.count != 0, f.stratifyBy != ""} {
		if on {
			modes++
		}
	}

	switch {
	case f.stratumRate != "" && f.stratifyBy == "":
		return errStratumNeedsCol
	case (f.minPer != 0 || f.maxStrata != 0) && f.stratifyBy == "":
		return errMinNeedsStratify
	case modes > 1:
		return errSampleFlagsMode
	case modes == 0:
		return errSampleNeedsMode
	default:
		return nil
	}
}

// fillSelection parses the rate, range and per-stratum flags into spec.
func (f sampleFlags) fillSelection(spec *streamio.SamplingSpec) error {
	if f.rate != "" {
		r, err := parseFraction(f.rate)
		if err == nil && (r <= 0 || r > 1) {
			err = fmt.Errorf("%w: got %q", errBadFraction, f.rate)
		}
		if err != nil {
			return fmt.Errorf("--sample: %w", err)
		}
		if f.stratifyBy != "" {
			spec.DefaultRate = r
		} else {
			spec.Rate = r
		}
	}

	if f.rng != "" {
		lo, hi, err := parseRange(f.rng)
		if err != nil {
			return err
		}
		spec.RangeLo, spec.RangeHi = lo, hi
	}

	if f.stratifyBy == "" {
		return nil
	}
	spec.StratifyBy = f.stratifyBy
	if f.stratumRate == "" {
		return nil
	}
	rates, def, err := parseStratumRates(f.stratumRate)
	if err != nil {
		return err
	}
	spec.StratumRates = rates
	if def != 0 || f.rate == "" {
		spec.DefaultRate = def
	}
	return nil
}

// warnMissingKey tells the user when records had no value for the sampling key, and what was done
// with them, since a key that is not always present quietly changes what a sample means.
func warnMissingKey(w io.Writer, n int64, f sampleFlags) {
	if n == 0 {
		return
	}

	policy := f.onMissing
	if policy == "" {
		policy = string(streamio.MissingKeyRow)
	}
	action := map[string]string{
		string(streamio.MissingKeyRow):  "sampled independently by their content",
		string(streamio.MissingKeyKeep): "kept",
		string(streamio.MissingKeyDrop): "dropped",
	}[policy]
	fmt.Fprintf(w, "warning: %d rows had no value for --sample-key (%s) and were %s\n",
		n, strings.Join(f.key, ", "), action)
}
