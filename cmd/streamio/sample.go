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
	errSampleFlagsMode = errors.New("--sample, --sample-range and --stratify select one sampling mode")
	errSampleNeedsMode = errors.New(
		"--sample-key, --sample-seed and --sample-weight need --sample, --sample-range or --stratify")
	errStratumNeedsCol = errors.New("--stratum-rate needs --stratify")
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
	weight      bool
}

func (f sampleFlags) any() bool {
	return f.rate != "" || f.rng != "" || f.stratifyBy != "" || f.stratumRate != "" ||
		len(f.key) > 0 || f.seed != "" || f.weight || f.onMissing != ""
}

// registerSampleFlags adds the sampling flags to cmd, bound to f.
func registerSampleFlags(cmd *cobra.Command, f *sampleFlags) {
	fl := cmd.Flags()
	fl.StringVar(&f.rate, "sample", "",
		"keep a deterministic fraction of records: 0.01 or 1% (the choice depends only on seed and key)")
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

	spec := streamio.SamplingSpec{
		Seed: f.seed, Key: f.key, OnMissingKey: streamio.MissingKeyPolicy(f.onMissing),
	}
	if f.weight {
		spec.WeightColumn = sample.DefaultWeightColumn
	}

	modes := 0
	if f.rate != "" {
		modes++
		r, err := parseFraction(f.rate)
		if err == nil && (r <= 0 || r > 1) {
			err = fmt.Errorf("%w: got %q", errBadFraction, f.rate)
		}
		if err != nil {
			return nil, fmt.Errorf("--sample: %w", err)
		}
		spec.Rate = r
	}
	if f.rng != "" {
		modes++
		lo, hi, err := parseRange(f.rng)
		if err != nil {
			return nil, err
		}
		spec.RangeLo, spec.RangeHi = lo, hi
	}
	if f.stratifyBy != "" {
		modes++
		spec.StratifyBy = f.stratifyBy
		if f.stratumRate != "" {
			rates, def, err := parseStratumRates(f.stratumRate)
			if err != nil {
				return nil, err
			}
			spec.StratumRates, spec.DefaultRate = rates, def
		}
	}

	switch {
	case f.stratumRate != "" && f.stratifyBy == "":
		return nil, errStratumNeedsCol
	case modes > 1:
		return nil, errSampleFlagsMode
	case modes == 0:
		return nil, errSampleNeedsMode
	}

	return []streamio.Option{streamio.WithSampling(spec)}, nil
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
