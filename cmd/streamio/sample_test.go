package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseFraction(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want float64
	}{{"0.01", 0.01}, {"1%", 0.01}, {"100%", 1}, {" 12.5% ", 0.125}, {"1", 1}} {
		got, err := parseFraction(tc.in)
		if err != nil {
			t.Errorf("parseFraction(%q): %v", tc.in, err)
			continue
		}
		if diff := got - tc.want; diff > 1e-12 || diff < -1e-12 {
			t.Errorf("parseFraction(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
	for _, in := range []string{"", "abc", "%", "1x%"} {
		if _, err := parseFraction(in); !errors.Is(err, errBadFraction) {
			t.Errorf("parseFraction(%q) err = %v, want errBadFraction", in, err)
		}
	}
}

func TestParseRange(t *testing.T) {
	lo, hi, err := parseRange("0.8:100%")
	if err != nil || lo != 0.8 || hi != 1 {
		t.Errorf("parseRange = %v, %v, %v; want 0.8, 1, nil", lo, hi, err)
	}
	for _, in := range []string{"0.8", "a:b", "0.1:0.2:0.3", ""} {
		if _, _, rangeErr := parseRange(in); !errors.Is(rangeErr, errBadRangeFlag) {
			t.Errorf("parseRange(%q) err = %v, want errBadRangeFlag", in, rangeErr)
		}
	}
}

func TestParseStratumRates(t *testing.T) {
	rates, def, err := parseStratumRates("ERROR=100%, WARN=10%,default=0.5%")
	if err != nil {
		t.Fatal(err)
	}
	if rates["ERROR"] != 1 || rates["WARN"] != 0.1 || def != 0.005 || len(rates) != 2 {
		t.Errorf("rates = %v default = %v", rates, def)
	}
	for _, in := range []string{"ERROR", "=5%", "A=x"} {
		if _, _, ratesErr := parseStratumRates(in); !errors.Is(ratesErr, errBadStratumRate) {
			t.Errorf("parseStratumRates(%q) err = %v, want errBadStratumRate", in, ratesErr)
		}
	}
}

func TestSampleOptions_RejectsInconsistentFlags(t *testing.T) {
	for name, tc := range map[string]struct {
		want  error
		flags sampleFlags
	}{
		"two modes":            {flags: sampleFlags{rate: "10%", rng: "0:0.5"}, want: errSampleFlagsMode},
		"key without mode":     {flags: sampleFlags{key: []string{"id"}}, want: errSampleNeedsMode},
		"weight without mode":  {flags: sampleFlags{weight: true}, want: errSampleNeedsMode},
		"stratum rate no col":  {flags: sampleFlags{stratumRate: "A=1"}, want: errStratumNeedsCol},
		"stratify and rate":    {flags: sampleFlags{stratifyBy: "level", rate: "5%"}, want: errSampleFlagsMode},
		"bad rate":             {flags: sampleFlags{rate: "lots"}, want: errBadFraction},
		"bad range":            {flags: sampleFlags{rng: "x"}, want: errBadRangeFlag},
		"bad stratum in rates": {flags: sampleFlags{stratifyBy: "level", stratumRate: "A"}, want: errBadStratumRate},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := sampleOptions(tc.flags); !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
		})
	}

	opts, err := sampleOptions(sampleFlags{})
	if err != nil || opts != nil {
		t.Errorf("no flags: opts = %v err = %v, want none", opts, err)
	}
}

func writeIDs(t *testing.T, n int) string {
	t.Helper()

	var sb strings.Builder
	for i := range n {
		fmt.Fprintf(&sb, "{\"id\":%d,\"level\":%q}\n", i, []string{"ERROR", "INFO", "INFO", "INFO"}[i%4])
	}
	path := filepath.Join(t.TempDir(), "in.ndjson")
	if err := os.WriteFile(path, []byte(sb.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunConvert_SampleKeepsAReproducibleSubset(t *testing.T) {
	in := writeIDs(t, 4000)
	dir := t.TempDir()
	flags := convertFlags{in: in, sample: sampleFlags{rate: "10%", key: []string{"id"}, seed: "x"}}

	var outs [2]string
	for i := range outs {
		flags.out = filepath.Join(dir, fmt.Sprintf("out%d.json", i))
		if err := runConvert(context.Background(), flags, "json"); err != nil {
			t.Fatalf("runConvert: %v", err)
		}
		outs[i] = flags.out
	}

	a, b := countNDJSONLines(t, outs[0]), countNDJSONLines(t, outs[1])
	if a != b {
		t.Errorf("two runs kept %d and %d rows, want the same", a, b)
	}
	if a < 300 || a > 500 {
		t.Errorf("kept %d of 4000 at 10%%, want about 400", a)
	}
}

func TestRunConvert_SampleRangesSplitTheData(t *testing.T) {
	in := writeIDs(t, 3000)
	dir := t.TempDir()

	total := 0
	for i, rng := range []string{"0:0.8", "0.8:1"} {
		out := filepath.Join(dir, fmt.Sprintf("part%d.json", i))
		flags := convertFlags{in: in, out: out, sample: sampleFlags{rng: rng, key: []string{"id"}, seed: "split"}}
		if err := runConvert(context.Background(), flags, "json"); err != nil {
			t.Fatalf("runConvert: %v", err)
		}
		total += countNDJSONLines(t, out)
	}
	if total != 3000 {
		t.Errorf("the two splits hold %d rows together, want all 3000 exactly once", total)
	}
}

func TestRunConvert_StratifiedSampleWithWeights(t *testing.T) {
	in := writeIDs(t, 4000)
	out := filepath.Join(t.TempDir(), "out.json")
	flags := convertFlags{in: in, out: out, sample: sampleFlags{
		stratifyBy: "level", stratumRate: "ERROR=100%,default=5%", key: []string{"id"}, seed: "w", weight: true,
	}}
	if err := runConvert(context.Background(), flags, "json"); err != nil {
		t.Fatalf("runConvert: %v", err)
	}

	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(data), `"level":"ERROR"`); got != 1000 {
		t.Errorf("kept %d ERROR rows, want all 1000", got)
	}
	if !strings.Contains(string(data), `"_sample_weight":20`) {
		t.Errorf("INFO rows should carry weight 20:\n%.300s", data)
	}
}

func TestRunStats_Sample(t *testing.T) {
	in := writeIDs(t, 2000)

	var full, sampled bytes.Buffer
	if err := runStats(context.Background(), &full, statsFlags{in: in, output: "json", top: 3}); err != nil {
		t.Fatal(err)
	}
	err := runStats(context.Background(), &sampled, statsFlags{
		in: in, output: "json", top: 3, sample: sampleFlags{rate: "10%", key: []string{"id"}, seed: "s"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(full.String(), `"records": 2000`) {
		t.Errorf("full profile should report 2000 records:\n%s", full.String())
	}
	if strings.Contains(sampled.String(), `"records": 2000`) {
		t.Errorf("sampled profile should report fewer records:\n%s", sampled.String())
	}
}

func TestSampleOptions_RateOutOfRangeIsReportedAsABadFraction(t *testing.T) {
	for _, rate := range []string{"0", "0%", "150%", "1.5", "-5%"} {
		_, err := sampleOptions(sampleFlags{rate: rate})
		if !errors.Is(err, errBadFraction) {
			t.Errorf("--sample %s: err = %v, want errBadFraction", rate, err)
		}
	}
	if _, err := sampleOptions(sampleFlags{rate: "100%"}); err != nil {
		t.Errorf("--sample 100%%: %v", err)
	}
}

func TestSampleOptions_MissingKeyPolicy(t *testing.T) {
	for _, policy := range []string{"", "row", "keep", "drop", "error"} {
		if _, err := sampleOptions(sampleFlags{rate: "10%", key: []string{"id"}, onMissing: policy}); err != nil {
			t.Errorf("policy %q: %v", policy, err)
		}
	}

	// A bad policy is reported when the options are applied, before any file is read.
	in := writeIDs(t, 10)
	flags := convertFlags{
		in: in, out: filepath.Join(t.TempDir(), "o.json"),
		sample: sampleFlags{rate: "10%", key: []string{"id"}, onMissing: "sometimes"},
	}
	if err := runConvert(context.Background(), flags, "json"); err == nil {
		t.Error("an unknown --sample-key-missing was accepted")
	}
}

func TestWarnMissingKey(t *testing.T) {
	var buf bytes.Buffer
	warnMissingKey(&buf, 0, sampleFlags{})
	if buf.Len() != 0 {
		t.Errorf("no missing keys should warn nothing, got %q", buf.String())
	}

	warnMissingKey(&buf, 12, sampleFlags{key: []string{"user_id"}})
	got := buf.String()
	for _, want := range []string{"12 rows", "user_id", "sampled independently"} {
		if !strings.Contains(got, want) {
			t.Errorf("warning %q lacks %q", got, want)
		}
	}

	buf.Reset()
	warnMissingKey(&buf, 3, sampleFlags{key: []string{"id"}, onMissing: "drop"})
	if !strings.Contains(buf.String(), "dropped") {
		t.Errorf("warning for drop = %q", buf.String())
	}
}
