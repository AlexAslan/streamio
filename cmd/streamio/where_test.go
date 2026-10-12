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

func writeAges(t *testing.T) string {
	t.Helper()

	var sb strings.Builder
	for i := range 200 {
		fmt.Fprintf(&sb, "{\"id\":%d,\"age\":%d,\"country\":%q}\n", i, i%50, []string{"US", "GR"}[i%2])
	}
	path := filepath.Join(t.TempDir(), "in.ndjson")
	if err := os.WriteFile(path, []byte(sb.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunConvert_Where(t *testing.T) {
	in := writeAges(t)
	out := filepath.Join(t.TempDir(), "out.json")

	flags := convertFlags{in: in, out: out, where: whereFlags{expr: "age >= 40 and country = 'GR'"}}
	if err := runConvert(context.Background(), flags, "json"); err != nil {
		t.Fatalf("runConvert: %v", err)
	}

	// ages 40..49 occur 4 times each over 200 rows (i%50), GR is odd i: half of them.
	if got := countNDJSONLines(t, out); got != 20 {
		t.Errorf("kept %d rows, want 20", got)
	}
}

func TestRunConvert_WhereErrors(t *testing.T) {
	in := writeAges(t)
	out := filepath.Join(t.TempDir(), "out.json")

	err := runConvert(context.Background(), convertFlags{in: in, out: out, where: whereFlags{expr: "age >"}}, "json")
	if err == nil || !strings.Contains(err.Error(), "invalid where expression") {
		t.Errorf("bad expression: err = %v", err)
	}

	err = runConvert(context.Background(), convertFlags{in: in, out: out, where: whereFlags{strict: true}}, "json")
	if !errors.Is(err, errWhereStrictNeedsWhere) {
		t.Errorf("--where-strict alone: err = %v, want errWhereStrictNeedsWhere", err)
	}
}

func TestRunStats_Where(t *testing.T) {
	in := writeAges(t)

	var out bytes.Buffer
	err := runStats(context.Background(), &out, statsFlags{
		in: in, output: "json", top: 3, where: whereFlags{expr: "country = 'GR'"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"records": 100`) {
		t.Errorf("want 100 GR records:\n%s", out.String())
	}
}

func TestWarnPredicateMisses(t *testing.T) {
	var buf bytes.Buffer
	warnWhere(&buf, "age > 1", 0, nil)
	if buf.Len() != 0 {
		t.Errorf("no misses should warn nothing, got %q", buf.String())
	}

	warnWhere(&buf, "score > 50", 20, nil)
	for _, want := range []string{"20 comparisons", `"score > 50"`, "--where-strict"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("warning %q lacks %q", buf.String(), want)
		}
	}
}

func TestRunStats_WhereReportsTypeMisses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messy.ndjson")
	data := "{\"age\":30}\n{\"age\":\"n/a\"}\n{\"age\":\"unknown\"}\n"
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	err := runStats(context.Background(), &out, statsFlags{
		in: path, output: "json", top: 1, where: whereFlags{expr: "age > 18"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"predicate_misses": 2`) {
		t.Errorf("want 2 predicate misses:\n%s", out.String())
	}
}

func TestWhereOptions_EmptyExpressionIsAnErrorWhenGiven(t *testing.T) {
	// Given but empty (a shell variable that was unset): an error, not "no filter".
	if _, err := whereOptions(whereFlags{expr: "", set: true}); !errors.Is(err, errEmptyWhere) {
		t.Errorf("--where \"\": err = %v, want errEmptyWhere", err)
	}
	if _, err := whereOptions(whereFlags{expr: "   ", set: true}); !errors.Is(err, errEmptyWhere) {
		t.Errorf("--where \"   \": err = %v, want errEmptyWhere", err)
	}

	// Not given at all: no filter.
	opts, err := whereOptions(whereFlags{})
	if err != nil || opts != nil {
		t.Errorf("no --where: opts = %v err = %v, want none", opts, err)
	}
}

func TestRootCmd_WhereGivenButEmptyFails(t *testing.T) {
	in := writeAges(t)

	root := newRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"convert", "--in", in, "--out", filepath.Join(t.TempDir(), "o.json"), "--where", ""})

	err := root.Execute()
	if !errors.Is(err, errEmptyWhere) {
		t.Errorf("err = %v, want errEmptyWhere", err)
	}
}

func TestWarnWhere(t *testing.T) {
	var buf bytes.Buffer
	warnWhere(&buf, "age > 1", 0, nil)
	if buf.Len() != 0 {
		t.Errorf("nothing to report should print nothing, got %q", buf.String())
	}

	warnWhere(&buf, "nmae = 'x'", 0, []string{"nmae"})
	if !strings.Contains(buf.String(), `column "nmae"`) {
		t.Errorf("unseen column not reported: %q", buf.String())
	}
}

func TestRunConvert_WhereStrictFailureStillExplainsItself(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messy.ndjson")
	if err := os.WriteFile(path, []byte("{\"age\":\"n/a\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := runConvert(context.Background(), convertFlags{
		in: path, out: filepath.Join(t.TempDir(), "o.json"), where: whereFlags{expr: "age > 18", strict: true},
	}, "json")
	if err == nil || !strings.Contains(err.Error(), `column "age"`) {
		t.Errorf("err = %v, want one naming column age", err)
	}
}
