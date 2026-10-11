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
	warnPredicateMisses(&buf, 0, "age > 1")
	if buf.Len() != 0 {
		t.Errorf("no misses should warn nothing, got %q", buf.String())
	}

	warnPredicateMisses(&buf, 20, "score > 50")
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
