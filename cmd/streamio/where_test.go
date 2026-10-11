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
