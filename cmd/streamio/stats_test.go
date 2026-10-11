package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const statsCLIFixture = "id,country,note\n1,US,a\n2,US,\n3,GR,b\n4,,c\n"

func writeStatsFixture(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "in.csv")
	if err := os.WriteFile(path, []byte(statsCLIFixture), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunStats_Table(t *testing.T) {
	var out bytes.Buffer
	err := runStats(context.Background(), &out, statsFlags{
		in: writeStatsFixture(t), output: "table", top: 5, csvHasHeader: true, maxValueWidth: 40,
	})
	if err != nil {
		t.Fatalf("runStats: %v", err)
	}

	got := out.String()
	for _, want := range []string{
		"records:", "4",
		"ATTRIBUTE", "NON-EMPTY",
		"country", "75.0%",
		"top values of country:",
		"US", "50.0%",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("table output missing %q:\n%s", want, got)
		}
	}
}

func TestRunStats_JSON(t *testing.T) {
	var out bytes.Buffer
	err := runStats(context.Background(), &out, statsFlags{
		in: writeStatsFixture(t), output: "json", top: 1, csvHasHeader: true, columns: []string{"country"},
	})
	if err != nil {
		t.Fatalf("runStats: %v", err)
	}

	var got struct {
		Columns []struct {
			Name      string `json:"name"`
			TopValues []struct {
				Value string `json:"value"`
				Count int64  `json:"count"`
			} `json:"top_values"`
			NonEmpty int64 `json:"non_empty"`
		} `json:"columns"`
		Records int64 `json:"records"`
	}
	if decodeErr := json.Unmarshal(out.Bytes(), &got); decodeErr != nil {
		t.Fatalf("decoding output: %v\n%s", decodeErr, out.String())
	}

	if got.Records != 4 {
		t.Errorf("records = %d, want 4", got.Records)
	}
	if len(got.Columns) != 1 || got.Columns[0].Name != "country" {
		t.Fatalf("columns = %+v, want only country", got.Columns)
	}
	c := got.Columns[0]
	if c.NonEmpty != 3 {
		t.Errorf("non_empty = %d, want 3", c.NonEmpty)
	}
	if len(c.TopValues) != 1 || c.TopValues[0].Value != "US" || c.TopValues[0].Count != 2 {
		t.Errorf("top_values = %+v, want [US x2]", c.TopValues)
	}
}

func TestRunStats_RejectsBadFlags(t *testing.T) {
	in := writeStatsFixture(t)

	var out bytes.Buffer
	err := runStats(context.Background(), &out, statsFlags{in: in, output: "yaml"})
	if !errors.Is(err, errUnknownStatsOutput) {
		t.Errorf("--output yaml: err = %v, want errUnknownStatsOutput", err)
	}

	err = runStats(context.Background(), &out, statsFlags{in: in, output: "table", top: -1})
	if err == nil {
		t.Error("--top -1: want an error")
	}

	err = runStats(context.Background(), &out, statsFlags{in: in, output: "table", inFormat: "xml"})
	if !errors.Is(err, errUnknownFormat) {
		t.Errorf("--in-format xml: err = %v, want errUnknownFormat", err)
	}
}

func TestTruncate(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
		n    int
	}{
		{"short", "short", 10},
		{"exactlyten", "exactlyten", 10},
		{"abcdefghijk", "abcdefghi…", 10},
		//nolint:gosmopolitan // Deliberately non-ASCII: truncation must count characters, not bytes.
		{"日本語日本語", "日本…", 3},
		{"anything", "anything", 0},
	} {
		if got := truncate(tc.in, tc.n); got != tc.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tc.in, tc.n, got, tc.want)
		}
	}
}

func TestRunStats_GzippedAndMislabeledInput(t *testing.T) {
	dir := t.TempDir()

	var zipped bytes.Buffer
	zw := gzip.NewWriter(&zipped)
	if _, err := zw.Write([]byte(statsCLIFixture)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	// CSV, gzipped, under a name that suggests neither.
	in := filepath.Join(dir, "data.bin")
	if err := os.WriteFile(in, zipped.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	err := runStats(context.Background(), &out, statsFlags{
		in: in, output: "table", top: 3, csvHasHeader: true, maxValueWidth: 40,
	})
	if err != nil {
		t.Fatalf("runStats: %v", err)
	}
	if !strings.Contains(out.String(), "75.0%") {
		t.Errorf("table output missing the country fill rate:\n%s", out.String())
	}
}
