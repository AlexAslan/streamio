package options_test

import (
	"strings"
	"testing"

	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

func TestPathTransformer_RenameAndDropNestedPaths(t *testing.T) {
	transformer, err := options.NewPathTransformer(
		options.RenamePath("Timestamp", "@timestamp"),
		options.RenamePath("attributes.service", "attributes.service_name"),
		options.DropPath("attributes.debug"),
		options.DropPath("TimestampTime"),
	)
	if err != nil {
		t.Fatalf("NewPathTransformer: %v", err)
	}

	in := record.Record{}.
		Append("Timestamp", record.Int64(123, record.SemanticNone)).
		Append("TimestampTime", record.Int64(456, record.SemanticNone)).
		Append("attributes", record.Map(record.Record{}.
			Append("service", record.Bytes([]byte("checkout"))).
			Append("debug", record.Bool(true)).
			Append("region", record.Bytes([]byte("eu")))))

	got, err := transformer.Transform(in)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}

	assertFieldNames(t, got, []string{"@timestamp", "attributes"})
	attrs := got[1].Value.Map
	assertFieldNames(t, attrs, []string{"service_name", "region"})
	if string(attrs[0].Value.Str) != "checkout" {
		t.Fatalf("renamed service value = %q, want checkout", attrs[0].Value.Str)
	}
}

func TestPathTransformer_LeavesMissingAndNonMapNestedPathsAlone(t *testing.T) {
	transformer, err := options.NewPathTransformer(
		options.RenamePath("attributes.service", "attributes.service_name"),
		options.DropPath("missing.field"),
	)
	if err != nil {
		t.Fatalf("NewPathTransformer: %v", err)
	}

	in := record.Record{}.
		Append("attributes", record.Bytes([]byte("not-a-map"))).
		Append("message", record.Bytes([]byte("hello")))

	got, err := transformer.Transform(in)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	assertFieldNames(t, got, []string{"attributes", "message"})
	if string(got[0].Value.Str) != "not-a-map" {
		t.Fatalf("attributes value changed to %q", got[0].Value.Str)
	}
}

func TestNewPathTransformer_RejectsInvalidPaths(t *testing.T) {
	tests := []struct {
		name string
		want string
		rule options.PathTransformRule
	}{
		{name: "empty source", rule: options.DropPath(""), want: "empty path"},
		{name: "empty source segment", rule: options.DropPath("a..b"), want: "empty path segment"},
		{name: "empty target", rule: options.RenamePath("a", ""), want: "empty path"},
		{name: "target depth", rule: options.RenamePath("a.b", "a.b.c"), want: "changes path depth"},
		{name: "target parent", rule: options.RenamePath("a.b", "c.b"), want: "changes path parent"},
		{name: "duplicate", rule: options.DropPath("a"), want: "duplicate transform rule"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rules := []options.PathTransformRule{tt.rule}
			if tt.name == "duplicate" {
				rules = []options.PathTransformRule{options.DropPath("a"), options.RenamePath("a", "b")}
			}
			_, err := options.NewPathTransformer(rules...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("NewPathTransformer error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func assertFieldNames(tb testing.TB, rec record.Record, want []string) {
	tb.Helper()
	if len(rec) != len(want) {
		tb.Fatalf("record has %d fields, want %d: %v", len(rec), len(want), rec)
	}
	for i := range want {
		if rec[i].Name != want[i] {
			tb.Fatalf("field %d = %q, want %q", i, rec[i].Name, want[i])
		}
	}
}
