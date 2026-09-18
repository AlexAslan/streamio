package options_test

import (
	"fmt"
	"testing"

	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

// genTransformBenchRecord builds a record with n top-level scalar fields plus one nested
// "attributes" map with its own few fields — wide enough to exercise both the top-level rename/drop
// path and the one-level-deep map-recursion path transformRecord takes for a KindMap field.
func genTransformBenchRecord(n int) record.Record {
	rec := record.Record{}
	for i := range n {
		rec = rec.Append(fmt.Sprintf("field%d", i), record.Int64(int64(i), record.SemanticNone))
	}
	return rec.Append("attributes", record.Map(record.Record{}.
		Append("service", record.Bytes([]byte("checkout"))).
		Append("debug", record.Bool(true)).
		Append("region", record.Bytes([]byte("eu-west-1")))))
}

// BenchmarkTransformRecord_RenameAndDrop measures PathTransformer.Transform's per-record cost — the
// hook every decoded record passes through on the pool's decode→transform→encode path whenever
// WithTransforms is configured (internal/pool/records.go's transformBatch) — across record widths.
// Nothing measured this per-record hook's cost before; it's on the hot path for every configured
// run, and there is no "baseline" worth benchmarking alongside it: transformBatch skips the call
// entirely when no transformer is configured (a nil check), so the un-configured cost is exactly
// zero by construction, not something to measure.
func BenchmarkTransformRecord_RenameAndDrop(b *testing.B) {
	transformer, err := options.NewPathTransformer(
		options.RenamePath("field0", "renamed_field0"),
		options.RenamePath("attributes.service", "attributes.service_name"),
		options.DropPath("field1"),
		options.DropPath("attributes.debug"),
	)
	if err != nil {
		b.Fatalf("NewPathTransformer: %v", err)
	}

	fieldCounts := []int{4, 16, 64}
	for _, n := range fieldCounts {
		rec := genTransformBenchRecord(n)

		b.Run(fmt.Sprintf("fields=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, transformErr := transformer.Transform(rec); transformErr != nil {
					b.Fatalf("Transform: %v", transformErr)
				}
			}
		})
	}
}
