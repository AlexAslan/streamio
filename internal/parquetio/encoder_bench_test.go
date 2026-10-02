package parquetio_test

import (
	"fmt"
	"testing"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/parquetio"
	"github.com/AlexAslan/streamio/internal/record"
)

// genEncoderBenchRows builds n representative records — a mix of int/float/bool/string columns,
// the same shape scalarBatch's typed row covers, so the encoder derives the same kind of schema a
// real NDJSON→Parquet conversion would.
func genEncoderBenchRows(n int) []record.Record {
	rows := make([]record.Record, n)
	for i := range rows {
		rows[i] = record.Record{}.
			Append("id", record.Int64(int64(i), record.SemanticNone)).
			Append("score", record.Float64(float64(i)*1.5)).
			Append("active", record.Bool(i%2 == 0)).
			Append("message", record.Bytes([]byte(fmt.Sprintf("row number %d, a representative log line", i))))
	}
	return rows
}

// newStreamingEncoderForBench builds a streaming encoder with the default options, failing the
// benchmark rather than returning an error nobody would check.
//
//nolint:ireturn // formatio.FinalizableRecordEncoder is what the constructor under test returns.
func newStreamingEncoderForBench(b *testing.B) formatio.FinalizableRecordEncoder {
	b.Helper()

	enc, err := parquetio.NewStreamingEncoder(options.New())
	if err != nil {
		b.Fatalf("NewStreamingEncoder: %v", err)
	}
	fin, ok := enc.(formatio.FinalizableRecordEncoder)
	if !ok {
		b.Fatalf("NewStreamingEncoder returned %T, want a formatio.FinalizableRecordEncoder", enc)
	}
	return fin
}

// BenchmarkEncoder_BatchVsStreaming compares NewEncoder (today's default: one complete, closed
// Parquet file per batch) against NewStreamingEncoder (one row group per batch, flushed into a
// single file that only becomes valid once Finalize runs) at the same batch sizes. Both are driven
// single-worker: streaming mode forces that in production (see pool.RunRecords's cfg.Workers = 1
// override for a formatio.FinalizableRecordEncoder), so a single-worker default encoder is the fair
// comparison rather than whatever concurrency the default encoder could otherwise use.
//
// The question this answers: does choosing single continuous-file output (the CLI's default for
// Parquet — see cmd/streamio/convert.go) cost anything over the existing default per-batch-file behavior.
func BenchmarkEncoder_BatchVsStreaming(b *testing.B) {
	batchSizes := []int{100, 1_000, 10_000}

	for _, batchSize := range batchSizes {
		batch := genEncoderBenchRows(batchSize)

		b.Run(fmt.Sprintf("batchSize=%d/default", batchSize), func(b *testing.B) {
			// Each batch is its own complete file under the default encoder, so a fresh encoder per
			// iteration matches how it's actually driven in production (one encoder per decode
			// worker, many batches) without the schema-reuse fast path masking a fixed one-time cost.
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				enc := newBatchEncoder(b)
				if _, err := enc.EncodeBatch(batch); err != nil {
					b.Fatalf("EncodeBatch: %v", err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/row")
		})

		b.Run(fmt.Sprintf("batchSize=%d/streaming", batchSize), func(b *testing.B) {
			// A fresh encoder per iteration too, so row-group metadata doesn't accumulate across
			// b.N iterations and skew steady-state per-call cost — that accumulation is exactly
			// what BenchmarkStreamingEncoder_MemoryBoundedness measures on purpose instead.
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				enc := newStreamingEncoderForBench(b)
				if _, err := enc.EncodeBatch(batch); err != nil {
					b.Fatalf("EncodeBatch: %v", err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/row")
		})
	}
}

// BenchmarkStreamingEncoder_MemoryBoundedness feeds NewStreamingEncoder an increasing number of
// batches at a few batch sizes — scaling total row count up to 100,000,000, the "does this still
// hold at real scale" size a 1GB+ single-file conversion actually needs — and separately reports
// the largest single chunk any EncodeBatch call ever returned ("max-flush-bytes") and the size of
// Finalize's own trailing document ("finalize-bytes").
//
// b.ReportAllocs()'s B/op is deliberately not used here: it is *cumulative* bytes allocated over
// the whole run, which necessarily grows with total output size regardless of design — you cannot
// return a 1GB file having cumulatively allocated zero bytes.
//
// The two metrics tell two different, both true, stories, which is why they're reported
// separately rather than folded into one "max chunk" number:
//   - max-flush-bytes is bounded by max(one batch's encoded size, the writer's small internal
//     write buffer) — not by a fixed constant, and not by total row/batch count. It stays flat
//     across growing *batch count* at a fixed batch size (the smallBatch cases below), but scales
//     with *batch size* itself (compare smallBatch's ~32KB to realisticBatch's several MB) since a
//     single large batch can produce more encoded bytes than the internal buffer holds before the
//     call's own Flush forces them out. Either way, it never scales with total row count for a
//     fixed batch size — which is the actual, more precisely stated, peak-memory-per-call bound
//     streaming_encoder.go's doc comment means by "bounded memory."
//   - finalize-bytes grows with the number of row groups (one per batch), since the Parquet footer
//     lists per-row-group column metadata for every row group in the file — that is inherent to the
//     Parquet format, not something this encoder could avoid.
//
// There is also a hard ceiling this benchmark deliberately does not exceed: parquet-go caps a file
// at MaxRowGroups (32,767, math.MaxInt16) row groups; EncodeBatch returns ErrTooManyRowGroups
// (wrapped) past that, failing the whole conversion outright rather than merely growing the
// footer. smallBatch's batch size (100) and count (10,000, so 10,000 row groups) stay comfortably
// under that ceiling; realisticBatch (1,000 row groups for the same 100,000,000-row total) is the
// sane choice for that scale and the one the CLI's default batch size approximates. Both facts are
// documented on streaming_encoder.go and in the CLI's --help, not just here.
func BenchmarkStreamingEncoder_MemoryBoundedness(b *testing.B) {
	cases := []struct {
		label      string
		batchSize  int
		batchCount int
	}{
		{label: "smallBatch", batchSize: 100, batchCount: 100},
		{label: "smallBatch", batchSize: 100, batchCount: 1_000},
		{label: "smallBatch", batchSize: 100, batchCount: 10_000},        // 1,000,000 rows, 10,000 row groups
		{label: "realisticBatch", batchSize: 100_000, batchCount: 1_000}, // 100,000,000 rows, 1,000 row groups
	}

	for _, tc := range cases {
		batch := genEncoderBenchRows(tc.batchSize)
		totalRows := tc.batchSize * tc.batchCount

		b.Run(fmt.Sprintf("%s/totalRows=%d", tc.label, totalRows), func(b *testing.B) {
			b.ResetTimer()

			var maxFlush, finalizeBytes int
			for range b.N {
				maxFlush, finalizeBytes = runStreamingEncoderOnce(b, batch, tc.batchCount)
			}

			b.ReportMetric(float64(maxFlush), "max-flush-bytes")
			b.ReportMetric(float64(finalizeBytes), "finalize-bytes")
		})
	}
}

// runStreamingEncoderOnce drives a fresh streaming encoder through batchCount copies of batch and
// Finalize, returning the largest single EncodeBatch chunk and the Finalize trailer's size.
func runStreamingEncoderOnce(b *testing.B, batch []record.Record, batchCount int) (int, int) {
	b.Helper()

	var maxFlush int
	enc := newStreamingEncoderForBench(b)
	for range batchCount {
		docs, err := enc.EncodeBatch(batch)
		if err != nil {
			b.Fatalf("EncodeBatch: %v", err)
		}
		for _, d := range docs {
			maxFlush = max(maxFlush, len(d))
		}
	}

	trailer, err := enc.Finalize()
	if err != nil {
		b.Fatalf("Finalize: %v", err)
	}
	return maxFlush, len(trailer)
}
