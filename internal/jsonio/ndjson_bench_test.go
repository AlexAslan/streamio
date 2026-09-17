package jsonio_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// generateNDJSON builds n JSON lines each of approximately docSize bytes.
func generateNDJSON(n, docSize int) []byte {
	// Fixed-size padding to hit target docSize.
	// Line format: {"id":NNNNNN,"msg":"xxx…xxx"}\n
	// overhead ≈ 17 bytes; pad the rest with 'x' characters.
	const overhead = 17
	padLen := docSize - overhead
	if padLen < 0 {
		padLen = 0
	}
	pad := strings.Repeat("x", padLen)

	line := fmt.Sprintf(`{"id":%06d,"msg":"%s"}`, 0, pad) + "\n"
	lineBytes := []byte(line)
	out := make([]byte, 0, len(lineBytes)*n)
	for range n {
		out = append(out, lineBytes...)
	}
	return out
}

func noopSink(context.Context, []byte) error { return nil }

// writeNDJSONFile writes data to a new file named name inside tb's temp dir and returns its path.
func writeNDJSONFile(tb testing.TB, name string, data []byte) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		tb.Fatalf("write ndjson file: %v", err)
	}
	return path
}

// BenchmarkNDJSONProcess measures single-worker Process throughput across doc sizes and buffer
// sizes.
func BenchmarkNDJSONProcess(b *testing.B) {
	type args struct {
		label      string
		docSize    int
		rowCount   int
		bufferSize int
	}
	cases := []args{
		{docSize: 200, rowCount: 10_000, bufferSize: 4 * 1024, label: "small-doc/small-buf"},
		{docSize: 200, rowCount: 10_000, bufferSize: 32 * 1024 * 1024, label: "small-doc/large-buf"},
		{docSize: 8_000, rowCount: 1_000, bufferSize: 32 * 1024 * 1024, label: "large-doc/large-buf"},
		{docSize: 32_000, rowCount: 500, bufferSize: 32 * 1024 * 1024, label: "very-large-doc"},
	}

	for _, tc := range cases {
		b.Run(tc.label, func(b *testing.B) {
			data := generateNDJSON(tc.rowCount, tc.docSize)
			path := writeNDJSONFile(b, "bench.ndjson", data)

			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()

			cfg := newConfig(1, 0, tc.bufferSize)
			for range b.N {
				if _, err := convertFile(context.Background(), path, noopSink, cfg); err != nil {
					b.Fatalf("Process: %v", err)
				}
			}
		})
	}
}

// writeSizedNdjsonFile writes an NDJSON file of at least targetBytes.
func writeSizedNdjsonFile(b *testing.B, targetBytes int) string {
	b.Helper()

	var buf bytes.Buffer
	for i := 0; buf.Len() < targetBytes; i++ {
		fmt.Fprintf(&buf, `{"i":%d,"label":"label-%08d","payload":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`+"\n", i, i)
	}
	return writeNDJSONFile(b, "large.ndjson", buf.Bytes())
}

// benchmarkChunkSize mirrors internal/suites/benchmark_http_loader.go's ndjsonChunkSize, sizing
// chunks to fileSize/workers (floored at 1MB) so `workers` chunks actually exist to claim.
// Duplicated here rather than imported, since ndjsonChunkSize is unexported.
func benchmarkChunkSize(fileSize int64, workers int) int64 {
	if workers <= 1 {
		return 0
	}
	chunkSize := fileSize / int64(workers)
	if chunkSize < 1*1024*1024 {
		chunkSize = 1 * 1024 * 1024
	}
	return chunkSize
}

// runProcessBenchmark runs Process for path once per b.N iteration and reports throughput.
func runProcessBenchmark(b *testing.B, path string, workers int, chunkSize int64) {
	b.Helper()
	b.ReportAllocs()

	var totalBytes atomic.Int64
	sink := func(_ context.Context, doc []byte) error {
		totalBytes.Add(int64(len(doc)))
		return nil
	}

	cfg := newConfig(workers, int(chunkSize), 1<<20)
	for range b.N {
		if _, err := convertFile(context.Background(), path, sink, cfg); err != nil {
			b.Fatalf("Process: %v", err)
		}
	}
	b.SetBytes(totalBytes.Load() / int64(b.N))
}

// BenchmarkNDJSONProcessParallel measures parallel speedup across worker counts, using chunk
// sizes computed the same way a real caller would (see benchmarkChunkSize) rather than a fixed
// size that would silently collapse every worker count to one chunk.
func BenchmarkNDJSONProcessParallel(b *testing.B) {
	sizes := []struct {
		name  string
		bytes int
	}{
		{"5MB", 5 * 1024 * 1024},
		{"100MB", 100 * 1024 * 1024},
	}

	for _, size := range sizes {
		path := writeSizedNdjsonFile(b, size.bytes)
		fileInfo, err := os.Stat(path)
		if err != nil {
			b.Fatalf("stat: %v", err)
		}

		for _, workers := range []int{1, 2, 4, 8} {
			chunkSize := benchmarkChunkSize(fileInfo.Size(), workers)
			b.Run(fmt.Sprintf("size=%s/workers=%d", size.name, workers), func(b *testing.B) {
				runProcessBenchmark(b, path, workers, chunkSize)
			})
		}
	}
}
