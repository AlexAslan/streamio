package streamio_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/AlexAslan/streamio"
)

// writeBenchParquet creates a small single-row parquet fixture for Process dispatch benchmarks.
func writeBenchParquet(b *testing.B, path string) {
	b.Helper()
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("create: %v", err)
	}
	w := parquetgo.NewGenericWriter[testParquetRow](f)
	if _, err = w.Write([]testParquetRow{{Value: "bench"}}); err != nil {
		b.Fatalf("write: %v", err)
	}
	if err = w.Close(); err != nil {
		b.Fatalf("close writer: %v", err)
	}
	if err = f.Close(); err != nil {
		b.Fatalf("close file: %v", err)
	}
}

// writeBenchNDJSON creates a small NDJSON fixture for Process dispatch benchmarks.
func writeBenchNDJSON(b *testing.B, path string) {
	b.Helper()
	if err := os.WriteFile(path, []byte(`{"value":"bench"}`+"\n"), 0o600); err != nil {
		b.Fatalf("write: %v", err)
	}
}

// BenchmarkProcess_Dispatch measures the overhead of Process's extension dispatch and option
// application, draining the one-row fixture each iteration.
func BenchmarkProcess_Dispatch(b *testing.B) {
	type args struct {
		setup func(b *testing.B, path string)
		label string
		ext   string
	}
	cases := []args{
		{label: "parquet", ext: ".parquet", setup: writeBenchParquet},
		{label: "ndjson", ext: ".ndjson", setup: writeBenchNDJSON},
	}

	for _, tc := range cases {
		b.Run(tc.label, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), fmt.Sprintf("data%s", tc.ext))
			tc.setup(b, path)

			b.ReportAllocs()
			b.ResetTimer()

			sink := func(context.Context, []byte) error { return nil }
			for range b.N {
				if _, err := streamio.ProcessFile(context.Background(), path, sink); err != nil {
					b.Fatalf("Process: %v", err)
				}
			}
		})
	}
}
