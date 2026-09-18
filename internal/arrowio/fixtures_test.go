package arrowio_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/AlexAslan/streamio/internal/options"
)

// newConfig builds an options.Config with the given batchSize/workers, leaving every other field
// at its default.
func newConfig(batchSize, workers int) options.Config {
	return options.New(
		options.WithBatchSize(batchSize),
		options.WithParallelWorkers(workers),
	)
}

// openSource opens path and returns an options.Source over it, closing the file on test cleanup —
// mirrors parquetio's own test helper of the same name.
func openSource(tb testing.TB, path string) options.Source {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open %s: %v", path, err)
	}
	tb.Cleanup(func() { f.Close() })

	stat, err := f.Stat()
	if err != nil {
		tb.Fatalf("stat %s: %v", path, err)
	}
	return options.Source{Reader: f, Size: stat.Size(), Name: path}
}

// testSchema returns the Arrow schema every fixture in this file uses: an int64 id and a string
// label.
func testSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

// writeArrowFile writes n rows (id=0..n-1, label="label-<i>") to path, split into numBatches record
// batches, and returns the path.
func writeArrowFile(tb testing.TB, n, numBatches int) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "test.arrow")
	writeArrowRows(tb, path, n, numBatches)
	return path
}

// writeArrowRows writes n rows split into numBatches record batches at path.
func writeArrowRows(tb testing.TB, path string, n, numBatches int) {
	tb.Helper()
	if numBatches < 1 {
		numBatches = 1
	}

	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	alloc := memory.NewGoAllocator()
	w, err := ipc.NewFileWriter(f, ipc.WithSchema(testSchema()), ipc.WithAllocator(alloc))
	if err != nil {
		tb.Fatalf("NewFileWriter: %v", err)
	}

	perBatch := (n + numBatches - 1) / numBatches
	if perBatch < 1 {
		perBatch = 1
	}

	for start := 0; start < n; start += perBatch {
		end := min(start+perBatch, n)
		rb := array.NewRecordBuilder(alloc, testSchema())
		idBld := rb.Field(0).(*array.Int64Builder)
		labelBld := rb.Field(1).(*array.StringBuilder)
		for i := start; i < end; i++ {
			idBld.Append(int64(i))
			labelBld.Append(fmt.Sprintf("label-%d", i))
		}
		rec := rb.NewRecordBatch()
		if writeErr := w.Write(rec); writeErr != nil {
			rec.Release()
			tb.Fatalf("Write: %v", writeErr)
		}
		rec.Release()
	}

	if closeErr := w.Close(); closeErr != nil {
		tb.Fatalf("Close writer: %v", closeErr)
	}
	if closeErr := f.Close(); closeErr != nil {
		tb.Fatalf("close file: %v", closeErr)
	}
}

// writeEmptyArrowFile writes a schema-only Arrow file with no record batches.
func writeEmptyArrowFile(tb testing.TB) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "empty.arrow")

	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	w, err := ipc.NewFileWriter(f, ipc.WithSchema(testSchema()))
	if err != nil {
		tb.Fatalf("NewFileWriter: %v", err)
	}
	if closeErr := w.Close(); closeErr != nil {
		tb.Fatalf("Close writer: %v", closeErr)
	}
	if closeErr := f.Close(); closeErr != nil {
		tb.Fatalf("close file: %v", closeErr)
	}
	return path
}
