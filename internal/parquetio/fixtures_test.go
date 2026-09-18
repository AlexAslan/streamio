package parquetio_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/AlexAslan/streamio/internal/options"
)

// newConfig builds a options.Config with the given batchSize/workers, leaving every other field
// at its default.
func newConfig(batchSize, workers int) options.Config {
	return options.New(
		options.WithBatchSize(batchSize),
		options.WithParallelWorkers(workers),
	)
}

// testRowWithMap has a map column to exercise Map(String,String) reconstruction.
type testRowWithMap struct {
	Attributes map[string]string `parquet:"attributes"`
	Name       string            `parquet:"name"`
}

// parallelTestRow is the fixture struct used across multi-row-group tests.
type parallelTestRow struct {
	Label string `parquet:"label"`
	ID    int64  `parquet:"id"`
}

// writeParquetRows writes rows to path (as one row group, or several if opts requests smaller
// ones), failing tb on any write error.
func writeParquetRows[T any](tb testing.TB, path string, rows []T, opts ...parquetgo.WriterOption) {
	tb.Helper()
	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create: %v", err)
	}
	w := parquetgo.NewGenericWriter[T](f, opts...)
	if _, err = w.Write(rows); err != nil {
		tb.Fatalf("write: %v", err)
	}
	if err = w.Close(); err != nil {
		tb.Fatalf("close writer: %v", err)
	}
	if err = f.Close(); err != nil {
		tb.Fatalf("close file: %v", err)
	}
}

// writeParquetWithRowGroups writes n rows split into row groups of rgSize rows each.
func writeParquetWithRowGroups(tb testing.TB, n, rgSize int) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "parallel.parquet")
	rows := make([]parallelTestRow, n)
	for i := range rows {
		rows[i] = parallelTestRow{ID: int64(i), Label: fmt.Sprintf("label-%d", i)}
	}
	writeParquetRows(tb, path, rows, parquetgo.MaxRowsPerRowGroup(int64(rgSize)))
	return path
}

// openSource opens path and returns an options.Source over it, closing the file on test cleanup.
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
