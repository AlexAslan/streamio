package csvio_test

import (
	"os"
	"path/filepath"
	"streamio/internal/options"
	"testing"
)

// newConfig builds an options.Config for the given delimiter/header settings, leaving every other
// field at its default.
func newConfig(workers int, chunkSize int, delimiter rune, hasHeader bool) options.Config {
	return options.New(
		options.WithParallelWorkers(workers),
		options.WithChunkSize(chunkSize),
		options.WithCSVDelimiter(delimiter),
		options.WithCSVHasHeader(hasHeader),
	)
}

// writeCSVFile writes content to a temp file and returns its path.
func writeCSVFile(tb testing.TB, content string) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		tb.Fatalf("write csv file: %v", err)
	}
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
