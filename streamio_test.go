package streamio_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"streamio"
	"strings"
	"sync"
	"testing"

	parquetgo "github.com/parquet-go/parquet-go"
)

// testParquetRow is a minimal flat struct used to create test parquet fixtures.
type testParquetRow struct {
	Value string `parquet:"value"`
}

// createParquetFile writes a single-row parquet file at path.
func createParquetFile(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create parquet file: %v", err)
	}
	w := parquetgo.NewGenericWriter[testParquetRow](f)
	if _, err = w.Write([]testParquetRow{{Value: "hello"}}); err != nil {
		t.Fatalf("write parquet rows: %v", err)
	}
	if err = w.Close(); err != nil {
		t.Fatalf("close parquet writer: %v", err)
	}
	if err = f.Close(); err != nil {
		t.Fatalf("close parquet file: %v", err)
	}
}

// createNDJSONFile writes a minimal NDJSON file at path.
func createNDJSONFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("{\"a\":1}\n"), 0o600); err != nil {
		t.Fatalf("write ndjson file: %v", err)
	}
}

// createNDJSONFileWithLines writes n distinct NDJSON lines at path.
func createNDJSONFileWithLines(t *testing.T, path string, n int) {
	t.Helper()
	buf := make([]byte, 0, n*16)
	for i := range n {
		buf = append(buf, []byte(fmt.Sprintf(`{"i":%d}`+"\n", i))...)
	}
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("write ndjson file: %v", err)
	}
}

// collectingSink returns a sink safe for concurrent workers that appends every document to a
// slice, plus a function to retrieve what was collected afterward.
func collectingSink() (streamio.DocumentHandler, func() [][]byte) {
	var (
		mu   sync.Mutex
		docs [][]byte
	)
	sink := func(_ context.Context, doc []byte) error {
		mu.Lock()
		docs = append(docs, doc)
		mu.Unlock()
		return nil
	}
	return sink, func() [][]byte { return docs }
}

// TestProcess_DispatchesParquet verifies that Process routes .parquet and .PARQUET extensions
// (case-insensitive) to the Parquet reader.
func TestProcess_DispatchesParquet(t *testing.T) {
	cases := []struct {
		name string
		ext  string
	}{
		{name: "lowercase extension", ext: ".parquet"},
		{name: "uppercase extension", ext: ".PARQUET"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "data"+tc.ext)
			createParquetFile(t, path)
			sink, collected := collectingSink()

			if _, err := streamio.ProcessFile(context.Background(), path, sink); err != nil {
				t.Fatalf("Process: %v", err)
			}
			docs := collected()
			if len(docs) != 1 || len(docs[0]) == 0 {
				t.Fatalf("got %d docs, want 1 non-empty document", len(docs))
			}
		})
	}
}

// TestProcess_DispatchesNDJSON verifies that Process routes .ndjson and unknown extensions to
// the NDJSON reader.
func TestProcess_DispatchesNDJSON(t *testing.T) {
	cases := []struct {
		name string
		ext  string
	}{
		{name: "ndjson extension", ext: ".ndjson"},
		{name: "unknown extension", ext: ".json"},
		{name: "no extension", ext: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "data"+tc.ext)
			createNDJSONFile(t, path)
			sink, collected := collectingSink()

			_, err := streamio.ProcessFile(context.Background(), path, sink, streamio.WithReadBufferSize(4096))
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			docs := collected()
			if len(docs) != 1 || string(docs[0]) != `{"a":1}` {
				t.Fatalf("got %v, want [%q]", docs, `{"a":1}`)
			}
		})
	}
}

// TestProcess_ParallelWorkers verifies that Process honors WithParallelWorkers for a non-Parquet
// file, decoding every line across multiple workers.
func TestProcess_ParallelWorkers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	createNDJSONFileWithLines(t, path, 100)
	sink, collected := collectingSink()

	result, err := streamio.ProcessFile(context.Background(), path, sink,
		streamio.WithParallelWorkers(4),
		streamio.WithChunkSize(64),
	)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(collected()) != 100 || result.Stats.RowsRead != 100 {
		t.Errorf("got %d documents, want 100", result.Stats.RowsRead)
	}
}

func TestProcessFile_InvalidTransformReturnsError(t *testing.T) {
	_, err := streamio.ProcessFile(
		context.Background(),
		filepath.Join(t.TempDir(), "missing.parquet"),
		func(context.Context, []byte) error { return nil },
		streamio.WithTransforms(streamio.RenamePath("attributes.service", "service")),
	)
	if err == nil || !strings.Contains(err.Error(), "changes path depth") {
		t.Fatalf("ProcessFile error = %v, want invalid transform path error", err)
	}
}
