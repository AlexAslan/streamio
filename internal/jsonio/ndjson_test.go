package jsonio_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"streamio/internal/options"
	"sync"
	"testing"
	"time"
)

// newConfig builds a options.Config with the given workers/chunkSize/readBufferSize, leaving
// every other field at its default.
func newConfig(workers int, chunkSize, readBufferSize int) options.Config {
	return options.New(
		options.WithParallelWorkers(workers),
		options.WithChunkSize(chunkSize),
		options.WithReadBufferSize(readBufferSize),
	)
}

// writeTemp creates a temporary file with the given content and returns its path.
func writeTemp(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.ndjson")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err = f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err = f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	return f.Name()
}

// writeNdjsonFile writes lines, each terminated with '\n', to a temp file and returns its path.
func writeNdjsonFile(tb testing.TB, lines []string) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "data.ndjson")
	var buf bytes.Buffer
	for _, l := range lines {
		buf.WriteString(l)
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		tb.Fatalf("write ndjson file: %v", err)
	}
	return path
}

// genLines returns n lines of the form {"i":<index>}.
func genLines(n int) []string {
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf(`{"i":%d}`, i)
	}
	return lines
}

// collectingSink returns a sink safe for concurrent workers that appends every document to a
// slice, plus a function to retrieve what was collected afterward.
func collectingSink() (options.DocumentHandler, func() []string) {
	var (
		mu   sync.Mutex
		docs []string
	)
	sink := func(_ context.Context, doc []byte) error {
		mu.Lock()
		docs = append(docs, string(doc))
		mu.Unlock()
		return nil
	}
	return sink, func() []string { return docs }
}

func TestProcess_StandardLines(t *testing.T) {
	path := writeTemp(t, "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n")
	sink, collected := collectingSink()

	result, err := convertFile(context.Background(), path, sink, newConfig(1, 0, 4096))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	want := []string{`{"a":1}`, `{"b":2}`, `{"c":3}`}
	if !slices.Equal(collected(), want) {
		t.Fatalf("got %v, want %v", collected(), want)
	}
	if result.Stats.RowsRead != int64(len(want)) {
		t.Errorf("Count = %d, want %d", result.Stats.RowsRead, len(want))
	}
}

func TestProcess_EmptyLinesSkipped(t *testing.T) {
	path := writeTemp(t, "{\"a\":1}\n\n{\"b\":2}\n\n")
	sink, collected := collectingSink()

	if _, err := convertFile(context.Background(), path, sink, newConfig(1, 0, 4096)); err != nil {
		t.Fatalf("Process: %v", err)
	}

	want := []string{`{"a":1}`, `{"b":2}`}
	if !slices.Equal(collected(), want) {
		t.Fatalf("got %v, want %v", collected(), want)
	}
}

func TestProcess_NoTrailingNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.ndjson")
	if err := os.WriteFile(path, []byte("{\"a\":1}\n{\"b\":2}"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	sink, collected := collectingSink()

	// Last line has no trailing newline — ReadBytes returns (data, io.EOF) simultaneously.
	if _, err := convertFile(context.Background(), path, sink, newConfig(1, 0, 4096)); err != nil {
		t.Fatalf("Process: %v", err)
	}

	want := []string{`{"a":1}`, `{"b":2}`}
	if !slices.Equal(collected(), want) {
		t.Fatalf("got %v, want %v", collected(), want)
	}
}

func TestProcess_EmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.ndjson")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	sink, collected := collectingSink()

	result, err := convertFile(context.Background(), path, sink, newConfig(4, 64, 4096))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(collected()) != 0 || result.Stats.RowsRead != 0 {
		t.Fatalf("got %d docs, want 0", result.Stats.RowsRead)
	}
}

// TestProcess_EvenSplit verifies all lines are returned when the file size divides evenly into
// chunkSize*workers.
func TestProcess_EvenSplit(t *testing.T) {
	path := writeNdjsonFile(t, genLines(1000))
	sink, collected := collectingSink()

	result, err := convertFile(context.Background(), path, sink, newConfig(4, 128, 4096))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(collected()) != 1000 || result.Stats.RowsRead != 1000 {
		t.Fatalf("got %d docs, want 1000", result.Stats.RowsRead)
	}
}

// TestProcess_UnevenSplit verifies all lines are returned when the line count and chunk size
// don't divide evenly across workers.
func TestProcess_UnevenSplit(t *testing.T) {
	path := writeNdjsonFile(t, genLines(997))
	sink, collected := collectingSink()

	if _, err := convertFile(context.Background(), path, sink, newConfig(3, 97, 4096)); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(collected()) != 997 {
		t.Fatalf("got %d docs, want 997", len(collected()))
	}
}

// TestProcess_MoreWorkersThanChunks verifies that surplus workers, which find no unclaimed chunk
// left, exit cleanly and all lines are still returned.
func TestProcess_MoreWorkersThanChunks(t *testing.T) {
	path := writeNdjsonFile(t, genLines(50))
	sink, collected := collectingSink()

	if _, err := convertFile(context.Background(), path, sink, newConfig(8, 1<<20, 4096)); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(collected()) != 50 {
		t.Fatalf("got %d docs, want 50", len(collected()))
	}
}

// TestProcess_ParityAcrossWorkerCounts verifies workers=1 and workers>1 return exactly the same
// set of documents, modulo order.
func TestProcess_ParityAcrossWorkerCounts(t *testing.T) {
	lines := genLines(2000)
	path := writeNdjsonFile(t, lines)

	oneSink, oneDocs := collectingSink()
	if _, err := convertFile(context.Background(), path, oneSink, newConfig(1, 0, 4096)); err != nil {
		t.Fatalf("Process(workers=1): %v", err)
	}

	manySink, manyDocs := collectingSink()
	if _, err := convertFile(context.Background(), path, manySink, newConfig(6, 173, 4096)); err != nil {
		t.Fatalf("Process(workers=6): %v", err)
	}

	got, want := manyDocs(), oneDocs()
	sort.Strings(got)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("got %d docs, want %d matching workers=1", len(got), len(want))
	}
}

// TestProcess_LineStraddlesBoundary uses a chunk size that is guaranteed to land mid-line for at
// least one boundary, and verifies no line is lost or duplicated.
func TestProcess_LineStraddlesBoundary(t *testing.T) {
	// Lines of very uneven length so a fixed chunk size can't align with every line boundary.
	lines := make([]string, 200)
	for i := range lines {
		pad := i % 37 // varying line length
		lines[i] = fmt.Sprintf(`{"i":%d,"pad":"%s"}`, i, string(make([]byte, pad)))
	}
	path := writeNdjsonFile(t, lines)
	sink, collected := collectingSink()

	// Deliberately small, prime-ish chunk size to force boundaries mid-line often.
	if _, err := convertFile(context.Background(), path, sink, newConfig(5, 31, 4096)); err != nil {
		t.Fatalf("Process: %v", err)
	}

	docs := collected()
	if len(docs) != len(lines) {
		t.Fatalf("got %d docs, want %d", len(docs), len(lines))
	}
	seen := make(map[string]int)
	for _, d := range docs {
		seen[d]++
	}
	for _, l := range lines {
		if seen[l] != 1 {
			t.Errorf("line %q seen %d times, want 1", l, seen[l])
		}
	}
}

// TestProcess_NoTrailingNewlineParallel verifies a file whose last line has no trailing newline
// is still read correctly by the last chunk when split across multiple workers.
func TestProcess_NoTrailingNewlineParallel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.ndjson")
	content := `{"i":0}` + "\n" + `{"i":1}` + "\n" + `{"i":2}` // no trailing newline
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	sink, collected := collectingSink()

	if _, err := convertFile(context.Background(), path, sink, newConfig(2, 8, 4096)); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(collected()) != 3 {
		t.Fatalf("got %d docs, want 3", len(collected()))
	}
}

// TestProcess_UnterminatedLineCrossesEarlierChunkBoundary verifies that when the file's final
// line has no trailing newline and a non-final chunk happens to be the one that reads it through
// to true EOF, Process still terminates cleanly instead of erroring.
func TestProcess_UnterminatedLineCrossesEarlierChunkBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.ndjson")
	content := "AAAA\nBBBB\nCCCC\nDDDDDDDDDDDD" // last line has no trailing '\n'
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	sink, collected := collectingSink()

	if _, err := convertFile(context.Background(), path, sink, newConfig(2, 10, 4096)); err != nil {
		t.Fatalf("Process: %v", err)
	}

	got := collected()
	want := []string{"AAAA", "BBBB", "CCCC", "DDDDDDDDDDDD"}
	sort.Strings(got)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestProcess_CancelledBeforeStart verifies that an already-cancelled context stops the load with
// an error instead of returning a short/empty result and nil, which would be reported as a
// complete run.
func TestProcess_CancelledBeforeStart(t *testing.T) {
	path := writeNdjsonFile(t, genLines(10))
	sink, _ := collectingSink()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := convertFile(ctx, path, sink, newConfig(4, 0, 4096)); !errors.Is(err, context.Canceled) {
		t.Fatalf("got err %v, want context.Canceled", err)
	}
}

// TestProcess_ContextCancellation verifies that cancelling the context mid-stream stops every
// worker and Process reports the cancellation rather than a clean nil error — a caller that saw
// nil here would treat a truncated stream as a complete one and report success on a partial read.
func TestProcess_ContextCancellation(t *testing.T) {
	path := writeNdjsonFile(t, genLines(50_000))

	ctx, cancel := context.WithCancel(context.Background())
	var (
		mu sync.Mutex
		n  int
	)
	sink := func(ctx context.Context, _ []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		mu.Lock()
		n++
		if n == 5 {
			cancel()
		}
		mu.Unlock()
		return nil
	}

	_, err := convertFile(ctx, path, sink, newConfig(4, 4096, 4096))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got err %v, want context.Canceled", err)
	}
}

// errFake stands in for a sink failure, such as a bulk indexer rejecting a document.
var errFake = errors.New("fake failure")

// TestProcess_DispatchErrorStopsEarly verifies that a sink error surfaces as Process's error,
// and that stats distinguish rows read from documents accepted before the failure.
func TestProcess_DispatchErrorStopsEarly(t *testing.T) {
	path := writeNdjsonFile(t, genLines(1)) // workers=1: exactly one call to sink
	sink := func(context.Context, []byte) error { return errFake }

	result, err := convertFile(context.Background(), path, sink, newConfig(1, 0, 4096))
	if !errors.Is(err, errFake) {
		t.Fatalf("got err %v, want errFake", err)
	}
	if result.Stats.RowsRead != 1 {
		t.Errorf("RowsRead = %d, want 1", result.Stats.RowsRead)
	}
	if result.Stats.DocumentsDispatched != 0 {
		t.Errorf("DocumentsDispatched = %d, want 0 (the single call failed)",
			result.Stats.DocumentsDispatched)
	}
}

// TestProcess_NoGoroutineLeak verifies that Process never returns while leaving a worker
// goroutine running, across many cancelled and completed runs.
func TestProcess_NoGoroutineLeak(t *testing.T) {
	path := writeNdjsonFile(t, genLines(10_000))
	before := runtime.NumGoroutine()

	for i := range 20 {
		sink, _ := collectingSink()
		ctx, cancel := context.WithCancel(context.Background())
		if i%2 == 0 {
			cancel() // half already-cancelled, half left to run to completion below
		}
		_, _ = convertFile(ctx, path, sink, newConfig(4, 4096, 4096))
		cancel()
	}

	deadline := time.Now().Add(2 * time.Second)
	var after int
	for time.Now().Before(deadline) {
		after = runtime.NumGoroutine()
		if after <= before {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if after > before {
		t.Errorf("goroutine leak: %d goroutines before, %d after 20 Process calls", before, after)
	}
}
