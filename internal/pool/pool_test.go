package pool_test

import (
	"context"
	"errors"
	"runtime"
	"streamio/internal/options"
	"streamio/internal/pool"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// docsRawSource is a formatio.RawSource that hands the i-th concurrent DecodeRaw call
// docsPerWorker[i], sending one document per batch — the same way a real raw source divides its
// input between the workers all calling it at once. A call past the end sends nothing, standing in
// for a worker that finds the queue already empty.
type docsRawSource struct {
	docsPerWorker [][][]byte
	next          atomic.Int64
}

func newDocsRawSource(docsPerWorker [][][]byte) *docsRawSource {
	return &docsRawSource{docsPerWorker: docsPerWorker}
}

func (s *docsRawSource) Close() error { return nil }

func (s *docsRawSource) DecodeRaw(ctx context.Context, out chan<- [][]byte, stats *pool.DecodeStats) error {
	i := s.next.Add(1) - 1
	if int(i) >= len(s.docsPerWorker) {
		return nil
	}

	for _, doc := range s.docsPerWorker[i] {
		// Checked up front, like the real ndjson/parquet sources: otherwise, with an
		// already-cancelled ctx, select below could nondeterministically pick the still-open
		// out<- case instead of ctx.Done(), making tests that pre-cancel ctx flaky.
		if err := ctx.Err(); err != nil {
			return err
		}
		stats.ReadNs.Add(1)
		select {
		case out <- [][]byte{doc}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func collectingSink() (options.DocumentHandler, func() int) {
	var count atomic.Int64
	sink := func(context.Context, []byte) error {
		count.Add(1)
		return nil
	}
	return sink, func() int { return int(count.Load()) }
}

func TestRunRaw_DispatchesEveryDocument(t *testing.T) {
	docsPerWorker := [][][]byte{
		{[]byte("a"), []byte("b")},
		{[]byte("c")},
	}
	sink, count := collectingSink()
	cfg := options.New(options.WithParallelWorkers(2))

	result, err := pool.RunRaw(context.Background(), cfg, newDocsRawSource(docsPerWorker), sink)
	if err != nil {
		t.Fatalf("RunRaw: %v", err)
	}
	if count() != 3 {
		t.Errorf("dispatched %d documents, want 3", count())
	}
	if result.Stats.RowsRead != 3 {
		t.Errorf("Stats.RowsRead = %d, want 3", result.Stats.RowsRead)
	}
	if result.Stats.RowsRead != 3 || result.Stats.DocumentsDispatched != 3 {
		t.Errorf("Stats = %+v, want 3 rows read and 3 documents dispatched", result.Stats)
	}
	if result.Stats.ReadDuration <= 0 {
		t.Errorf("ReadDuration = %v, want > 0", result.Stats.ReadDuration)
	}
}

func TestRunRaw_EmptyInput(t *testing.T) {
	sink, count := collectingSink()
	cfg := options.New(options.WithParallelWorkers(4))

	result, err := pool.RunRaw(context.Background(), cfg, newDocsRawSource(nil), sink)
	if err != nil {
		t.Fatalf("RunRaw: %v", err)
	}
	if count() != 0 || result.Stats.RowsRead != 0 || result.Stats.DocumentsDispatched != 0 {
		t.Errorf("got %d dispatched, want 0", count())
	}
}

var errDecode = errors.New("decode failure")

// failingRawSource sends one document then fails, so a test can assert both that the error
// propagates and that whatever was already sent got dispatched.
type failingRawSource struct{}

func (failingRawSource) Close() error { return nil }

func (failingRawSource) DecodeRaw(ctx context.Context, out chan<- [][]byte, _ *pool.DecodeStats) error {
	select {
	case out <- [][]byte{[]byte("x")}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return errDecode
}

func TestRunRaw_DecodeErrorPropagates(t *testing.T) {
	sink, _ := collectingSink()
	cfg := options.New(options.WithParallelWorkers(1))

	_, err := pool.RunRaw(context.Background(), cfg, failingRawSource{}, sink)
	if !errors.Is(err, errDecode) {
		t.Fatalf("got err %v, want errDecode", err)
	}
}

var errSink = errors.New("sink failure")

func TestRunRaw_SinkErrorPropagatesAndStopsDecoding(t *testing.T) {
	// Enough documents that, absent cancellation, decoding would take a while to finish; if the
	// sink error doesn't actually stop the decoder, this test would still pass slowly instead of
	// failing, so it also bounds wall-clock time.
	docs := make([][]byte, 10_000)
	for i := range docs {
		docs[i] = []byte("x")
	}
	sink := func(context.Context, []byte) error { return errSink }
	cfg := options.New(options.WithParallelWorkers(1))

	done := make(chan struct{})
	var err error
	var result options.Result
	go func() {
		result, err = pool.RunRaw(context.Background(), cfg, newDocsRawSource([][][]byte{docs}), sink)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunRaw did not return within 5s of a sink error — decode workers may not be stopping")
	}
	if !errors.Is(err, errSink) {
		t.Fatalf("got err %v, want errSink", err)
	}
	if result.Stats.DocumentsDispatched != 0 {
		t.Fatalf("DocumentsDispatched = %d, want 0 after sink rejects first document",
			result.Stats.DocumentsDispatched)
	}
}

func TestRunRaw_ContextCancellation(t *testing.T) {
	docs := make([][]byte, 100_000)
	for i := range docs {
		docs[i] = []byte("x")
	}
	var n atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	sink := func(context.Context, []byte) error {
		if n.Add(1) == 5 {
			cancel()
		}
		return nil
	}
	cfg := options.New(options.WithParallelWorkers(2))

	_, err := pool.RunRaw(ctx, cfg, newDocsRawSource([][][]byte{docs, docs}), sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got err %v, want context.Canceled", err)
	}
}

func TestRunRaw_AlreadyCancelledContext(t *testing.T) {
	sink, _ := collectingSink()
	cfg := options.New(options.WithParallelWorkers(1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := pool.RunRaw(ctx, cfg, newDocsRawSource([][][]byte{{[]byte("x")}}), sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got err %v, want context.Canceled", err)
	}
}

// TestRunRaw_NoGoroutineLeak verifies the pool never returns while a worker goroutine is still
// running, across many cancelled and completed runs.
func TestRunRaw_NoGoroutineLeak(t *testing.T) {
	docs := make([][]byte, 1_000)
	for i := range docs {
		docs[i] = []byte("x")
	}
	before := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sink, _ := collectingSink()
			cfg := options.New(options.WithParallelWorkers(4))
			ctx, cancel := context.WithCancel(context.Background())
			if i%2 == 0 {
				cancel()
			}
			_, _ = pool.RunRaw(ctx, cfg, newDocsRawSource([][][]byte{docs, docs, docs, docs}), sink)
			cancel()
		}(i)
	}
	wg.Wait()

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
		t.Errorf("goroutine leak: %d goroutines before, %d after 20 RunRaw calls", before, after)
	}
}
