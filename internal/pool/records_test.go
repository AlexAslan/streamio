package pool_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/pool"
	"github.com/AlexAslan/streamio/internal/record"
)

// countingDecoder yields total records numbered from a shared counter, so siblings handed out by
// Split divide the same stream between them exactly as a real decoder's shared queue does.
type countingDecoder struct {
	next   *atomic.Int64
	closed *atomic.Int64
	total  int64
	failAt int64 // when > 0, the record number the decode fails at
}

func newCountingDecoder(total int64) *countingDecoder {
	return &countingDecoder{next: &atomic.Int64{}, total: total, closed: &atomic.Int64{}}
}

func (d *countingDecoder) DecodeNext(_ context.Context, batch []record.Record) (int, error) {
	n := 0
	for n < len(batch) {
		i := d.next.Add(1) - 1
		if i >= d.total {
			return n, io.EOF
		}
		if d.failAt > 0 && i == d.failAt {
			return n, errDecodeRecords
		}
		batch[n] = batch[n].Reset().Append("i", record.Int64(i, record.SemanticNone))
		n++
	}
	return n, nil
}

func (d *countingDecoder) Close() error {
	d.closed.Add(1)
	return nil
}

// fork returns a sibling sharing this decoder's counter, the way a real decoder's siblings share
// its input.
func (d *countingDecoder) fork() *countingDecoder {
	return &countingDecoder{next: d.next, closed: d.closed, total: d.total, failAt: d.failAt}
}

// splittableDecoder is a countingDecoder that advertises the optional Split capability.
type splittableDecoder struct {
	*countingDecoder
}

func (d splittableDecoder) Split(limit int) []formatio.RecordDecoder {
	decoders := make([]formatio.RecordDecoder, 0, limit)
	decoders = append(decoders, d)
	for range limit - 1 {
		decoders = append(decoders, splittableDecoder{countingDecoder: d.fork()})
	}
	return decoders
}

var (
	errDecodeRecords = errors.New("record decode failure")
	errBuildEncoder  = errors.New("encoder construction failure")
)

// fieldEncoder renders each record as its own "name=value;" document — the per-record shape, the
// one the two real formats' JSON encoder uses — without pulling a real format's encoder into the
// pool's own tests. See batchFieldEncoder for the other shape.
type fieldEncoder struct {
	failOn int64 // when >= 0, the record value the encode fails at
}

var errEncodeRecords = errors.New("record encode failure")

func (e fieldEncoder) EncodeBatch(batch []record.Record) ([][]byte, error) {
	docs := make([][]byte, 0, len(batch))
	for _, rec := range batch {
		var buf bytes.Buffer
		for _, f := range rec {
			if e.failOn >= 0 && f.Value.I64 == e.failOn {
				return nil, errEncodeRecords
			}
			buf.WriteString(f.Name)
			buf.WriteByte('=')
			buf.WriteString(strconv.FormatInt(f.Value.I64, 10))
			buf.WriteByte(';')
		}
		docs = append(docs, buf.Bytes())
	}
	return docs, nil
}

// newFieldEncoder is the factory shape RunRecords takes.
func newFieldEncoder(failOn int64) func() (formatio.RecordEncoder, error) {
	return func() (formatio.RecordEncoder, error) { return fieldEncoder{failOn: failOn}, nil }
}

// batchFieldEncoder renders a whole batch as one document, the shape a column-oriented format like
// Parquet is limited to. It exists here so the pool's own tests cover both encoder shapes without
// importing a format package.
type batchFieldEncoder struct{}

func (batchFieldEncoder) EncodeBatch(batch []record.Record) ([][]byte, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	for _, rec := range batch {
		for _, f := range rec {
			buf.WriteString(f.Name)
			buf.WriteByte('=')
			buf.WriteString(strconv.FormatInt(f.Value.I64, 10))
			buf.WriteByte(';')
		}
	}
	return [][]byte{buf.Bytes()}, nil
}

// newBatchFieldEncoder is newFieldEncoder for the whole-batch shape.
func newBatchFieldEncoder() func() (formatio.RecordEncoder, error) {
	return func() (formatio.RecordEncoder, error) { return batchFieldEncoder{}, nil }
}

// finalizingEncoder wraps fieldEncoder with a Finalize that appends one trailing "done;" document —
// the shape formatio.FinalizableRecordEncoder implementations like the streaming Parquet encoder
// take.
type finalizingEncoder struct {
	fieldEncoder
}

func (finalizingEncoder) Finalize() ([]byte, error) {
	return []byte("done;"), nil
}

// newFinalizingEncoder is the factory shape RunRecords takes. built counts how many times RunRecords
// actually calls it, which is how many decode workers it constructed: a FinalizableRecordEncoder
// must never see more than one, since Finalize needs every batch in order.
func newFinalizingEncoder(built *atomic.Int64) func() (formatio.RecordEncoder, error) {
	return func() (formatio.RecordEncoder, error) {
		built.Add(1)
		return finalizingEncoder{fieldEncoder: fieldEncoder{failOn: -1}}, nil
	}
}

// collectingDocSink records every dispatched document.
func collectingDocSink() (options.DocumentHandler, func() []string) {
	var (
		mu   sync.Mutex
		docs []string
	)
	sink := func(_ context.Context, doc []byte) error {
		mu.Lock()
		defer mu.Unlock()
		docs = append(docs, string(doc))
		return nil
	}
	return sink, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return docs
	}
}

// TestRunRecords_EncodesAndDispatchesEveryRecord covers the happy path across both split and
// unsplit decoders, and across batch sizes that do and don't divide the record count.
func TestRunRecords_EncodesAndDispatchesEveryRecord(t *testing.T) {
	type args struct {
		name      string
		total     int64
		workers   int
		batchSize int
		splitter  bool
	}

	tests := []args{
		{name: "single worker", total: 100, workers: 1, batchSize: 8, splitter: true},
		{name: "batch divides the total", total: 64, workers: 1, batchSize: 8, splitter: true},
		{name: "batch does not divide the total", total: 65, workers: 1, batchSize: 8, splitter: true},
		{name: "several workers share the decoder", total: 500, workers: 4, batchSize: 16, splitter: true},
		{name: "unsplittable decoder runs on one worker", total: 100, workers: 4, batchSize: 8, splitter: false},
		{name: "empty input", total: 0, workers: 2, batchSize: 8, splitter: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := newCountingDecoder(tt.total)
			var dec formatio.RecordDecoder = base
			if tt.splitter {
				dec = splittableDecoder{countingDecoder: base}
			}

			sink, collected := collectingDocSink()
			cfg := options.New(options.WithParallelWorkers(tt.workers), options.WithBatchSize(tt.batchSize))

			result, err := pool.RunRecords(context.Background(), cfg, dec, newFieldEncoder(-1), nil, sink)
			if err != nil {
				t.Fatalf("RunRecords: %v", err)
			}
			if result.Stats.RowsRead != tt.total {
				t.Errorf("Stats.RowsRead = %d, want %d", result.Stats.RowsRead, tt.total)
			}
			if result.Stats.RowsRead != tt.total {
				t.Errorf("RowsRead = %d, want %d", result.Stats.RowsRead, tt.total)
			}
			if result.Stats.DocumentsDispatched != tt.total {
				t.Errorf("DocumentsDispatched = %d, want %d", result.Stats.DocumentsDispatched, tt.total)
			}

			assertEncodedDocs(t, collected(), tt.total)
		})
	}
}

// TestRunRecords_WholeBatchEncoderCountsRows pins the counting rule the unified encoder interface
// makes necessary: when an encoder returns one document per batch rather than one per record,
// rows read and documents dispatched are intentionally different.
func TestRunRecords_WholeBatchEncoderCountsRows(t *testing.T) {
	const (
		total     = 100
		batchSize = 8
	)

	base := newCountingDecoder(total)
	sink, collected := collectingDocSink()
	cfg := options.New(options.WithParallelWorkers(1), options.WithBatchSize(batchSize))

	result, err := pool.RunRecords(
		context.Background(), cfg, splittableDecoder{countingDecoder: base}, newBatchFieldEncoder(), nil, sink,
	)
	if err != nil {
		t.Fatalf("RunRecords: %v", err)
	}
	if result.Stats.RowsRead != total {
		t.Errorf("Stats.RowsRead = %d, want %d rows", result.Stats.RowsRead, total)
	}
	if result.Stats.RowsRead != total {
		t.Errorf("RowsRead = %d, want %d rows", result.Stats.RowsRead, total)
	}

	wantDocs := (total + batchSize - 1) / batchSize
	if got := len(collected()); got != wantDocs {
		t.Errorf("dispatched %d documents, want %d (one per batch)", got, wantDocs)
	}
	if result.Stats.DocumentsDispatched != int64(wantDocs) {
		t.Errorf("DocumentsDispatched = %d, want %d", result.Stats.DocumentsDispatched, wantDocs)
	}
}

// assertEncodedDocs checks docs holds exactly one encoded record per index 0..total-1.
func assertEncodedDocs(tb testing.TB, docs []string, total int64) {
	tb.Helper()

	if int64(len(docs)) != total {
		tb.Fatalf("dispatched %d documents, want %d", len(docs), total)
	}

	seen := make(map[string]bool, len(docs))
	for _, doc := range docs {
		if seen[doc] {
			tb.Fatalf("document %q dispatched twice", doc)
		}
		seen[doc] = true
	}
	for i := range total {
		want := "i=" + strconv.FormatInt(i, 10) + ";"
		if !seen[want] {
			tb.Errorf("document %q never dispatched", want)
		}
	}
}

// TestRunRecords_DoesNotCloseTheDecoder pins the documented ownership rule: the caller that built
// the decoder closes it, exactly as with RunRaw's source.
func TestRunRecords_DoesNotCloseTheDecoder(t *testing.T) {
	base := newCountingDecoder(10)
	sink, _ := collectingDocSink()
	cfg := options.New(options.WithParallelWorkers(2), options.WithBatchSize(4))

	dec := splittableDecoder{countingDecoder: base}
	if _, err := pool.RunRecords(context.Background(), cfg, dec, newFieldEncoder(-1), nil, sink); err != nil {
		t.Fatalf("RunRecords: %v", err)
	}
	if got := base.closed.Load(); got != 0 {
		t.Errorf("RunRecords closed the decoder %d times, want 0", got)
	}
}

// TestRunRecords_ErrorPropagation checks a failure in any of the three stages — building an
// encoder, decoding, encoding — comes back from RunRecords rather than being swallowed.
func TestRunRecords_ErrorPropagation(t *testing.T) {
	type args struct {
		want       error
		newEncoder func() (formatio.RecordEncoder, error)
		name       string
		failDecode int64
	}

	tests := []args{
		{
			name:       "encoder construction",
			newEncoder: func() (formatio.RecordEncoder, error) { return nil, errBuildEncoder },
			want:       errBuildEncoder,
		},
		{
			name:       "decode",
			newEncoder: newFieldEncoder(-1),
			failDecode: 50,
			want:       errDecodeRecords,
		},
		{
			name:       "encode",
			newEncoder: newFieldEncoder(50),
			want:       errEncodeRecords,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := newCountingDecoder(1000)
			base.failAt = tt.failDecode

			sink, _ := collectingDocSink()
			cfg := options.New(options.WithParallelWorkers(2), options.WithBatchSize(8))

			_, err := pool.RunRecords(
				context.Background(), cfg, splittableDecoder{countingDecoder: base}, tt.newEncoder, nil, sink,
			)
			if !errors.Is(err, tt.want) {
				t.Fatalf("RunRecords error = %v, want %v", err, tt.want)
			}
		})
	}
}

// TestRunRecords_FinalizableEncoderIsSingleWorkerAndDispatchesFinalize checks the two guarantees
// RunRecords makes for a FinalizableRecordEncoder: it is never split across more than one decode
// worker even when the caller asked for several, and Finalize's trailing bytes are dispatched after
// every batch's own documents.
func TestRunRecords_FinalizableEncoderIsSingleWorkerAndDispatchesFinalize(t *testing.T) {
	const total = 100

	base := newCountingDecoder(total)
	sink, collected := collectingDocSink()
	cfg := options.New(options.WithParallelWorkers(4), options.WithBatchSize(8))

	built := &atomic.Int64{}
	_, err := pool.RunRecords(
		context.Background(), cfg, splittableDecoder{countingDecoder: base}, newFinalizingEncoder(built), nil, sink,
	)
	if err != nil {
		t.Fatalf("RunRecords: %v", err)
	}

	if got := built.Load(); got != 1 {
		t.Errorf("RunRecords built %d encoders for a FinalizableRecordEncoder, want 1", got)
	}

	docs := collected()
	assertEncodedDocs(t, docs[:len(docs)-1], total)
	if last := docs[len(docs)-1]; last != "done;" {
		t.Errorf("last dispatched document = %q, want the Finalize trailer %q", last, "done;")
	}
}

// TestRunRecords_ContextCancellation checks a cancelled context stops the run rather than draining
// the decoder.
func TestRunRecords_ContextCancellation(t *testing.T) {
	base := newCountingDecoder(1_000_000)
	sink, _ := collectingDocSink()
	cfg := options.New(options.WithParallelWorkers(2), options.WithBatchSize(8))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := pool.RunRecords(ctx, cfg, splittableDecoder{countingDecoder: base}, newFieldEncoder(-1), nil, sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunRecords error = %v, want context.Canceled", err)
	}
}
