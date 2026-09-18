// Package formatio defines the per-format capabilities streamio composes: streaming a file's own
// bytes (RawSource), decoding into canonical records (RecordDecoder), and encoding records as
// another format's documents (RecordEncoder).
//
// Decoders and encoders are registered independently, so every decoder pairs with every encoder
// and adding a format touches no existing one.
package formatio

import (
	"context"
	"io"
	"streamio/internal/record"
	"sync"
	"sync/atomic"
)

// RawSource streams a file's own native-format bytes straight to the sink, bypassing any
// intermediate record representation.
//
// DecodeRaw is called once per decode worker and must be safe for concurrent use. Close is called
// once, after every worker has returned.
type RawSource interface {
	DecodeRaw(ctx context.Context, out chan<- [][]byte, stats *DecodeStats) error
	io.Closer
}

// RecordDecoder reads a source file as canonical records.
//
// One decoder drives one decode worker and carries that worker's scratch, so a decoder is not safe
// for concurrent use; see SplittableRecordDecoder for how several workers share one open file.
type RecordDecoder interface {
	// DecodeNext fills batch with up to len(batch) reused records and returns how many it filled,
	// or io.EOF once the source is exhausted.
	DecodeNext(ctx context.Context, batch []record.Record) (int, error)

	// Close releases what the decoder holds, typically the open input file. Split siblings are not
	// closed individually.
	io.Closer
}

// SplittableRecordDecoder is the optional capability of a RecordDecoder whose input several decode
// workers can share. A RecordDecoder that doesn't implement it is driven by exactly one worker.
type SplittableRecordDecoder interface {
	RecordDecoder

	// Split returns up to limit decoders sharing this one's input, the receiver first, fewer if the
	// input has less claimable work than that.
	Split(limit int) []RecordDecoder
}

// RowError wraps a DecodeNext error that is specifically a malformed row — not an I/O failure, not
// context cancellation — leaving the decoder positioned to resume at the next row on the following
// DecodeNext call. Only a decoder that can make this distinction should ever wrap an error in it;
// wrapping an I/O or structural error would tell the pool it's safe to retry past when it isn't.
// NDJSON and CSV/TSV wrap their per-row decode errors this way; Parquet does not, since parquet-go
// reads and validates a whole row group at once, so its errors have no smaller recoverable unit —
// WithMaxRowErrors has no effect there, since no error it returns is ever a RowError.
type RowError struct {
	Err error
}

func (e *RowError) Error() string { return e.Err.Error() }

func (e *RowError) Unwrap() error { return e.Err }

// RowErrorTracker is the shared, cross-worker state behind WithMaxRowErrors: every decode worker on
// the generic record path records a skipped row error through the same tracker, so the limit means
// the same thing regardless of worker count — a per-worker counter would let the real threshold
// scale with cfg.Run.Workers, which isn't what "max_bad_records"-style semantics promise.
type RowErrorTracker struct {
	errs   []error
	limit  int
	mu     sync.Mutex
	broken bool
}

// NewRowErrorTracker returns a tracker allowing up to limit row errors before Record reports the
// limit exceeded.
func NewRowErrorTracker(limit int) *RowErrorTracker {
	return &RowErrorTracker{limit: limit}
}

// Record adds err to the tracker, returning false once doing so would exceed the configured
// limit — the row that tips it over is still recorded, so the caller's returned error (see
// Errors) includes it, per "report the errors up to that point" including the one that failed.
// Safe for concurrent use by every decode worker sharing this tracker.
func (t *RowErrorTracker) Record(err error) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.errs = append(t.errs, err)
	if len(t.errs) > t.limit {
		t.broken = true
	}
	return !t.broken
}

// Errors returns every row error recorded so far, in recording order. The caller must not mutate
// the returned slice.
func (t *RowErrorTracker) Errors() []error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.errs
}

// Limit returns the tracker's configured limit.
func (t *RowErrorTracker) Limit() int {
	return t.limit
}

// RecordEncoder renders a decoded batch as however many self-contained documents the format
// produces from it — one per record for most formats, one per whole batch for a column-oriented
// one like Parquet. One encoder drives one decode worker and need not be safe for concurrent use.
type RecordEncoder interface {
	// EncodeBatch renders batch as a slice of self-contained documents, none of which may alias
	// batch or its records past the call. An empty batch returns no documents.
	EncodeBatch(batch []record.Record) ([][]byte, error)
}

// FinalizableRecordEncoder is the optional capability of a RecordEncoder that spans more than one
// batch, accumulating state (an open writer, a derived schema) until Finalize closes it out. It is
// restricted to a single decode worker, since Finalize must see every batch in order.
type FinalizableRecordEncoder interface {
	RecordEncoder

	// Finalize returns the trailing bytes that complete the document, called once after the last
	// batch. A nil result means there is nothing to append.
	Finalize() ([]byte, error)
}

// DecodeStats collects what decode workers measure and count. All fields are safe for concurrent
// use by every worker.
type DecodeStats struct {
	// ReadNs accumulates decode time, added once per batch.
	ReadNs atomic.Int64

	// Records is the row count, set by any path whose dispatched items aren't one row each (a
	// whole row group, or a whole encoded batch). Left at zero otherwise, letting the pool derive
	// it from dispatch count.
	Records atomic.Int64

	// RowsSkipped counts rows WithMaxRowErrors allowed to be skipped. Always zero under the
	// default MaxRowErrors of 0.
	RowsSkipped atomic.Int64
}
