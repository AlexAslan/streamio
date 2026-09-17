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
}
