package options

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	// defaultReadBufferSize is used when WithReadBufferSize isn't given.
	defaultReadBufferSize = 32 * 1024 * 1024

	// defaultChunkSize is used when WithChunkSize isn't given.
	defaultChunkSize = 32 * 1024 * 1024

	// defaultWorkers is used when WithParallelWorkers isn't given.
	defaultWorkers = 1

	// defaultBatchSize is used when WithBatchSize isn't given; see its doc comment.
	defaultBatchSize = 512

	// DispatchQueueDepthPerWorker sizes the channel connecting decode workers to dispatch
	// workers, per decode worker, for both Parquet and NDJSON. Not user-configurable — there's
	// no corresponding WithXxx option — so it's a plain exported constant, not a Config field.
	DispatchQueueDepthPerWorker = 4
)

// New returns a Config with the given options applied over its defaults.
func New(opts ...Option) Config {
	cfg := Config{
		Run:    RunConfig{ReadBufferSize: defaultReadBufferSize, Workers: defaultWorkers, BatchSize: defaultBatchSize},
		NDJSON: NDJSONConfig{ChunkSize: defaultChunkSize},
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	// An explicit WithXxx(n<=0) means "use the default" (see each Option's doc comment), same as
	// omitting the option entirely — so these defaults must survive being overwritten by zero,
	// not just being absent.
	if cfg.Run.BatchSize <= 0 {
		cfg.Run.BatchSize = defaultBatchSize
	}

	if cfg.Run.Workers <= 0 {
		cfg.Run.Workers = defaultWorkers
	}

	if cfg.Run.ReadBufferSize <= 0 {
		cfg.Run.ReadBufferSize = defaultReadBufferSize
	}

	if cfg.NDJSON.ChunkSize <= 0 {
		cfg.NDJSON.ChunkSize = defaultChunkSize
	}

	// MaxOpenReaders' default is Workers, not a fixed constant: it bounds simultaneously-open
	// Parquet row-group readers, and defaulting it lower than the requested worker count would
	// silently serialize decoding below what the caller asked for.
	if cfg.Parquet.MaxOpenReaders <= 0 {
		cfg.Parquet.MaxOpenReaders = cfg.Run.Workers
	}

	// The zero OutputFormat means "caller didn't ask", which is JSON — the format every existing
	// call site expects. Normalising here keeps every consumer free of the empty-string case.
	if cfg.OutputFormat == "" {
		cfg.OutputFormat = FormatJSON
	}

	// Cap ReadBufferSize to ChunkSize: without this, a small ChunkSize (e.g. the floor for a
	// small file with many workers) still pays for a full ReadBufferSize buffer per worker.
	if cfg.Run.ReadBufferSize > cfg.NDJSON.ChunkSize {
		cfg.Run.ReadBufferSize = cfg.NDJSON.ChunkSize
	}

	// CSV.Delimiter has no single fixed default: ',' for CSV, '\t' for TSV. Leaving it at the zero
	// value here lets csvio pick the right one for the format it was actually asked to read or
	// write.
	return cfg
}

// DocumentHandler receives one decoded document at a time. It may be called concurrently and must
// be safe for that.
type DocumentHandler func(ctx context.Context, doc []byte) error

// Stats reports what one ProcessFile call did.
type Stats struct {
	// RowsRead is how many input rows were decoded, regardless of how many documents they became.
	RowsRead int64
	// RowsSkipped is how many rows WithMaxRowErrors allowed to be skipped. Always zero under the
	// default MaxRowErrors of 0.
	RowsSkipped int64
	// DocumentsDispatched is how many documents were handed to the caller's handler. It can differ
	// from RowsRead when an encoder renders a whole batch as one document (Parquet) rather than one
	// document per row (NDJSON).
	DocumentsDispatched int64
	// ReadDuration sums every worker's decode+encode time.
	ReadDuration time.Duration
	// DispatchDuration sums every worker's time spent in the caller's handler.
	DispatchDuration time.Duration
}

// Result reports what one ProcessFile call did.
type Result struct {
	Stats Stats
}

// ErrTooManyRowErrors is TooManyRowErrorsError's sentinel: wrapped so a caller can check for this
// failure with errors.Is without depending on the concrete type.
var ErrTooManyRowErrors = errors.New("streamio: too many row errors")

// TooManyRowErrorsError is returned when the number of skipped row errors exceeds MaxRowErrors: the
// (MaxRowErrors+1)th row error stops the run rather than being skipped, and this wraps every row
// error collected up to and including that one — not just a count, since Stats.RowsSkipped (still
// populated on this failure path) already gives the count on its own.
type TooManyRowErrorsError struct {
	// Errors holds every row error collected before the run stopped, in the order encountered
	// across every decode worker sharing the run's limit.
	Errors []error
	// Limit is the MaxRowErrors value the run was configured with.
	Limit int
}

func (e *TooManyRowErrorsError) Error() string {
	return fmt.Sprintf("%s: limit %d, got %d errors (first: %v)",
		ErrTooManyRowErrors, e.Limit, len(e.Errors), e.Errors[0])
}

func (e *TooManyRowErrorsError) Unwrap() error { return ErrTooManyRowErrors }
