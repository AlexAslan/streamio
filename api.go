package streamio

import (
	"context"

	"github.com/AlexAslan/streamio/internal/options"
)

// Format identifies an input or output document encoding.
type Format = options.OutputFormat

const (
	// FormatJSON selects newline-delimited JSON.
	FormatJSON = options.FormatJSON
	// FormatParquet selects Apache Parquet.
	FormatParquet = options.FormatParquet
	// FormatCSV selects comma-separated values.
	FormatCSV = options.FormatCSV
	// FormatTSV selects tab-separated values.
	FormatTSV = options.FormatTSV
	// FormatArrow selects Apache Arrow IPC (the file/random-access variant).
	FormatArrow = options.FormatArrow
)

// DocumentHandler is invoked once per output document. It may be called concurrently by multiple
// workers, so implementations must be safe for that.
type DocumentHandler func(context.Context, []byte) error

type (
	// Option configures an optional aspect of a ProcessFile run.
	Option = options.Option
	// Result reports the outcome of a ProcessFile run.
	Result = options.Result
	// Stats holds counters describing a completed ProcessFile run.
	Stats = options.Stats
	// Logger receives ProcessFile diagnostics.
	Logger = options.Logger
)

// TransformRule is a single rename or drop rule built by RenamePath or DropPath.
type TransformRule = options.PathTransformRule

// ErrTooManyRowErrors is TooManyRowErrorsError's sentinel: check for this failure with errors.Is
// without depending on the concrete type.
var ErrTooManyRowErrors = options.ErrTooManyRowErrors

// TooManyRowErrorsError is returned when WithMaxRowErrors' limit is exceeded: it wraps every row
// error collected up to and including the one that exceeded it.
type TooManyRowErrorsError = options.TooManyRowErrorsError

// WithInputFormat sets the source file's format explicitly, instead of letting ProcessFile infer
// it from the file extension.
func WithInputFormat(f Format) Option {
	return options.WithInputFormat(f)
}

// WithOutputFormat sets the format documents are delivered in.
func WithOutputFormat(f Format) Option {
	return options.WithOutputFormat(f)
}

// WithReadBufferSize sets the read buffer size, in bytes, used while scanning the input file.
// n<=0 falls back to the default.
func WithReadBufferSize(n int) Option {
	return options.WithReadBufferSize(n)
}

// WithBatchSize sets how many documents are batched together per dispatch. n<=0 falls back to
// the default.
func WithBatchSize(n int) Option {
	return options.WithBatchSize(n)
}

// WithParallelWorkers sets the number of concurrent decode workers. n<=0 falls back to the
// default.
func WithParallelWorkers(n int) Option {
	return options.WithParallelWorkers(n)
}

// WithChunkSize sets the fixed work-queue chunk size, in bytes, used to split NDJSON input across
// workers. n<=0 falls back to the default.
func WithChunkSize(n int) Option {
	return options.WithChunkSize(n)
}

// WithMaxOpenReaders caps the number of simultaneously open Parquet row-group readers. n<=0 falls
// back to the configured number of parallel workers, not a fixed default.
func WithMaxOpenReaders(n int) Option {
	return options.WithMaxOpenReaders(n)
}

// WithLogger sets the logger that receives ProcessFile diagnostics.
func WithLogger(l Logger) Option {
	return options.WithLogger(l)
}

// WithSingleFileOutput requests one continuous output document spanning the whole run, for
// formats that support it, instead of one document per batch. Forces decoding to a single worker.
func WithSingleFileOutput(b bool) Option {
	return options.WithSingleFileOutput(b)
}

// WithCSVDelimiter sets the field delimiter CSV or TSV reads and writes. Pass ',' for CSV or '\t'
// for TSV; the zero value falls back to the format's own default.
func WithCSVDelimiter(r rune) Option {
	return options.WithCSVDelimiter(r)
}

// WithCSVHasHeader says whether a CSV or TSV file's first row names its columns rather than
// holding data. It defaults to false.
func WithCSVHasHeader(b bool) Option {
	return options.WithCSVHasHeader(b)
}

// RenamePath builds a transform rule that renames one field path.
func RenamePath(from, to string) TransformRule {
	return options.RenamePath(from, to)
}

// DropPath builds a transform rule that removes one field path.
func DropPath(path string) TransformRule {
	return options.DropPath(path)
}

// WithTransforms applies a set of rename/drop rules to every decoded record before it is
// encoded into the requested output format.
func WithTransforms(rules ...TransformRule) Option {
	return options.WithTransforms(rules...)
}

// WithMaxRowErrors caps how many rows may fail to decode and be skipped before the run fails,
// mirroring BigQuery's load-job max_bad_records: n<=0 means fail immediately on the first row
// error (the default), matching streamio's original behavior. onSkip, if non-nil, is called once
// per row skipped under the cap. Only NDJSON and CSV/TSV field errors count against the cap — a
// CSV/TSV syntax error and every Parquet decode error always fail the run regardless of n.
//
// Exceeding the cap fails the run with a *TooManyRowErrorsError wrapping every row error collected
// up to and including the one that exceeded it; Result.Stats.RowsSkipped is still populated on
// that failure path.
func WithMaxRowErrors(n int, onSkip func(err error)) Option {
	return options.WithMaxRowErrors(n, onSkip)
}
