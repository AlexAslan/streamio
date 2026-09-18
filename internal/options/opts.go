// Package options defines streamio's Config and the functional Option funcs that build it,
// along with the transform rules and format constants ProcessFile's callers configure a run with.
package options

import (
	"io"

	"github.com/AlexAslan/streamio/internal/record"
)

// Source is a sized, randomly-addressable byte source: what every format's chunked-parallel
// decoder actually needs, satisfied by an *os.File or anything else providing ReadAt over a known
// span. Name labels it in diagnostics and error text; it need not be a real file path.
type Source struct {
	Reader io.ReaderAt
	Name   string
	Size   int64
}

// Config holds the options passed to Process.
type Config struct {
	// Logger receives ProcessFile diagnostics, when set.
	Logger Logger
	// Transformer changes each decoded record before it is encoded, when set.
	Transformer Transformer
	// OnRowErrorSkip, if set, is called once per row MaxRowErrors allows to be skipped, with the
	// decode error that row raised. It must be safe for concurrent use: one decode worker per
	// goroutine may call it.
	OnRowErrorSkip func(err error)
	// OptionErr is a deferred validation-error slot: an option func that fails validation stores
	// its error here instead of panicking, and New's caller checks it once after every option has
	// run.
	OptionErr error
	// InputFormat is the source file's format, or empty to detect it from the file extension.
	InputFormat OutputFormat
	// OutputFormat is the format documents are delivered in.
	OutputFormat OutputFormat
	// Run holds settings for the decode/dispatch pool, shared by every format.
	Run RunConfig
	// NDJSON holds settings only the NDJSON format reads.
	NDJSON NDJSONConfig
	// Parquet holds settings only the Parquet format reads.
	Parquet ParquetConfig
	// CSV holds settings the CSV and TSV formats read; the two differ only in CSV.Delimiter.
	CSV CSVConfig
	// MaxRowErrors caps how many rows may fail to decode and be skipped before the run fails,
	// mirroring BigQuery's load-job max_bad_records: its zero value means fail immediately on the
	// first row error, matching every version of streamio before this option existed. Only a
	// decoder error that identifies itself as safely skippable (see formatio.RowError) counts
	// against this cap — currently NDJSON and CSV/TSV field errors, not Parquet or a CSV/TSV syntax
	// error, which always fail the run regardless of MaxRowErrors.
	MaxRowErrors int
}

// RunConfig holds settings for the decode/dispatch pool that apply no matter which format is
// being read or written.
type RunConfig struct {
	// ReadBufferSize is the read buffer size, in bytes, used while scanning the input file.
	ReadBufferSize int
	// BatchSize is how many decoded documents a decode worker batches into one send to a dispatch
	// worker.
	BatchSize int
	// Workers is the number of concurrent decode workers.
	Workers int
	// SingleFileOutput requests one continuous output document spanning the whole run, for formats
	// that support it, instead of one document per batch.
	//
	// It forces Workers down to a single decode worker: see formatio.FinalizableRecordEncoder.
	SingleFileOutput bool
}

// NDJSONConfig holds settings specific to reading or writing NDJSON.
type NDJSONConfig struct {
	// ChunkSize is the reader's fixed work-queue chunk size, in bytes.
	ChunkSize int
}

// ParquetConfig holds settings specific to reading or writing Parquet.
type ParquetConfig struct {
	// MaxOpenReaders caps simultaneously-open row-group readers.
	MaxOpenReaders int
}

// CSVConfig holds settings specific to reading or writing CSV or TSV.
type CSVConfig struct {
	// Delimiter separates fields on a row. It defaults to ',' for CSV and '\t' for TSV.
	Delimiter rune
	// HasHeader says whether the first row names the columns rather than holding data.
	HasHeader bool
}

// Option configures an optional aspect of a Config.
type Option func(*Config)

// Transformer changes a decoded canonical record before it is encoded into the requested output
// format. It is only used on the record path; raw passthrough never decodes records and therefore
// cannot transform them.
type Transformer interface {
	Transform(record.Record) (record.Record, error)
}

// WithReadBufferSize sets the chunked readers' (NDJSON, CSV, TSV) read buffer size, in bytes.
// n<=0 falls back to the default.
func WithReadBufferSize(n int) Option {
	return func(o *Config) { o.Run.ReadBufferSize = n }
}

// WithBatchSize sets how many decoded documents a decode worker batches per send. n<=0 falls back
// to the default.
func WithBatchSize(n int) Option {
	return func(o *Config) { o.Run.BatchSize = n }
}

// WithParallelWorkers sets the number of decode workers: row-group workers for Parquet, or
// byte-range chunk workers for NDJSON. n<=0 falls back to the default.
func WithParallelWorkers(n int) Option {
	return func(o *Config) { o.Run.Workers = n }
}

// WithChunkSize sets the NDJSON reader's fixed work-queue chunk size, in bytes. n<=0 falls back
// to the default.
func WithChunkSize(n int) Option {
	return func(o *Config) { o.NDJSON.ChunkSize = n }
}

// WithMaxOpenReaders caps simultaneously-open Parquet row-group readers. n<=0 falls back to the
// configured worker count, not a fixed default.
func WithMaxOpenReaders(n int) Option {
	return func(o *Config) { o.Parquet.MaxOpenReaders = n }
}

// WithCSVDelimiter sets the field delimiter CSV or TSV reads and writes. Pass ',' for CSV or '\t'
// for TSV; the zero value falls back to the format's own default.
func WithCSVDelimiter(r rune) Option {
	return func(o *Config) { o.CSV.Delimiter = r }
}

// WithCSVHasHeader says whether a CSV or TSV file's first row names its columns rather than
// holding data. It defaults to false, so a headerless file must be explicitly opted out of.
func WithCSVHasHeader(b bool) Option {
	return func(o *Config) { o.CSV.HasHeader = b }
}

// WithInputFormat sets the source file's format explicitly. Omitting it, or passing the zero
// value, lets ProcessFile infer the input format from the file extension.
func WithInputFormat(f OutputFormat) Option {
	return func(o *Config) { o.InputFormat = f }
}

// WithOutputFormat sets the format documents are delivered in. Omitting it means FormatJSON.
func WithOutputFormat(f OutputFormat) Option {
	return func(o *Config) { o.OutputFormat = f }
}

// WithSingleFileOutput requests one continuous output document spanning the whole run, for
// formats that support it, instead of one document per batch. See RunConfig.SingleFileOutput.
func WithSingleFileOutput(b bool) Option {
	return func(o *Config) { o.Run.SingleFileOutput = b }
}

// WithLogger sets the logger that receives ProcessFile diagnostics.
func WithLogger(l Logger) Option {
	return func(c *Config) {
		c.Logger = l
	}
}

// WithMaxRowErrors caps how many rows may fail to decode and be skipped before the run fails,
// mirroring BigQuery's load-job max_bad_records: n<=0 means fail immediately on the first row
// error (the default). onSkip, if non-nil, is called once per row skipped under the cap.
func WithMaxRowErrors(n int, onSkip func(err error)) Option {
	return func(o *Config) {
		o.MaxRowErrors = n
		o.OnRowErrorSkip = onSkip
	}
}

// WithTransforms applies a set of rename/drop rules to every decoded record before encoding. An
// invalid rule set is stored on Config.OptionErr rather than returned directly.
func WithTransforms(rules ...PathTransformRule) Option {
	return func(c *Config) {
		if c.OptionErr != nil {
			return
		}
		transformer, err := NewPathTransformer(rules...)
		if err != nil {
			c.OptionErr = err
			return
		}
		c.Transformer = transformer
	}
}
