// Package streamio converts between document stream formats (NDJSON, Parquet), decoding
// an input file into canonical records and re-encoding it in the requested output format.
package streamio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlexAslan/streamio/internal/arrowio"
	"github.com/AlexAslan/streamio/internal/csvio"
	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/jsonio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/parquetio"
	"github.com/AlexAslan/streamio/internal/pool"
)

// ErrNoConversionPath is returned when the requested output format can't be produced from the
// input file's native format: the pair needs a decoder for the input and an encoder for the
// output, and at least one of the two isn't registered. Wrapped with both formats by ProcessFile.
var ErrNoConversionPath = errors.New("streamio: no conversion path")

// formatSupport is one format's entry in the registry: what it can do as an input and as an
// output. A nil field means that capability isn't implemented for the format.
type formatSupport struct {
	// newRawSource opens src for raw passthrough. Only consulted when the format is both the
	// input's native format and the requested output format.
	newRawSource func(cfg options.Config, src options.Source) (formatio.RawSource, error)

	// newRecordDecoder opens src as a stream of canonical records.
	newRecordDecoder func(cfg options.Config, src options.Source) (formatio.RecordDecoder, error)

	// newRecordEncoder builds an encoder rendering a batch of canonical records as documents. How
	// many documents a batch becomes is the encoder's own business — see formatio.RecordEncoder.
	newRecordEncoder func(cfg options.Config) (formatio.RecordEncoder, error)
}

// formatSupportFor is the format registry, keyed by a switch so the exhaustive linter flags any
// newly added OutputFormat left unwired. ok is false for a format with no support at all.
func formatSupportFor(f options.OutputFormat) (formatSupport, bool) {
	switch f {
	case options.FormatJSON:
		// jsonio's encoder renders one document per record: a JSON document is self-contained
		// per row.
		return formatSupport{
			newRawSource:     jsonio.NewRawSource,
			newRecordDecoder: jsonio.NewDecoder,
			newRecordEncoder: jsonio.NewEncoder,
		}, true
	case options.FormatParquet:
		// parquet's encoder renders one document per *batch*: a Parquet document is a schema, a
		// columnar layout, and a footer spanning many rows, so a lone record is not one. Both shapes
		// satisfy formatio.RecordEncoder, which is why the pool needs to know neither.
		return formatSupport{
			newRawSource:     parquetio.NewRawSource,
			newRecordDecoder: parquetio.NewDecoder,
			newRecordEncoder: parquetio.NewEncoder,
		}, true
	case options.FormatCSV, options.FormatTSV:
		// csvio implements both CSV and TSV: they differ only in field delimiter, which csvio
		// resolves from cfg.CSV.Delimiter or the format's own default. Its encoder renders one
		// document per batch, like parquetio's, since a header row is shared across every row
		// in a document.
		return formatSupport{
			newRawSource:     csvio.NewRawSource,
			newRecordDecoder: csvio.NewDecoder,
			newRecordEncoder: csvio.NewEncoder,
		}, true
	case options.FormatArrow:
		// arrowio has no raw source: ipc.FileWriter only accepts a live, already-decoded
		// arrow.RecordBatch, with no verbatim-bytes splice analogous to parquet-go's
		// Writer.WriteRowGroup, so even Arrow-to-Arrow goes through the generic decode/re-encode
		// path. Its encoder renders one document per batch, like parquetio's and csvio's.
		return formatSupport{
			newRecordDecoder: arrowio.NewDecoder,
			newRecordEncoder: arrowio.NewEncoder,
		}, true
	default:
		return formatSupport{}, false
	}
}

// DetectInputFormat returns the native format of the file at path, derived from its extension
// (case-insensitive): any extension not named below means NDJSON. ProcessFile calls this itself
// when WithInputFormat isn't given; it's exported so a caller that needs the format ahead of time
// (for example, to build format-specific options before calling ProcessFile) can run the same
// detection instead of duplicating or guessing at it.
func DetectInputFormat(path string) Format {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".parquet":
		return options.FormatParquet
	case ".csv":
		return options.FormatCSV
	case ".tsv":
		return options.FormatTSV
	case ".arrow":
		return options.FormatArrow
	default:
		return options.FormatJSON
	}
}

// ProcessFile reads the file at path and calls handler once per document, delivering each one in
// cfg.OutputFormat (options.FormatJSON unless WithOutputFormat says otherwise); see Option for
// tuning.
func ProcessFile(
	ctx context.Context,
	path string,
	handler DocumentHandler,
	opts ...Option,
) (Result, error) {
	// Validated before opening path, so an invalid option (e.g. a bad transform rule) is reported
	// as itself rather than masked by an unrelated "file not found" when path also happens not to
	// exist.
	if cfg := options.New(opts...); cfg.OptionErr != nil {
		return Result{}, cfg.OptionErr
	}

	f, err := os.Open(path)
	if err != nil {
		return Result{}, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return Result{}, err
	}

	return ProcessReaderAt(ctx, Source{Reader: f, Size: stat.Size(), Name: path}, handler, opts...)
}

// Source is a sized, randomly-addressable byte source ProcessReaderAt reads from: an *os.File, an
// in-memory buffer, or anything else providing ReadAt over a known span. Name labels it in
// diagnostics and error text and need not be a real file path.
type Source = options.Source

// ProcessReaderAt reads src and calls handler once per document, the same way ProcessFile does for
// an on-disk file — src need not be backed by a real file, only support ReadAt over its declared
// Size, which is what every format's chunked-parallel decoder actually requires.
func ProcessReaderAt(
	ctx context.Context,
	src Source,
	handler DocumentHandler,
	opts ...Option,
) (Result, error) {
	cfg := options.New(opts...)
	if cfg.OptionErr != nil {
		return Result{}, cfg.OptionErr
	}
	sink := options.DocumentHandler(handler)
	in := cfg.InputFormat
	if in == "" {
		in = DetectInputFormat(src.Name)
		cfg.InputFormat = in
	}

	// Requested output matches the input's own native format: raw passthrough, sending the
	// input's bytes to the handler untouched with no decode, no encode, no intermediate record.
	// Otherwise the generic path pairs the input's record decoder with the output's record
	// encoder through canonical record.Records (e.g. Parquet in/JSON out, NDJSON in/Parquet out).
	//
	// WithSingleFileOutput is one thing that rules raw passthrough out even when the formats
	// match: raw passthrough dispatches one independent, already-closed document per input chunk
	// (one standalone file per Parquet row group) with no Finalize step to stitch them into one
	// file, so concatenating several of them is not a valid file — only a FinalizableRecordEncoder
	// on the generic path can produce one. Parquet-in/Parquet-out with WithSingleFileOutput
	// therefore decodes to records and re-encodes through streamingEncoder instead of splicing
	// row groups verbatim, trading that verbatim-copy optimization for a file that is actually
	// valid.
	//
	// A format with no raw source at all (arrowio, which has no verbatim-bytes splice analogous to
	// parquet-go's WriteRowGroup — see formatSupportFor's FormatArrow comment) always takes the
	// generic path too, same-format conversion included, rather than reaching processRaw only to
	// have it fail with ErrNoConversionPath despite the decoder/encoder pair actually existing.
	support, hasRawSource := formatSupportFor(in)
	if cfg.OutputFormat == in && !cfg.Run.SingleFileOutput && hasRawSource && support.newRawSource != nil {
		return processRaw(ctx, src, sink, cfg, in)
	}
	return processRecords(ctx, src, sink, cfg, in)
}

// processRaw streams the input's own bytes to the handler, when that's already what was asked for.
func processRaw(
	ctx context.Context,
	src options.Source,
	sink options.DocumentHandler,
	cfg options.Config,
	in options.OutputFormat,
) (options.Result, error) {
	support, ok := formatSupportFor(in)
	if !ok || support.newRawSource == nil {
		return options.Result{}, fmt.Errorf("%w: %s has no raw source", ErrNoConversionPath, in)
	}

	raw, err := support.newRawSource(cfg, src)
	if err != nil {
		return options.Result{}, err
	}
	defer raw.Close()

	return pool.RunRaw(ctx, cfg, raw, sink)
}

// processRecords is the generic cross-format path: decode the input to canonical records, then
// encode them in the requested output format, via the same pool raw passthrough uses.
//
// The two formats never know about each other; they meet only through record.Record, which is
// what lets a new format be added by filling in one formatSupport entry.
func processRecords(
	ctx context.Context,
	src options.Source,
	sink options.DocumentHandler,
	cfg options.Config,
	in options.OutputFormat,
) (options.Result, error) {
	out := cfg.OutputFormat

	inSupport, inOK := formatSupportFor(in)
	if !inOK || inSupport.newRecordDecoder == nil {
		return options.Result{}, fmt.Errorf("%w from %s to %s: %s has no record decoder",
			ErrNoConversionPath, in, out, in)
	}

	outSupport, outOK := formatSupportFor(out)
	if !outOK || outSupport.newRecordEncoder == nil {
		return options.Result{}, fmt.Errorf("%w from %s to %s: %s has no record encoder",
			ErrNoConversionPath, in, out, out)
	}

	dec, err := inSupport.newRecordDecoder(cfg, src)
	if err != nil {
		return options.Result{}, err
	}
	defer dec.Close()

	newEncoder := func() (formatio.RecordEncoder, error) { return outSupport.newRecordEncoder(cfg) }

	return pool.RunRecords(ctx, cfg, dec, newEncoder, cfg.Transformer, sink)
}
