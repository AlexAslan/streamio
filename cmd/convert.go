package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"

	"github.com/AlexAslan/streamio"
)

// errUnknownFormat is returned when --in-format or --out-format names an unsupported format.
var errUnknownFormat = errors.New("unknown format: want json, parquet, csv, tsv, or arrow")

// errInvalidCSVDelimiter is returned when --csv-delimiter isn't exactly one character.
var errInvalidCSVDelimiter = errors.New("--csv-delimiter must be exactly one character")

// errNegativeMaxRowErrors is returned when --max-row-errors is negative.
var errNegativeMaxRowErrors = errors.New("--max-row-errors must not be negative")

// convertFlags holds the convert subcommand's flag values. outFormat is passed alongside these
// rather than folded in, since runConvert needs it before the rest are turned into options.
type convertFlags struct {
	in, out        string
	inFormat       string
	csvDelimiter   string
	readBufferSize int
	batchSize      int
	workers        int
	chunkSize      int
	maxOpenReaders int
	maxRowErrors   int
	csvHasHeader   bool
}

func getConvertCmd() *cobra.Command {
	var flags convertFlags
	var outFormat string

	cmd := &cobra.Command{
		Use:   "convert",
		Short: "Convert a document file between NDJSON, Parquet, CSV, TSV, and Arrow IPC",
		Long: `Convert reads --in, decodes it as NDJSON, Parquet, CSV, TSV, or Arrow IPC (auto-detected
from its extension, or overridden with --in-format), and writes it back out as --out-format to --out.

Parquet and Arrow output are always a single file: internally each streams every batch of rows into
it as its own row group (Parquet) or record batch (Arrow), keeping memory bounded to one batch
regardless of the input's total size, so converting a multi-gigabyte input does not require holding
it all in memory. Because a single writer for either format can't be driven by more than one
goroutine at once, --workers is forced to 1 whenever --out-format is parquet or arrow, regardless of
what it's set to. Arrow has no raw-passthrough route even Arrow-to-Arrow, unlike Parquet: it always
decodes to records and re-encodes.

For very large inputs, choose --batch-size so total rows / batch-size stays well under 32,767:
Parquet caps a file at that many row groups (one per batch), and a batch size too small for the
input's total size fails the whole conversion outright rather than merely costing a larger footer.

--csv-delimiter and --csv-has-header only apply when --in-format or --out-format is csv or tsv.
--csv-delimiter defaults to ',' for csv and '\t' for tsv; --csv-has-header defaults to false.

--max-row-errors caps how many rows may fail to decode and be skipped before the run fails,
mirroring BigQuery's load-job max_bad_records: 0 (the default) fails immediately on the first row
error; a positive N skips up to N bad rows before failing on the (N+1)th. Only applies to NDJSON
and CSV/TSV field errors — a CSV/TSV syntax error (e.g. an unterminated quoted field) and every
Parquet decode error always fail the run regardless of --max-row-errors.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runConvert(cmd.Context(), flags, outFormat)
		},
	}

	cmd.Flags().StringVar(&flags.in, "in", "", "input file path (required)")
	cmd.Flags().StringVar(&flags.out, "out", "", "output file path (required)")
	cmd.Flags().StringVar(&outFormat, "out-format", "json", "output format: json|parquet|csv|tsv|arrow")
	cmd.Flags().StringVar(&flags.inFormat, "in-format", "", "override input format detection: json|parquet|csv|tsv|arrow")
	cmd.Flags().IntVar(&flags.readBufferSize, "read-buffer-size", 0, "read buffer size in bytes (0 = default)")
	cmd.Flags().IntVar(&flags.batchSize, "batch-size", 0, "documents batched per dispatch (0 = default)")
	cmd.Flags().IntVar(&flags.workers, "workers", 0, "number of concurrent decode workers (0 = default)")
	cmd.Flags().IntVar(&flags.chunkSize, "chunk-size", 0, "NDJSON work-queue chunk size in bytes (0 = default)")
	cmd.Flags().IntVar(&flags.maxOpenReaders, "max-open-readers", 0,
		"max simultaneously open Parquet row-group readers (0 = default)")
	cmd.Flags().StringVar(&flags.csvDelimiter, "csv-delimiter", "",
		"CSV/TSV field delimiter, one character (default: ',' for csv, tab for tsv)")
	cmd.Flags().BoolVar(&flags.csvHasHeader, "csv-has-header", false,
		"CSV/TSV first row names the columns rather than holding data")
	cmd.Flags().IntVar(&flags.maxRowErrors, "max-row-errors", 0,
		"rows allowed to fail and be skipped before the run fails (0 = fail on the first)")

	for _, name := range []string{"in", "out"} {
		if err := cmd.MarkFlagRequired(name); err != nil {
			panic(fmt.Sprintf("convert: marking --%s required: %v", name, err))
		}
	}

	return cmd
}

// parseFormat maps a --in-format/--out-format flag value onto a streamio.Format.
func parseFormat(s string) (streamio.Format, error) {
	switch strings.ToLower(s) {
	case "", "json":
		return streamio.FormatJSON, nil
	case "parquet":
		return streamio.FormatParquet, nil
	case "csv":
		return streamio.FormatCSV, nil
	case "tsv":
		return streamio.FormatTSV, nil
	case "arrow":
		return streamio.FormatArrow, nil
	default:
		return "", fmt.Errorf("%w: %q", errUnknownFormat, s)
	}
}

// isCSVFamily reports whether f is CSV or TSV, the two formats --csv-delimiter/--csv-has-header
// apply to.
func isCSVFamily(f streamio.Format) bool {
	return f == streamio.FormatCSV || f == streamio.FormatTSV
}

// parseCSVDelimiter converts a --csv-delimiter flag value into the single rune streamio.Option
// expects, rejecting anything but exactly one character.
func parseCSVDelimiter(s string) (rune, error) {
	runes := []rune(s)
	if len(runes) != 1 {
		return 0, fmt.Errorf("%w: got %q", errInvalidCSVDelimiter, s)
	}
	return runes[0], nil
}

// csvOptions builds the CSV/TSV-specific options for a run involving inFormat or outFormat,
// returning none when neither is CSV or TSV.
func csvOptions(flags convertFlags, inFormat, outFormat streamio.Format) ([]streamio.Option, error) {
	if !isCSVFamily(inFormat) && !isCSVFamily(outFormat) {
		return nil, nil
	}

	opts := []streamio.Option{streamio.WithCSVHasHeader(flags.csvHasHeader)}
	if flags.csvDelimiter != "" {
		delimiter, err := parseCSVDelimiter(flags.csvDelimiter)
		if err != nil {
			return nil, fmt.Errorf("--csv-delimiter: %w", err)
		}
		opts = append(opts, streamio.WithCSVDelimiter(delimiter))
	}
	return opts, nil
}

// buildConvertOptions turns flags and outFormat into the streamio.Options ProcessFile needs.
func buildConvertOptions(flags convertFlags, outFormat streamio.Format) ([]streamio.Option, error) {
	opts := []streamio.Option{streamio.WithOutputFormat(outFormat)}
	inFormat := streamio.Format("")
	if flags.inFormat != "" {
		var err error
		inFormat, err = parseFormat(flags.inFormat)
		if err != nil {
			return nil, fmt.Errorf("--in-format: %w", err)
		}
		opts = append(opts, streamio.WithInputFormat(inFormat))
	}

	// csvOptions needs the input's actual format even when --in-format wasn't given, since
	// --csv-has-header/--csv-delimiter must apply to an auto-detected CSV/TSV input too; detect it
	// from --in the same way ProcessFile itself would.
	effectiveInFormat := inFormat
	if effectiveInFormat == "" {
		effectiveInFormat = streamio.DetectInputFormat(flags.in)
	}

	csvOpts, err := csvOptions(flags, effectiveInFormat, outFormat)
	if err != nil {
		return nil, err
	}
	opts = append(opts, csvOpts...)

	if flags.readBufferSize > 0 {
		opts = append(opts, streamio.WithReadBufferSize(flags.readBufferSize))
	}
	if flags.batchSize > 0 {
		opts = append(opts, streamio.WithBatchSize(flags.batchSize))
	}
	if flags.chunkSize > 0 {
		opts = append(opts, streamio.WithChunkSize(flags.chunkSize))
	}
	if flags.maxOpenReaders > 0 {
		opts = append(opts, streamio.WithMaxOpenReaders(flags.maxOpenReaders))
	}

	// A single Parquet or Arrow writer can't be driven by more than one goroutine, so single-file
	// streaming output forces one decode worker regardless of --workers; see WithSingleFileOutput's
	// doc.
	//
	// CSV/TSV output needs the same treatment: with more than one worker and no streaming encoder,
	// dispatch delivers batches in completion order (not input order) and each worker's own encoder
	// derives and (if --csv-has-header) writes its own header from its first batch, so the output
	// file would gain reordered rows and a repeated header. Forcing single-file streaming output
	// keeps one encoder deriving the header once and emitting rows in dispatch order for the whole
	// run.
	if outFormat == streamio.FormatParquet || outFormat == streamio.FormatArrow || isCSVFamily(outFormat) {
		opts = append(opts, streamio.WithSingleFileOutput(true), streamio.WithParallelWorkers(1))
	} else if flags.workers > 0 {
		opts = append(opts, streamio.WithParallelWorkers(flags.workers))
	}

	if flags.maxRowErrors < 0 {
		return nil, fmt.Errorf("%w: got %d", errNegativeMaxRowErrors, flags.maxRowErrors)
	}
	if flags.maxRowErrors > 0 {
		opts = append(opts, streamio.WithMaxRowErrors(flags.maxRowErrors, func(err error) {
			fmt.Fprintf(os.Stderr, "skipping row: %v\n", err)
		}))
	}

	return opts, nil
}

// runConvert builds the options ProcessFile needs from flags and outFormat, and drives the run to
// completion, writing every dispatched document to --out.
func runConvert(ctx context.Context, flags convertFlags, outFormatFlag string) (err error) {
	outFormat, err := parseFormat(outFormatFlag)
	if err != nil {
		return fmt.Errorf("--out-format: %w", err)
	}

	opts, err := buildConvertOptions(flags, outFormat)
	if err != nil {
		return err
	}

	out, err := newFileWriter(flags.out)
	if err != nil {
		return err
	}
	// fileWriter buffers output, so a flush/close failure (a full or failing filesystem) must
	// surface as a run failure rather than being silently discarded by a bare defer — otherwise
	// runConvert can report success after losing buffered output bytes.
	defer func() {
		if closeErr := out.close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing %s: %w", flags.out, closeErr)
		}
	}()

	// JSON dispatches one document per row with no separator of its own, so newlineWriter adds
	// one. Every other format's encoder already returns fully self-terminated batches — csvio's
	// rows and Parquet's row groups both concatenate correctly as-is.
	handler := out.write
	if outFormat == streamio.FormatJSON {
		handler = newlineWriter{fileWriter: out}.write
	}

	var result streamio.Result
	result, err = streamio.ProcessFile(ctx, flags.in, handler, opts...)
	if err != nil {
		if errors.Is(err, parquetgo.ErrTooManyRowGroups) {
			return fmt.Errorf(
				"converting %s to %s: %w (this input has more rows than --batch-size × 32,767 "+
					"row groups can hold; pass a larger --batch-size)",
				flags.in, flags.out, err)
		}
		var tooMany *streamio.TooManyRowErrorsError
		if errors.As(err, &tooMany) {
			for i, rowErr := range tooMany.Errors {
				fmt.Fprintf(os.Stderr, "row error %d/%d: %v\n", i+1, len(tooMany.Errors), rowErr)
			}
		}
		return fmt.Errorf("converting %s to %s: %w", flags.in, flags.out, err)
	}

	if result.Stats.RowsSkipped > 0 {
		fmt.Fprintf(os.Stdout, "wrote %s: %d rows read, %d rows skipped, %d documents dispatched\n",
			flags.out, result.Stats.RowsRead, result.Stats.RowsSkipped, result.Stats.DocumentsDispatched)
		return nil
	}

	fmt.Fprintf(os.Stdout, "wrote %s: %d rows read, %d documents dispatched\n",
		flags.out, result.Stats.RowsRead, result.Stats.DocumentsDispatched)
	return nil
}
