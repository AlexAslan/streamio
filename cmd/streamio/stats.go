package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/AlexAslan/streamio"
)

// Defaults for stats' display flags, and its table's column padding.
const (
	defaultStatsTop      = 10
	defaultMaxValueWidth = 40
	tablePadding         = 2
	outputTable          = "table"
	outputJSON           = "json"
)

// errNegativeTop is returned when --top is negative.
var errNegativeTop = errors.New("--top must not be negative")

// errUnknownStatsOutput is returned when --output names an unsupported rendering.
var errUnknownStatsOutput = errors.New("unknown output: want table or json")

// statsFlags holds the stats subcommand's flag values.
type statsFlags struct {
	in            string
	inFormat      string
	output        string
	csvDelimiter  string
	columns       []string
	top           int
	maxDistinct   int
	maxValueWidth int
	workers       int
	batchSize     int
	chunkSize     int
	maxOpenReader int
	maxRowErrors  int
	csvHasHeader  bool
}

func getStatsCmd() *cobra.Command {
	var flags statsFlags

	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Profile a document file: record count, per-attribute fill rate, and top values",
		Long: `Stats reads --in in one pass and reports the total record count and, for every attribute,
how many records have a non-empty value (and what share of all records that is), how many distinct
values it holds, and its --top most frequent values with their counts and shares.

An attribute is empty when it is missing, null, or a zero-length string; whitespace is not empty.
Nested Parquet map entries are reported as dotted paths (attrs.key).

Counting distinct values costs memory in proportion to how many there are, so each attribute stops
adding new distinct values after --max-distinct. Fill rates stay exact; an attribute past the cap is
marked approximate (distinct and value counts are then lower bounds).

Column order is first-seen. With --workers above 1 and NDJSON records whose keys differ from row to
row, that order can vary between runs.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runStats(cmd.Context(), os.Stdout, flags)
		},
	}

	cmd.Flags().StringVar(&flags.in, "in", "", "input file path (required)")
	cmd.Flags().StringVar(&flags.inFormat, "in-format", "", "override input format detection: json|parquet|csv|tsv|arrow")
	cmd.Flags().StringVar(&flags.output, "output", outputTable, "output rendering: table|json")
	cmd.Flags().IntVar(&flags.top, "top", defaultStatsTop, "most frequent values to show per attribute")
	cmd.Flags().IntVar(&flags.maxDistinct, "max-distinct", 0,
		"distinct values counted per attribute before it is marked approximate (0 = default 100000)")
	cmd.Flags().StringSliceVar(&flags.columns, "columns", nil, "only profile these attributes (comma-separated)")
	cmd.Flags().IntVar(&flags.maxValueWidth, "max-value-width", defaultMaxValueWidth,
		"truncate displayed values to this many characters")
	cmd.Flags().IntVar(&flags.workers, "workers", 0, "number of concurrent decode workers (0 = default)")
	cmd.Flags().IntVar(&flags.batchSize, "batch-size", 0, "records decoded per batch (0 = default)")
	cmd.Flags().IntVar(&flags.chunkSize, "chunk-size", 0, "NDJSON/CSV/TSV work-queue chunk size in bytes (0 = default)")
	cmd.Flags().IntVar(&flags.maxOpenReader, "max-open-readers", 0,
		"max simultaneously open Parquet row-group readers (0 = default)")
	cmd.Flags().StringVar(&flags.csvDelimiter, "csv-delimiter", "",
		"CSV/TSV field delimiter, one character (default: ',' for csv, tab for tsv)")
	cmd.Flags().BoolVar(&flags.csvHasHeader, "csv-has-header", false,
		"CSV/TSV first row names the columns rather than holding data")
	cmd.Flags().IntVar(&flags.maxRowErrors, "max-row-errors", 0,
		"rows allowed to fail and be skipped before the run fails (0 = fail on the first)")

	if err := cmd.MarkFlagRequired("in"); err != nil {
		panic(fmt.Sprintf("stats: marking --in required: %v", err))
	}
	return cmd
}

// buildStatsOptions turns flags into the streamio.Options ProfileFile needs.
func buildStatsOptions(flags statsFlags) ([]streamio.Option, error) {
	var opts []streamio.Option

	var inFormat streamio.Format
	if flags.inFormat != "" {
		var err error
		inFormat, err = parseFormat(flags.inFormat)
		if err != nil {
			return nil, fmt.Errorf("--in-format: %w", err)
		}
		opts = append(opts, streamio.WithInputFormat(inFormat))
	} else {
		inFormat = streamio.DetectInputFormat(flags.in)
	}

	// Reuses convert's CSV flag handling; stats has no output format, so only the input matters.
	csvOpts, err := csvOptions(convertFlags{
		csvDelimiter: flags.csvDelimiter,
		csvHasHeader: flags.csvHasHeader,
	}, inFormat, inFormat)
	if err != nil {
		return nil, err
	}
	opts = append(opts, csvOpts...)

	if flags.top < 0 {
		return nil, fmt.Errorf("%w: got %d", errNegativeTop, flags.top)
	}
	if flags.maxRowErrors < 0 {
		return nil, fmt.Errorf("%w: got %d", errNegativeMaxRowErrors, flags.maxRowErrors)
	}

	opts = append(opts, streamio.WithProfileTopN(flags.top), streamio.WithProfileMaxDistinct(flags.maxDistinct))
	if len(flags.columns) > 0 {
		opts = append(opts, streamio.WithProfileColumns(flags.columns...))
	}
	if flags.workers > 0 {
		opts = append(opts, streamio.WithParallelWorkers(flags.workers))
	}
	if flags.batchSize > 0 {
		opts = append(opts, streamio.WithBatchSize(flags.batchSize))
	}
	if flags.chunkSize > 0 {
		opts = append(opts, streamio.WithChunkSize(flags.chunkSize))
	}
	if flags.maxOpenReader > 0 {
		opts = append(opts, streamio.WithMaxOpenReaders(flags.maxOpenReader))
	}
	if flags.maxRowErrors > 0 {
		opts = append(opts, streamio.WithMaxRowErrors(flags.maxRowErrors, func(err error) {
			fmt.Fprintf(os.Stderr, "skipping row: %v\n", err)
		}))
	}
	return opts, nil
}

// runStats profiles --in and writes the result to w.
func runStats(ctx context.Context, w io.Writer, flags statsFlags) error {
	switch flags.output {
	case outputTable, outputJSON:
	default:
		return fmt.Errorf("--output: %w: %q", errUnknownStatsOutput, flags.output)
	}

	opts, err := buildStatsOptions(flags)
	if err != nil {
		return err
	}

	prof, err := streamio.ProfileFile(ctx, flags.in, opts...)
	if err != nil {
		var tooMany *streamio.TooManyRowErrorsError
		if errors.As(err, &tooMany) {
			for i, rowErr := range tooMany.Errors {
				fmt.Fprintf(os.Stderr, "row error %d/%d: %v\n", i+1, len(tooMany.Errors), rowErr)
			}
		}
		return fmt.Errorf("profiling %s: %w", flags.in, err)
	}

	if flags.output == outputJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(prof)
	}
	return renderStatsTable(w, flags.in, prof, flags.maxValueWidth)
}

// renderStatsTable writes prof as a summary table followed by each attribute's top values.
func renderStatsTable(w io.Writer, path string, prof streamio.Profile, maxWidth int) error {
	tw := tabwriter.NewWriter(w, 0, 0, tablePadding, ' ', 0)

	fmt.Fprintf(tw, "file:\t%s\n", path)
	fmt.Fprintf(tw, "records:\t%d\n", prof.Records)
	if prof.RowsSkipped > 0 {
		fmt.Fprintf(tw, "skipped:\t%d\n", prof.RowsSkipped)
	}
	fmt.Fprintf(tw, "attributes:\t%d\n\n", len(prof.Columns))

	fmt.Fprintln(tw, "ATTRIBUTE\tNON-EMPTY\t%\tDISTINCT")
	for _, c := range prof.Columns {
		approx := ""
		if !c.DistinctExact {
			approx = "+"
		}
		fmt.Fprintf(tw, "%s\t%d\t%.1f%%\t%d%s\n",
			truncate(c.Name, maxWidth), c.NonEmpty, c.Pct, c.Distinct, approx)
	}

	for _, c := range prof.Columns {
		if len(c.TopValues) == 0 {
			continue
		}
		fmt.Fprintf(tw, "\ntop values of %s", truncate(c.Name, maxWidth))
		if !c.DistinctExact {
			fmt.Fprint(tw, " (approximate: distinct cap reached)")
		}
		fmt.Fprintln(tw, ":")
		for _, v := range c.TopValues {
			fmt.Fprintf(tw, "  %s\t%d\t%.1f%%\n", truncate(quoteIfBlank(v.Value), maxWidth), v.Count, v.Pct)
		}
	}

	if prof.ColumnsTruncated {
		fmt.Fprintln(tw, "\nnote: more attributes exist than are tracked; the rest were left out")
	}
	return tw.Flush()
}

// quoteIfBlank makes whitespace-only values visible, since "empty" ones are never listed.
func quoteIfBlank(s string) string {
	if strings.TrimSpace(s) == "" {
		return fmt.Sprintf("%q", s)
	}
	return s
}

// truncate shortens s to at most n characters, ending with an ellipsis; n<=0 means no limit.
func truncate(s string, n int) string {
	if n <= 0 || utf8.RuneCountInString(s) <= n {
		return s
	}
	runes := []rune(s)
	if n == 1 {
		return string(runes[:1])
	}
	return string(runes[:n-1]) + "…"
}
