package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"unicode"
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

	// tinyPct is the share below which a non-zero percentage would print as 0.0%.
	tinyPct = 0.05
)

// errInvalidTop is returned when --top is below 1.
var errInvalidTop = errors.New("--top must be at least 1")

// errUnknownStatsOutput is returned when --output names an unsupported rendering.
var errUnknownStatsOutput = errors.New("unknown output: want table or json")

// statsFlags holds the stats subcommand's flag values.
type statsFlags struct {
	in            string
	inFormat      string
	inCompression string
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
values it holds, and its --top most frequent values with their counts and shares. Compressed input
is detected and decompressed transparently.

An attribute is empty when it is missing, null, a zero-length string, or an empty list; whitespace
is not empty. Nested entries are reported as dotted paths (attrs.key), a list counts as the single
value [list], and an attribute repeated within one record counts once. A value is counted by the
text it renders as, so the number 1 and the string "1" are one value. CSV and TSV have no header
unless --csv-has-header is given, in which case the first row names the columns.

Counting distinct values costs memory in proportion to how many there are, so each decode worker
tracks at most --max-distinct values per attribute exactly. Past that it keeps approximate counts of
the most frequent values instead: the attribute is marked with + in the table, its counts are
estimates, and ~ marks a count that may be off. Non-empty counts and percentages stay exact.

Columns appear in first-seen order. With --workers above 1 and NDJSON whose keys differ from record
to record, that order can vary between runs. A file under one chunk (32 MiB by default) is read by
a single worker; lower --chunk-size to spread a smaller file across workers.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runStats(cmd.Context(), os.Stdout, flags)
		},
	}

	cmd.Flags().StringVar(&flags.in, "in", "", "input file path (required)")
	cmd.Flags().StringVar(&flags.inFormat, "in-format", "", "override input format detection: json|parquet|csv|tsv|arrow")
	cmd.Flags().StringVar(&flags.inCompression, "in-compression", "", inCompressionHelp)
	cmd.Flags().StringVar(&flags.output, "output", outputTable, "output rendering: table|json")
	cmd.Flags().IntVar(&flags.top, "top", defaultStatsTop, "most frequent values to show per attribute (at least 1)")
	cmd.Flags().IntVar(&flags.maxDistinct, "max-distinct", 0,
		"distinct values tracked exactly per attribute per worker (0 = default 100000)")
	cmd.Flags().StringSliceVar(&flags.columns, "columns", nil,
		"only profile these attributes and what is nested under them (comma-separated)")
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
	var (
		opts     []streamio.Option
		inFormat streamio.Format
	)
	if flags.inFormat != "" {
		var err error
		inFormat, err = parseFormat(flags.inFormat)
		if err != nil {
			return nil, fmt.Errorf("--in-format: %w", err)
		}
	} else {
		// Sniffed from the contents, like convert, so CSV options apply to a mislabeled file too.
		detected, err := detectFileFormat(flags.in)
		if err != nil {
			return nil, err
		}
		inFormat = detected
	}
	// Pinned so the library doesn't sniff the same bytes a second time.
	opts = append(opts, streamio.WithInputFormat(inFormat))

	if flags.inCompression != "" {
		comp, err := parseCompression(flags.inCompression)
		if err != nil {
			return nil, fmt.Errorf("--in-compression: %w", err)
		}
		opts = append(opts, streamio.WithInputCompression(comp))
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

	if flags.top < 1 {
		return nil, fmt.Errorf("%w: got %d", errInvalidTop, flags.top)
	}

	readOpts, err := readOptions(readSizes{
		batchSize:      flags.batchSize,
		chunkSize:      flags.chunkSize,
		maxOpenReaders: flags.maxOpenReader,
		maxRowErrors:   flags.maxRowErrors,
	})
	if err != nil {
		return nil, err
	}
	opts = append(opts, readOpts...)

	opts = append(opts, streamio.WithProfileTopN(flags.top), streamio.WithProfileMaxDistinct(flags.maxDistinct))
	if len(flags.columns) > 0 {
		opts = append(opts, streamio.WithProfileColumns(flags.columns...))
	}
	if flags.workers > 0 {
		opts = append(opts, streamio.WithParallelWorkers(flags.workers))
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

	for _, name := range missingColumns(flags.columns, prof) {
		fmt.Fprintf(os.Stderr, "warning: --columns %q matched no attribute\n", name)
	}

	if flags.output == outputJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(jsonSafe(prof))
	}
	return renderStatsTable(w, flags.in, prof, flags.maxValueWidth)
}

// missingColumns returns the requested --columns that produced no attribute in prof.
func missingColumns(requested []string, prof streamio.Profile) []string {
	var missing []string
	for _, want := range requested {
		found := false
		for _, c := range prof.Columns {
			if c.Name == want || strings.HasPrefix(c.Name, want+".") {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, want)
		}
	}
	return missing
}

// jsonSafe returns prof with every value that isn't valid UTF-8 quoted: encoding/json would turn
// each invalid byte into U+FFFD, making different binary values print identically.
func jsonSafe(prof streamio.Profile) streamio.Profile {
	out := prof
	out.Columns = make([]streamio.ColumnStats, len(prof.Columns))
	for i, c := range prof.Columns {
		out.Columns[i] = c
		out.Columns[i].TopValues = make([]streamio.ValueCount, len(c.TopValues))
		for j, v := range c.TopValues {
			if !utf8.ValidString(v.Value) {
				v.Value = strconv.Quote(v.Value)
			}
			out.Columns[i].TopValues[j] = v
		}
	}
	return out
}

// renderStatsTable writes prof as a summary table followed by each attribute's top values.
func renderStatsTable(w io.Writer, path string, prof streamio.Profile, maxWidth int) error {
	tw := tabwriter.NewWriter(w, 0, 0, tablePadding, ' ', 0)

	fmt.Fprintf(tw, "file:\t%s\n", display(path, maxWidth))
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
		fmt.Fprintf(tw, "%s\t%d\t%s\t%d%s\n", display(c.Name, maxWidth), c.NonEmpty, formatPct(c.Pct), c.Distinct, approx)
	}

	for _, c := range prof.Columns {
		if len(c.TopValues) == 0 {
			continue
		}
		fmt.Fprintf(tw, "\ntop values of %s", display(c.Name, maxWidth))
		if !c.DistinctExact {
			fmt.Fprint(tw, " (approximate: distinct limit reached)")
		}
		fmt.Fprintln(tw, ":")
		for _, v := range c.TopValues {
			count := strconv.FormatInt(v.Count, 10)
			if v.MaxError > 0 {
				count = "~" + count
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\n", display(v.Value, maxWidth), count, formatPct(v.Pct))
		}
	}

	if prof.ColumnsTruncated {
		fmt.Fprintln(tw, "\nnote: more attributes exist than are reported; the rest were left out")
	}
	return tw.Flush()
}

// formatPct renders a 0–100 percentage, keeping a tiny non-zero share from reading as zero.
func formatPct(p float64) string {
	if p > 0 && p < tinyPct {
		return "<0.1%"
	}
	return fmt.Sprintf("%.1f%%", p)
}

// display makes s safe to print in one table cell: a value holding control characters or invalid
// UTF-8, or nothing but whitespace, is shown quoted, and anything longer than maxWidth is cut.
func display(s string, maxWidth int) string {
	if !utf8.ValidString(s) || strings.IndexFunc(s, unicode.IsControl) >= 0 ||
		(s != "" && strings.TrimSpace(s) == "") {
		s = strconv.Quote(s)
	}
	return truncate(s, maxWidth)
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
