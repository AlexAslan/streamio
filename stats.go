package streamio

import (
	"context"
	"os"

	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/profile"
)

// ValueCount is one value and how many records held it.
type ValueCount struct {
	// Value is the value rendered as text, the way the converters render it.
	Value string `json:"value"`
	// Count is how many records held the value; an estimate when MaxError is non-zero.
	Count int64 `json:"count"`
	// MaxError bounds how far Count may be from the true count. It is zero unless the attribute
	// exceeded WithProfileMaxDistinct.
	MaxError int64 `json:"max_error,omitempty"`
	// Pct is Count as a percentage (0–100) of all records.
	Pct float64 `json:"pct"`
}

// ColumnStats summarizes one attribute of a profiled file.
type ColumnStats struct {
	// Name is the attribute path; nested map entries are dotted.
	Name string `json:"name"`
	// TopValues are the most frequent non-empty values, most frequent first.
	TopValues []ValueCount `json:"top_values"`
	// NonEmpty counts the records where the attribute is present and neither null nor empty.
	NonEmpty int64 `json:"non_empty"`
	// Pct is NonEmpty as a percentage (0–100) of all records.
	Pct float64 `json:"pct"`
	// Distinct is how many distinct non-empty values were counted.
	Distinct int64 `json:"distinct"`
	// DistinctExact is false once the attribute exceeded WithProfileMaxDistinct, making Distinct a
	// lower bound and the TopValues counts estimates (see ValueCount.MaxError).
	DistinctExact bool `json:"distinct_exact"`
}

// Profile is the result of ProfileFile.
type Profile struct {
	// Columns lists every attribute in first-seen order.
	Columns []ColumnStats `json:"columns"`
	// Records is the number of records read.
	Records int64 `json:"records"`
	// RowsSkipped is how many rows WithMaxRowErrors allowed to be skipped; they are not in Records.
	RowsSkipped int64 `json:"rows_skipped"`
	// PredicateMisses is how many WithWhere comparisons found a value of the wrong type for their
	// literal and did not match.
	PredicateMisses int64 `json:"predicate_misses,omitempty"`
	// ColumnsTruncated is true when the file had more distinct attribute paths than are tracked.
	ColumnsTruncated bool `json:"columns_truncated"`
}

// ProfileFile reads the file at path and summarizes it: the record count, how often each attribute
// is non-empty, and its most frequent values.
//
// An attribute is empty when it is missing, null, a zero-length string, or an empty list; whitespace
// is not empty. Nested entries are named by dotted path, and an attribute repeated within one
// record counts once, from its first occurrence. A value counts under the text it renders as, so
// the number 1 and the string "1" are one value, and a list counts under the single value "[list]".
//
// Each decode worker tracks up to WithProfileMaxDistinct values per attribute exactly, then falls
// back to approximate heavy-hitter counting; non-empty counts stay exact either way. Columns are
// reported in first-seen order, which for NDJSON whose keys differ between records can vary with
// the worker count.
func ProfileFile(ctx context.Context, path string, opts ...Option) (Profile, error) {
	f, err := os.Open(path)
	if err != nil {
		return Profile{}, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return Profile{}, err
	}
	return ProfileReaderAt(ctx, Source{Reader: f, Size: stat.Size(), Name: path}, opts...)
}

// ProfileReaderAt summarizes src the same way ProfileFile does for an on-disk file.
//
// Every input goes through the same canonical-record path ProcessFile uses, so equal data gives an
// equal profile whichever format or compression it arrives in. See ProfileFile for what counts as
// empty and how attributes are named.
func ProfileReaderAt(ctx context.Context, src Source, opts ...Option) (Profile, error) {
	cfg := options.New(opts...)
	if cfg.OptionErr != nil {
		return Profile{}, cfg.OptionErr
	}

	set := profile.NewSet(profile.Config{
		Columns:     cfg.Profile.Columns,
		TopN:        cfg.Profile.TopN,
		MaxDistinct: cfg.Profile.MaxDistinct,
	})
	newCollector := func() (RecordConsumer, error) { return set.NewCollector() }

	result, err := ConsumeReaderAt(ctx, src, newCollector, opts...)
	if err != nil {
		return Profile{}, err
	}

	merged := set.Merge()
	out := Profile{
		Records:          merged.Records,
		RowsSkipped:      result.Stats.RowsSkipped,
		PredicateMisses:  result.Stats.PredicateMisses,
		ColumnsTruncated: merged.ColumnsTruncated,
		Columns:          make([]ColumnStats, 0, len(merged.Columns)),
	}
	for _, c := range merged.Columns {
		cs := ColumnStats{
			Name:          c.Name,
			NonEmpty:      c.NonEmpty,
			Pct:           c.Pct,
			Distinct:      c.Distinct,
			DistinctExact: c.DistinctExact,
			TopValues:     make([]ValueCount, 0, len(c.TopValues)),
		}
		for _, v := range c.TopValues {
			cs.TopValues = append(cs.TopValues, ValueCount(v))
		}
		out.Columns = append(out.Columns, cs)
	}
	return out, nil
}
