package parquetio

import (
	"bytes"
	"errors"
	"fmt"
	"streamio/internal/formatio"
	"streamio/internal/options"
	"streamio/internal/record"

	parquetgo "github.com/parquet-go/parquet-go"
)

// errEncodeAfterFinalize is returned by EncodeBatch once Finalize has already run: the writer is
// closed by then, and without this guard parquet-go's WriteRows/Flush on a closed writer silently
// accept the call and discard the rows — a nil error with no document, indistinguishable from an
// ordinary small batch not yet crossing the internal flush threshold. Rather than let that data
// loss pass unnoticed, EncodeBatch refuses outright once finalized.
var errEncodeAfterFinalize = errors.New("parquet: EncodeBatch called after Finalize")

// streamingEncoder renders every batch it is given as one row group of a single Parquet file
// spanning the whole run, instead of one complete file per batch. It implements
// formatio.FinalizableRecordEncoder.
//
// Peak memory per call is bounded regardless of total row or batch count, which is what makes
// multi-gigabyte single-file output practical (see BenchmarkStreamingEncoder_MemoryBoundedness).
// Choose a batch size that keeps total rows ÷ batch size well under 32,767: that many row groups
// is parquet-go's hard cap, and going over fails the whole conversion rather than just growing the
// footer.
type streamingEncoder struct {
	rowBuilder

	// writer is constructed once the schema is known from the first batch, and lives until Finalize
	// closes it.
	writer *parquetgo.GenericWriter[any]

	// buf is the writer's destination, reset after every flush so it holds only the bytes flushed
	// since the last EncodeBatch or Finalize call — those are the ones copied out and returned, in
	// contrast to encoder's buf, which holds a whole file.
	buf bytes.Buffer

	// finalized is set once Finalize has closed the writer, so a stray EncodeBatch call after that
	// point is rejected instead of silently discarding rows into a closed writer.
	finalized bool
}

// NewStreamingEncoder returns a formatio.FinalizableRecordEncoder producing one continuous
// multi-row-group Parquet document across every batch in the run.
//
//nolint:ireturn // formatio.RecordEncoder is the constructor type streamio's format registry stores.
func NewStreamingEncoder(_ options.Config) (formatio.RecordEncoder, error) {
	return &streamingEncoder{}, nil
}

// EncodeBatch derives the schema from the first batch if needed, then writes batch as one row
// group and flushes it: the returned document is that row group's bytes, not yet a complete file.
// An empty batch returns no document.
func (e *streamingEncoder) EncodeBatch(batch []record.Record) ([][]byte, error) {
	if e.finalized {
		return nil, errEncodeAfterFinalize
	}
	if len(batch) == 0 {
		return nil, nil
	}

	if e.schema == nil {
		if err := e.deriveSchema(batch); err != nil {
			return nil, err
		}
		e.writer = parquetgo.NewGenericWriter[any](&e.buf, e.schema)
	}

	rows, err := e.buildRows(batch)
	if err != nil {
		return nil, err
	}

	e.buf.Reset()
	if _, err = e.writer.WriteRows(rows); err != nil {
		return nil, fmt.Errorf("parquet: writing %d rows: %w", len(rows), err)
	}
	if err = e.writer.Flush(); err != nil {
		return nil, fmt.Errorf("parquet: flushing row group: %w", err)
	}
	if e.buf.Len() == 0 {
		return nil, nil
	}
	return [][]byte{cloneBuf(&e.buf)}, nil
}

// Finalize closes the writer, emitting the footer and trailer that make every row group EncodeBatch
// returned, concatenated in order, one valid Parquet file. An empty run returns no document.
func (e *streamingEncoder) Finalize() ([]byte, error) {
	e.finalized = true
	if e.writer == nil {
		return nil, nil
	}

	e.buf.Reset()
	if err := e.writer.Close(); err != nil {
		return nil, fmt.Errorf("parquet: closing streaming writer: %w", err)
	}
	if e.buf.Len() == 0 {
		return nil, nil
	}
	return cloneBuf(&e.buf), nil
}
