package arrowio

import (
	"errors"
	"fmt"
	"streamio/internal/formatio"
	"streamio/internal/options"
	"streamio/internal/record"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// errEncodeAfterFinalize is returned by EncodeBatch once Finalize has already run — the same guard
// parquetio.streamingEncoder uses, since ipc.FileWriter's Close (like parquet-go's) makes the
// writer unusable afterward and this package would rather refuse a stray call outright than risk
// silently losing rows.
var errEncodeAfterFinalize = errors.New("arrow: EncodeBatch called after Finalize")

// growingSink is an io.Writer that appends everything written to it into buf, and hands EncodeBatch
// only the bytes written since the last Drain — the delta, not the whole file so far.
//
// This exists because ipc.FileWriter, unlike parquet-go's writer, has no Reset/Flush: it tracks a
// running byte offset internally (for the file's own block-offset table, read back by Close when it
// writes the footer) that must stay continuous across the whole file, so the destination it writes
// to cannot be swapped or truncated mid-stream the way parquetio's streamingEncoder resets its
// buffer onto a fresh one every batch. Draining (copying out and truncating) after every Write
// instead keeps memory bounded to one batch's own bytes without disturbing that offset — the sink
// only forgets bytes it has already handed out, never the position the writer itself is tracking.
type growingSink struct {
	buf []byte
}

func (s *growingSink) Write(p []byte) (int, error) {
	s.buf = append(s.buf, p...)
	return len(p), nil
}

// Drain returns everything written since the last Drain, copied out so the caller can hold it past
// the next Write, and truncates the sink back to empty.
func (s *growingSink) Drain() []byte {
	if len(s.buf) == 0 {
		return nil
	}
	out := make([]byte, len(s.buf))
	copy(out, s.buf)
	s.buf = s.buf[:0]
	return out
}

// streamingEncoder renders every batch it is given as one record batch of a single Arrow IPC file
// spanning the whole run, instead of one complete file per batch. It implements
// formatio.FinalizableRecordEncoder.
//
// Unlike parquetio.streamingEncoder, this cannot bound peak memory to one batch's own row data
// while the file is open: ipc.FileWriter accumulates a small (offset, length) block-table entry per
// record batch written so far, kept until Close writes the footer, but never buffers the row data
// itself past a Write call — data bytes go straight to growingSink and are drained out immediately,
// so peak memory is still one batch's encoded size, not the whole run's.
type streamingEncoder struct {
	alloc  memory.Allocator
	rb     *array.RecordBuilder
	writer *ipc.FileWriter
	schemaBuilder
	sink      growingSink
	finalized bool
}

// NewStreamingEncoder returns a formatio.FinalizableRecordEncoder producing one continuous Arrow
// IPC file, with one record batch per call to EncodeBatch, across every batch in the run.
//
//nolint:ireturn // formatio.RecordEncoder is the constructor type streamio's format registry stores.
func NewStreamingEncoder(_ options.Config) (formatio.RecordEncoder, error) {
	return &streamingEncoder{alloc: memory.NewGoAllocator()}, nil
}

// EncodeBatch derives the schema from the first batch if needed, then writes batch as one record
// batch: the returned document is the bytes written for that one record batch, not yet a complete
// file. An empty batch returns no document.
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
		e.rb = array.NewRecordBuilder(e.alloc, e.schema)

		w, err := ipc.NewFileWriter(&e.sink, ipc.WithSchema(e.schema), ipc.WithAllocator(e.alloc))
		if err != nil {
			return nil, fmt.Errorf("arrow: opening file writer: %w", err)
		}
		e.writer = w
	}

	rec, err := e.buildRecord(e.rb, batch)
	if err != nil {
		return nil, err
	}
	defer rec.Release()

	if err = e.writer.Write(rec); err != nil {
		return nil, fmt.Errorf("arrow: writing record batch: %w", err)
	}

	if doc := e.sink.Drain(); len(doc) > 0 {
		return [][]byte{doc}, nil
	}
	return nil, nil
}

// Finalize closes the writer, emitting the footer and trailer that make every record batch
// EncodeBatch returned, concatenated in order, one valid Arrow IPC file. An empty run (no batch
// ever derived a schema) returns no document.
func (e *streamingEncoder) Finalize() ([]byte, error) {
	e.finalized = true
	if e.writer == nil {
		return nil, nil
	}

	if err := e.writer.Close(); err != nil {
		return nil, fmt.Errorf("arrow: closing streaming writer: %w", err)
	}
	return e.sink.Drain(), nil
}
