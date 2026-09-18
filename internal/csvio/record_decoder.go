package csvio

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

// errFieldCountMismatch reports a row whose field count doesn't match the header (or, headerless,
// the first row read), which fixes how many columns every row must have.
var errFieldCountMismatch = errors.New("csv: row has a different field count than the header")

// recordDecoder decodes a disjoint share of s's byte-range chunks into canonical records, one per
// row. It implements formatio.SplittableRecordDecoder.
//
// Unlike jsonio's line-at-a-time decoder, this one drives one persistent *csv.Reader per claimed
// chunk instead of parsing each row through a throwaway csv.Reader: encoding/csv has no exported
// way to repoint an existing Reader at a new source, so reuse means reading many rows off one
// continuous stream, not one Reader per row. This is also what lets ReuseRecord apply at all —
// see reader.go's csvRowReader for the reused-slice contract this implies for field lifetimes.
type recordDecoder struct {
	s *sharedState

	// rows reads the chunk this decoder currently owns, nil when it owns none.
	rows *csvRowReader
}

// NewDecoder opens src as a stream of canonical records. NewDecoder does not take ownership of
// src.Reader; the caller closes it, if it needs closing, once done with every decoder Split hands
// out.
//
//nolint:ireturn // formatio.RecordDecoder is the constructor type streamio's format registry stores.
func NewDecoder(cfg options.Config, src options.Source) (formatio.RecordDecoder, error) {
	s, err := openShared(cfg, src, cfg.InputFormat)
	if err != nil {
		return nil, err
	}

	return s.newRecordDecoder(), nil
}

// newRecordDecoder builds one decode worker's decoder over s.
func (s *sharedState) newRecordDecoder() *recordDecoder {
	return &recordDecoder{s: s}
}

// Split returns up to limit decoders over the same file, this one first, capped at the number of
// chunks the file actually has, mirroring jsonio.recordDecoder.Split.
func (d *recordDecoder) Split(limit int) []formatio.RecordDecoder {
	n := limit
	if n < 1 {
		n = 1
	}
	body := d.s.size - d.s.bodyStart
	if chunks := int((body + int64(d.s.chunkSize) - 1) / int64(d.s.chunkSize)); n > chunks && chunks > 0 {
		n = chunks
	}

	decoders := make([]formatio.RecordDecoder, 0, n)
	decoders = append(decoders, d)
	for range n - 1 {
		decoders = append(decoders, d.s.newRecordDecoder())
	}
	return decoders
}

// Close is a no-op: recordDecoder does not own src.Reader.
func (d *recordDecoder) Close() error {
	return nil
}

// DecodeNext fills batch with the next rows of this decoder's share of the file, claiming further
// chunks as it exhausts them, and returns io.EOF once the shared queue is empty.
func (d *recordDecoder) DecodeNext(ctx context.Context, batch []record.Record) (int, error) {
	n := 0

	for n < len(batch) {
		if err := ctx.Err(); err != nil {
			return n, err
		}

		if d.rows == nil {
			opened, err := d.openNextChunk()
			if err != nil {
				return n, err
			}
			if !opened {
				return n, io.EOF
			}
		}

		fields, err := d.rows.next()
		if errors.Is(err, io.EOF) {
			d.rows = nil
			continue
		}
		if err != nil {
			return n, err
		}

		rec, err := d.decodeRow(batch[n], fields)
		if err != nil {
			// decodeRow's own field-count check runs after d.rows.next() already returned a
			// complete row — the reader's position is unaffected by decodeRow rejecting it — so
			// this, unlike a raw csv.Reader parse error (see next()'s doc for why that one is not
			// safely skippable), is exactly the error formatio.RowError exists to let the pool skip
			// past under RowErrorSkip.
			return n, &formatio.RowError{Err: err}
		}
		batch[n] = rec
		n++
	}

	return n, nil
}

// openNextChunk claims the next unclaimed chunk and opens a row reader over it, reporting false
// when the file is exhausted.
func (d *recordDecoder) openNextChunk() (bool, error) {
	chunkIdx, chunkStart, ok := d.s.claimChunk()
	if !ok {
		return false, nil
	}

	rows, err := d.s.openChunkRows(chunkIdx, chunkStart)
	if err != nil {
		return false, fmt.Errorf("csv %s: chunk %d: %w", d.s.name, chunkIdx, err)
	}

	d.rows = rows
	return true, nil
}

// decodeRow refills rec from one already-parsed CSV/TSV row, one field per column, in header
// order. fields is only valid until the next call to d.rows.next (csvRowReader sets ReuseRecord),
// so each field is copied into rec rather than aliased.
func (d *recordDecoder) decodeRow(rec record.Record, fields []string) (record.Record, error) {
	header := d.s.ensureHeader(len(fields))
	if len(fields) != len(header) {
		return rec, fmt.Errorf("csv %s: %w: got %d fields, want %d",
			d.s.name, errFieldCountMismatch, len(fields), len(header))
	}

	rec = rec.Reset()
	for i, v := range fields {
		rec = rec.Append(header[i], record.Bytes([]byte(v)))
	}
	return rec, nil
}

// ensureHeader returns s.header, synthesizing it from fieldCount on first use when the file has no
// header row: "col0".."colN" is a pure function of the row's own field count, not of its content,
// so every worker that happens to synthesize it independently derives the identical names.
// sync.Once just skips the redundant work after the first caller has done it, and blocks any
// concurrent caller until that first one finishes, so every caller sees the final s.header.
func (s *sharedState) ensureHeader(fieldCount int) []string {
	if s.hasHeader {
		return s.header
	}

	s.headerOnce.Do(func() {
		names := make([]string, fieldCount)
		for i := range names {
			names[i] = fmt.Sprintf("col%d", i)
		}
		s.header = names
	})
	return s.header
}

// csvRowReader drives one persistent *csv.Reader over one claimed chunk's byte range, so
// encoding/csv's own buffering and ReuseRecord actually apply across many rows instead of being
// rebuilt (and thereby defeated) on every row.
//
// Its underlying section reader extends to EOF, not just to the chunk's nominal chunkSize (see
// sharedState.openChunkReader): next stops claiming further rows once csv.Reader.InputOffset
// reports at least chunkSize bytes consumed, but only *after* finishing whatever row was in
// progress at that point — InputOffset reports "the end of the most recently read row," so this
// check can only ever look at completed-row boundaries, never truncate mid-row. This can never
// collide with the next chunk's own claim: chunk boundaries are strictly increasing offsets, and
// the next chunk's own boundary resolution (openChunkReader) independently skips forward past any
// row it lands inside — exactly the same skip this decoder's own start already relies on — so the
// row this reader finishes past its nominal end is the same row the next chunk's reader would
// otherwise have had to skip past.
type csvRowReader struct {
	r    *csv.Reader
	stop int64 // InputOffset value past which no further row is claimed.
}

// openChunkRows opens a csvRowReader over chunk chunkIdx, starting at chunkStart.
func (s *sharedState) openChunkRows(chunkIdx, chunkStart int64) (*csvRowReader, error) {
	br, boundaryBytesRead, err := s.openChunkReader(chunkIdx, chunkStart)
	if err != nil {
		return nil, err
	}

	cr := csv.NewReader(br)
	cr.Comma = s.delimiter
	cr.FieldsPerRecord = -1
	cr.ReuseRecord = true

	return &csvRowReader{
		r:    cr,
		stop: int64(s.chunkSize) - boundaryBytesRead,
	}, nil
}

// next returns this chunk's next row, or io.EOF once the chunk's share of rows is exhausted. The
// returned slice is only valid until the next call to next, since the underlying csv.Reader has
// ReuseRecord set.
//
// A parse error from c.r.Read() (e.g. an unterminated quoted field) is deliberately not wrapped as
// a formatio.RowError: encoding/csv's Reader can consume an unbounded number of further physical
// lines while looking for a quoted field's closing quote before giving up, so "skip just this row"
// has no well-defined meaning here — the reader's position after such an error is not guaranteed
// to be the start of the next row. Only decodeRow's own field-count check, which runs after a
// complete row has already been read successfully, is safe to treat as skippable.
func (c *csvRowReader) next() ([]string, error) {
	if c.r.InputOffset() >= c.stop {
		return nil, io.EOF
	}
	return c.r.Read()
}
