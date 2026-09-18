package jsonio

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

// recordDecoder decodes a disjoint share of s's byte-range chunks into canonical records, one per
// line. It implements formatio.SplittableRecordDecoder.
//
// Scope is flat top-level objects of scalar values only; see errNestedValue.
type recordDecoder struct {
	s *sharedState

	// lines iterates the chunk this decoder currently owns, nil when it owns none.
	lines *chunkLines

	// json decodes one NDJSON line at a time into a canonical record.
	json *ObjectDecoder
}

// NewDecoder opens src as a stream of canonical records. Lines are parsed as JSON tokens rather
// than through reflection, so a scalar field costs no boxing and no map insert.
//
// NewDecoder does not take ownership of src.Reader; the caller closes it, if it needs closing, once
// done with every decoder Split hands out.
//
//nolint:ireturn // formatio.RecordDecoder is the constructor type streamio's format registry stores.
func NewDecoder(cfg options.Config, src options.Source) (formatio.RecordDecoder, error) {
	return openShared(cfg, src).newRecordDecoder(), nil
}

// newRecordDecoder builds one decode worker's decoder over s, with its own scratch and no
// ownership of the input file.
func (s *sharedState) newRecordDecoder() *recordDecoder {
	return &recordDecoder{s: s, json: NewObjectDecoder()}
}

// Split returns up to limit decoders over the same file, this one first, capped at the number of
// chunks the file actually has: a chunk is the smallest claimable unit (chunks are claimed from a
// shared counter), so a decoder beyond that count would only ever find the queue empty and report
// io.EOF on its first call. Mirrors parquetio.recordDecoder.Split capping at row-group count.
func (d *recordDecoder) Split(limit int) []formatio.RecordDecoder {
	n := limit
	if n < 1 {
		n = 1
	}
	if chunks := int((d.s.size + int64(d.s.chunkSize) - 1) / int64(d.s.chunkSize)); n > chunks && chunks > 0 {
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

// DecodeNext fills batch with the next lines of this decoder's share of the file, claiming further
// chunks as it exhausts them, and returns io.EOF once the shared queue is empty.
func (d *recordDecoder) DecodeNext(ctx context.Context, batch []record.Record) (int, error) {
	n := 0

	for n < len(batch) {
		if err := ctx.Err(); err != nil {
			return n, err
		}

		if d.lines == nil {
			opened, err := d.openNextChunk()
			if err != nil {
				return n, err
			}
			if !opened {
				return n, io.EOF
			}
		}

		line, err := d.lines.next()
		if errors.Is(err, io.EOF) {
			d.lines = nil
			continue
		}
		if err != nil {
			return n, err
		}

		rec, err := d.decodeLine(batch[n], line)
		if err != nil {
			// A malformed line never invalidates d.lines' position in the rest of the chunk — the
			// next DecodeNext call picks up at the following line — so this is exactly the error
			// formatio.RowError exists to let the pool skip past under RowErrorSkip.
			return n, &formatio.RowError{Err: err}
		}
		batch[n] = rec
		n++
	}

	return n, nil
}

// openNextChunk claims the next unclaimed chunk and opens a line iterator over it, reporting false
// when the file is exhausted.
func (d *recordDecoder) openNextChunk() (bool, error) {
	chunkIdx, chunkStart, ok := d.s.claimChunk()
	if !ok {
		return false, nil
	}

	lines, err := d.s.openChunk(chunkIdx, chunkStart)
	if err != nil {
		return false, fmt.Errorf("ndjson %s: chunk %d: %w", d.s.name, chunkIdx, err)
	}

	d.lines = lines
	return true, nil
}

// decodeLine refills rec from one NDJSON line, in the line's own field order.
func (d *recordDecoder) decodeLine(rec record.Record, line []byte) (record.Record, error) {
	rec, err := d.json.Decode(rec, line)
	if err != nil {
		return rec, fmt.Errorf("ndjson %s: %w", d.s.name, err)
	}
	return rec, nil
}
