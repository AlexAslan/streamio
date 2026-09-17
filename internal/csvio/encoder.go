package csvio

import (
	"errors"
	"fmt"
	"streamio/internal/formatio"
	"streamio/internal/options"
	"streamio/internal/record"
)

// errUnsupportedKind reports a record.Kind this encoder has no CSV rendering for, which can only
// mean a Kind was added without teaching the encoders about it.
var errUnsupportedKind = errors.New("csv: unsupported value kind")

// errMapUnsupported reports a record.KindMap field: CSV has no nested-value representation, so a
// map column has nowhere to go. Mirrors parquetio.NewEncoder's identical scope limit.
var errMapUnsupported = errors.New("csv: map fields are not supported")

// errSchemaMismatch reports a record whose field count or names don't match the header the first
// batch fixed.
var errSchemaMismatch = errors.New("csv: record does not match the derived header")

// errEmptyHeader reports a leading record with no fields, which fixes no columns and so cannot
// define a header for the rest of the batch.
var errEmptyHeader = errors.New("csv: cannot derive a header from a record with no fields")

// encoder renders one batch of canonical records as one standalone CSV/TSV document: an optional
// header row followed by the batch's rows. It implements formatio.RecordEncoder, returning exactly
// one document per batch — matching parquetio's own batch-to-document granularity, since a CSV
// header is shared across every row in a document the same way a Parquet schema is shared across a
// row group.
//
// The header, when cfg.CSV.HasHeader is set, is derived from the first batch's field names and
// then fixed: every later batch's records must carry the same fields in the same order, mirroring
// parquetio.encoder's own schema-from-first-batch rule.
type encoder struct {
	rw        *rowWriter
	header    []string
	rowBuf    rowScratch
	delimiter rune
	hasHeader bool
}

// NewEncoder returns an encoder rendering a batch of canonical records as one CSV/TSV document.
// SingleFileOutput selects the streaming encoder below instead: see NewStreamingEncoder.
//
//nolint:ireturn // formatio.RecordEncoder is the constructor type streamio's format registry stores.
func NewEncoder(cfg options.Config) (formatio.RecordEncoder, error) {
	if cfg.Run.SingleFileOutput {
		return NewStreamingEncoder(cfg)
	}
	delimiter := delimiterFor(cfg, cfg.OutputFormat)
	return &encoder{
		delimiter: delimiter,
		hasHeader: cfg.CSV.HasHeader,
		rw:        newRowWriter(delimiter),
	}, nil
}

// EncodeBatch renders every record in batch as one CSV/TSV document: an optional header row, then
// one row per record. An empty batch produces no document.
func (e *encoder) EncodeBatch(batch []record.Record) ([][]byte, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	e.rw.reset()
	if e.header == nil {
		header, err := deriveHeader(batch[0])
		if err != nil {
			return nil, err
		}
		e.header = header
		if e.hasHeader {
			if err = e.rw.writeRow(headerFields(header)); err != nil {
				return nil, err
			}
		}
	}
	if err := writeRows(e.rw, e.header, batch, &e.rowBuf); err != nil {
		return nil, err
	}

	doc := e.rw.flush()
	if len(doc) == 0 {
		return nil, nil
	}
	return [][]byte{cloneBytes(doc)}, nil
}

// deriveHeader fixes a header from rec's own field names and order.
func deriveHeader(rec record.Record) ([]string, error) {
	if len(rec) == 0 {
		return nil, errEmptyHeader
	}
	names := make([]string, len(rec))
	for i := range rec {
		names[i] = rec[i].Name
	}
	return names, nil
}

// rowScratch is one row's rendered fields, held as sub-slices of one shared, reused byte buffer
// rather than N separately allocated []byte or string values — renderValue's formatting therefore
// allocates nothing per field; the only growth is buf's own occasional reallocation as it grows to
// the widest row's size, amortized across the whole batch. fields' sub-slices are only valid until
// the next reset call, since a later field's append can reallocate buf out from under them —
// reset() is always called before a row starts, not mid-row, so this never happens within a row.
type rowScratch struct {
	buf    []byte
	fields [][]byte
}

// reset prepares s to hold n fields for the next row, reusing buf's backing array.
func (s *rowScratch) reset(n int) {
	s.buf = s.buf[:0]
	if cap(s.fields) < n {
		s.fields = make([][]byte, n)
	} else {
		s.fields = s.fields[:n]
	}
}

// append renders v into the field at index i, extending s.buf and pointing fields[i] at the
// portion just appended.
func (s *rowScratch) append(i int, v record.Value) error {
	start := len(s.buf)
	buf, err := renderValue(s.buf, v)
	if err != nil {
		return err
	}
	s.buf = buf
	s.fields[i] = s.buf[start:len(s.buf):len(s.buf)]
	return nil
}

// writeRows renders every record in batch as one row each, checking each against header. rowBuf
// is reused across every row so rendering a batch allocates only for rowBuf's own occasional
// growth, not per row or per field.
func writeRows(rw *rowWriter, header []string, batch []record.Record, rowBuf *rowScratch) error {
	for i := range batch {
		if err := rowFields(rowBuf, header, batch[i]); err != nil {
			return err
		}
		if err := rw.writeRow(rowBuf.fields); err != nil {
			return err
		}
	}
	return nil
}

// rowFields renders rec's values into rowBuf, in header order, checking rec's field names and
// count match header exactly.
func rowFields(rowBuf *rowScratch, header []string, rec record.Record) error {
	if len(rec) != len(header) {
		return fmt.Errorf("%w: got %d fields, want %d", errSchemaMismatch, len(rec), len(header))
	}
	rowBuf.reset(len(rec))
	for i := range rec {
		if rec[i].Name != header[i] {
			return fmt.Errorf("%w: field %d is %q, want %q", errSchemaMismatch, i, rec[i].Name, header[i])
		}
		if err := rowBuf.append(i, rec[i].Value); err != nil {
			return err
		}
	}
	return nil
}

// headerFields renders a header row's column names as [][]byte, the shape writeRow expects.
func headerFields(header []string) [][]byte {
	fields := make([][]byte, len(header))
	for i, name := range header {
		fields[i] = []byte(name)
	}
	return fields
}

// cloneBytes copies b, since the caller's buffer is reused by the next call.
func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
