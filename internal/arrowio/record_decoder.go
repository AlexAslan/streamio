package arrowio

import (
	"context"
	"fmt"
	"io"
	"streamio/internal/formatio"
	"streamio/internal/options"
	"streamio/internal/record"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// columnMeta is one top-level column's schema-derived classification, looked up once per file
// rather than reclassified per row.
type columnMeta struct {
	dataType arrow.DataType
	name     string
	kind     record.Kind
	semantic record.Semantic
}

// recordDecoder decodes a disjoint share of s's record batches into canonical records. It
// implements formatio.SplittableRecordDecoder.
type recordDecoder struct {
	batch   arrow.RecordBatch
	s       *sharedState
	columns []columnMeta
	row     int
}

// NewDecoder opens src as a stream of canonical records. NewDecoder does not take ownership of
// src.Reader; the caller closes it, if it needs closing, once done with every decoder Split hands
// out. The caller must still Close the returned decoder, to release any record batch it holds.
//
//nolint:ireturn // formatio.RecordDecoder is the constructor type streamio's format registry stores.
func NewDecoder(cfg options.Config, src options.Source) (formatio.RecordDecoder, error) {
	s, err := openShared(cfg, src)
	if err != nil {
		return nil, err
	}

	columns, err := classifySchema(s.schema)
	if err != nil {
		return nil, err
	}

	return &recordDecoder{s: s, columns: columns}, nil
}

// classifySchema builds one columnMeta per top-level field, in schema order.
func classifySchema(schema *arrow.Schema) ([]columnMeta, error) {
	fields := schema.Fields()
	columns := make([]columnMeta, len(fields))
	for i, f := range fields {
		kind, semantic, err := classify(f.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", f.Name, err)
		}
		columns[i] = columnMeta{name: f.Name, dataType: f.Type, kind: kind, semantic: semantic}
	}
	return columns, nil
}

// Split returns up to limit decoders over the same file, this one first, capped at the number of
// record batches the file actually has: a record batch is the smallest claimable unit, so a
// decoder beyond that count would only ever find the queue empty. Mirrors
// parquetio.recordDecoder.Split capping at row-group count.
func (d *recordDecoder) Split(limit int) []formatio.RecordDecoder {
	n := min(limit, d.s.numRecords)
	if n < 1 {
		n = 1
	}

	decoders := make([]formatio.RecordDecoder, 0, n)
	decoders = append(decoders, d)
	for range n - 1 {
		decoders = append(decoders, &recordDecoder{s: d.s, columns: d.columns})
	}
	return decoders
}

// Close releases any record batch this decoder still holds; it does not own src.Reader or the
// shared *ipc.FileReader (every Split sibling references the same one, so only ProcessFile's own
// wrapper — which owns src.Reader — is positioned to close it, the same division of ownership
// parquetio's decoder uses for its shared *parquetgo.File).
func (d *recordDecoder) Close() error {
	d.closeBatch()
	return nil
}

// closeBatch releases the currently open record batch, if any. Safe to call repeatedly.
func (d *recordDecoder) closeBatch() {
	if d.batch != nil {
		d.batch.Release()
		d.batch = nil
	}
	d.row = 0
}

// DecodeNext fills batch with the next rows of this decoder's share of the file, claiming further
// record batches as it exhausts them, and returns io.EOF once the shared queue is empty.
func (d *recordDecoder) DecodeNext(ctx context.Context, batch []record.Record) (int, error) {
	n := 0

	for n < len(batch) {
		if err := ctx.Err(); err != nil {
			return n, err
		}

		if d.batch == nil {
			opened, err := d.openNextBatch()
			if err != nil {
				return n, err
			}
			if !opened {
				return n, io.EOF
			}
		}

		if d.row >= int(d.batch.NumRows()) {
			d.closeBatch()
			continue
		}

		rec, err := d.buildRecord(batch[n])
		if err != nil {
			return n, err
		}
		batch[n] = rec
		d.row++
		n++
	}

	return n, nil
}

// openNextBatch claims the next unclaimed record batch and opens it, reporting false when the file
// is exhausted. RecordBatchAt is documented safe for concurrent use, so every decoder claiming
// against the same atomic counter can call it without further coordination.
func (d *recordDecoder) openNextBatch() (bool, error) {
	idx := d.s.nextBatch.Add(1) - 1
	if idx >= int64(d.s.numRecords) {
		return false, nil
	}

	rb, err := d.s.reader.RecordBatchAt(int(idx))
	if err != nil {
		return false, fmt.Errorf("arrow: record batch %d: %w", idx, err)
	}

	d.batch = rb
	d.row = 0
	return true, nil
}

// buildRecord refills rec from row d.row of the currently open batch: one field per top-level
// column, in schema order. Every value is copied out of the batch's arrays before this returns —
// see readColumnValue and its nested-value callees — since the batch (and everything it owns) is
// Release()'d once exhausted, and record.Value's own documented lifetime promises a caller only
// that a value survives until the next decode call, not past a batch boundary either way.
func (d *recordDecoder) buildRecord(rec record.Record) (record.Record, error) {
	rec = rec.Reset()

	for c, col := range d.columns {
		arr := d.batch.Column(c)
		v, err := readColumnValue(arr, d.row, col.kind, col.semantic)
		if err != nil {
			return rec, fmt.Errorf("field %q: %w", col.name, err)
		}
		rec = rec.Append(col.name, v)
	}

	return rec, nil
}

// readColumnValue reads arr's value at row i as a canonical Value, dispatching to a nested
// reader for KindMap/KindList and to scalarValue for everything else. Recursion has no depth
// limit — a List of Struct of List, etc., decodes to whatever depth the schema actually has,
// mirroring jsonio.ObjectDecoder's own recursive decodeValue.
func readColumnValue(arr arrow.Array, i int, kind record.Kind, semantic record.Semantic) (record.Value, error) {
	if arr.IsNull(i) {
		return record.Null(), nil
	}

	switch kind {
	case record.KindMap:
		st, ok := arr.(*array.Struct)
		if !ok {
			return record.Value{}, fmt.Errorf("%w: %T is not a struct array", errUnsupportedArrowType, arr)
		}
		return readStruct(st, i)
	case record.KindList:
		l, ok := arr.(*array.List)
		if !ok {
			return record.Value{}, fmt.Errorf("%w: %T is not a list array", errUnsupportedArrowType, arr)
		}
		return readList(l, i)
	case record.KindNull, record.KindBool, record.KindInt64, record.KindFloat64, record.KindBytes:
		return scalarValue(arr, i, kind, semantic)
	default:
		return scalarValue(arr, i, kind, semantic)
	}
}

// readStruct reads row i of a Struct array as a record.KindMap: one field per struct field, in the
// struct's own schema order, field types classified fresh (a nested struct's fields are not
// necessarily the same shape as the top-level schema).
func readStruct(a *array.Struct, i int) (record.Value, error) {
	st, ok := a.DataType().(*arrow.StructType)
	if !ok {
		return record.Value{}, fmt.Errorf("%w: struct array has non-struct type %s", errUnsupportedArrowType, a.DataType())
	}

	entries := make(record.Record, 0, st.NumFields())
	for f := range st.NumFields() {
		field := st.Field(f)
		kind, semantic, err := classify(field.Type)
		if err != nil {
			return record.Value{}, fmt.Errorf("field %q: %w", field.Name, err)
		}

		v, err := readColumnValue(a.Field(f), i, kind, semantic)
		if err != nil {
			return record.Value{}, fmt.Errorf("field %q: %w", field.Name, err)
		}
		entries = entries.Append(field.Name, v)
	}

	return record.Map(entries), nil
}

// readList reads row i of a List array as a record.KindList: every element in the row's own
// value range, classified from the list's declared element type.
func readList(a *array.List, i int) (record.Value, error) {
	lt, ok := a.DataType().(*arrow.ListType)
	if !ok {
		return record.Value{}, fmt.Errorf("%w: list array has non-list type %s", errUnsupportedArrowType, a.DataType())
	}
	kind, semantic, err := classify(lt.Elem())
	if err != nil {
		return record.Value{}, err
	}

	start, end := a.ValueOffsets(i)
	values := a.ListValues()

	elements := make([]record.Value, 0, end-start)
	for j := start; j < end; j++ {
		v, elemErr := readColumnValue(values, int(j), kind, semantic)
		if elemErr != nil {
			return record.Value{}, elemErr
		}
		elements = append(elements, v)
	}

	return record.List(elements), nil
}
