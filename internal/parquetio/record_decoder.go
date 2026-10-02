package parquetio

import (
	"context"
	"errors"
	"io"

	parquetgo "github.com/AlexAslan/parquet-go"
	"github.com/AlexAslan/parquet-go/format"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

// recordDecoder decodes a disjoint share of s's row groups into canonical records. It implements
// formatio.SplittableRecordDecoder.
type recordDecoder struct {
	s *sharedState

	// reader is the current row group's reader, nil when none is open.
	reader *parquetgo.GenericReader[any]

	// mapKeys and mapVals accumulate one row's Map(String,String) columns, which arrive as two
	// separate leaf columns and can only be zipped once both have been seen. Keys become Field
	// names and so must be strings; values are handed on as-is, referencing the page buffer.
	mapKeys map[string][]string
	mapVals map[string][][]byte

	// mapEntries holds the nested Record per map column, one slot per row position in the current
	// batch (sized to s.batchSize) rather than one shared buffer reused on every row: like rows
	// (below), a slot is embedded by reference into the record returned for that row position and
	// must survive until the caller is done with the whole batch, so it can only be reset and
	// reused starting the next DecodeNext call — never again within the same batch-fill loop.
	mapEntries map[string][]record.Record

	// rows is the ReadRows landing buffer. Its Values are only released at the start of the next
	// DecodeNext, since the records handed to the caller reference the pages behind them.
	rows []parquetgo.Row

	// held is how many of rows are still referenced by the batch the caller is working on.
	held int

	// slotHeld tracks whether this decoder currently owns a readerSlots slot.
	slotHeld bool
}

// NewDecoder opens src as a stream of canonical records. NewDecoder does not take ownership of
// src.Reader; the caller closes it, if it needs closing, once done with every decoder Split hands
// out. The caller must still Close the returned decoder, to release any row group reader it holds.
//
//nolint:ireturn // formatio.RecordDecoder is the constructor type streamio's format registry stores.
func NewDecoder(cfg options.Config, src options.Source) (formatio.RecordDecoder, error) {
	s, err := openShared(cfg, src)
	if err != nil {
		return nil, err
	}

	return s.newRecordDecoder(), nil
}

// newRecordDecoder builds one decode worker's decoder over s, with its own scratch and no
// ownership of the input file.
func (s *sharedState) newRecordDecoder() *recordDecoder {
	d := &recordDecoder{
		s:    s,
		rows: make([]parquetgo.Row, s.batchSize),
	}

	if len(s.mapOrder) > 0 {
		d.mapKeys = make(map[string][]string, len(s.mapOrder))
		d.mapVals = make(map[string][][]byte, len(s.mapOrder))
		d.mapEntries = make(map[string][]record.Record, len(s.mapOrder))
		for _, field := range s.mapOrder {
			d.mapEntries[field] = make([]record.Record, s.batchSize)
		}
	}

	return d
}

// Split returns up to limit decoders over the same file, this one first, capped at the row-group
// count: a row group is the smallest claimable unit, so extra decoders past that would only find
// the queue empty.
func (d *recordDecoder) Split(limit int) []formatio.RecordDecoder {
	n := min(limit, len(d.s.rowGroups))
	if n < 1 {
		n = 1
	}

	decoders := make([]formatio.RecordDecoder, 0, n)
	decoders = append(decoders, d)
	for range n - 1 {
		decoders = append(decoders, d.s.newRecordDecoder())
	}
	return decoders
}

// Close releases any row group this decoder still holds; it does not own src.Reader.
func (d *recordDecoder) Close() error {
	d.closeRowGroup()
	return nil
}

// DecodeNext fills batch with the next rows of this decoder's share of the file, crossing row
// group boundaries as needed, and returns io.EOF once the shared queue is empty.
func (d *recordDecoder) DecodeNext(ctx context.Context, batch []record.Record) (int, error) {
	n, err := d.decodeNext(ctx, batch)
	if err != nil && !errors.Is(err, io.EOF) {
		// Nothing else will call back into this decoder, so release the row group now rather than
		// leaving its reader — and its readerSlots slot — held until Close.
		d.closeRowGroup()
	}
	return n, err
}

// decodeNext is DecodeNext's body, split out so its every error return releases the current row
// group without each one having to remember to.
func (d *recordDecoder) decodeNext(ctx context.Context, batch []record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	d.releaseRows()

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		if d.reader == nil {
			opened, err := d.openNextRowGroup(ctx)
			if err != nil {
				return 0, err
			}
			if !opened {
				return 0, io.EOF
			}
		}

		n, err := d.readOpenRowGroup(batch)
		if err != nil {
			return 0, err
		}
		if n > 0 {
			return n, nil
		}
		// An exhausted row group that yielded nothing: claim the next one rather than reporting a
		// zero-length batch the caller would have to loop on itself.
	}
}

// readOpenRowGroup reads the currently open row group into batch, building a record per row, and
// closes that row group once it runs out. A zero n means this row group had nothing left and the
// caller should claim the next one.
func (d *recordDecoder) readOpenRowGroup(batch []record.Record) (int, error) {
	n, readErr := d.reader.ReadRows(d.rows[:min(len(batch), len(d.rows))])
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return 0, readErr
	}
	d.held = n

	for i := range d.rows[:n] {
		batch[i] = d.buildRecord(batch[i], d.rows[i], i)
	}

	if errors.Is(readErr, io.EOF) {
		d.closeRowGroup()
	}
	return n, nil
}

// releaseRows drops the previous call's references to the row-group reader's page buffers, now
// that the caller has finished with the batch built from them. It can't happen any earlier: the
// records handed out reference those pages directly rather than copying out of them.
func (d *recordDecoder) releaseRows() {
	for i := range d.rows[:d.held] {
		d.rows[i] = nil
	}
	d.held = 0
}

// openNextRowGroup claims the next unclaimed row group and opens a reader over it, reporting false
// when the queue is empty. The reader slot is acquired here and released by closeRowGroup.
func (d *recordDecoder) openNextRowGroup(ctx context.Context) (bool, error) {
	idx := d.s.nextRowGroup.Add(1) - 1
	if idx >= int64(len(d.s.rowGroups)) {
		return false, nil
	}

	select {
	case d.s.readerSlots <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	}

	d.slotHeld = true
	d.reader = parquetgo.NewGenericRowGroupReader[any](d.s.rowGroups[idx])
	return true, nil
}

// closeRowGroup releases the current row-group reader and its slot, if either is held. It is safe
// to call repeatedly. The reader's Close only releases read-side buffers — there is no flush to
// fail — so its error is deliberately dropped rather than displacing the decode error that
// usually brings us here.
func (d *recordDecoder) closeRowGroup() {
	if d.reader != nil {
		_ = d.reader.Close()
		d.reader = nil
	}
	if d.slotHeld {
		<-d.s.readerSlots
		d.slotHeld = false
	}
}

// buildRecord refills rec from one parquet row: scalar columns in schema order, then every
// non-empty Map(String,String) column in the schema-derived mapOrder. Field names come straight
// from the schema — format decoders keep source field names intact, and caller-specific
// rename/drop policy belongs in a configured record transformer instead — and map ordering goes
// through the schema-derived mapOrder, so the field set a record carries depends only on the
// file's schema and never on a given row's values. slot is row's position within the current
// batch, threaded through to appendMapFields so each row's Map field gets its own scratch buffer
// rather than one shared across every row in the batch; see mapEntries' doc.
func (d *recordDecoder) buildRecord(rec record.Record, row parquetgo.Row, slot int) record.Record {
	rec = rec.Reset()
	d.resetMapScratch()

	row.Range(func(columnIndex int, columnValues []parquetgo.Value) bool {
		if columnIndex >= len(d.s.leafPaths) {
			return true
		}

		path := d.s.leafPaths[columnIndex]
		name := path[0]

		if isMapLeaf(path) {
			d.collectMapLeaf(name, path[2], columnValues)
			return true
		}

		// A repeated or absent column yields no field at all: there is no single value to carry.
		if len(columnValues) == 1 {
			rec = rec.Append(name, scalarValue(columnValues[0], d.s.columnMeta[columnIndex].logicalType))
		}
		return true
	})

	return d.appendMapFields(rec, slot)
}

// resetMapScratch truncates each map column's scratch to zero length rather than clearing the
// maps: clear removes the entries themselves, so the next row's append would start from a nil
// slice and reallocate, defeating the reuse this scratch exists for.
func (d *recordDecoder) resetMapScratch() {
	for _, field := range d.s.mapOrder {
		d.mapKeys[field] = d.mapKeys[field][:0]
		d.mapVals[field] = d.mapVals[field][:0]
	}
}

// collectMapLeaf accumulates one leaf — the "key" half or the "value" half — of field's
// Map(String,String) column. Nulls are skipped, which is what makes vals potentially shorter than
// keys; see appendMapFields.
func (d *recordDecoder) collectMapLeaf(field, leaf string, columnValues []parquetgo.Value) {
	for _, v := range columnValues {
		if v.IsNull() {
			continue
		}
		if leaf == "key" {
			d.mapKeys[field] = append(d.mapKeys[field], string(v.ByteArray()))
			continue
		}
		d.mapVals[field] = append(d.mapVals[field], v.ByteArray())
	}
}

// appendMapFields appends one KindMap field per non-empty map column, in mapOrder. A key with no
// corresponding value is dropped rather than reconstructed, and an empty map omits its field
// entirely rather than carrying an empty object nothing was stored in. slot selects this row's own
// scratch buffer per field (see mapEntries' doc) — critical, not just an optimization: the
// returned rec embeds entries by reference, and every row in the batch is built before the caller
// reads any of them, so reusing one buffer across rows here would let a later row's Append
// silently overwrite an earlier row's already-returned Map value in place.
func (d *recordDecoder) appendMapFields(rec record.Record, slot int) record.Record {
	for _, field := range d.s.mapOrder {
		keys := d.mapKeys[field]
		if len(keys) == 0 {
			continue
		}

		vals := d.mapVals[field]
		entries := d.mapEntries[field][slot].Reset()
		for i, k := range keys {
			if i >= len(vals) {
				break
			}
			entries = entries.Append(k, record.Bytes(vals[i]))
		}
		d.mapEntries[field][slot] = entries

		rec = rec.Append(field, record.Map(entries))
	}

	return rec
}

// scalarValue classifies one parquet column value as a canonical Value, stopping at
// classification: a date stays a raw day count and a timestamp stays a raw epoch integer, tagged
// with what they mean, for the encoder to render.
func scalarValue(v parquetgo.Value, lt *format.LogicalType) record.Value {
	if v.IsNull() {
		return record.Null()
	}

	switch v.Kind() {
	case parquetgo.Boolean:
		return record.Bool(v.Boolean())
	case parquetgo.Int32:
		return int32Value(v, lt)
	case parquetgo.Int64:
		return int64Value(v, lt)
	case parquetgo.Int96:
		// Deprecated physical type; carried as its decimal big-integer string, since no Kind here
		// holds a 96-bit integer and the string is what every encoder can render.
		return record.Bytes([]byte(v.Int96().String()))
	case parquetgo.Float:
		return record.Float32(v.Float())
	case parquetgo.Double:
		return record.Float64(v.Double())
	case parquetgo.ByteArray, parquetgo.FixedLenByteArray:
		return record.Bytes(v.ByteArray())
	default:
		return record.Bytes([]byte(v.String()))
	}
}

// int32Value classifies an Int32 value, honoring DateType/TimestampType/unsigned IntType
// logical-type annotations.
func int32Value(v parquetgo.Value, lt *format.LogicalType) record.Value {
	if lt != nil {
		switch t := lt.Value.(type) {
		case *format.DateType:
			return record.Int64(int64(v.Int32()), record.SemanticDate)
		case *format.TimestampType:
			return record.Int64(int64(v.Int32()), timestampSemantic(t.Unit))
		case *format.IntType:
			if !t.IsSigned {
				return record.Int64(int64(v.Uint32()), record.SemanticUnsigned)
			}
		}
	}

	return record.Int64(int64(v.Int32()), record.SemanticNone)
}

// int64Value classifies an Int64 value, honoring TimestampType/unsigned IntType logical-type
// annotations. An unsigned value keeps its bit pattern, which is what record.SemanticUnsigned
// means.
func int64Value(v parquetgo.Value, lt *format.LogicalType) record.Value {
	if lt != nil {
		switch t := lt.Value.(type) {
		case *format.TimestampType:
			return record.Int64(v.Int64(), timestampSemantic(t.Unit))
		case *format.IntType:
			if !t.IsSigned {
				//nolint:gosec // Deliberate: SemanticUnsigned means "these bits are a uint64".
				return record.Int64(int64(v.Uint64()), record.SemanticUnsigned)
			}
		}
	}

	return record.Int64(v.Int64(), record.SemanticNone)
}

// timestampSemantic maps a parquet TimeUnit annotation onto the matching record semantic, keeping
// the unit with the value so an encoder renders it at the precision the source actually carried.
// An unrecognised unit falls back to milliseconds, Parquet's own default for an unannotated
// timestamp.
func timestampSemantic(unit format.TimeUnit) record.Semantic {
	switch unit.Value.(type) {
	case *format.NanoSeconds:
		return record.SemanticTimestampNanos
	case *format.MicroSeconds:
		return record.SemanticTimestampMicros
	default:
		return record.SemanticTimestampMillis
	}
}
