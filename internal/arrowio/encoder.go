package arrowio

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

// Errors a batch can fail to encode with. They are deliberately specific about what NewEncoder
// does not accept, mirroring parquetio's own encoder error set.
var (
	// errEmptySchema reports a leading record with no fields, which fixes no columns and so cannot
	// define a schema for the rest of the batch.
	errEmptySchema = errors.New("arrow: cannot derive a schema from a record with no fields")

	// errDuplicateField reports two fields of the same name in one record. Arrow models a row as
	// named columns, so the second would silently displace the first.
	errDuplicateField = errors.New("arrow: duplicate field name")

	// errUnsupportedKind reports a record.Kind with no Arrow column type, which can only mean a
	// Kind was added without teaching this encoder about it.
	errUnsupportedKind = errors.New("arrow: unsupported value kind")

	// errSchemaMismatch reports a record that doesn't fit the schema the first one fixed. Coercing
	// or dropping the offending field instead would silently change what the caller's data says.
	errSchemaMismatch = errors.New("arrow: record does not match the derived schema")
)

// schemaColumn is one record field's binding to its derived Arrow type, in the record's own field
// order — every later record's value for this field must either be null or carry exactly this
// Kind/Semantic pair; see errSchemaMismatch.
type schemaColumn struct {
	dataType arrow.DataType
	name     string
	kind     record.Kind
	semantic record.Semantic
}

// schemaBuilder is the schema-derivation core shared by encoder (one Arrow IPC file per batch) and
// streamingEncoder (one Arrow IPC file spanning every batch). Both need identical schema/column
// handling; only how the resulting records are written to a file differs between them.
type schemaBuilder struct {
	// schema is nil until the first non-empty batch derives it.
	schema *arrow.Schema

	// columns binds record field positions to their derived Arrow types, in the record's field
	// order (Arrow's own field order, unlike Parquet's, is exactly the schema's declared order —
	// no separate name-sorted lookup is needed).
	columns []schemaColumn
}

// deriveSchema fixes the column set from batch: one nullable field per field of the first record,
// named after it and typed from the first non-null value that column carries anywhere in this
// batch — scanning the whole batch, not just the first record, since a leading null says nothing
// about a column's type, mirroring parquetio.deriveSchema/typeSource exactly.
func (b *schemaBuilder) deriveSchema(batch []record.Record) error {
	first := batch[0]
	if len(first) == 0 {
		return errEmptySchema
	}

	seen := make(map[string]struct{}, len(first))
	columns := make([]schemaColumn, len(first))
	fields := make([]arrow.Field, len(first))

	for i := range first {
		name := first[i].Name
		if _, dup := seen[name]; dup {
			return fmt.Errorf("%w: %q", errDuplicateField, name)
		}
		seen[name] = struct{}{}

		value := typeSource(batch, i, name)
		dt, err := dataTypeFor(name, value)
		if err != nil {
			return err
		}

		kind, semantic := value.Kind, value.Semantic
		if kind == record.KindNull {
			// An untyped column is a string column, matching parquetio's identical fallback.
			kind, semantic = record.KindBytes, record.SemanticNone
		}
		columns[i] = schemaColumn{name: name, dataType: dt, kind: kind, semantic: semantic}
		fields[i] = arrow.Field{Name: name, Type: dt, Nullable: true}
	}

	b.schema = arrow.NewSchema(fields, nil)
	b.columns = columns
	return nil
}

// typeSource picks the value that types column i, named name: the first non-null one the batch
// carries for it. Identical reasoning to parquetio.typeSource.
func typeSource(batch []record.Record, i int, name string) record.Value {
	for _, rec := range batch {
		if i >= len(rec) || rec[i].Name != name {
			continue
		}
		if rec[i].Value.Kind != record.KindNull {
			return rec[i].Value
		}
	}
	return record.Null()
}

// dataTypeFor maps one column's typing value onto the Arrow DataType that holds it, recursing for
// KindMap/KindList — a nested column's own element/field types are derived from that same value's
// shape, to whatever depth it has.
//
//nolint:ireturn // arrow.DataType is an interface in arrow-go; every type constructor returns one.
func dataTypeFor(name string, value record.Value) (arrow.DataType, error) {
	switch value.Kind {
	case record.KindNull:
		// No record in the batch typed this column: least-presumptuous fallback, matching
		// parquetio's identical choice — a later non-null value of some other kind still trips
		// errSchemaMismatch rather than being coerced.
		return arrow.BinaryTypes.String, nil
	case record.KindBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case record.KindInt64:
		return intDataTypeFor(value.Semantic), nil
	case record.KindFloat64:
		if value.Semantic == record.SemanticFloat32 {
			return arrow.PrimitiveTypes.Float32, nil
		}
		return arrow.PrimitiveTypes.Float64, nil
	case record.KindBytes:
		return arrow.BinaryTypes.String, nil
	case record.KindMap:
		return structDataTypeFor(name, value.Map)
	case record.KindList:
		return listDataTypeFor(name, value.List)
	default:
		return nil, fmt.Errorf("%w: field %q has kind %v", errUnsupportedKind, name, value.Kind)
	}
}

// intDataTypeFor picks the Arrow integer/date/timestamp type for an integer column, keeping the
// unit with the type so an encoder renders it at the precision the source actually carried —
// mirrors parquetio.intNodeFor.
//
//nolint:ireturn // arrow.DataType is an interface in arrow-go; every type constructor returns one.
func intDataTypeFor(semantic record.Semantic) arrow.DataType {
	switch semantic {
	case record.SemanticDate:
		return arrow.FixedWidthTypes.Date32
	case record.SemanticTimestampMillis:
		return arrow.FixedWidthTypes.Timestamp_ms
	case record.SemanticTimestampMicros:
		return arrow.FixedWidthTypes.Timestamp_us
	case record.SemanticTimestampNanos:
		return arrow.FixedWidthTypes.Timestamp_ns
	case record.SemanticUnsigned:
		return arrow.PrimitiveTypes.Uint64
	case record.SemanticNone, record.SemanticFloat32:
		return arrow.PrimitiveTypes.Int64
	default:
		return arrow.PrimitiveTypes.Int64
	}
}

// structDataTypeFor derives a StructType from one KindMap value's own entries, the same
// first-non-null-value strategy deriveSchema uses for the top-level schema, applied recursively —
// a nested KindMap's shape is fixed from this one sample value, not re-derived per row, matching
// every other column's fixed-from-first-batch contract.
//
//nolint:ireturn // arrow.DataType is an interface in arrow-go; every type constructor returns one.
func structDataTypeFor(parentName string, sample record.Record) (arrow.DataType, error) {
	fields := make([]arrow.Field, len(sample))
	for i := range sample {
		dt, err := dataTypeFor(fmt.Sprintf("%s.%s", parentName, sample[i].Name), sample[i].Value)
		if err != nil {
			return nil, err
		}
		fields[i] = arrow.Field{Name: sample[i].Name, Type: dt, Nullable: true}
	}
	return arrow.StructOf(fields...), nil
}

// listDataTypeFor derives a ListType from one KindList value's own elements: the first element's
// type, if any — an empty sample list falls back to String, the same untyped-column fallback
// dataTypeFor's own KindNull case uses.
//
//nolint:ireturn // arrow.DataType is an interface in arrow-go; every type constructor returns one.
func listDataTypeFor(parentName string, sample []record.Value) (arrow.DataType, error) {
	if len(sample) == 0 {
		return arrow.ListOf(arrow.BinaryTypes.String), nil
	}
	elem, err := dataTypeFor(parentName+"[]", sample[0])
	if err != nil {
		return nil, err
	}
	return arrow.ListOf(elem), nil
}

// buildRecord builds one Arrow record batch from batch, using b to append every row's values
// through the schema's own builders, and rb.NewRecord() to finish it. rb is caller-owned so it can
// be reused across calls without reallocating its builders each time.
//
//nolint:ireturn // arrow.RecordBatch is an interface in arrow-go; RecordBuilder.NewRecordBatch returns one.
func (b *schemaBuilder) buildRecord(rb *array.RecordBuilder, batch []record.Record) (arrow.RecordBatch, error) {
	for i := range batch {
		if err := b.checkRecord(i, batch[i]); err != nil {
			return nil, err
		}
		for c := range b.columns {
			if err := appendValue(rb.Field(c), batch[i][c].Value, b.columns[c]); err != nil {
				return nil, fmt.Errorf("record %d: %w", i, err)
			}
		}
	}
	return rb.NewRecordBatch(), nil
}

// checkRecord verifies rec has the same fields, in the same order, as the record the schema was
// derived from — identical reasoning to parquetio.rowBuilder.checkRecord.
func (b *schemaBuilder) checkRecord(index int, rec record.Record) error {
	if len(rec) != len(b.columns) {
		return fmt.Errorf("%w: record %d has %d fields, the schema has %d",
			errSchemaMismatch, index, len(rec), len(b.columns))
	}
	for c := range b.columns {
		if rec[c].Name != b.columns[c].name {
			return fmt.Errorf("%w: record %d field %d is %q, the schema has %q",
				errSchemaMismatch, index, c, rec[c].Name, b.columns[c].name)
		}
	}
	return nil
}

// appendValue appends v to bld, the column builder for col. A null is always accepted — every
// column is nullable — and anything else must carry exactly the Kind/Semantic col was derived
// from.
func appendValue(bld array.Builder, v record.Value, col schemaColumn) error {
	if v.Kind == record.KindNull {
		bld.AppendNull()
		return nil
	}
	if v.Kind != col.kind || v.Semantic != col.semantic {
		return fmt.Errorf("%w: field %q is %v/%v, the schema has %v/%v",
			errSchemaMismatch, col.name, v.Kind, v.Semantic, col.kind, col.semantic)
	}

	switch v.Kind {
	case record.KindBool:
		b, ok := bld.(*array.BooleanBuilder)
		if !ok {
			return fmt.Errorf("%w: field %q has builder %T, want *array.BooleanBuilder", errUnsupportedKind, col.name, bld)
		}
		b.Append(v.Bool)
	case record.KindInt64:
		return appendInt(bld, v, col)
	case record.KindFloat64:
		return appendFloat(bld, v, col)
	case record.KindBytes:
		s, ok := bld.(*array.StringBuilder)
		if !ok {
			return fmt.Errorf("%w: field %q has builder %T, want *array.StringBuilder", errUnsupportedKind, col.name, bld)
		}
		s.Append(string(v.Str))
	case record.KindMap:
		st, ok := bld.(*array.StructBuilder)
		if !ok {
			return fmt.Errorf("%w: field %q has builder %T, want *array.StructBuilder", errUnsupportedKind, col.name, bld)
		}
		return appendStruct(st, v.Map)
	case record.KindList:
		l, ok := bld.(*array.ListBuilder)
		if !ok {
			return fmt.Errorf("%w: field %q has builder %T, want *array.ListBuilder", errUnsupportedKind, col.name, bld)
		}
		return appendList(l, v.List)
	case record.KindNull:
		bld.AppendNull()
	default:
		return fmt.Errorf("%w: field %q has kind %v", errUnsupportedKind, col.name, v.Kind)
	}
	return nil
}

// appendFloat appends a float value to bld at its declared width.
func appendFloat(bld array.Builder, v record.Value, col schemaColumn) error {
	if v.Semantic == record.SemanticFloat32 {
		f32, ok := bld.(*array.Float32Builder)
		if !ok {
			return fmt.Errorf("%w: field %q has builder %T, want *array.Float32Builder", errUnsupportedKind, col.name, bld)
		}
		f32.Append(float32(v.F64))
		return nil
	}
	f64, ok := bld.(*array.Float64Builder)
	if !ok {
		return fmt.Errorf("%w: field %q has builder %T, want *array.Float64Builder", errUnsupportedKind, col.name, bld)
	}
	f64.Append(v.F64)
	return nil
}

// appendInt appends an integer value to bld, honoring the semantic col was derived from — a plain
// int64, an unsigned int64 (bits read as-is, per record.SemanticUnsigned's own contract), a date
// day count, or a timestamp at its declared unit.
func appendInt(bld array.Builder, v record.Value, col schemaColumn) error {
	switch col.semantic {
	case record.SemanticUnsigned:
		b, ok := bld.(*array.Uint64Builder)
		if !ok {
			return fmt.Errorf("%w: field %q has builder %T, want *array.Uint64Builder", errUnsupportedKind, col.name, bld)
		}
		//nolint:gosec // Deliberate: SemanticUnsigned means "these bits are a uint64".
		b.Append(uint64(v.I64))
	case record.SemanticDate:
		b, ok := bld.(*array.Date32Builder)
		if !ok {
			return fmt.Errorf("%w: field %q has builder %T, want *array.Date32Builder", errUnsupportedKind, col.name, bld)
		}
		//nolint:gosec // A DATE is a day count; it cannot overflow int32 within Arrow's own range.
		b.Append(arrow.Date32(v.I64))
	case record.SemanticTimestampMillis, record.SemanticTimestampMicros, record.SemanticTimestampNanos:
		b, ok := bld.(*array.TimestampBuilder)
		if !ok {
			return fmt.Errorf("%w: field %q has builder %T, want *array.TimestampBuilder", errUnsupportedKind, col.name, bld)
		}
		b.Append(arrow.Timestamp(v.I64))
	case record.SemanticNone, record.SemanticFloat32:
		b, ok := bld.(*array.Int64Builder)
		if !ok {
			return fmt.Errorf("%w: field %q has builder %T, want *array.Int64Builder", errUnsupportedKind, col.name, bld)
		}
		b.Append(v.I64)
	default:
		b, ok := bld.(*array.Int64Builder)
		if !ok {
			return fmt.Errorf("%w: field %q has builder %T, want *array.Int64Builder", errUnsupportedKind, col.name, bld)
		}
		b.Append(v.I64)
	}
	return nil
}

// appendStruct appends one KindMap value's entries into a StructBuilder, one child builder per
// field in the struct's own declared order — entries must match that order exactly, the same
// positional contract checkRecord enforces at the top level, since a struct column's shape is
// fixed from the first batch's sample value the same way the top-level schema is.
func appendStruct(bld *array.StructBuilder, entries record.Record) error {
	bld.Append(true)
	st, ok := bld.Type().(*arrow.StructType)
	if !ok {
		return fmt.Errorf("%w: struct builder has non-struct type %s", errUnsupportedKind, bld.Type())
	}
	if len(entries) != st.NumFields() {
		return fmt.Errorf("%w: struct value has %d fields, the column schema has %d",
			errSchemaMismatch, len(entries), st.NumFields())
	}

	for i := range entries {
		field := st.Field(i)
		if entries[i].Name != field.Name {
			return fmt.Errorf("%w: struct field %d is %q, the column schema has %q",
				errSchemaMismatch, i, entries[i].Name, field.Name)
		}
		kind, semantic, err := classify(field.Type)
		if err != nil {
			return err
		}
		if appendErr := appendValue(bld.FieldBuilder(i), entries[i].Value, schemaColumn{
			name: field.Name, dataType: field.Type, kind: kind, semantic: semantic,
		}); appendErr != nil {
			return appendErr
		}
	}
	return nil
}

// appendList appends one KindList value's elements into a ListBuilder, using its own declared
// element type for every element — a heterogeneous list (elements of differing Kind/Semantic) is
// rejected the same way a schema-mismatched top-level field is, since a ListType's own element
// type is fixed once derived.
func appendList(bld *array.ListBuilder, elements []record.Value) error {
	bld.Append(true)
	lt, ok := bld.Type().(*arrow.ListType)
	if !ok {
		return fmt.Errorf("%w: list builder has non-list type %s", errUnsupportedKind, bld.Type())
	}
	kind, semantic, err := classify(lt.Elem())
	if err != nil {
		return err
	}

	elemBld := bld.ValueBuilder()
	col := schemaColumn{dataType: lt.Elem(), kind: kind, semantic: semantic}
	for i := range elements {
		if appendErr := appendValue(elemBld, elements[i], col); appendErr != nil {
			return fmt.Errorf("element %d: %w", i, appendErr)
		}
	}
	return nil
}

// encoder turns one batch of canonical records into one standalone Arrow IPC file, one document
// per batch. Its schema is derived once, from the first batch, and checked against every record
// after — identical contract to parquetio.encoder, since Arrow is columnar/batch-oriented the same
// way. There is no raw-passthrough route for Arrow-to-Arrow: ipc.FileWriter only accepts a live,
// already-decoded arrow.RecordBatch (Write(rec arrow.RecordBatch) error) — there is no
// verbatim-bytes splice analogous to parquet-go's Writer.WriteRowGroup, so even a same-format
// conversion goes through the generic decode/re-encode path like every non-Parquet format.
type encoder struct {
	schemaBuilder

	// alloc is shared across every RecordBuilder/array this encoder constructs, matching Arrow's
	// own convention of one allocator per logical "session" of related arrays.
	alloc memory.Allocator

	// rb builds one arrow.RecordBatch per EncodeBatch call from the derived schema; nil until the
	// schema is known.
	rb *array.RecordBuilder

	// buf is the write destination, reset per batch so it holds only this batch's own standalone
	// file — in contrast to streamingEncoder's buf, which accumulates across the whole run.
	buf bytes.Buffer
}

// NewEncoder returns an encoder rendering a whole batch of canonical records as one self-contained
// Arrow IPC file, one document per batch rather than per record.
//
// The schema is derived from the first batch and then fixed: column type comes from each column's
// first non-null value, an untyped column becomes a string column, and a later record whose shape
// doesn't match is an error rather than a silent coercion. Every column is nullable, since there's
// no cheap way to know up front which fields will stay non-null — mirrors parquetio.NewEncoder's
// identical contract. Unlike parquetio, record.KindMap and record.KindList are supported: Arrow's
// StructType/ListType map onto them directly, so a nested value round-trips through Arrow the same
// way it already does through jsonio. SingleFileOutput selects the streaming encoder below
// instead: see NewStreamingEncoder.
//
//nolint:ireturn // formatio.RecordEncoder is the constructor type streamio's format registry stores.
func NewEncoder(cfg options.Config) (formatio.RecordEncoder, error) {
	if cfg.Run.SingleFileOutput {
		return NewStreamingEncoder(cfg)
	}
	return &encoder{alloc: memory.NewGoAllocator()}, nil
}

// EncodeBatch renders every record in batch as one standalone Arrow IPC file. An empty batch
// produces no document.
func (e *encoder) EncodeBatch(batch []record.Record) ([][]byte, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	if e.schema == nil {
		if err := e.deriveSchema(batch); err != nil {
			return nil, err
		}
		e.rb = array.NewRecordBuilder(e.alloc, e.schema)
	}

	rec, err := e.buildRecord(e.rb, batch)
	if err != nil {
		return nil, err
	}
	defer rec.Release()

	e.buf.Reset()
	w, err := ipc.NewFileWriter(&e.buf, ipc.WithSchema(e.schema), ipc.WithAllocator(e.alloc))
	if err != nil {
		return nil, fmt.Errorf("arrow: opening file writer: %w", err)
	}
	if err = w.Write(rec); err != nil {
		return nil, fmt.Errorf("arrow: writing record batch: %w", err)
	}
	if err = w.Close(); err != nil {
		return nil, fmt.Errorf("arrow: closing file writer: %w", err)
	}
	if e.buf.Len() == 0 {
		return nil, nil
	}
	return [][]byte{cloneBuf(&e.buf)}, nil
}

// cloneBuf copies buf's contents, since the caller's buffer is reused by the next call.
func cloneBuf(buf *bytes.Buffer) []byte {
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out
}
