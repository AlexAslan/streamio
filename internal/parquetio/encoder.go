package parquetio

import (
	"bytes"
	"errors"
	"fmt"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

const (
	// schemaName is the root message name given to every schema this encoder derives. Parquet
	// requires the root node to be named; nothing reads the name back.
	schemaName = "document"

	// int64BitWidth is the width every integer column this encoder derives is given. record.Value
	// carries every integer as an int64, and narrowing a column to the widest value a batch happens
	// to contain would make the schema depend on the data rather than on its type.
	int64BitWidth = 64
)

// Errors a batch can fail to encode with. They are deliberately specific about what NewEncoder
// does not accept, since every one of them means "this batch cannot be expressed under the schema
// the first record fixed" rather than "something is corrupt".
var (
	// errEmptySchema reports a leading record with no fields, which fixes no columns and so cannot
	// define a schema for the rest of the batch.
	errEmptySchema = errors.New("parquet: cannot derive a schema from a record with no fields")

	// errDuplicateField reports two fields of the same name in one record. Parquet models a row as
	// named columns, so the second would silently displace the first.
	errDuplicateField = errors.New("parquet: duplicate field name")

	// errMapUnsupported reports a record.KindMap or record.KindList field. See NewEncoder's doc for
	// why this is a deliberate scope limit rather than an oversight.
	errMapUnsupported = errors.New("parquet: map and list fields are not supported by NewEncoder")

	// errUnsupportedKind reports a record.Kind with no Parquet column type, which can only mean a
	// Kind was added without teaching this encoder about it.
	errUnsupportedKind = errors.New("parquet: unsupported value kind")

	// errSchemaMismatch reports a record that doesn't fit the schema the first one fixed. Coercing
	// or dropping the offending field instead would silently change what the caller's data says.
	errSchemaMismatch = errors.New("parquet: record does not match the derived schema")
)

// batchColumn is one record field's binding to the column it writes into: where the value goes in a
// parquetgo.Row, and what the schema says that column holds. Columns are stored in the *record's*
// field order, while columnIndex points into the schema's own (name-sorted) order.
type batchColumn struct {
	// name is the field name, as it appears in both the record and the schema.
	name string

	// kind and semantic are what the column was derived from. Every later record's value for this
	// field must either be null or carry exactly this pair; see errSchemaMismatch.
	kind     record.Kind
	semantic record.Semantic

	// columnIndex is this field's position in the schema's leaf-column order.
	columnIndex int

	// definitionLevel is the level a non-null value carries. Every column here is an optional leaf
	// directly under the root, so it is always 1; it is read from the schema rather than hardcoded.
	definitionLevel int
}

// rowBuilder is the schema-derivation and row-building core shared by encoder (one Parquet file
// per batch) and streamingEncoder (one Parquet file, many row groups, spanning every batch). Both
// need the identical schema/column/scratch handling; only how the resulting rows are written to a
// file — closed per batch, or flushed per batch and closed once — differs between them.
type rowBuilder struct {
	// schema is nil until the first non-empty batch derives it.
	schema *parquetgo.Schema

	// columns binds record field positions to schema columns, in the record's field order.
	columns []batchColumn

	// rows and values are the row-building scratch: values is one flat backing array that rows
	// slices into, so a batch costs one growth rather than one allocation per row.
	rows   []parquetgo.Row
	values []parquetgo.Value
}

// encoder turns one batch of canonical records into one standalone Parquet file, one document per
// batch. Its schema is derived once, from the first batch, and checked against every record after.
type encoder struct {
	rowBuilder

	// writer is reset onto the destination buffer per batch rather than rebuilt, since the schema it
	// was constructed with never changes.
	writer *parquetgo.GenericWriter[any]

	// buf is the write destination, reused across every batch so it grows to the widest document
	// once rather than per batch. Each document is copied out of it before being returned. It sits
	// last because it is the only field here that isn't wholly pointer data.
	buf bytes.Buffer
}

// NewEncoder returns an encoder rendering a whole batch of canonical records as one self-contained
// Parquet file, one document per batch rather than per record.
//
// The schema is derived from the first batch and then fixed: column type comes from each column's
// first non-null value, an untyped column becomes a string column, and a later record whose shape
// doesn't match is an error rather than a silent coercion. Every column is optional, since there's
// no cheap way to know up front which fields will stay non-null. record.KindMap is not supported.
// SingleFileOutput selects the streaming encoder below instead: see NewStreamingEncoder.
//
//nolint:ireturn // formatio.RecordEncoder is the constructor type streamio's format registry stores.
func NewEncoder(cfg options.Config) (formatio.RecordEncoder, error) {
	if cfg.Run.SingleFileOutput {
		return NewStreamingEncoder(cfg)
	}
	return &encoder{}, nil
}

// EncodeBatch renders every record in batch as one standalone Parquet file: magic bytes, one row
// group, footer. An empty batch produces no document.
func (e *encoder) EncodeBatch(batch []record.Record) ([][]byte, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	if e.schema == nil {
		if err := e.deriveSchema(batch); err != nil {
			return nil, err
		}
	}

	rows, err := e.buildRows(batch)
	if err != nil {
		return nil, err
	}

	e.buf.Reset()
	if err = e.writeRows(rows); err != nil {
		return nil, err
	}
	if e.buf.Len() == 0 {
		return nil, nil
	}
	return [][]byte{cloneBuf(&e.buf)}, nil
}

// deriveSchema fixes the column set from batch: one optional leaf column per field of the first
// record, named after it and typed from the first non-null value that column carries anywhere in
// this batch. The resulting schema's own column order is the name-sorted order parquetgo.Group
// imposes, which is why each column's index is looked up rather than assumed to be its field
// position.
func (e *rowBuilder) deriveSchema(batch []record.Record) error {
	first := batch[0]
	if len(first) == 0 {
		return errEmptySchema
	}

	group := make(parquetgo.Group, len(first))
	columns := make([]batchColumn, len(first))

	for i := range first {
		name := first[i].Name
		if _, duplicate := group[name]; duplicate {
			return fmt.Errorf("%w: %q", errDuplicateField, name)
		}

		value := typeSource(batch, i, name)
		node, err := nodeFor(name, value)
		if err != nil {
			return err
		}

		// Always optional: see NewEncoder's doc for why this is a deliberate simplification.
		group[name] = parquetgo.Optional(node)

		kind, semantic := value.Kind, value.Semantic
		if kind == record.KindNull {
			// An untyped column is a string column, so record it as one: that is what it now
			// accepts, and saying so here is what lets a later string land in it.
			kind, semantic = record.KindBytes, record.SemanticNone
		}
		columns[i] = batchColumn{name: name, kind: kind, semantic: semantic}
	}

	schema := parquetgo.NewSchema(schemaName, group)
	for i := range columns {
		leaf, ok := schema.Lookup(columns[i].name)
		if !ok {
			return fmt.Errorf("%w: column %q is missing from the derived schema",
				errSchemaMismatch, columns[i].name)
		}
		columns[i].columnIndex = leaf.ColumnIndex
		columns[i].definitionLevel = leaf.MaxDefinitionLevel
	}

	e.schema = schema
	e.columns = columns
	return nil
}

// typeSource picks the value that types column i, named name: the first non-null one the batch
// carries for it. Scanning the whole first batch rather than only its first record is what lets a
// file whose leading row happens to have a null in a column still get that column typed — a JSON
// null says nothing about the column's type, so the first record alone is often not enough evidence.
//
// Records whose shape doesn't match the first one contribute nothing; buildRows rejects them.
// When no record in the batch has a non-null value, the zero Value is returned and nodeFor's
// KindNull branch settles on a string column.
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

// nodeFor maps one column's typing value onto the Parquet column type that holds it, letting the
// value's Semantic pick the logical-type annotation for the values whose physical representation
// doesn't say what they mean on its own.
//
//nolint:ireturn // parquetgo.Node is an interface in parquet-go; every node constructor returns one.
func nodeFor(name string, value record.Value) (parquetgo.Node, error) {
	switch value.Kind {
	case record.KindNull:
		// No record in the first batch typed this column. String is the least presumptuous column
		// that still accepts something: it takes the nulls seen so far plus any later string, and a
		// later value of some other kind trips errSchemaMismatch rather than being coerced.
		return parquetgo.String(), nil
	case record.KindBool:
		return parquetgo.Leaf(parquetgo.BooleanType), nil
	case record.KindInt64:
		return intNodeFor(value.Semantic), nil
	case record.KindFloat64:
		if value.Semantic == record.SemanticFloat32 {
			return parquetgo.Leaf(parquetgo.FloatType), nil
		}
		return parquetgo.Leaf(parquetgo.DoubleType), nil
	case record.KindBytes:
		return parquetgo.String(), nil
	case record.KindMap, record.KindList:
		return nil, fmt.Errorf("%w: field %q", errMapUnsupported, name)
	default:
		return nil, fmt.Errorf("%w: field %q has kind %v", errUnsupportedKind, name, value.Kind)
	}
}

// intNodeFor picks the annotation for an integer column. Date and timestamp columns keep the raw
// day count or epoch integer the decoder classified, with the logical type carrying what it means —
// the annotation lives on the schema node, never on the value.
//
//nolint:ireturn // parquetgo.Node is an interface in parquet-go; every node constructor returns one.
func intNodeFor(semantic record.Semantic) parquetgo.Node {
	switch semantic {
	case record.SemanticDate:
		return parquetgo.Date()
	case record.SemanticTimestampMillis:
		return parquetgo.Timestamp(parquetgo.Millisecond)
	case record.SemanticTimestampMicros:
		return parquetgo.Timestamp(parquetgo.Microsecond)
	case record.SemanticTimestampNanos:
		return parquetgo.Timestamp(parquetgo.Nanosecond)
	case record.SemanticUnsigned:
		return parquetgo.Uint(int64BitWidth)
	case record.SemanticNone, record.SemanticFloat32:
		// SemanticFloat32 says nothing about an integer; treat it as the plain value it is rather
		// than failing on a tag that simply doesn't apply to this Kind, matching jsonio.
		return parquetgo.Int(int64BitWidth)
	default:
		return parquetgo.Int(int64BitWidth)
	}
}

// buildRows fills the reused row scratch from batch, one parquetgo.Row per record with each value
// placed at its own column index. Rows come out in schema column order by construction, which is
// what WriteRows expects.
func (e *rowBuilder) buildRows(batch []record.Record) ([]parquetgo.Row, error) {
	width := len(e.columns)
	e.growScratch(len(batch), width)

	for i := range batch {
		rec := batch[i]
		if err := e.checkRecord(i, rec); err != nil {
			return nil, err
		}

		row := e.values[i*width : (i+1)*width : (i+1)*width]
		for c := range e.columns {
			value, err := e.columns[c].valueOf(rec[c].Value)
			if err != nil {
				return nil, fmt.Errorf("record %d: %w", i, err)
			}
			row[e.columns[c].columnIndex] = value
		}
		e.rows[i] = row
	}

	return e.rows[:len(batch)], nil
}

// growScratch resizes the row scratch to hold rows × width values, keeping whatever capacity
// previous batches already grew it to.
func (e *rowBuilder) growScratch(rows, width int) {
	if cap(e.values) < rows*width {
		e.values = make([]parquetgo.Value, rows*width)
	}
	e.values = e.values[:rows*width]

	if cap(e.rows) < rows {
		e.rows = make([]parquetgo.Row, rows)
	}
	e.rows = e.rows[:rows]
}

// checkRecord verifies rec has the same fields, in the same order, as the record the schema was
// derived from. Field order is checked rather than looked up by name because a reordered record
// almost always means the input's rows aren't the uniform shape this encoder requires, and matching
// by name would hide that until some later row introduced a genuinely new field.
func (e *rowBuilder) checkRecord(index int, rec record.Record) error {
	if len(rec) != len(e.columns) {
		return fmt.Errorf("%w: record %d has %d fields, the schema has %d",
			errSchemaMismatch, index, len(rec), len(e.columns))
	}
	for c := range e.columns {
		if rec[c].Name != e.columns[c].name {
			return fmt.Errorf("%w: record %d field %d is %q, the schema has %q",
				errSchemaMismatch, index, c, rec[c].Name, e.columns[c].name)
		}
	}
	return nil
}

// valueOf renders one canonical value as the physical Parquet value its column holds, levelled onto
// that column. A null is always accepted — every column is optional — and anything else must carry
// exactly the Kind and Semantic the column was derived from.
func (c batchColumn) valueOf(v record.Value) (parquetgo.Value, error) {
	// The zero parquetgo.Value is the null value; definition level 0 is what marks it absent.
	if v.Kind == record.KindNull {
		return parquetgo.Value{}.Level(0, 0, c.columnIndex), nil
	}

	if v.Kind != c.kind || v.Semantic != c.semantic {
		return parquetgo.Value{}, fmt.Errorf("%w: field %q is %v/%v, the schema has %v/%v",
			errSchemaMismatch, c.name, v.Kind, v.Semantic, c.kind, c.semantic)
	}

	physical, err := physicalValue(c.name, v)
	if err != nil {
		return parquetgo.Value{}, err
	}
	return physical.Level(0, c.definitionLevel, c.columnIndex), nil
}

// physicalValue builds the untyped physical value for v, matching the column type nodeFor derived
// from the same Kind/Semantic pair. The typed constructors are used rather than parquetgo.ValueOf
// so a per-row value costs no boxing into an any; they produce the identical physical value.
func physicalValue(name string, v record.Value) (parquetgo.Value, error) {
	switch v.Kind {
	case record.KindBool:
		return parquetgo.BooleanValue(v.Bool), nil
	case record.KindInt64:
		// A DATE column is physically an int32, unlike every other annotated integer here.
		if v.Semantic == record.SemanticDate {
			//nolint:gosec // A DATE is a day count; it cannot overflow int32 within Parquet's own range.
			return parquetgo.Int32Value(int32(v.I64)), nil
		}
		// SemanticUnsigned needs no conversion: it means the I64 bits already are the uint64, and
		// an unsigned int64 column stores exactly those bits.
		return parquetgo.Int64Value(v.I64), nil
	case record.KindFloat64:
		if v.Semantic == record.SemanticFloat32 {
			return parquetgo.FloatValue(float32(v.F64)), nil
		}
		return parquetgo.DoubleValue(v.F64), nil
	case record.KindBytes:
		// ByteArrayValue references v.Str rather than copying it, which is safe here because the
		// whole batch is written and the file finalised before the decoder refills these records.
		return parquetgo.ByteArrayValue(v.Str), nil
	case record.KindNull, record.KindMap, record.KindList:
		return parquetgo.Value{}, fmt.Errorf("%w: field %q has kind %v", errUnsupportedKind, name, v.Kind)
	default:
		return parquetgo.Value{}, fmt.Errorf("%w: field %q has kind %v", errUnsupportedKind, name, v.Kind)
	}
}

// writeRows writes rows to the encoder's buffer as a complete Parquet file. Close is what emits the
// footer and magic trailer, so it happens once per batch: each batch is a distinct standalone
// document, and without the Close its bytes are not a Parquet file at all.
//
// The writer is reset onto the (just-truncated) buffer rather than rebuilt, since the schema it was
// constructed with is fixed for this encoder's lifetime and reconstructing it would re-derive the
// whole column layout per batch.
func (e *encoder) writeRows(rows []parquetgo.Row) error {
	if e.writer == nil {
		e.writer = parquetgo.NewGenericWriter[any](&e.buf, e.schema)
	} else {
		e.writer.Reset(&e.buf)
	}

	if _, err := e.writer.WriteRows(rows); err != nil {
		return fmt.Errorf("parquet: writing %d rows: %w", len(rows), err)
	}
	if err := e.writer.Close(); err != nil {
		return fmt.Errorf("parquet: closing batch writer: %w", err)
	}
	return nil
}
