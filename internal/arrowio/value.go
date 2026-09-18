package arrowio

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/AlexAslan/streamio/internal/record"
)

// errUnsupportedArrowType reports an Arrow DataType this decoder has no record.Kind classification
// for, which can only mean a type was added without teaching the decoder about it.
var errUnsupportedArrowType = errors.New("arrow: unsupported column type")

// classify maps an Arrow DataType to the record.Kind/Semantic pair its values decode into,
// schema-derived once per column rather than sniffed per value — the same strategy parquetio uses
// via its own columnMeta, since Arrow (like Parquet) declares a column's type up front.
func classify(dt arrow.DataType) (record.Kind, record.Semantic, error) {
	switch t := dt.(type) {
	case *arrow.BooleanType:
		return record.KindBool, record.SemanticNone, nil
	case *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type:
		return record.KindInt64, record.SemanticNone, nil
	case *arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type:
		return record.KindInt64, record.SemanticUnsigned, nil
	case *arrow.Float32Type:
		return record.KindFloat64, record.SemanticFloat32, nil
	case *arrow.Float64Type:
		return record.KindFloat64, record.SemanticNone, nil
	case *arrow.StringType, *arrow.LargeStringType, *arrow.BinaryType, *arrow.LargeBinaryType:
		return record.KindBytes, record.SemanticNone, nil
	case *arrow.Date32Type, *arrow.Date64Type:
		return record.KindInt64, record.SemanticDate, nil
	case *arrow.TimestampType:
		return record.KindInt64, timestampSemantic(t.Unit), nil
	case *arrow.ListType:
		return record.KindList, record.SemanticNone, nil
	case *arrow.StructType:
		return record.KindMap, record.SemanticNone, nil
	default:
		return 0, 0, fmt.Errorf("%w: %s", errUnsupportedArrowType, dt)
	}
}

// timestampSemantic maps an Arrow TimeUnit to the matching record semantic, keeping the unit with
// the value so an encoder renders it at the precision the source actually carried. Second-unit
// timestamps are widened to millisecond precision, since record.Semantic has no SemanticTimestampSec
// — Arrow's own second-resolution timestamps are rare enough (most producers use millis or finer)
// that adding a whole new semantic for them isn't warranted yet.
func timestampSemantic(unit arrow.TimeUnit) record.Semantic {
	switch unit {
	case arrow.Nanosecond:
		return record.SemanticTimestampNanos
	case arrow.Microsecond:
		return record.SemanticTimestampMicros
	case arrow.Second, arrow.Millisecond:
		return record.SemanticTimestampMillis
	default:
		return record.SemanticTimestampMillis
	}
}

// scalarValue reads one non-null value at row i of arr — a leaf array, never a List/Struct, which
// buildRecord handles separately — as a canonical Value, using kind/semantic already derived from
// the column's schema type by classify.
func scalarValue(arr arrow.Array, i int, kind record.Kind, semantic record.Semantic) (record.Value, error) {
	switch kind {
	case record.KindBool:
		b, ok := arr.(*array.Boolean)
		if !ok {
			return record.Value{}, fmt.Errorf("%w: %T is not a boolean array", errUnsupportedArrowType, arr)
		}
		return record.Bool(b.Value(i)), nil
	case record.KindInt64:
		return intValue(arr, i, semantic)
	case record.KindFloat64:
		return floatValue(arr, i, semantic)
	case record.KindBytes:
		return bytesValue(arr, i), nil
	case record.KindNull, record.KindMap, record.KindList:
		return record.Value{}, fmt.Errorf("%w: kind %v is not a leaf value", errUnsupportedArrowType, kind)
	default:
		return record.Value{}, fmt.Errorf("%w: kind %v", errUnsupportedArrowType, kind)
	}
}

// floatValue reads a float-classified leaf at its declared width.
func floatValue(arr arrow.Array, i int, semantic record.Semantic) (record.Value, error) {
	if semantic == record.SemanticFloat32 {
		f32, ok := arr.(*array.Float32)
		if !ok {
			return record.Value{}, fmt.Errorf("%w: %T is not a float32 array", errUnsupportedArrowType, arr)
		}
		return record.Float32(f32.Value(i)), nil
	}
	f64, ok := arr.(*array.Float64)
	if !ok {
		return record.Value{}, fmt.Errorf("%w: %T is not a float64 array", errUnsupportedArrowType, arr)
	}
	return record.Float64(f64.Value(i)), nil
}

// intValue reads an integer-classified leaf, honoring the semantic classify already derived: a
// widened-to-int64 signed integer, an unsigned integer (bits read as-is per record.SemanticUnsigned's
// own contract), a date day count, or a timestamp at its original unit.
func intValue(arr arrow.Array, i int, semantic record.Semantic) (record.Value, error) {
	switch a := arr.(type) {
	case *array.Int8:
		return record.Int64(int64(a.Value(i)), semantic), nil
	case *array.Int16:
		return record.Int64(int64(a.Value(i)), semantic), nil
	case *array.Int32:
		return record.Int64(int64(a.Value(i)), semantic), nil
	case *array.Int64:
		return record.Int64(a.Value(i), semantic), nil
	case *array.Uint8:
		return record.Int64(int64(a.Value(i)), semantic), nil
	case *array.Uint16:
		return record.Int64(int64(a.Value(i)), semantic), nil
	case *array.Uint32:
		return record.Int64(int64(a.Value(i)), semantic), nil
	case *array.Uint64:
		//nolint:gosec // Deliberate: SemanticUnsigned means "these bits are a uint64".
		return record.Int64(int64(a.Value(i)), semantic), nil
	case *array.Date32:
		return record.Int64(int64(a.Value(i)), record.SemanticDate), nil
	case *array.Date64:
		// Date64 counts milliseconds since the epoch at day granularity; narrow to the day count
		// record.SemanticDate expects, matching Date32's own unit.
		const millisPerDay = 24 * 60 * 60 * 1000
		return record.Int64(int64(a.Value(i))/millisPerDay, record.SemanticDate), nil
	case *array.Timestamp:
		return record.Int64(int64(a.Value(i)), semantic), nil
	default:
		return record.Value{}, fmt.Errorf("%w: %T", errUnsupportedArrowType, arr)
	}
}

// bytesValue reads a string/binary leaf without copying: the returned Value aliases the Arrow
// array's own backing buffer, valid only until that array is Release()'d — buildRecord copies it
// out before that happens, matching every other decoder's documented Value lifetime.
func bytesValue(arr arrow.Array, i int) record.Value {
	switch a := arr.(type) {
	case *array.String:
		return record.Bytes([]byte(a.Value(i)))
	case *array.LargeString:
		return record.Bytes([]byte(a.Value(i)))
	case *array.Binary:
		return record.Bytes(a.Value(i))
	case *array.LargeBinary:
		return record.Bytes(a.Value(i))
	default:
		return record.Bytes([]byte(arr.ValueStr(i)))
	}
}
