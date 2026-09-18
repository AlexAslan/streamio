package csvio

import (
	"fmt"
	"strconv"
	"streamio/internal/jsonio"
	"streamio/internal/record"
)

// renderValue appends v's plain-text CSV/TSV rendering to dst and returns the extended slice, with
// no per-field allocation at all: dst is the caller's reused row buffer (rowScratch), and every
// Kind formats straight into it via strconv.AppendX or jsonio.AppendFloat rather than through an
// intermediate string. Date and timestamp values reuse jsonio's own rendering helpers rather than
// reimplementing them, so a date or timestamp renders identically regardless of which output
// format it went through.
func renderValue(dst []byte, v record.Value) ([]byte, error) {
	switch v.Kind {
	case record.KindNull:
		return dst, nil
	case record.KindBool:
		return strconv.AppendBool(dst, v.Bool), nil
	case record.KindInt64:
		return renderInt(dst, v), nil
	case record.KindFloat64:
		if v.Semantic == record.SemanticFloat32 {
			return jsonio.AppendFloat(dst, v.F64, jsonio.Bits32), nil
		}
		return jsonio.AppendFloat(dst, v.F64, jsonio.Bits64), nil
	case record.KindBytes:
		return append(dst, v.Str...), nil
	case record.KindMap, record.KindList:
		return dst, fmt.Errorf("%w", errMapUnsupported)
	default:
		return dst, fmt.Errorf("%w: %v", errUnsupportedKind, v.Kind)
	}
}

// decimalBase is the radix every plain integer field renders at.
const decimalBase = 10

// renderInt appends an integer value to dst as whatever its Semantic says it really is, matching
// jsonio's own writeInt rendering choices for each semantic. The timestamp/date renderers return
// a string rather than appending, since they format through time.Time.Format; that string is
// still only one allocation, the same as before this change.
func renderInt(dst []byte, v record.Value) []byte {
	switch v.Semantic {
	case record.SemanticDate:
		return append(dst, jsonio.Date(v.I64)...)
	case record.SemanticTimestampMillis:
		return append(dst, jsonio.TimestampMillis(v.I64)...)
	case record.SemanticTimestampMicros:
		return append(dst, jsonio.TimestampMicros(v.I64)...)
	case record.SemanticTimestampNanos:
		return append(dst, jsonio.TimestampNanos(v.I64)...)
	case record.SemanticUnsigned:
		//nolint:gosec // Deliberate: SemanticUnsigned means the I64 bits are a uint64.
		return strconv.AppendUint(dst, uint64(v.I64), decimalBase)
	case record.SemanticNone, record.SemanticFloat32:
		return strconv.AppendInt(dst, v.I64, decimalBase)
	default:
		return strconv.AppendInt(dst, v.I64, decimalBase)
	}
}
