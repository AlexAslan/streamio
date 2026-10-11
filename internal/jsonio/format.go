// JSON value-rendering helpers, kept beside the encoder so every JSON-producing path renders a
// given value the same way.
package jsonio

import (
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"math"

	"github.com/AlexAslan/streamio/internal/textfmt"
)

// Bits32 and Bits64 select the precision WriteFloat and AppendFloat format at.
const (
	Bits32 = textfmt.Bits32
	Bits64 = textfmt.Bits64
)

// ErrNonFinite is returned for NaN and ±Inf, which have no JSON number representation.
var ErrNonFinite = errors.New("json: unsupported value: non-finite float")

// WriteFloat writes f to enc as a raw JSON number formatted at bitSize precision.
func WriteFloat(enc *jsontext.Encoder, f float64, bitSize int) error {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Errorf("%w: %v", ErrNonFinite, f)
	}
	return enc.WriteValue(jsontext.Value(AppendFloat(nil, f, bitSize)))
}

// AppendFloat formats f exactly as encoding/json's floatEncoder does and appends it to dst.
func AppendFloat(dst []byte, f float64, bitSize int) []byte {
	return textfmt.AppendFloat(dst, f, bitSize)
}

// AppendDate appends a day count since the Unix epoch to dst as an ISO-8601 calendar date.
func AppendDate(dst []byte, days int64) []byte {
	return textfmt.AppendDate(dst, days)
}

// AppendTimestampMillis appends an epoch timestamp in milliseconds to dst as an ISO-8601 instant.
func AppendTimestampMillis(dst []byte, v int64) []byte {
	return textfmt.AppendTimestampMillis(dst, v)
}

// AppendTimestampMicros appends an epoch timestamp in microseconds to dst as an ISO-8601 instant.
func AppendTimestampMicros(dst []byte, v int64) []byte {
	return textfmt.AppendTimestampMicros(dst, v)
}

// AppendTimestampNanos appends an epoch timestamp in nanoseconds to dst as an ISO-8601 instant.
func AppendTimestampNanos(dst []byte, v int64) []byte {
	return textfmt.AppendTimestampNanos(dst, v)
}

// Date renders a day count since the Unix epoch as an ISO-8601 calendar date.
func Date(days int64) string {
	return string(AppendDate(nil, days))
}

// TimestampMillis renders an epoch timestamp in milliseconds as an ISO-8601 instant.
func TimestampMillis(v int64) string {
	return string(AppendTimestampMillis(nil, v))
}

// TimestampMicros renders an epoch timestamp in microseconds as an ISO-8601 instant.
func TimestampMicros(v int64) string {
	return string(AppendTimestampMicros(nil, v))
}

// TimestampNanos renders an epoch timestamp in nanoseconds as an ISO-8601 instant.
func TimestampNanos(v int64) string {
	return string(AppendTimestampNanos(nil, v))
}
