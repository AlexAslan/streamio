// JSON value-rendering helpers, kept beside the encoder so every JSON-producing path renders a
// given value the same way.
package jsonio

import (
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

// Bits32 and Bits64 select the precision WriteFloat and AppendFloat format at.
const (
	Bits32 = 32
	Bits64 = 64
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

// expCutoff is encoding/json's threshold, in both directions, for switching a float's formatting
// from fixed-point to scientific notation.
const expCutoff = 1e21

// expFloor is encoding/json's threshold below which a nonzero float switches from fixed-point to
// scientific notation.
const expFloor = 1e-6

// AppendFloat formats f exactly as encoding/json's floatEncoder does and appends it to dst.
func AppendFloat(dst []byte, f float64, bitSize int) []byte {
	abs := math.Abs(f)
	fmtByte := byte('f')
	// Must compare the float32 rounding of abs, not the float64 value, to get the cutoff exactly
	// right for a genuine float32 input (matches encoding/json's own comment to this effect).
	if abs != 0 {
		switch {
		case bitSize == Bits64 && (abs < expFloor || abs >= expCutoff):
			fmtByte = 'e'
		case bitSize == Bits32 && (float32(abs) < expFloor || float32(abs) >= expCutoff):
			fmtByte = 'e'
		}
	}

	start := len(dst)
	dst = strconv.AppendFloat(dst, f, fmtByte, -1, bitSize)
	if fmtByte == 'e' {
		// clean up e-09 to e-9, matching encoding/json's own post-processing.
		n := len(dst)
		if n-start >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	}
	return dst
}

// secondsPerDay converts a DATE logical type's day count into a Unix timestamp.
const secondsPerDay = 86400

// Layouts a rendered date or timestamp uses. Each timestamp layout carries exactly the fractional
// digits its unit can express: widening a millisecond value to nanosecond digits would invent six
// digits the source never had.
const (
	dateLayout     = "2006-01-02"
	millisLayout   = "2006-01-02T15:04:05.000Z"
	microsLayout   = "2006-01-02T15:04:05.000000Z"
	nanosLayout    = "2006-01-02T15:04:05.000000000Z"
	nanosPerSecond = 1_000_000_000
)

// Date renders a day count since the Unix epoch as an ISO-8601 calendar date.
func Date(days int64) string {
	return time.Unix(days*secondsPerDay, 0).UTC().Format(dateLayout)
}

// TimestampMillis renders an epoch timestamp in milliseconds as an ISO-8601 instant.
func TimestampMillis(v int64) string {
	return time.UnixMilli(v).UTC().Format(millisLayout)
}

// TimestampMicros renders an epoch timestamp in microseconds as an ISO-8601 instant.
func TimestampMicros(v int64) string {
	return time.UnixMicro(v).UTC().Format(microsLayout)
}

// TimestampNanos renders an epoch timestamp in nanoseconds as an ISO-8601 instant.
func TimestampNanos(v int64) string {
	return time.Unix(v/nanosPerSecond, v%nanosPerSecond).UTC().Format(nanosLayout)
}
