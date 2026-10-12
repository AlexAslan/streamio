package expr

import "math"

// decimalBase is the radix integers are read in.
const decimalBase = 10

// parseInteger reads b as a base-10 whole number with an optional sign, returning its sign and
// magnitude. ok is false for anything else (a decimal point, an exponent, a stray character) and
// when the magnitude does not fit: -2^63 up to 2^64-1 are accepted.
func parseInteger(b []byte) (bool, uint64, bool) {
	if len(b) == 0 {
		return false, 0, false
	}

	var (
		neg bool
		mag uint64
	)
	i := 0
	switch b[0] {
	case '-':
		neg = true
		i++
	case '+':
		i++
	}
	if i == len(b) {
		return false, 0, false
	}

	for ; i < len(b); i++ {
		d := b[i]
		if d < '0' || d > '9' {
			return false, 0, false
		}
		digit := uint64(d - '0')
		if mag > (math.MaxUint64-digit)/decimalBase {
			return false, 0, false
		}
		mag = mag*decimalBase + digit
	}

	if neg && mag > 1<<63 {
		return false, 0, false
	}
	// -0 is 0.
	return neg && mag != 0, mag, true
}

// plainNumber reports whether b looks like a decimal number: digits with an optional sign, decimal
// point and exponent, and nothing else. It excludes the spellings strconv.ParseFloat would also
// take (inf, nan, hex floats, underscores) so that text a person would not call a number is not
// treated as one.
func plainNumber(b []byte) bool {
	digits := false
	for _, c := range b {
		switch {
		case c >= '0' && c <= '9':
			digits = true
		case c == '.' || c == '+' || c == '-' || c == 'e' || c == 'E':
		default:
			return false
		}
	}
	return digits
}

// cmpMag orders two integers given as sign and magnitude.
func cmpMag(aNeg bool, aMag uint64, bNeg bool, bMag uint64) int {
	switch {
	case aNeg && !bNeg:
		return -1
	case !aNeg && bNeg:
		return 1
	}

	c := 0
	switch {
	case aMag < bMag:
		c = -1
	case aMag > bMag:
		c = 1
	}
	if aNeg {
		return -c
	}
	return c
}

// signMag splits an int64 into sign and magnitude.
func signMag(i int64) (bool, uint64) {
	if i < 0 {
		// -(i+1) cannot overflow, which -i would for the minimum int64.
		return true, uint64(-(i + 1)) + 1 //nolint:gosec // Non-negative by construction.
	}
	return false, uint64(i)
}

// cmpFloat orders two floats. ordered is false when either is NaN, which compares as neither less,
// equal nor greater.
func cmpFloat(a, b float64) (int, bool) {
	switch {
	case a < b:
		return -1, true
	case a > b:
		return 1, true
	case a == b:
		return 0, true
	default:
		return 0, false
	}
}

// satisfies applies op to an ordering. An unordered pair (a NaN) satisfies only !=.
func satisfies(order int, ordered bool, op cmpOp) bool {
	if !ordered {
		return op == opNe
	}
	switch op {
	case opEq:
		return order == 0
	case opNe:
		return order != 0
	case opLt:
		return order < 0
	case opLe:
		return order <= 0
	case opGt:
		return order > 0
	case opGe:
		return order >= 0
	}
	return false
}
