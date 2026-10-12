package expr_test

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio/internal/expr"
	"github.com/AlexAslan/streamio/record"
)

// run compiles src and evaluates it on rec, returning the result and how many type misses it saw.
func run(t *testing.T, src string, rec record.Record) (bool, int64) {
	t.Helper()

	p, err := expr.Compile(src, false)
	require.NoError(t, err, src)
	ev := p.NewEvaluator()
	ok, err := ev.Match(rec)
	require.NoError(t, err, src)
	return ok, ev.Misses()
}

func one(name string, v record.Value) record.Record {
	return record.Record{}.Append(name, v)
}

func TestLexer_NonASCIIColumnNames(t *testing.T) {
	t.Parallel()

	//nolint:gosmopolitan // Deliberately non-ASCII: column names are not limited to ASCII.
	for _, name := range []string{"é", "你好", "naïve_col", "größe", "名前-1"} {
		ok, _ := run(t, name+" = 1", one(name, num(1)))
		assert.True(t, ok, "column %q unquoted", name)
	}

	_, err := expr.Compile("\xff = 1", false)
	require.ErrorIs(t, err, expr.ErrSyntax)
	assert.ErrorContains(t, err, "invalid UTF-8")
}

func TestSyntaxErrorColumnsCountCharacters(t *testing.T) {
	t.Parallel()

	_, err := expr.Compile(`name = 'é' and age ~ 1`, false)
	require.Error(t, err)
	// n,a,m,e,space,=,space,',é,',space,a,n,d,space,a,g,e,space is 19 characters; ~ is the 20th.
	assert.Contains(t, err.Error(), "column 20")
	assert.Contains(t, err.Error(), `"~"`)

	//nolint:gosmopolitan // Deliberately non-ASCII: columns count characters, not bytes.
	_, err = expr.Compile(`你 = 'x' and ~`, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column 13")
}

func TestExpressionLengthIsBounded(t *testing.T) {
	t.Parallel()

	_, err := expr.Compile("a = 1"+strings.Repeat(" or a = 1", 20_000), false)
	require.ErrorIs(t, err, expr.ErrSyntax)
	require.ErrorContains(t, err, "limit")
}

func TestKeywordAsColumnName(t *testing.T) {
	t.Parallel()

	_, err := expr.Compile("not = 1", false)
	require.ErrorIs(t, err, expr.ErrSyntax)
	require.ErrorContains(t, err, "backticks")

	ok, _ := run(t, "`not` = 1", one("not", num(1)))
	assert.True(t, ok)

	// Words that cannot start an expression otherwise need no quoting.
	for _, name := range []string{"and", "or", "in", "is", "empty", "contains"} {
		ok, _ = run(t, name+" = 1", one(name, num(1)))
		assert.True(t, ok, name)
	}
}

func TestIn_TypeMissesAreOrderIndependent(t *testing.T) {
	t.Parallel()

	rec := one("x", str("abc"))
	for _, src := range []string{`x in (1, 'abc')`, `x in ('abc', 1)`} {
		ok, misses := run(t, src, rec)
		assert.True(t, ok, src)
		assert.Zero(t, misses, "%s: a literal that matched settles it", src)
	}

	// No literal matches and one is the wrong type: a single miss, and neither in nor not in holds.
	rec = one("x", str("n/a"))
	for _, src := range []string{`x in (1, 2)`, `x not in (1, 2)`} {
		ok, misses := run(t, src, rec)
		assert.False(t, ok, src)
		assert.Equal(t, int64(1), misses, "%s: one miss per row, not per literal", src)
	}

	// No miss: not in holds.
	ok, misses := run(t, `x not in ('p', 'q')`, rec)
	assert.True(t, ok)
	assert.Zero(t, misses)
}

func TestIn_StrictFailsOnlyWhenNothingMatched(t *testing.T) {
	t.Parallel()

	strict := func(src string, rec record.Record) error {
		p, err := expr.Compile(src, true)
		require.NoError(t, err)
		_, err = p.NewEvaluator().Match(rec)
		return err
	}

	require.NoError(t, strict(`x in (1, 'abc')`, one("x", str("abc"))))
	require.NoError(t, strict(`x in ('abc', 1)`, one("x", str("abc"))))
	require.ErrorIs(t, strict(`x in (1, 2)`, one("x", str("abc"))), expr.ErrUncomparable)
}

func TestTextIntegersCompareExactly(t *testing.T) {
	t.Parallel()

	// 2^53 + 1 is not representable as a float64.
	big := one("id", str("9007199254740993"))
	ok, _ := run(t, `id = 9007199254740993`, big)
	assert.True(t, ok)
	ok, _ = run(t, `id = 9007199254740992`, big)
	assert.False(t, ok, "a float64 compare would call these equal")
	ok, _ = run(t, `id > 9007199254740992`, big)
	assert.True(t, ok)

	// Beyond int64, still exact as an unsigned magnitude.
	huge := one("id", str("18446744073709551615"))
	ok, _ = run(t, `id = 18446744073709551615`, huge)
	assert.True(t, ok)
	ok, _ = run(t, `id < 18446744073709551615`, huge)
	assert.False(t, ok)

	// Negatives and signs.
	neg := one("n", str("-9007199254740993"))
	ok, _ = run(t, `n = -9007199254740993`, neg)
	assert.True(t, ok)
	ok, _ = run(t, `n < -9007199254740992`, neg)
	assert.True(t, ok)
	ok, _ = run(t, `n = 5`, one("n", str("+5")))
	assert.True(t, ok)
}

func TestNumericTextMustLookLikeANumber(t *testing.T) {
	t.Parallel()

	for _, text := range []string{
		"inf", "Infinity", "NaN", "nan", "0x1p-2", "0x10", "1_000", " 42", "42 ", "1e999x", "e", "-", ".",
	} {
		ok, misses := run(t, `x > 0`, one("x", str(text)))
		assert.False(t, ok, "%q", text)
		assert.Equal(t, int64(1), misses, "%q is a type miss", text)
	}

	for text, want := range map[string]bool{"1e3": true, "4.5": true, ".5": true, "-0.0": false, "+7": true} {
		ok, misses := run(t, `x > 0`, one("x", str(text)))
		assert.Equal(t, want, ok, "%q", text)
		assert.Zero(t, misses, "%q", text)
	}
}

func TestUnsignedValuesCompareAsUnsigned(t *testing.T) {
	t.Parallel()

	maxU := one("u", record.Int64(-1, record.SemanticUnsigned)) // 2^64-1
	ok, _ := run(t, `u > 5`, maxU)
	assert.True(t, ok)
	ok, _ = run(t, `u = 18446744073709551615`, maxU)
	assert.True(t, ok)
	ok, _ = run(t, `u > -1`, maxU)
	assert.True(t, ok, "an unsigned value is never below a negative literal")
	ok, _ = run(t, `u = '18446744073709551615'`, maxU)
	assert.True(t, ok, "the text and number spellings agree")

	small := one("u", record.Int64(7, record.SemanticUnsigned))
	ok, _ = run(t, `u = 7`, small)
	assert.True(t, ok)
}

func TestSignedValuesAgainstHugeLiterals(t *testing.T) {
	t.Parallel()

	v := one("i", num(math.MaxInt64))
	ok, _ := run(t, `i < 18446744073709551615`, v)
	assert.True(t, ok)
	ok, _ = run(t, `i = 9223372036854775807`, v)
	assert.True(t, ok)

	m := one("i", num(math.MinInt64))
	ok, _ = run(t, `i = -9223372036854775808`, m)
	assert.True(t, ok)
	ok, _ = run(t, `i < -9223372036854775807`, m)
	assert.True(t, ok)
}

func TestFloat32ComparesAtItsOwnPrecision(t *testing.T) {
	t.Parallel()

	v := one("f", record.Float32(0.1))
	for src, want := range map[string]bool{
		`f = 0.1`: true, `f = '0.1'`: true, `f > 0.09`: true, `f >= 0.1`: true, `f < 0.1`: false,
	} {
		ok, _ := run(t, src, v)
		assert.Equal(t, want, ok, src)
	}

	// A float64 keeps its full precision.
	d := one("f", record.Float64(0.1))
	ok, _ := run(t, `f = 0.1`, d)
	assert.True(t, ok)
}

func TestFloatEdges(t *testing.T) {
	t.Parallel()

	nan := one("f", record.Float64(math.NaN()))
	for src, want := range map[string]bool{
		`f = 1`: false, `f != 1`: true, `f < 1`: false, `f > 1`: false, `f >= 1`: false,
	} {
		ok, misses := run(t, src, nan)
		assert.Equal(t, want, ok, src)
		assert.Zero(t, misses, src)
	}

	negZero := one("f", record.Float64(math.Copysign(0, -1)))
	ok, _ := run(t, `f = 0`, negZero)
	assert.True(t, ok)

	ok, _ = run(t, `f > 1e300`, one("f", record.Float64(math.Inf(1))))
	assert.True(t, ok)
}

func TestNumberLiteralsAgainstTemporalValuesAreMisses(t *testing.T) {
	t.Parallel()

	for name, v := range map[string]record.Value{
		"date":   record.Int64(19723, record.SemanticDate),
		"millis": record.Int64(1_704_067_200_000, record.SemanticTimestampMillis),
		"micros": record.Int64(1_704_067_200_000_000, record.SemanticTimestampMicros),
		"nanos":  record.Int64(1_704_067_200_000_000_000, record.SemanticTimestampNanos),
	} {
		ok, misses := run(t, `t > 1000`, one("t", v))
		assert.False(t, ok, name)
		assert.Equal(t, int64(1), misses, "%s: a bare number would silently depend on the unit", name)
	}

	// The string spelling compares as the instant it is.
	rec := one("t", record.Int64(1_704_067_200_000, record.SemanticTimestampMillis))
	ok, misses := run(t, `t >= '2024-01-01' and t < '2024-01-02'`, rec)
	assert.True(t, ok)
	assert.Zero(t, misses)

	p, err := expr.Compile(`t > 1000`, true)
	require.NoError(t, err)
	_, err = p.NewEvaluator().Match(rec)
	require.ErrorIs(t, err, expr.ErrUncomparable)
	require.ErrorContains(t, err, "ISO-8601")
}

func TestEmptyContainersMatchNothingAndAreNotMisses(t *testing.T) {
	t.Parallel()

	for name, v := range map[string]record.Value{
		"empty list": record.List(nil),
		"empty map":  record.Map(record.Record{}),
	} {
		for _, src := range []string{`x = 1`, `x = 'a'`, `x = true`, `x != 1`, `x contains 'a'`, `x in (1, 2)`} {
			ok, misses := run(t, src, one("x", v))
			assert.False(t, ok, "%s: %s", name, src)
			assert.Zero(t, misses, "%s: %s: an empty container is an absent value", name, src)
		}
		ok, _ := run(t, `x is empty`, one("x", v))
		assert.True(t, ok, name)
	}

	// A non-empty container is a type miss for every comparison, contains included.
	list := one("x", record.List([]record.Value{num(1)}))
	for _, src := range []string{`x = 1`, `x = 'a'`, `x contains 'a'`, `x starts_with 'a'`} {
		ok, misses := run(t, src, list)
		assert.False(t, ok, src)
		assert.Equal(t, int64(1), misses, src)
	}
}

func TestContainsRendersNumbersAndBooleans(t *testing.T) {
	t.Parallel()

	ok, _ := run(t, `n contains '23'`, one("n", num(12345)))
	assert.True(t, ok)
	ok, _ = run(t, `b contains 'ru'`, one("b", record.Bool(true)))
	assert.True(t, ok)
}

func TestDuplicateFieldNamesResolveToTheFirst(t *testing.T) {
	t.Parallel()

	p, err := expr.Compile(`a = 2`, false)
	require.NoError(t, err)
	ev := p.NewEvaluator()

	// Different layouts back to back must not let a stale position pick a later duplicate.
	first := record.Record{}.Append("x", num(0)).Append("a", num(1))
	second := record.Record{}.Append("a", num(2)).Append("a", num(9))

	ok, _ := ev.Match(first)
	assert.False(t, ok)
	ok, _ = ev.Match(second)
	assert.True(t, ok, "the first a is 2")
}

func TestShortCircuitCountsOnlyEvaluatedTerms(t *testing.T) {
	t.Parallel()

	rec := record.Record{}.Append("a", num(5)).Append("b", str("zz"))

	_, misses := run(t, `a > 1 or b > 1`, rec)
	assert.Zero(t, misses, "a > 1 settles the or, so b is never compared")

	_, misses = run(t, `b > 1 or a > 1`, rec)
	assert.Equal(t, int64(1), misses)

	// not of a miss is true: a mistyped value does not match, so its negation does.
	ok, misses := run(t, `not (b > 1)`, rec)
	assert.True(t, ok)
	assert.Equal(t, int64(1), misses)
}

func TestColumnsReportsWhichComparedColumnsWereSeen(t *testing.T) {
	t.Parallel()

	p, err := expr.Compile(`age > 1 and nmae = 'x' and tags is empty and age < 9`, false)
	require.NoError(t, err)
	ev := p.NewEvaluator()
	_, err = ev.Match(record.Record{}.Append("age", num(5)).Append("name", str("x")))
	require.NoError(t, err)

	names, found := ev.Columns()
	assert.Equal(t, []string{"age", "nmae"}, names, "is empty tests and repeats are not listed")
	// `and` short-circuits, so nmae was never looked up and stays unseen for this record.
	assert.Equal(t, []bool{true, false}, found)
}

func TestLongNumericTextDoesNotAllocate(t *testing.T) {
	long := strings.Repeat("0", 60) + "123.5"
	p, err := expr.Compile(`x > 100`, false)
	require.NoError(t, err)
	ev := p.NewEvaluator()
	rec := one("x", str(long))

	ok, err := ev.Match(rec)
	require.NoError(t, err)
	require.True(t, ok)

	assert.Zero(t, testing.AllocsPerRun(1000, func() { _, _ = ev.Match(rec) }))

	bigInt := one("x", str(strings.Repeat("0", 60)+"7"))
	assert.Zero(t, testing.AllocsPerRun(1000, func() { _, _ = ev.Match(bigInt) }))
}

func TestAllocationFreeAcrossValueKinds(t *testing.T) {
	rec := record.Record{}.
		Append("i", num(42)).
		Append("u", record.Int64(-1, record.SemanticUnsigned)).
		Append("f32", record.Float32(0.1)).
		Append("d", record.Int64(19723, record.SemanticDate)).
		Append("s", str("hello world")).
		Append("l", record.List([]record.Value{num(1)})).
		Append("m", record.Map(record.Record{}.Append("k", str("v"))))

	for _, src := range []string{
		`i > 1 and u > 5 and f32 = 0.1`,
		`d >= '2023-01-01'`,
		`s in ('a', 'hello world')`,
		`l = 1 or m.k = 'v'`,
		`i in (1, 2, 42) and s contains 'lo w'`,
		`d > 5`, // a miss is still allocation-free
	} {
		p, err := expr.Compile(src, false)
		require.NoError(t, err)
		ev := p.NewEvaluator()
		_, _ = ev.Match(rec)

		assert.Zero(t, testing.AllocsPerRun(500, func() { _, _ = ev.Match(rec) }), src)
	}
}
