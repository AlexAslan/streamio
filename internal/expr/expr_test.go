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

func str(s string) record.Value { return record.Bytes([]byte(s)) }

func num(n int64) record.Value { return record.Int64(n, record.SemanticNone) }

// sample is one record covering every value kind the predicates must handle.
func sample() record.Record {
	return record.Record{}.
		Append("name", str("alice")).
		Append("age", num(30)).
		Append("score", record.Float64(4.5)).
		Append("age_text", str("42")).
		Append("active", record.Bool(true)).
		Append("flag_text", str("TRUE")).
		Append("empty", str("")).
		Append("nothing", record.Null()).
		Append("tags", record.List([]record.Value{str("a")})).
		Append("attrs", record.Map(record.Record{}.
			Append("env", str("prod")).
			Append("a.b", str("dotted")).
			Append("deep", record.Map(record.Record{}.Append("n", num(7)))))).
		Append("msg", str("connection timeout while reading"))
}

func match(t *testing.T, src string, rec record.Record) bool {
	t.Helper()

	p, err := expr.Compile(src, false)
	require.NoError(t, err, src)
	ok, err := p.NewEvaluator().Match(rec)
	require.NoError(t, err, src)
	return ok
}

func TestEval(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		src  string
		want bool
	}{
		// strings
		{`name = 'alice'`, true},
		{`name = "alice"`, true},
		{`name != 'bob'`, true},
		{`name = 'bob'`, false},
		{`name < 'b'`, true},
		{`name >= 'alice'`, true},
		{`NAME = 'alice'`, false}, // names are case-sensitive
		// numbers
		{`age = 30`, true},
		{`age > 18`, true},
		{`age >= 30`, true},
		{`age < 30`, false},
		{`age != 30`, false},
		{`age > 29.5`, true},
		{`score > 4`, true},
		{`score = 4.5`, true},
		{`age_text > 40`, true}, // text parsed as a number
		{`age_text = 42`, true},
		{`age = '30'`, true}, // a string literal compares the value's text
		// booleans
		{`active = true`, true},
		{`active != true`, false},
		{`flag_text = true`, true},
		{`active = false`, false},
		// emptiness and absence
		{`empty is empty`, true},
		{`nothing is empty`, true},
		{`missing is empty`, true},
		{`name is empty`, false},
		{`name is not empty`, true},
		{`tags is empty`, false},
		{`empty = ''`, false}, // an empty value matches no comparison
		{`nothing = 1`, false},
		{`missing != 'x'`, false},
		// nesting and quoting
		{`attrs.env = 'prod'`, true},
		{`attrs.deep.n = 7`, true},
		{"attrs.`a.b` = 'dotted'", true},
		{`attrs.missing = 'x'`, false},
		{`name.sub = 'x'`, false}, // descending into a scalar
		// sets
		{`name in ('alice', 'bob')`, true},
		{`name in ('carol')`, false},
		{`name not in ('carol')`, true},
		{`name not in ('alice')`, false},
		{`missing in ('a')`, false},
		{`missing not in ('a')`, false},
		{`age in (1, 30)`, true},
		// strings ops
		{`msg contains 'timeout'`, true},
		{`msg contains 'refused'`, false},
		{`msg starts_with 'connection'`, true},
		{`msg starts_with 'timeout'`, false},
		{`missing contains 'x'`, false},
		// boolean logic
		{`age > 18 and name = 'alice'`, true},
		{`age > 40 or name = 'alice'`, true},
		{`age > 40 or name = 'bob'`, false},
		{`not age > 40`, true},
		{`not (age > 18 and name = 'alice')`, false},
		{`age > 18 and (name = 'bob' or active = true)`, true},
		{`AGE > 1`, false},
		{`age > 18 AND name = 'alice'`, true},
		{`not missing = 1`, true}, // a missing value matches nothing, so its negation holds
	} {
		t.Run(tc.src, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, match(t, tc.src, sample()))
		})
	}
}

func TestParseErrors(t *testing.T) {
	t.Parallel()

	for _, src := range []string{
		``, `age`, `age >`, `age > > 3`, `age = `, `(age = 1`, `age = 1)`, `age = 1 and`, `= 1`,
		`age = 'unterminated`, "attrs.`open = 1", `age ~ 1`, `age in ()`, `age in (1,`, `age in 1`,
		`age is`, `age is nothing`, `active < true`, `name contains 5`, `age = 1 age = 2`, `a..b = 1`,
		`age = 1e`, `not`, `age = 'bad\q'`,
	} {
		_, err := expr.Compile(src, false)
		require.ErrorIs(t, err, expr.ErrSyntax, "%q should not parse", src)
	}
}

func TestParseErrorsNameTheColumn(t *testing.T) {
	t.Parallel()

	_, err := expr.Compile(`age > 18 and name = `, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column 21")
}

func TestDeepNestingIsBounded(t *testing.T) {
	t.Parallel()

	src := strings.Repeat("(", 200) + "a = 1" + strings.Repeat(")", 200)
	_, err := expr.Compile(src, false)
	require.ErrorIs(t, err, expr.ErrSyntax)
}

func TestTypeMissesAreCountedOrFail(t *testing.T) {
	t.Parallel()

	rec := record.Record{}.Append("name", str("alice")).Append("tags", record.List([]record.Value{num(1)}))

	lenient, err := expr.Compile(`name > 5 or tags = 'x'`, false)
	require.NoError(t, err)
	ev := lenient.NewEvaluator()
	ok, err := ev.Match(rec)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, int64(2), ev.Misses())

	strict, err := expr.Compile(`name > 5`, true)
	require.NoError(t, err)
	_, err = strict.NewEvaluator().Match(rec)
	require.ErrorIs(t, err, expr.ErrUncomparable)
	assert.ErrorContains(t, err, `"name"`)
}

func TestIntegerPrecisionIsKeptForIntegerLiterals(t *testing.T) {
	t.Parallel()

	// 2^53+1 is not representable as a float64, so a float compare would call these equal.
	rec := record.Record{}.Append("id", num(9007199254740993))
	assert.True(t, match(t, `id = 9007199254740993`, rec))
	assert.False(t, match(t, `id = 9007199254740992`, rec))
}

func TestEvaluatorsAreIndependent(t *testing.T) {
	t.Parallel()

	p, err := expr.Compile(`name contains 'ali'`, false)
	require.NoError(t, err)
	a, b := p.NewEvaluator(), p.NewEvaluator()

	for range 3 {
		okA, _ := a.Match(sample())
		okB, _ := b.Match(record.Record{}.Append("name", str("bob")))
		assert.True(t, okA)
		assert.False(t, okB)
	}
}

func TestEvalAllocatesNothing(t *testing.T) {
	for _, src := range []string{
		`age > 18 and name = 'alice'`,
		`name in ('alice', 'bob')`,
		`age_text > 40`,
		`msg contains 'timeout'`,
		`attrs.deep.n = 7`,
		`empty is empty or not active = false`,
	} {
		t.Run(src, func(t *testing.T) {
			p, err := expr.Compile(src, false)
			require.NoError(t, err)
			ev := p.NewEvaluator()
			rec := sample()
			_, _ = ev.Match(rec)

			allocs := testing.AllocsPerRun(1000, func() { _, _ = ev.Match(rec) })
			assert.Zero(t, allocs)
		})
	}
}

// fuzzRecords covers every value kind and Semantic a predicate can meet.
func fuzzRecords() []record.Record {
	return []record.Record{
		sample(),
		{},
		record.Record{}.
			Append("u", record.Int64(-1, record.SemanticUnsigned)).
			Append("f", record.Float32(0.1)).
			Append("n", record.Float64(math.NaN())).
			Append("d", record.Int64(19723, record.SemanticDate)).
			Append("ts", record.Int64(1_704_067_200_000, record.SemanticTimestampMillis)).
			Append("l", record.List(nil)).
			Append("m", record.Map(record.Record{})).
			Append("big", str("9007199254740993")).
			Append("u8", str("\xff\xfe")),
	}
}

func FuzzCompileAndEval(f *testing.F) {
	for _, seed := range []string{
		`age > 18 and name = 'alice'`, `a in (1, 2, 3)`, `not (x is empty)`, "`a.b` contains 'x'",
		`((((a = 1))))`, `x = 'unterminated`, `a = 1e999`, `a.b.c >= -1.5e3`, `u > 5 or f = 0.1`,
		`d >= '2024-01-01' and ts < 5`, `é = 1`, `big = 9007199254740993`, `x not in (1, 'a')`,
	} {
		f.Add(seed)
	}

	recs := fuzzRecords()
	f.Fuzz(func(_ *testing.T, src string) {
		for _, strict := range []bool{false, true} {
			p, err := expr.Compile(src, strict)
			if err != nil {
				return
			}
			ev := p.NewEvaluator()
			for _, rec := range recs {
				_, _ = ev.Match(rec)
			}
		}
	})
}

func BenchmarkMatch(b *testing.B) {
	p, err := expr.Compile(`age > 18 and name = 'alice' and attrs.env in ('prod', 'staging')`, false)
	require.NoError(b, err)
	ev := p.NewEvaluator()
	rec := sample()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = ev.Match(rec)
	}
}
