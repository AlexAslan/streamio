package expr_test

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio/internal/expr"
	"github.com/AlexAslan/streamio/record"
)

// This file checks the evaluator against a deliberately naive reference written from the
// documented semantics alone: random expressions over int, text, boolean and absent values are
// rendered to source, evaluated by both, and must agree on the result and on the miss count.

//nolint:govet // A throwaway test model; readability over packing.
type refValue struct {
	kind string // "int", "text", "bool", "null", "missing"
	i    int64
	s    string
	b    bool
}

//nolint:govet // A throwaway test model; readability over packing.
type refLit struct {
	kind string // "int", "text", "bool"
	i    int64
	s    string
	b    bool
}

func (l refLit) source() string {
	switch l.kind {
	case "int":
		return strconv.FormatInt(l.i, 10)
	case "text":
		return "'" + l.s + "'"
	default:
		return strconv.FormatBool(l.b)
	}
}

type outcomeKind int

const (
	refNoValue outcomeKind = iota
	refFalse
	refTrue
	refMiss
)

func (v refValue) empty() bool {
	return v.kind == "null" || v.kind == "missing" || (v.kind == "text" && v.s == "")
}

func (v refValue) textForm() string {
	switch v.kind {
	case "int":
		return strconv.FormatInt(v.i, 10)
	case "bool":
		return strconv.FormatBool(v.b)
	default:
		return v.s
	}
}

func refCompare(v refValue, l refLit, op string) outcomeKind {
	if v.empty() {
		return refNoValue
	}

	sat := func(c int) bool {
		switch op {
		case "=":
			return c == 0
		case "!=":
			return c != 0
		case "<":
			return c < 0
		case "<=":
			return c <= 0
		case ">":
			return c > 0
		default:
			return c >= 0
		}
	}
	res := func(b bool) outcomeKind {
		if b {
			return refTrue
		}
		return refFalse
	}

	switch l.kind {
	case "text":
		return res(sat(strings.Compare(v.textForm(), l.s)))
	case "int":
		switch v.kind {
		case "int":
			return res(sat(cmpI(v.i, l.i)))
		case "text":
			n, err := strconv.ParseInt(v.s, 10, 64)
			if err != nil {
				return refMiss
			}
			return res(sat(cmpI(n, l.i)))
		}
		return refMiss
	default: // bool
		switch v.kind {
		case "bool":
			return res(sat(cmpB(v.b, l.b)))
		case "text":
			switch strings.ToLower(v.s) {
			case "true":
				return res(sat(cmpB(true, l.b)))
			case "false":
				return res(sat(cmpB(false, l.b)))
			}
		}
		return refMiss
	}
}

func cmpI(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func cmpB(a, b bool) int {
	if a == b {
		return 0
	}
	return 1 // only = and != are generated for booleans
}

// refExpr is a reference expression tree.
//
//nolint:govet // A throwaway test model; readability over packing.
type refExpr struct {
	kind string // "cmp", "in", "notin", "empty", "notempty", "and", "or", "not"
	col  string
	op   string
	lit  refLit
	lits []refLit
	l, r *refExpr
}

func (e *refExpr) source() string {
	switch e.kind {
	case "cmp":
		return e.col + " " + e.op + " " + e.lit.source()
	case "in", "notin":
		parts := make([]string, len(e.lits))
		for i, l := range e.lits {
			parts[i] = l.source()
		}
		kw := " in "
		if e.kind == "notin" {
			kw = " not in "
		}
		return e.col + kw + "(" + strings.Join(parts, ", ") + ")"
	case "empty":
		return e.col + " is empty"
	case "notempty":
		return e.col + " is not empty"
	case "and":
		return "(" + e.l.source() + " and " + e.r.source() + ")"
	case "or":
		return "(" + e.l.source() + " or " + e.r.source() + ")"
	default:
		return "not (" + e.l.source() + ")"
	}
}

// eval returns the result and the number of misses, short-circuiting like the documentation says.
func (e *refExpr) eval(rec map[string]refValue) (bool, int64) {
	switch e.kind {
	case "cmp":
		switch refCompare(rec[e.col], e.lit, e.op) {
		case refTrue:
			return true, 0
		case refMiss:
			return false, 1
		case refNoValue, refFalse:
			return false, 0
		}
		return false, 0
	case "in", "notin":
		v := rec[e.col]
		if v.empty() {
			return false, 0
		}
		missed := false
		for _, l := range e.lits {
			switch refCompare(v, l, "=") {
			case refTrue:
				return e.kind == "in", 0
			case refMiss:
				missed = true
			case refNoValue, refFalse:
			}
		}
		if missed {
			return false, 1
		}
		return e.kind == "notin", 0
	case "empty":
		return rec[e.col].empty(), 0
	case "notempty":
		return !rec[e.col].empty(), 0
	case "and":
		a, ma := e.l.eval(rec)
		if !a {
			return false, ma
		}
		b, mb := e.r.eval(rec)
		return b, ma + mb
	case "or":
		a, ma := e.l.eval(rec)
		if a {
			return true, ma
		}
		b, mb := e.r.eval(rec)
		return b, ma + mb
	default:
		a, m := e.l.eval(rec)
		return !a, m
	}
}

func diffCols() []string { return []string{"a", "b", "c", "d", "e"} }

func diffTexts() []string {
	return []string{"", "abc", "12", "7", "-3", "true", "FALSE", "x y", "007"}
}

func genLit(rng *rand.Rand) refLit {
	switch rng.IntN(3) {
	case 0:
		return refLit{kind: "int", i: int64(rng.IntN(21) - 10)}
	case 1:
		words := []string{"abc", "12", "7", "true", "zzz", "0", "x y"}
		return refLit{kind: "text", s: words[rng.IntN(len(words))]}
	default:
		return refLit{kind: "bool", b: rng.IntN(2) == 0}
	}
}

func genExpr(rng *rand.Rand, depth int) *refExpr {
	if depth > 0 && rng.IntN(100) < 55 {
		switch rng.IntN(3) {
		case 0:
			return &refExpr{kind: "and", l: genExpr(rng, depth-1), r: genExpr(rng, depth-1)}
		case 1:
			return &refExpr{kind: "or", l: genExpr(rng, depth-1), r: genExpr(rng, depth-1)}
		default:
			return &refExpr{kind: "not", l: genExpr(rng, depth-1)}
		}
	}

	col := diffCols()[rng.IntN(len(diffCols()))]
	switch rng.IntN(5) {
	case 0:
		return &refExpr{kind: "empty", col: col}
	case 1:
		return &refExpr{kind: "notempty", col: col}
	case 2, 3:
		lit := genLit(rng)
		ops := []string{"=", "!=", "<", "<=", ">", ">="}
		if lit.kind == "bool" {
			ops = ops[:2]
		}
		return &refExpr{kind: "cmp", col: col, op: ops[rng.IntN(len(ops))], lit: lit}
	default:
		n := 1 + rng.IntN(3)
		lits := make([]refLit, n)
		for i := range lits {
			lits[i] = genLit(rng)
		}
		kind := "in"
		if rng.IntN(2) == 0 {
			kind = "notin"
		}
		return &refExpr{kind: kind, col: col, lits: lits}
	}
}

func genRecord(rng *rand.Rand) map[string]refValue {
	rec := make(map[string]refValue, len(diffCols()))
	for _, c := range diffCols() {
		switch rng.IntN(6) {
		case 0:
			rec[c] = refValue{kind: "missing"}
		case 1:
			rec[c] = refValue{kind: "null"}
		case 2:
			rec[c] = refValue{kind: "int", i: int64(rng.IntN(21) - 10)}
		case 3:
			rec[c] = refValue{kind: "bool", b: rng.IntN(2) == 0}
		default:
			rec[c] = refValue{kind: "text", s: diffTexts()[rng.IntN(len(diffTexts()))]}
		}
	}
	return rec
}

func toRecord(rv map[string]refValue) record.Record {
	var rec record.Record
	for _, c := range diffCols() {
		v := rv[c]
		switch v.kind {
		case "missing":
		case "null":
			rec = rec.Append(c, record.Null())
		case "int":
			rec = rec.Append(c, record.Int64(v.i, record.SemanticNone))
		case "bool":
			rec = rec.Append(c, record.Bool(v.b))
		default:
			rec = rec.Append(c, record.Bytes([]byte(v.s)))
		}
	}
	return rec
}

func TestDifferentialAgainstAReferenceEvaluator(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(20260811, 7))
	for trial := range 4000 {
		e := genExpr(rng, 3)
		src := e.source()

		p, err := expr.Compile(src, false)
		require.NoError(t, err, "trial %d: %s", trial, src)
		ev := p.NewEvaluator()

		for range 6 {
			rv := genRecord(rng)
			wantOK, wantMiss := e.eval(rv)

			before := ev.Misses()
			gotOK, matchErr := ev.Match(toRecord(rv))
			require.NoError(t, matchErr)

			require.Equal(t, wantOK, gotOK, "result of %s on %v", src, describe(rv))
			require.Equal(t, wantMiss, ev.Misses()-before, "misses of %s on %v", src, describe(rv))
		}
	}
}

func describe(rv map[string]refValue) string {
	parts := make([]string, 0, len(diffCols()))
	for _, c := range diffCols() {
		v := rv[c]
		switch v.kind {
		case "int":
			parts = append(parts, fmt.Sprintf("%s=%d", c, v.i))
		case "bool":
			parts = append(parts, fmt.Sprintf("%s=%t", c, v.b))
		case "text":
			parts = append(parts, fmt.Sprintf("%s=%q", c, v.s))
		default:
			parts = append(parts, c+"="+v.kind)
		}
	}
	return strings.Join(parts, " ")
}
