package expr

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/AlexAslan/streamio/record"
)

// Predicate is a compiled expression. Build one worker's evaluator with NewEvaluator; a Predicate
// itself holds only the parsed tree and is safe to share.
type Predicate struct {
	root   *node
	source string
	strict bool
}

// ErrUncomparable is returned in strict mode when a value cannot be compared with a literal's type.
var ErrUncomparable = errors.New("value cannot be compared")

// Compile parses src. In strict mode a value that cannot be compared with its literal (a
// non-numeric string against a number) is an error; otherwise it simply doesn't match and is
// counted.
func Compile(src string, strict bool) (*Predicate, error) {
	n, err := parse(src)
	if err != nil {
		return nil, err
	}
	return &Predicate{root: n, source: src, strict: strict}, nil
}

// Source returns the expression text.
func (p *Predicate) Source() string { return p.source }

// Evaluator evaluates a Predicate on records. It owns scratch buffers, so one serves one worker.
type Evaluator struct {
	eval    func(rec record.Record) (bool, error)
	scratch []byte
	misses  int64
	strict  bool
}

// NewEvaluator builds an evaluator for one worker.
func (p *Predicate) NewEvaluator() *Evaluator {
	e := &Evaluator{strict: p.strict}
	e.eval = e.build(p.root)
	return e
}

// Match reports whether rec satisfies the predicate.
func (e *Evaluator) Match(rec record.Record) (bool, error) {
	return e.eval(rec)
}

// Misses is how many comparisons found a value of the wrong type for their literal and did not
// match. It is always zero in strict mode, which fails instead.
func (e *Evaluator) Misses() int64 { return e.misses }

func (e *Evaluator) build(n *node) func(record.Record) (bool, error) {
	switch n.kind {
	case nodeOr:
		l, r := e.build(n.l), e.build(n.r)
		return func(rec record.Record) (bool, error) {
			if ok, err := l(rec); ok || err != nil {
				return ok, err
			}
			return r(rec)
		}
	case nodeAnd:
		l, r := e.build(n.l), e.build(n.r)
		return func(rec record.Record) (bool, error) {
			if ok, err := l(rec); !ok || err != nil {
				return false, err
			}
			return r(rec)
		}
	case nodeNot:
		x := e.build(n.l)
		return func(rec record.Record) (bool, error) {
			ok, err := x(rec)
			return !ok, err
		}
	case nodeCmp:
		return e.buildCmp(n)
	case nodeIn:
		return e.buildIn(n)
	case nodeEmpty:
		return e.buildEmpty(n)
	case nodeStr:
		return e.buildStr(n)
	}
	return func(record.Record) (bool, error) { return false, nil }
}

// lookup finds a possibly nested field, trying hint first so flat records resolve without a scan.
func lookup(rec record.Record, path []string, hint *int) (record.Value, bool) {
	for depth, seg := range path {
		idx := -1
		if depth == 0 && *hint < len(rec) && rec[*hint].Name == seg {
			idx = *hint
		} else {
			for j := range rec {
				if rec[j].Name == seg {
					idx = j
					break
				}
			}
			if depth == 0 && idx >= 0 {
				*hint = idx
			}
		}
		if idx < 0 {
			return record.Value{}, false
		}
		if depth == len(path)-1 {
			return rec[idx].Value, true
		}
		if rec[idx].Value.Kind != record.KindMap {
			return record.Value{}, false
		}
		rec = rec[idx].Value.Map
	}
	return record.Value{}, false
}

// outcome of comparing one value with one literal.
type outcome uint8

const (
	outNoValue outcome = iota // missing, null or empty: matches nothing
	outFalse
	outTrue
	outMiss // wrong type for the literal
)

func (e *Evaluator) buildCmp(n *node) func(record.Record) (bool, error) {
	hint := 0
	return func(rec record.Record) (bool, error) {
		v, ok := lookup(rec, n.path, &hint)
		if !ok {
			return false, nil
		}
		return e.settle(e.compare(v, n.lit, n.op), n.path, n.lit)
	}
}

func (e *Evaluator) buildIn(n *node) func(record.Record) (bool, error) {
	hint := 0
	return func(rec record.Record) (bool, error) {
		v, ok := lookup(rec, n.path, &hint)
		if !ok {
			return false, nil
		}

		sawValue, matched := false, false
		for _, lit := range n.lits {
			switch e.compare(v, lit, opEq) {
			case outTrue:
				sawValue, matched = true, true
			case outFalse:
				sawValue = true
			case outMiss:
				if _, err := e.settle(outMiss, n.path, lit); err != nil {
					return false, err
				}
				sawValue = true
			case outNoValue:
			}
			if matched {
				break
			}
		}
		if !sawValue {
			return false, nil
		}
		return matched != n.negate, nil
	}
}

func (e *Evaluator) buildEmpty(n *node) func(record.Record) (bool, error) {
	hint := 0
	return func(rec record.Record) (bool, error) {
		v, ok := lookup(rec, n.path, &hint)
		empty := !ok || isEmpty(v)
		return empty != n.negate, nil
	}
}

func isEmpty(v record.Value) bool {
	switch v.Kind {
	case record.KindNull:
		return true
	case record.KindBytes:
		return len(v.Str) == 0
	case record.KindList:
		return len(v.List) == 0
	case record.KindMap:
		return len(v.Map) == 0
	case record.KindBool, record.KindInt64, record.KindFloat64:
		return false
	}
	return false
}

func (e *Evaluator) buildStr(n *node) func(record.Record) (bool, error) {
	hint := 0
	lit := []byte(n.text)
	return func(rec record.Record) (bool, error) {
		v, ok := lookup(rec, n.path, &hint)
		if !ok {
			return false, nil
		}
		var text bool
		e.scratch, text = v.AppendText(e.scratch[:0])
		if !text {
			return false, nil
		}
		if n.sop == strContains {
			return bytes.Contains(e.scratch, lit), nil
		}
		return bytes.HasPrefix(e.scratch, lit), nil
	}
}

// settle turns an outcome into a result, recording or failing on a type miss.
func (e *Evaluator) settle(o outcome, path []string, lit literal) (bool, error) {
	switch o {
	case outTrue:
		return true, nil
	case outMiss:
		if e.strict {
			return false, fmt.Errorf("%w: column %q against %s", ErrUncomparable, joinPath(path), lit)
		}
		e.misses++
		return false, nil
	case outNoValue, outFalse:
		return false, nil
	}
	return false, nil
}

func joinPath(path []string) string {
	return strings.Join(path, ".")
}

// compare compares v with lit under op.
func (e *Evaluator) compare(v record.Value, lit literal, op cmpOp) outcome {
	switch lit.kind {
	case litString:
		return e.compareText(v, lit, op)
	case litNumber:
		return e.compareNumber(v, lit, op)
	case litBool:
		return e.compareBool(v, lit, op)
	}
	return outMiss
}

func (e *Evaluator) compareText(v record.Value, lit literal, op cmpOp) outcome {
	var ok bool
	e.scratch, ok = v.AppendText(e.scratch[:0])
	if !ok {
		if v.Kind == record.KindMap || v.Kind == record.KindList {
			return outMiss
		}
		return outNoValue
	}
	return boolOutcome(applyOrder(bytes.Compare(e.scratch, lit.bs), op))
}

func (e *Evaluator) compareNumber(v record.Value, lit literal, op cmpOp) outcome {
	switch v.Kind {
	case record.KindNull:
		return outNoValue
	case record.KindInt64:
		if lit.isInt {
			return boolOutcome(applyOrder(cmpInt(v.I64, lit.i), op))
		}
		return boolOutcome(applyOrder(cmpFloat(float64(v.I64), lit.f), op))
	case record.KindFloat64:
		return boolOutcome(applyOrder(cmpFloat(v.F64, lit.f), op))
	case record.KindBytes:
		if len(v.Str) == 0 {
			return outNoValue
		}
		f, err := strconv.ParseFloat(string(v.Str), 64)
		if err != nil {
			return outMiss
		}
		return boolOutcome(applyOrder(cmpFloat(f, lit.f), op))
	case record.KindBool, record.KindMap, record.KindList:
		return outMiss
	}
	return outMiss
}

func (e *Evaluator) compareBool(v record.Value, lit literal, op cmpOp) outcome {
	var got bool
	switch v.Kind {
	case record.KindNull:
		return outNoValue
	case record.KindBool:
		got = v.Bool
	case record.KindBytes:
		switch {
		case len(v.Str) == 0:
			return outNoValue
		case equalFoldASCII(v.Str, "true"):
			got = true
		case equalFoldASCII(v.Str, "false"):
			got = false
		default:
			return outMiss
		}
	case record.KindInt64, record.KindFloat64, record.KindMap, record.KindList:
		return outMiss
	}
	eq := got == lit.b
	if op == opNe {
		eq = !eq
	}
	return boolOutcome(eq)
}

// equalFoldASCII reports whether b equals lower, a lower-case ASCII word, ignoring case.
func equalFoldASCII(b []byte, lower string) bool {
	if len(b) != len(lower) {
		return false
	}
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != lower[i] {
			return false
		}
	}
	return true
}

func boolOutcome(b bool) outcome {
	if b {
		return outTrue
	}
	return outFalse
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// cmpFloat orders floats, treating NaN as unordered: it compares as neither less nor greater, and
// is not equal to anything, so a NaN never matches.
func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	case a == b:
		return 0
	default:
		return nanOrder
	}
}

// nanOrder is a sentinel applyOrder maps to "no comparison holds".
const nanOrder = 2

func applyOrder(c int, op cmpOp) bool {
	if c == nanOrder {
		return op == opNe
	}
	switch op {
	case opEq:
		return c == 0
	case opNe:
		return c != 0
	case opLt:
		return c < 0
	case opLe:
		return c <= 0
	case opGt:
		return c > 0
	case opGe:
		return c >= 0
	}
	return false
}
