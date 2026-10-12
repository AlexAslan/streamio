package expr

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/AlexAslan/streamio/record"
)

// Predicate is a compiled expression. Build one worker's evaluator with NewEvaluator; a Predicate
// itself holds only the parsed tree and is safe to share.
type Predicate struct {
	root   *node
	source string
	strict bool
}

// ErrUncomparable is returned in strict mode when a value cannot be compared with its literal's
// type.
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

// pathRef is one column the expression compares, and whether any record had it.
type pathRef struct {
	name  string
	path  []string
	found bool
}

// Evaluator evaluates a Predicate on records. It owns scratch buffers, so one serves one worker.
type Evaluator struct {
	eval    func(rec record.Record) (bool, error)
	reason  string
	refs    []pathRef
	scratch []byte
	misses  int64
	// pending is how many compared columns no record has had yet; it reaches zero and then the
	// per-record check is skipped.
	pending int
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
	if e.pending > 0 {
		e.observe(rec)
	}
	return e.eval(rec)
}

// observe notes which compared columns rec has. It is independent of evaluation, so a column that a
// short-circuited and/or never looked at still counts as seen.
func (e *Evaluator) observe(rec record.Record) {
	for i := range e.refs {
		r := &e.refs[i]
		if r.found {
			continue
		}
		if _, ok := lookup(rec, r.path); ok {
			r.found = true
			e.pending--
		}
	}
}

// Misses is how many comparisons found a value of the wrong type for their literal and did not
// match. A comparison that is not reached because of and/or short-circuiting is not counted. It is
// always zero in strict mode, which fails instead.
func (e *Evaluator) Misses() int64 { return e.misses }

// Columns returns the columns the expression compares and, for each, whether any record had it.
// `is empty` tests are left out: asking about a column that may be absent is their point.
func (e *Evaluator) Columns() ([]string, []bool) {
	names := make([]string, 0, len(e.refs))
	found := make([]bool, 0, len(e.refs))
	for _, r := range e.refs {
		names = append(names, r.name)
		found = append(found, r.found)
	}
	return names, found
}

// ref registers a column the expression compares.
func (e *Evaluator) ref(path []string) {
	name := strings.Join(path, ".")
	if slices.ContainsFunc(e.refs, func(r pathRef) bool { return r.name == name }) {
		return
	}
	e.refs = append(e.refs, pathRef{name: name, path: path})
	e.pending++
}

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

// lookup finds a possibly nested field. A name repeated in one record resolves to its first
// occurrence.
func lookup(rec record.Record, path []string) (record.Value, bool) {
	for depth, seg := range path {
		idx := -1
		for j := range rec {
			if rec[j].Name == seg {
				idx = j
				break
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

// Reasons a comparison is a type miss, reported in strict mode's error.
const (
	reasonNotNumber = "is not a number"
	reasonContainer = "is a list or map"
	reasonBool      = "is a boolean, not a number"
	reasonNumBool   = "is not true or false"
	reasonTemporal  = "is a date or timestamp: compare it with a quoted ISO-8601 string"
)

// view is a value plus its text rendering, produced at most once per evaluation.
//
//nolint:govet // Field order follows meaning; the struct lives on the stack.
type view struct {
	v        record.Value
	text     []byte
	rendered bool
}

// text renders the value once per evaluation. An empty or container value never reaches here.
func (e *Evaluator) text(vw *view) []byte {
	if !vw.rendered {
		e.scratch, _ = vw.v.AppendText(e.scratch[:0])
		vw.text, vw.rendered = e.scratch, true
	}
	return vw.text
}

func (e *Evaluator) buildCmp(n *node) func(record.Record) (bool, error) {
	e.ref(n.path)
	return func(rec record.Record) (bool, error) {
		v, ok := lookup(rec, n.path)
		if !ok {
			return false, nil
		}

		vw := view{v: v}
		return e.settle(e.compare(&vw, n.lit, n.op), n.path, n.lit)
	}
}

// buildIn matches when any literal equals the value. The result does not depend on the order of
// the literals: a literal that matches wins whatever the others did, and when none match, a
// literal of the wrong type makes the whole test a single miss rather than a no.
func (e *Evaluator) buildIn(n *node) func(record.Record) (bool, error) {
	e.ref(n.path)
	return func(rec record.Record) (bool, error) {
		v, ok := lookup(rec, n.path)
		if !ok {
			return false, nil
		}

		vw := view{v: v}
		var missed *literal
		for i := range n.lits {
			switch e.compare(&vw, n.lits[i], opEq) {
			case outTrue:
				return !n.negate, nil
			case outNoValue:
				return false, nil
			case outMiss:
				if missed == nil {
					missed = &n.lits[i]
				}
			case outFalse:
			}
		}

		if missed != nil {
			return e.settle(outMiss, n.path, *missed)
		}
		return n.negate, nil
	}
}

func (e *Evaluator) buildEmpty(n *node) func(record.Record) (bool, error) {
	return func(rec record.Record) (bool, error) {
		v, ok := lookup(rec, n.path)
		return (!ok || isEmpty(v)) != n.negate, nil
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
	e.ref(n.path)
	lit := []byte(n.text)
	litRef := literal{kind: litString, s: n.text}
	return func(rec record.Record) (bool, error) {
		v, ok := lookup(rec, n.path)
		if !ok {
			return false, nil
		}

		switch {
		case isEmpty(v):
			return false, nil
		case v.Kind == record.KindMap || v.Kind == record.KindList:
			e.reason = reasonContainer
			return e.settle(outMiss, n.path, litRef)
		}

		vw := view{v: v}
		text := e.text(&vw)
		if n.sop == strContains {
			return bytes.Contains(text, lit), nil
		}
		return bytes.HasPrefix(text, lit), nil
	}
}

// settle turns an outcome into a result, recording or failing on a type miss.
func (e *Evaluator) settle(o outcome, path []string, lit literal) (bool, error) {
	switch o {
	case outTrue:
		return true, nil
	case outMiss:
		if e.strict {
			return false, fmt.Errorf("%w: column %q %s, and the expression compares it with %s",
				ErrUncomparable, strings.Join(path, "."), e.reason, lit)
		}
		e.misses++
		return false, nil
	case outNoValue, outFalse:
		return false, nil
	}
	return false, nil
}

// compare compares the value with lit under op.
func (e *Evaluator) compare(vw *view, lit literal, op cmpOp) outcome {
	// An absent value matches no comparison. An empty list or map is absent; a non-empty one is
	// simply not something a scalar literal can be compared with.
	switch {
	case isEmpty(vw.v):
		return outNoValue
	case vw.v.Kind == record.KindMap || vw.v.Kind == record.KindList:
		e.reason = reasonContainer
		return outMiss
	}

	switch lit.kind {
	case litString:
		return e.compareText(vw, lit, op)
	case litNumber:
		return e.compareNumber(vw.v, lit, op)
	case litBool:
		return e.compareBool(vw.v, lit, op)
	}
	return outMiss
}

func (e *Evaluator) compareText(vw *view, lit literal, op cmpOp) outcome {
	text := e.text(vw)
	return boolOutcome(satisfies(bytes.Compare(text, lit.bs), true, op))
}

func (e *Evaluator) compareNumber(v record.Value, lit literal, op cmpOp) outcome {
	switch v.Kind {
	case record.KindInt64:
		return e.compareInt(v, lit, op)
	case record.KindFloat64:
		f, target := v.F64, lit.f
		if v.Semantic == record.SemanticFloat32 {
			// The value is a float32 widened to 64 bits; compare at the precision it was written at, or
			// 0.1 would not equal the literal 0.1.
			target = float64(float32(lit.f))
		}
		order, ordered := cmpFloat(f, target)
		return boolOutcome(satisfies(order, ordered, op))
	case record.KindBytes:
		return e.compareNumericText(v.Str, lit, op)
	case record.KindBool:
		e.reason = reasonBool
		return outMiss
	case record.KindNull, record.KindMap, record.KindList:
	}
	return outNoValue
}

func (e *Evaluator) compareInt(v record.Value, lit literal, op cmpOp) outcome {
	switch v.Semantic {
	case record.SemanticDate, record.SemanticTimestampMillis, record.SemanticTimestampMicros,
		record.SemanticTimestampNanos:
		// The stored number is a day or tick count, so comparing it with a bare number would silently
		// depend on the unit. A quoted ISO-8601 string compares as the date or time it is.
		e.reason = reasonTemporal
		return outMiss
	case record.SemanticUnsigned:
		//nolint:gosec // SemanticUnsigned says the int64's bits are a uint64.
		return boolOutcome(compareInteger(false, uint64(v.I64), lit, op))
	case record.SemanticNone, record.SemanticFloat32:
	}

	neg, mag := signMag(v.I64)
	return boolOutcome(compareInteger(neg, mag, lit, op))
}

// compareInteger compares an integer, exactly, with a number literal.
func compareInteger(neg bool, mag uint64, lit literal, op cmpOp) bool {
	if lit.isInteger {
		return satisfies(cmpMag(neg, mag, lit.neg, lit.mag), true, op)
	}

	f := float64(mag)
	if neg {
		f = -f
	}
	order, ordered := cmpFloat(f, lit.f)
	return satisfies(order, ordered, op)
}

// compareNumericText reads a text value as a number. Whole numbers compare exactly, so ids above
// 2^53 stay distinct; anything with a decimal point or exponent compares as a float64. Text that
// is not a plain decimal number (inf, nan, hex, underscores, padding) is a miss.
func (e *Evaluator) compareNumericText(b []byte, lit literal, op cmpOp) outcome {
	if !plainNumber(b) {
		e.reason = reasonNotNumber
		return outMiss
	}

	if neg, mag, ok := parseInteger(b); ok {
		return boolOutcome(compareInteger(neg, mag, lit, op))
	}

	// strconv does not retain its argument (an error clones it), so viewing the bytes as a string
	// is safe and keeps a long numeric text from allocating.
	f, err := strconv.ParseFloat(unsafe.String(unsafe.SliceData(b), len(b)), 64)
	if err != nil {
		e.reason = reasonNotNumber
		return outMiss
	}

	order, ordered := cmpFloat(f, lit.f)
	return boolOutcome(satisfies(order, ordered, op))
}

func (e *Evaluator) compareBool(v record.Value, lit literal, op cmpOp) outcome {
	var got bool
	switch v.Kind {
	case record.KindBool:
		got = v.Bool
	case record.KindBytes:
		switch {
		case equalFoldASCII(v.Str, "true"):
			got = true
		case equalFoldASCII(v.Str, "false"):
			got = false
		default:
			e.reason = reasonNumBool
			return outMiss
		}
	case record.KindInt64, record.KindFloat64:
		e.reason = reasonNumBool
		return outMiss
	case record.KindNull, record.KindMap, record.KindList:
		return outNoValue
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
