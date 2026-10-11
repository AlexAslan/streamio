package expr

import (
	"strconv"
	"strings"
)

type cmpOp uint8

const (
	opEq cmpOp = iota
	opNe
	opLt
	opLe
	opGt
	opGe
)

type strOp uint8

const (
	strContains strOp = iota
	strStartsWith
)

type litKind uint8

const (
	litString litKind = iota
	litNumber
	litBool
)

// literal is a parsed constant.
type literal struct {
	s     string
	bs    []byte
	f     float64
	i     int64
	kind  litKind
	isInt bool
	b     bool
}

type nodeKind uint8

const (
	nodeOr nodeKind = iota
	nodeAnd
	nodeNot
	nodeCmp
	nodeIn
	nodeEmpty
	nodeStr
)

// node is one parsed expression. Which fields are set depends on kind: or and and use l and r, not
// uses l, cmp uses path, op and lit, in uses path, lits and negate, empty uses path and negate, and
// str uses path, sop and text.
type node struct {
	l, r   *node
	path   []string
	lits   []literal
	text   string
	lit    literal
	kind   nodeKind
	op     cmpOp
	sop    strOp
	negate bool
}

type parser struct {
	toks []token
	pos  int
}

// maxDepth bounds nesting so a hostile expression can't exhaust the stack.
const maxDepth = 64

// Parse parses src into an expression tree.
func parse(src string) (*node, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	n, err := p.parseOr(0)
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != tokEOF {
		return nil, syntaxErr(t.pos, "unexpected %q after the expression", t.text)
	}
	return n, nil
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}
	return t
}

func (p *parser) keyword(word string) bool {
	t := p.peek()
	if t.kind == tokIdent && strings.EqualFold(t.text, word) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) peekKeyword(word string) bool {
	t := p.peek()
	return t.kind == tokIdent && strings.EqualFold(t.text, word)
}

func (p *parser) parseOr(depth int) (*node, error) {
	left, err := p.parseAnd(depth)
	if err != nil {
		return nil, err
	}
	for p.keyword("or") {
		right, rerr := p.parseAnd(depth)
		if rerr != nil {
			return nil, rerr
		}
		left = &node{kind: nodeOr, l: left, r: right}
	}
	return left, nil
}

func (p *parser) parseAnd(depth int) (*node, error) {
	left, err := p.parseNot(depth)
	if err != nil {
		return nil, err
	}
	for p.keyword("and") {
		right, rerr := p.parseNot(depth)
		if rerr != nil {
			return nil, rerr
		}
		left = &node{kind: nodeAnd, l: left, r: right}
	}
	return left, nil
}

func (p *parser) parseNot(depth int) (*node, error) {
	if depth > maxDepth {
		return nil, syntaxErr(p.peek().pos, "expression nests deeper than %d", maxDepth)
	}
	if p.keyword("not") {
		x, err := p.parseNot(depth + 1)
		if err != nil {
			return nil, err
		}
		return &node{kind: nodeNot, l: x}, nil
	}
	return p.parsePrimary(depth)
}

func (p *parser) parsePrimary(depth int) (*node, error) {
	if p.peek().kind == tokLParen {
		p.next()
		n, err := p.parseOr(depth + 1)
		if err != nil {
			return nil, err
		}
		if t := p.next(); t.kind != tokRParen {
			return nil, syntaxErr(t.pos, "expected ) but found %q", describe(t))
		}
		return n, nil
	}

	path, err := p.parsePath()
	if err != nil {
		return nil, err
	}
	return p.parsePredicate(path)
}

func describe(t token) string {
	if t.kind == tokEOF {
		return "end of expression"
	}
	return t.text
}

func (p *parser) parsePath() ([]string, error) {
	t := p.next()
	if t.kind != tokIdent && t.kind != tokQuotedIdent {
		return nil, syntaxErr(t.pos, "expected a column name but found %q", describe(t))
	}
	path := []string{t.text}
	for p.peek().kind == tokDot {
		p.next()
		seg := p.next()
		if seg.kind != tokIdent && seg.kind != tokQuotedIdent {
			return nil, syntaxErr(seg.pos, "expected a name after . but found %q", describe(seg))
		}
		path = append(path, seg.text)
	}
	return path, nil
}

func (p *parser) parsePredicate(path []string) (*node, error) {
	t := p.peek()

	switch {
	case t.kind == tokOp:
		return p.parseComparison(path)
	case p.keyword("is"):
		negate := p.keyword("not")
		if !p.keyword("empty") {
			return nil, syntaxErr(p.peek().pos, "expected empty after is")
		}
		return &node{kind: nodeEmpty, path: path, negate: negate}, nil
	case p.peekKeyword("not"):
		p.next()
		if !p.keyword("in") {
			return nil, syntaxErr(p.peek().pos, "expected in after not")
		}
		return p.parseIn(path, true)
	case p.keyword("in"):
		return p.parseIn(path, false)
	case p.keyword("contains"):
		return p.parseStrOp(path, strContains)
	case p.keyword("starts_with"):
		return p.parseStrOp(path, strStartsWith)
	default:
		return nil, syntaxErr(t.pos, "expected a comparison after the column name but found %q", describe(t))
	}
}

func (p *parser) parseComparison(path []string) (*node, error) {
	opTok := p.next()
	var op cmpOp
	switch opTok.text {
	case "=", "==":
		op = opEq
	case "!=":
		op = opNe
	case "<":
		op = opLt
	case "<=":
		op = opLe
	case ">":
		op = opGt
	case ">=":
		op = opGe
	}

	lit, err := p.parseLiteral()
	if err != nil {
		return nil, err
	}
	if lit.kind == litBool && op != opEq && op != opNe {
		return nil, syntaxErr(opTok.pos, "%s cannot compare true or false: use = or !=", opTok.text)
	}
	return &node{kind: nodeCmp, path: path, op: op, lit: lit}, nil
}

func (p *parser) parseIn(path []string, negate bool) (*node, error) {
	if t := p.next(); t.kind != tokLParen {
		return nil, syntaxErr(t.pos, "expected ( after in but found %q", describe(t))
	}
	var lits []literal
	for {
		lit, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		lits = append(lits, lit)

		t := p.next()
		if t.kind == tokRParen {
			break
		}
		if t.kind != tokComma {
			return nil, syntaxErr(t.pos, "expected , or ) but found %q", describe(t))
		}
	}
	return &node{kind: nodeIn, path: path, lits: lits, negate: negate}, nil
}

func (p *parser) parseStrOp(path []string, op strOp) (*node, error) {
	t := p.next()
	if t.kind != tokString {
		return nil, syntaxErr(t.pos, "expected a quoted string but found %q", describe(t))
	}
	return &node{kind: nodeStr, path: path, sop: op, text: t.text}, nil
}

func (p *parser) parseLiteral() (literal, error) {
	t := p.next()
	switch t.kind {
	case tokString:
		return literal{kind: litString, s: t.text, bs: []byte(t.text)}, nil
	case tokNumber:
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return literal{}, syntaxErr(t.pos, "%q is not a number", t.text)
		}
		lit := literal{kind: litNumber, f: f, s: t.text}
		if i, ierr := strconv.ParseInt(t.text, 10, 64); ierr == nil {
			lit.i, lit.isInt = i, true
		}
		return lit, nil
	case tokIdent:
		switch strings.ToLower(t.text) {
		case "true":
			return literal{kind: litBool, b: true}, nil
		case "false":
			return literal{kind: litBool}, nil
		}
	case tokEOF, tokQuotedIdent, tokLParen, tokRParen, tokComma, tokDot, tokOp:
	}
	return literal{}, syntaxErr(t.pos, "expected a value (number, quoted string, true or false), found %q", describe(t))
}

// String renders a literal for error messages.
func (l literal) String() string {
	switch l.kind {
	case litString:
		return strconv.Quote(l.s)
	case litNumber:
		return l.s
	case litBool:
		return strconv.FormatBool(l.b)
	}
	return ""
}
