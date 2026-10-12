// Package expr compiles a small row-predicate language into a function over decoded records.
//
// The grammar:
//
//	expr     = or
//	or       = and { "or" and }
//	and      = not { "and" not }
//	not      = "not" not | primary
//	primary  = "(" expr ")" | path op literal | path ["not"] "in" "(" literal {"," literal} ")"
//	         | path "is" ["not"] "empty" | path ("contains" | "starts_with") string
//	op       = "=" | "!=" | "<" | "<=" | ">" | ">="
//	path     = name { "." name }        name is an identifier or a `quoted` segment
//	literal  = number | 'string' | "string" | true | false
//
// A literal's type decides how a value is compared: a string literal compares the value's text, a
// number literal compares numerically (a text value is parsed), a boolean literal compares true or
// false. A missing, null or empty value matches no comparison; use "is empty" to test for it.
package expr

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokenKind uint8

const (
	tokEOF tokenKind = iota
	tokIdent
	tokQuotedIdent
	tokString
	tokNumber
	tokLParen
	tokRParen
	tokComma
	tokDot
	tokOp
)

type token struct {
	text string
	pos  int
	kind tokenKind
}

// ErrSyntax is wrapped by every parse error.
var ErrSyntax = errors.New("syntax error")

// syntaxErr reports a problem at byte offset pos of src, as a 1-based column counted in characters.
func syntaxErr(src string, pos int, format string, args ...any) error {
	column := utf8.RuneCountInString(src[:min(pos, len(src))]) + 1
	return fmt.Errorf("%w at column %d: %s", ErrSyntax, column, fmt.Sprintf(format, args...))
}

// maxExpressionLength bounds how large an expression may be, so a hostile or accidental one cannot
// build an arbitrarily deep evaluator.
const maxExpressionLength = 1 << 16

// quoteOverhead is the two backticks around a quoted name.
const quoteOverhead = 2

// lex splits src into tokens.
func lex(src string) ([]token, error) {
	if len(src) > maxExpressionLength {
		return nil, syntaxErr(src, 0, "expression is %d bytes, over the %d byte limit", len(src), maxExpressionLength)
	}

	var toks []token
	for i := 0; i < len(src); {
		t, n, err := lexOne(src, i)
		if err != nil {
			return nil, err
		}
		if n == 0 { // whitespace
			i++
			continue
		}
		toks = append(toks, t)
		i += n
	}
	toks = append(toks, token{kind: tokEOF, pos: len(src)})
	return toks, nil
}

// lexOne reads the token starting at src[i], returning it and how many bytes it spans; a width of
// zero means src[i] is whitespace.
func lexOne(src string, i int) (token, int, error) {
	c := src[i]
	switch {
	case c == ' ' || c == '\t' || c == '\n' || c == '\r':
		return token{}, 0, nil
	case c == '(':
		return token{kind: tokLParen, text: "(", pos: i}, 1, nil
	case c == ')':
		return token{kind: tokRParen, text: ")", pos: i}, 1, nil
	case c == ',':
		return token{kind: tokComma, text: ",", pos: i}, 1, nil
	case c == '.':
		return token{kind: tokDot, text: ".", pos: i}, 1, nil
	case c == '\'' || c == '"':
		s, n, err := lexString(src, i)
		return token{kind: tokString, text: s, pos: i}, n, err
	case c == '`':
		end := strings.IndexByte(src[i+1:], '`')
		if end < 0 {
			return token{}, 0, syntaxErr(src, i, "unterminated quoted name")
		}
		return token{kind: tokQuotedIdent, text: src[i+1 : i+1+end], pos: i}, end + quoteOverhead, nil
	case isOpChar(c):
		return lexOperator(src, i)
	case c == '-' || (c >= '0' && c <= '9'):
		j := i + 1
		for j < len(src) && isNumberChar(src[j]) {
			j++
		}
		return token{kind: tokNumber, text: src[i:j], pos: i}, j - i, nil
	default:
		return lexIdent(src, i)
	}
}

// lexIdent reads a bare name: a letter or underscore, then letters, digits, underscores and
// hyphens, in any script.
func lexIdent(src string, i int) (token, int, error) {
	r, size := utf8.DecodeRuneInString(src[i:])
	if r == utf8.RuneError && size <= 1 {
		return token{}, 0, syntaxErr(src, i, "invalid UTF-8")
	}
	if !isIdentStart(r) {
		return token{}, 0, syntaxErr(src, i, "unexpected character %q", string(r))
	}

	j := i + size
	for j < len(src) {
		next, n := utf8.DecodeRuneInString(src[j:])
		if (next == utf8.RuneError && n <= 1) || !isIdentPart(next) {
			break
		}
		j += n
	}
	return token{kind: tokIdent, text: src[i:j], pos: i}, j - i, nil
}

func lexOperator(src string, i int) (token, int, error) {
	j := i + 1
	if j < len(src) && isOpChar(src[j]) {
		j++
	}
	switch op := src[i:j]; op {
	case "=", "!=", "<", "<=", ">", ">=", "==":
		return token{kind: tokOp, text: op, pos: i}, j - i, nil
	default:
		return token{}, 0, syntaxErr(src, i, "unknown operator %q", op)
	}
}

func lexString(src string, start int) (string, int, error) {
	quote := src[start]
	var sb strings.Builder
	for i := start + 1; i < len(src); i++ {
		switch c := src[i]; {
		case c == '\\' && i+1 < len(src):
			i++
			switch src[i] {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case '\\', '\'', '"':
				sb.WriteByte(src[i])
			default:
				return "", 0, syntaxErr(src, i-1, "unknown escape \\%c", src[i])
			}
		case c == quote:
			return sb.String(), i - start + 1, nil
		default:
			sb.WriteByte(c)
		}
	}
	return "", 0, syntaxErr(src, start, "unterminated string")
}

func isOpChar(c byte) bool { return c == '=' || c == '!' || c == '<' || c == '>' }

func isNumberChar(c byte) bool {
	return (c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-'
}

func isIdentStart(r rune) bool { return r == '_' || unicode.IsLetter(r) }

func isIdentPart(r rune) bool {
	return r == '_' || r == '-' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
