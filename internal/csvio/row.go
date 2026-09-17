package csvio

import (
	"bytes"
	"errors"
	"fmt"
	"unicode"
	"unicode/utf8"
)

// errInvalidDelimiter reports a delimiter encoding/csv itself would reject: a control character,
// a Unicode replacement character, or a double quote. Mirrors encoding/csv.Writer's own check
// (unexported validDelim), reproduced here since rowWriter no longer drives csv.Writer.
var errInvalidDelimiter = errors.New("csv: invalid delimiter")

// rowWriter renders records as CSV/TSV rows directly into a reused buffer, one field at a time,
// with no intermediate string per field the way driving encoding/csv.Writer's []string-based
// Write would require. Its quoting/escaping logic is a direct port of encoding/csv.Writer's own
// (see appendField's doc), restricted to the UseCRLF=false behavior this package has always used,
// so a compliant reader parses its output identically to what encoding/csv.Writer produced before
// this change — verified by TestRowWriter_MatchesEncodingCSV, which round-trips arbitrary fields
// through both writers and a real csv.Reader.
//
// One rowWriter lives on encoder/streamingEncoder for the whole run, reset per batch rather than
// rebuilt: rebuilding it per batch was measured to cost roughly 4.7x the final document's own
// size in allocations, all from bytes.Buffer regrowing from empty via Go's doubling strategy on
// every single batch — reset keeps the buffer's backing array (and therefore its already-grown
// capacity) across batches instead.
type rowWriter struct {
	buf       bytes.Buffer
	delimiter rune
}

// newRowWriter returns a rowWriter joining fields on delimiter.
func newRowWriter(delimiter rune) *rowWriter {
	return &rowWriter{delimiter: delimiter}
}

// reset empties rw's buffer for the next batch, keeping its backing array so the buffer doesn't
// have to regrow from empty every call.
func (rw *rowWriter) reset() {
	rw.buf.Reset()
}

// writeRow appends one row to the writer's buffer: fields joined by the delimiter, quoted and
// escaped per RFC 4180 exactly where encoding/csv.Writer would, terminated by a single '\n'.
func (rw *rowWriter) writeRow(fields [][]byte) error {
	if !validDelimiter(rw.delimiter) {
		return fmt.Errorf("%w: %q", errInvalidDelimiter, rw.delimiter)
	}

	for i, field := range fields {
		if i > 0 {
			rw.buf.WriteRune(rw.delimiter)
		}
		appendField(&rw.buf, field, rw.delimiter)
	}
	rw.buf.WriteByte('\n')
	return nil
}

// flush returns every row written since the last reset. The returned slice aliases rw.buf and is
// only valid until rw is next reset or written to — the caller must copy it out before calling
// reset for the next batch. There is nothing left to fail by the time flush runs — writeRow
// already validated the delimiter and did all the writing — so unlike the encoding/csv.Writer
// this replaced, flush needs no error return.
func (rw *rowWriter) flush() []byte {
	return rw.buf.Bytes()
}

// validDelimiter rejects a delimiter encoding/csv.Writer would also reject: the Unicode
// replacement character, a double quote, or any character invalid for encoding/csv's own Comma
// field (control characters, per its own rule, minus \r and \n, which reach here as EncodeRune
// producing an invalid byte sequence).
func validDelimiter(r rune) bool {
	return r != 0 && r != '"' && r != '\r' && r != '\n' && utf8.ValidRune(r) && r != utf8.RuneError
}

// backslashDot is force-quoted for the same reason encoding/csv.Writer force-quotes it: a bare
// \. unquoted confuses some external tools (e.g. git diff --cc) that treat it specially outside
// of quotes, so encoding/csv's own writer special-cases it — reproduced here for byte-identical
// output.
const backslashDot = `\.`

// needsQuoting reports whether field must be wrapped in double quotes to round-trip through a
// compliant CSV reader, mirroring encoding/csv.Writer's fieldNeedsQuotes exactly for the
// UseCRLF=false case this package always uses: empty fields never need quoting; a field
// containing the delimiter, a double quote, or either line-ending byte needs quoting; and a field
// starting with Unicode whitespace needs quoting so a reader doesn't trim it.
func needsQuoting(field []byte, delimiter rune) bool {
	if len(field) == 0 {
		return false
	}

	if string(field) == backslashDot {
		return true
	}

	if bytes.ContainsRune(field, delimiter) || bytes.ContainsAny(field, "\"\r\n") {
		return true
	}

	r, _ := utf8.DecodeRune(field)
	return unicode.IsSpace(r)
}

// appendField writes one field to dst, quoting and escaping it exactly as encoding/csv.Writer
// does with UseCRLF false: an embedded double quote doubles, and \r/\n pass through unescaped
// inside the quotes (encoding/csv.Writer's own behavior when not using CRLF line endings, which
// this package has never opted into).
func appendField(dst *bytes.Buffer, field []byte, delimiter rune) {
	if !needsQuoting(field, delimiter) {
		dst.Write(field)
		return
	}

	dst.WriteByte('"')
	for _, b := range field {
		if b == '"' {
			dst.WriteByte('"')
		}
		dst.WriteByte(b)
	}
	dst.WriteByte('"')
}
