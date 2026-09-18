package csvio_test

import (
	"bytes"
	"encoding/csv"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/AlexAslan/streamio/internal/record"
)

// TestEncode_MatchesEncodingCSVQuoting checks the encoder's output is byte-identical to
// encoding/csv.Writer's own output for the same single-column field, across every RFC 4180
// quoting rule this package's rowWriter reimplements: empty, plain, containing the delimiter,
// containing a double quote, embedded newlines (\n, \r\n, bare \r), leading whitespace, and the
// stdlib's own \. special case.
//
// This exists because internal/csvio/row.go no longer drives encoding/csv.Writer for output — it
// writes rows directly to a reused buffer to avoid a per-field string allocation, reimplementing
// encoding/csv.Writer's quoting logic in the process (see row.go's rowWriter doc). A reimplemented
// RFC 4180 quoting/escaping routine is exactly the kind of small-but-easy-to-get-subtly-wrong logic
// this test exists to pin against the real standard library, not just against hand-picked examples.
func TestEncode_MatchesEncodingCSVQuoting(t *testing.T) {
	fields := []string{
		"", "a", "plain field",
		"has,comma",
		`has"quote`,
		"has\nnewline",
		"has\r\ncrlf",
		"has\rbarecr",
		" leading space",
		"\tleading tab",
		"trailing space ",
		`\.`,
		`\..`,
		`.\`,
		//nolint:gosmopolitan // Deliberately non-ASCII: verifying multi-byte UTF-8 quoting.
		"unicode: héllo wörld 日本語",
		`"already quoted"`,
		"a,b,c\nd,e,f",
	}

	for _, f := range fields {
		t.Run("", func(t *testing.T) {
			want := stdlibEncodeOneField(t, f, ',')
			got := csvioEncodeOneField(t, f, ',')
			if got != want {
				t.Errorf("field %q: encoder produced %q, want (stdlib) %q", f, got, want)
			}
		})
	}
}

// TestEncode_MatchesEncodingCSVQuoting_Fuzz does the same comparison for many randomly generated
// multi-field rows, and additionally round-trips the encoder's own output through a real
// csv.Reader to confirm the values decode back unchanged — catching a bug shared between this
// package's writer and encoding/csv.Writer's byte-for-byte comparison alone could miss.
func TestEncode_MatchesEncodingCSVQuoting_Fuzz(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed=%d (pass this to rand.NewSource to reproduce a failure)", seed)
	rng := rand.New(rand.NewSource(seed))
	//nolint:gosmopolitan // Deliberately non-ASCII: fuzzing multi-byte UTF-8 runes.
	alphabet := []rune("ab,\"\n\r\t xyz日本語\\.")

	for iter := range 300 {
		numFields := rng.Intn(4) + 1
		row := make([]string, numFields)
		for i := range row {
			n := rng.Intn(10)
			field := make([]rune, n)
			for j := range field {
				field[j] = alphabet[rng.Intn(len(alphabet))]
			}
			row[i] = string(field)
		}

		want := stdlibEncodeRow(t, row, ',')
		got := csvioEncodeRow(t, row, ',')
		if got != want {
			t.Fatalf("iter %d: encoder produced %q, want (stdlib) %q, row = %#v", iter, got, want, row)
		}

		// A single row consisting of one empty field encodes as a blank line ("\n"), which
		// encoding/csv.Reader treats as no record at all (a documented CSV convention: blank
		// lines are skipped, not "one empty field") rather than a read error — this is stdlib's
		// own behavior for this exact input, confirmed separately, not something to round-trip
		// through the reader here.
		if len(row) == 1 && row[0] == "" {
			continue
		}

		r := csv.NewReader(bytes.NewReader([]byte(got)))
		decoded, err := r.Read()
		if err != nil {
			t.Fatalf("iter %d: decoding encoder's own output failed: %v (row = %#v, encoded = %q)",
				iter, err, row, got)
		}
		if len(decoded) != len(row) {
			t.Fatalf("iter %d: decoded %d fields, want %d (row = %#v)", iter, len(decoded), len(row), row)
		}
		for i := range row {
			// csv.Reader normalizes every \r\n in its input to \n, even inside a quoted field
			// (documented at encoding/csv/reader.go: "The Reader converts all \r\n sequences in
			// its input to plain \n") — this is stdlib's own behavior, reproduced identically by
			// encoding/csv.Writer's own output (verified separately), not something this
			// package's writer introduces, so the round-trip comparison has to expect it too.
			wantField := strings.ReplaceAll(row[i], "\r\n", "\n")
			if decoded[i] != wantField {
				t.Fatalf("iter %d: field %d decoded as %q, want %q (row = %#v)", iter, i, decoded[i], wantField, row)
			}
		}
	}
}

func stdlibEncodeRow(tb testing.TB, row []string, delimiter rune) string {
	tb.Helper()
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.Comma = delimiter
	if err := w.Write(row); err != nil {
		tb.Fatalf("stdlib csv.Writer.Write: %v", err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		tb.Fatalf("stdlib csv.Writer.Flush: %v", err)
	}
	return buf.String()
}

func stdlibEncodeOneField(tb testing.TB, field string, delimiter rune) string {
	tb.Helper()
	return stdlibEncodeRow(tb, []string{field, "x"}, delimiter)
}

// csvioEncodeRow renders row through this package's real NewEncoder/EncodeBatch path (no header),
// using field names f0..fN-1 so rowFields' name-matching passes trivially.
func csvioEncodeRow(tb testing.TB, values []string, delimiter rune) string {
	tb.Helper()
	enc := newEncoder(tb, delimiter, false)
	var rec record.Record
	for i, v := range values {
		rec = rec.Append(fieldName(i), record.Bytes([]byte(v)))
	}
	docs, err := enc.EncodeBatch([]record.Record{rec})
	if err != nil {
		tb.Fatalf("EncodeBatch: %v", err)
	}
	if len(docs) != 1 {
		tb.Fatalf("EncodeBatch returned %d documents, want 1", len(docs))
	}
	return string(docs[0])
}

func csvioEncodeOneField(tb testing.TB, field string, delimiter rune) string {
	tb.Helper()
	return csvioEncodeRow(tb, []string{field, "x"}, delimiter)
}

func fieldName(i int) string {
	return string(rune('a' + i))
}
