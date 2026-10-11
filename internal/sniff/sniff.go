// Package sniff identifies an input's compression and document format from its leading bytes,
// using the file extension only as a hint for inputs the content can't settle.
package sniff

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"

	"github.com/AlexAslan/streamio/internal/options"
)

// HeadSize is how many leading bytes Compression and Format need to see.
const HeadSize = 4096

// maxSampleLines bounds how many lines delimiter scoring looks at.
const maxSampleLines = 16

// ErrUnknownFormat is returned when the content matches no supported format and the extension
// offers no hint.
var ErrUnknownFormat = errors.New("streamio: cannot determine input format")

// ErrEncryptedParquet is returned for a Parquet file with an encrypted footer ("PARE" magic).
var ErrEncryptedParquet = errors.New("streamio: encrypted Parquet footers are not supported")

const (
	gzipMagic    = "\x1f\x8b"
	zstdMagic    = "\x28\xb5\x2f\xfd"
	bzip2Magic   = "BZh"
	xzMagic      = "\xfd7zXZ\x00"
	lz4Magic     = "\x04\x22\x4d\x18"
	snappyMagic  = "\xff\x06\x00\x00sNaPpY"
	parquetMagic = "PAR1"
	parquetEnc   = "PARE"
	arrowMagic   = "ARROW1"
	utf8BOM      = "\xef\xbb\xbf"
)

// hasMagic reports whether b begins with magic, without allocating.
func hasMagic(b []byte, magic string) bool {
	return len(b) >= len(magic) && string(b[:len(magic)]) == magic
}

// Compression returns the compression head begins with, or CompressionNone.
func Compression(head []byte) options.Compression {
	switch {
	case hasMagic(head, gzipMagic):
		return options.CompressionGzip
	case hasMagic(head, zstdMagic):
		return options.CompressionZstd
	case hasMagic(head, xzMagic):
		return options.CompressionXZ
	case hasMagic(head, lz4Magic):
		return options.CompressionLZ4
	case hasMagic(head, snappyMagic):
		return options.CompressionSnappy
	case isBzip2(head):
		return options.CompressionBzip2
	default:
		return options.CompressionNone
	}
}

// isBzip2 reports whether head opens like a bzip2 stream: "BZh", a block-size digit 1–9, then the
// first block's magic. The extra bytes keep text that merely starts with "BZh" from matching.
func isBzip2(head []byte) bool {
	const blockMagic = "\x31\x41\x59\x26\x53\x59" // pi, per the bzip2 format
	if !hasMagic(head, bzip2Magic) || len(head) < len(bzip2Magic)+1+len(blockMagic) {
		return false
	}
	level := head[len(bzip2Magic)]
	return level >= '1' && level <= '9' && string(head[4:4+len(blockMagic)]) == blockMagic
}

// Format returns the document format of head, a prefix of an already-decompressed input.
//
// Content decides first: container magic for Parquet and Arrow, then a leading '{' for NDJSON,
// then delimiter scoring for CSV and TSV. hint (a format guessed from the extension, or empty) only
// settles what the content leaves open: an empty file, or text with no consistent delimiter.
func Format(head []byte, hint options.OutputFormat) (options.OutputFormat, error) {
	switch {
	case hasMagic(head, parquetMagic):
		return options.FormatParquet, nil
	case hasMagic(head, parquetEnc):
		return "", ErrEncryptedParquet
	case hasMagic(head, arrowMagic):
		return options.FormatArrow, nil
	case bytes.IndexByte(head, 0) >= 0:
		// A NUL means binary or UTF-16, neither of which any text decoder here reads.
		return "", ErrUnknownFormat
	}

	text := bytes.TrimLeft(bytes.TrimPrefix(head, []byte(utf8BOM)), " \t\r\n")
	if len(text) == 0 {
		if hint != "" {
			return hint, nil
		}
		return options.FormatJSON, nil
	}
	if text[0] == '{' {
		return options.FormatJSON, nil
	}

	if f, ok := scoreDelimiters(text, len(head) >= HeadSize, hint); ok {
		return f, nil
	}
	if hint != "" {
		return hint, nil
	}
	return "", ErrUnknownFormat
}

// FormatFromExt maps a file name's extension to a format, reporting false when it names none.
func FormatFromExt(name string) (options.OutputFormat, bool) {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".parquet":
		return options.FormatParquet, true
	case ".csv":
		return options.FormatCSV, true
	case ".tsv":
		return options.FormatTSV, true
	case ".arrow":
		return options.FormatArrow, true
	case ".json", ".ndjson", ".jsonl":
		return options.FormatJSON, true
	default:
		return "", false
	}
}

// StripCompressionExt removes a trailing compression extension (.gz, .zst, .bz2, .xz, .lz4, .sz,
// .br and their long spellings) from name, returning the remaining name and the compression the
// extension named.
func StripCompressionExt(name string) (string, options.Compression) {
	ext := strings.ToLower(filepath.Ext(name))
	var c options.Compression
	switch ext {
	case ".gz", ".gzip":
		c = options.CompressionGzip
	case ".zst", ".zstd":
		c = options.CompressionZstd
	case ".bz2", ".bzip2":
		c = options.CompressionBzip2
	case ".xz":
		c = options.CompressionXZ
	case ".lz4":
		c = options.CompressionLZ4
	case ".sz", ".snappy":
		c = options.CompressionSnappy
	case ".br":
		c = options.CompressionBrotli
	default:
		return name, options.CompressionNone
	}
	return name[:len(name)-len(ext)], c
}

// NeedsExtension reports whether c has no magic number, so only a file name or an explicit option
// can say a stream uses it.
func NeedsExtension(c options.Compression) bool {
	return c == options.CompressionBrotli
}

// Detect returns the compression of a file: its magic number when it has one, else the compression
// its name's extension names, but only for codecs with no magic of their own. A name like x.csv.gz
// over plain text is therefore read as plain text rather than failing to gunzip.
func Detect(head []byte, name string) options.Compression {
	if c := Compression(head); c != options.CompressionNone {
		return c
	}
	if _, c := StripCompressionExt(name); NeedsExtension(c) {
		return c
	}
	return options.CompressionNone
}

// candidate is one delimiter's score over the sampled lines.
type candidate struct {
	format  options.OutputFormat
	perLine int
	matches int
}

// scoreDelimiters picks CSV or TSV by which delimiter appears a consistent, non-zero number of
// times (outside quotes) on most sampled lines. ok is false when neither does.
func scoreDelimiters(text []byte, truncated bool, hint options.OutputFormat) (options.OutputFormat, bool) {
	lines := sampleLines(text, truncated)
	if len(lines) == 0 {
		return "", false
	}

	best := candidate{}
	tied := false
	for _, c := range []struct {
		format options.OutputFormat
		delim  byte
	}{{options.FormatCSV, ','}, {options.FormatTSV, '\t'}} {
		cand := score(lines, c.delim)
		cand.format = c.format
		if cand.matches == 0 {
			continue
		}
		switch {
		case best.matches == 0 || cand.matches > best.matches:
			best, tied = cand, false
		case cand.matches == best.matches:
			tied = true
		}
	}

	if best.matches == 0 {
		return "", false
	}
	if tied && (hint == options.FormatCSV || hint == options.FormatTSV) {
		return hint, true
	}
	// A true tie with no usable hint resolves to CSV, the more common of the two.
	if tied {
		return options.FormatCSV, true
	}
	return best.format, true
}

// score counts delim on the first line and how many lines agree with that count. A candidate
// scores zero unless the first line has the delimiter and a strict majority of lines agree.
func score(lines [][]byte, delim byte) candidate {
	first := countOutsideQuotes(lines[0], delim)
	if first == 0 {
		return candidate{}
	}
	matches := 0
	for _, l := range lines {
		if countOutsideQuotes(l, delim) == first {
			matches++
		}
	}
	if len(lines) > 1 && matches*2 <= len(lines) {
		return candidate{}
	}
	return candidate{perLine: first, matches: matches}
}

// sampleLines returns up to maxSampleLines non-empty lines of text, dropping a final line the head
// may have cut short.
func sampleLines(text []byte, truncated bool) [][]byte {
	raw := bytes.Split(text, []byte{'\n'})
	if truncated && len(raw) > 1 {
		raw = raw[:len(raw)-1]
	}
	lines := make([][]byte, 0, min(len(raw), maxSampleLines))
	for _, l := range raw {
		l = bytes.TrimRight(l, "\r")
		if len(l) == 0 {
			continue
		}
		lines = append(lines, l)
		if len(lines) == maxSampleLines {
			break
		}
	}
	return lines
}

// countOutsideQuotes counts delim in line, ignoring occurrences inside double quotes.
func countOutsideQuotes(line []byte, delim byte) int {
	n, inQuote := 0, false
	for _, b := range line {
		switch {
		case b == '"':
			inQuote = !inQuote
		case b == delim && !inQuote:
			n++
		}
	}
	return n
}
