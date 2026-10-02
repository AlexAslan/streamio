package csvio

import (
	"bufio"
	"errors"
	"io"
)

// chunkLines iterates the physical lines belonging to one claimed byte-range chunk. It is used only
// by raw passthrough (raw.go), which copies each dispatched row's bytes verbatim without parsing
// them as CSV; record decoding (record_decoder.go) instead drives a persistent *csv.Reader
// directly, since real CSV parsing needs encoding/csv's own row tokenizing, not a physical-newline
// split.
//
// Splitting on physical newlines is an approximation for CSV: a quoted field containing a raw
// newline will not split correctly across a chunk boundary under multiple workers. This is an
// accepted limitation for raw passthrough specifically — a CSV→CSV raw-passthrough run reproduces
// the input's own bytes exactly (each dispatched line keeps its original terminator, and a blank
// line is never dropped), so a row that happens to get split across two dispatched raw documents is
// still those same bytes concatenated back in order; it only matters if something in between
// re-parses each dispatched item as a standalone row, which raw passthrough's own contract never
// promises. A single-worker run has no such restriction, since it reads the whole file through
// encoding/csv in one pass.
type chunkLines struct {
	br *bufio.Reader

	// bytesRead counts from the chunk's start, including the bytes the boundary resolution
	// consumed before the first line of this chunk's body.
	bytesRead int64

	// chunkSize is the point past which this chunk stops claiming new lines.
	chunkSize int64

	// isLast marks the chunk covering the end of the file, which reads to EOF rather than to
	// chunkSize.
	isLast bool

	// eof records that the underlying reader is spent, so a later call doesn't read again.
	eof bool
}

// openChunk resolves chunk chunkIdx, starting at chunkStart, onto a line boundary and returns an
// iterator over the lines it owns.
func (s *sharedState) openChunk(chunkIdx, chunkStart int64) (*chunkLines, error) {
	br, boundaryBytesRead, err := s.openChunkReader(chunkIdx, chunkStart)
	if err != nil {
		return nil, err
	}

	return &chunkLines{
		br:        br,
		bytesRead: boundaryBytesRead,
		chunkSize: int64(s.chunkSize),
		isLast:    chunkStart+int64(s.chunkSize) >= s.size,
	}, nil
}

// next returns this chunk's next physical line, including its original line ending exactly as read
// (so the raw-passthrough contract of reproducing the input's own bytes actually holds when
// dispatched lines are concatenated back to back with no separator of their own — see
// cmd/streamio/writer.go's fileWriter), or io.EOF once the chunk is done. A blank line is a real line and is
// returned as-is, not skipped: dropping it would itself be a loss of the original bytes. The
// returned slice is freshly allocated per line by bufio and is the caller's.
func (c *chunkLines) next() ([]byte, error) {
	if c.eof || (c.bytesRead >= c.chunkSize && !c.isLast) {
		return nil, io.EOF
	}

	raw, err := c.br.ReadBytes('\n')
	c.bytesRead += int64(len(raw))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if errors.Is(err, io.EOF) {
		c.eof = true
		if len(raw) == 0 {
			return nil, io.EOF
		}
	}
	return raw, nil
}
