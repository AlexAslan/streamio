package jsonio

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

// chunkLines iterates the non-empty lines belonging to one claimed byte-range chunk, shared by raw
// passthrough and the record decoder so both resolve chunk boundaries identically.
//
// A chunk owns every line that starts within it, plus the one crossing its end.
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

// next returns this chunk's next non-empty line, stripped of its line ending, or io.EOF once the
// chunk is done.
func (c *chunkLines) next() ([]byte, error) {
	for {
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
		}

		if line := bytes.TrimRight(raw, "\r\n"); len(line) > 0 {
			return line, nil
		}
	}
}

// claimChunk claims the next unclaimed chunk from the shared queue, returning its index and start
// offset. The bool is false once the file is exhausted.
func (s *sharedState) claimChunk() (int64, int64, bool) {
	idx := s.nextChunk.Add(1) - 1
	start := idx * int64(s.chunkSize)
	return idx, start, start < s.size
}
