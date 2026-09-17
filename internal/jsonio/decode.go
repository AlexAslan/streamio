package jsonio

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"streamio/internal/pool"
)

// chunkDecoder decodes a disjoint share of s's byte-range chunks, claimed from its shared queue.
// One is built per decode worker by rawSource.DecodeRaw, which owns and drives it.
type chunkDecoder struct {
	s  *sharedState
	rb *recordBatcher
}

// Decode claims chunk indices from the shared queue until none remain or ctx is cancelled.
func (d *chunkDecoder) Decode(ctx context.Context, out chan<- [][]byte, stats *pool.DecodeStats) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		chunkIdx, chunkStart, ok := d.s.claimChunk()
		if !ok {
			return nil
		}

		if err := d.s.processChunk(ctx, chunkIdx, chunkStart, d.rb, out, stats); err != nil {
			return fmt.Errorf("ndjson %s: chunk %d: %w", d.s.path, chunkIdx, err)
		}
	}
}

// processChunk positions a reader at a resolved line boundary for chunk chunkIdx and reads its body.
func (s *sharedState) processChunk(
	ctx context.Context,
	chunkIdx, chunkStart int64,
	rb *recordBatcher,
	out chan<- [][]byte,
	stats *pool.DecodeStats,
) error {
	lines, err := s.openChunk(chunkIdx, chunkStart)
	if err != nil {
		return err
	}

	return s.readChunkBody(ctx, lines, rb, out, stats)
}

// openChunkReader returns a reader over the shared file positioned at a resolved line boundary
// for chunk chunkIdx, plus the number of bytes read past chunkStart while resolving that
// boundary.
func (s *sharedState) openChunkReader(chunkIdx, chunkStart int64) (*bufio.Reader, int64, error) {
	if chunkIdx == 0 {
		sr := io.NewSectionReader(s.f, 0, s.size)
		return bufio.NewReaderSize(sr, s.readBufferSize), 0, nil
	}

	sr := io.NewSectionReader(s.f, chunkStart-1, s.size-(chunkStart-1))
	br := bufio.NewReaderSize(sr, s.readBufferSize)

	first, err := br.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	if first == '\n' {
		return br, 0, nil
	}

	discarded, err := br.ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, err
	}

	return br, int64(len(discarded)), nil
}

// readChunkBody batches the lines of one chunk into rb (see recordBatcher), enqueuing them for a
// dispatch worker as each batch fills and once the chunk runs out.
func (s *sharedState) readChunkBody(
	ctx context.Context,
	lines *chunkLines,
	rb *recordBatcher,
	out chan<- [][]byte,
	stats *pool.DecodeStats,
) error {
	for {
		line, err := lines.next()
		if errors.Is(err, io.EOF) {
			return s.flushLines(ctx, rb, out, stats)
		}
		if err != nil {
			return err
		}

		if err = s.addLine(ctx, rb, line, out, stats); err != nil {
			return err
		}
	}
}
