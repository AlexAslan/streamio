package csvio

import (
	"context"
	"errors"
	"io"
	"os"
	"streamio/internal/formatio"
	"streamio/internal/options"
	"streamio/internal/pool"
)

// rawSource streams a CSV/TSV file's own data rows to the sink, one document per row. It
// implements formatio.RawSource.
type rawSource struct {
	f *os.File
	s *sharedState
}

// NewRawSource opens the CSV/TSV file at path for raw passthrough: each dispatched document is one
// row's raw bytes. The caller owns the returned source and must Close it.
//
//nolint:ireturn // formatio.RawSource is the constructor type streamio's format registry stores.
func NewRawSource(cfg options.Config, path string) (formatio.RawSource, error) {
	f, s, err := openShared(cfg, path, cfg.OutputFormat)
	if err != nil {
		return nil, err
	}
	return &rawSource{f: f, s: s}, nil
}

// Close releases the open input file.
func (r *rawSource) Close() error {
	return r.f.Close()
}

// DecodeRaw claims byte-range chunks from the shared queue until none remain or ctx is cancelled.
func (r *rawSource) DecodeRaw(ctx context.Context, out chan<- [][]byte, stats *pool.DecodeStats) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		idx, start, ok := r.s.claimChunk()
		if !ok {
			return nil
		}

		if err := r.decodeChunk(ctx, idx, start, out, stats); err != nil {
			return err
		}
	}
}

// decodeChunk streams every row of one claimed chunk to out in fixed-size batches.
func (r *rawSource) decodeChunk(
	ctx context.Context,
	chunkIdx, chunkStart int64,
	out chan<- [][]byte,
	stats *pool.DecodeStats,
) error {
	lines, err := r.s.openChunk(chunkIdx, chunkStart)
	if err != nil {
		return err
	}

	batch := make([][]byte, 0, r.s.batchSize)
	for {
		line, lineErr := lines.next()
		if errors.Is(lineErr, io.EOF) {
			return r.flush(ctx, batch, out, stats)
		}
		if lineErr != nil {
			return lineErr
		}

		row := make([]byte, len(line))
		copy(row, line)
		batch = append(batch, row)

		if len(batch) >= r.s.batchSize {
			if err = r.flush(ctx, batch, out, stats); err != nil {
				return err
			}
			batch = make([][]byte, 0, r.s.batchSize)
		}
	}
}

// flush sends batch to out, recording its size, unless batch is empty.
func (r *rawSource) flush(
	ctx context.Context,
	batch [][]byte,
	out chan<- [][]byte,
	stats *pool.DecodeStats,
) error {
	if len(batch) == 0 {
		return nil
	}
	stats.Records.Add(int64(len(batch)))

	select {
	case out <- batch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
