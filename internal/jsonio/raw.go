package jsonio

import (
	"context"
	"os"
	"streamio/internal/formatio"
	"streamio/internal/options"
	"streamio/internal/pool"
)

// rawSource streams an NDJSON file's own lines to the sink. It implements formatio.RawSource.
type rawSource struct {
	f *os.File
	s *sharedState
}

// NewRawSource opens the NDJSON file at path for raw passthrough: each dispatched document is one
// line, handed to the sink as-is. The caller owns the returned source and must Close it.
//
//nolint:ireturn // formatio.RawSource is the constructor type streamio's format registry stores.
func NewRawSource(cfg options.Config, path string) (formatio.RawSource, error) {
	f, s, err := openShared(cfg, path)
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
// Safe for concurrent use.
func (r *rawSource) DecodeRaw(ctx context.Context, out chan<- [][]byte, stats *pool.DecodeStats) error {
	d := chunkDecoder{s: r.s, rb: r.s.newRecordBatcher()}
	return d.Decode(ctx, out, stats)
}
