package jsonio

import (
	"context"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/pool"
)

// rawSource streams an NDJSON source's own lines to the sink. It implements formatio.RawSource.
type rawSource struct {
	s *sharedState
}

// NewRawSource opens src for raw passthrough: each dispatched document is one line, handed to the
// sink as-is. NewRawSource does not take ownership of src.Reader; the caller closes it, if it needs
// closing, once done with the returned source.
//
//nolint:ireturn // formatio.RawSource is the constructor type streamio's format registry stores.
func NewRawSource(cfg options.Config, src options.Source) (formatio.RawSource, error) {
	return &rawSource{s: openShared(cfg, src)}, nil
}

// Close is a no-op: rawSource does not own src.Reader.
func (r *rawSource) Close() error {
	return nil
}

// DecodeRaw claims byte-range chunks from the shared queue until none remain or ctx is cancelled.
// Safe for concurrent use.
func (r *rawSource) DecodeRaw(ctx context.Context, out chan<- [][]byte, stats *pool.DecodeStats) error {
	d := chunkDecoder{s: r.s, rb: r.s.newRecordBatcher()}
	return d.Decode(ctx, out, stats)
}
