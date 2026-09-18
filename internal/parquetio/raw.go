package parquetio

import (
	"bytes"
	"context"
	"fmt"
	"streamio/internal/formatio"
	"streamio/internal/options"
	"streamio/internal/pool"
	"time"

	parquetgo "github.com/parquet-go/parquet-go"
)

// rawSource streams a Parquet source's own bytes to the sink, one standalone row group per
// document. It implements formatio.RawSource.
type rawSource struct {
	s    *sharedState
	name string
}

// NewRawSource opens src for raw passthrough: each dispatched document is one of the source's row
// groups, re-framed as a self-contained Parquet file. NewRawSource does not take ownership of
// src.Reader; the caller closes it, if it needs closing, once done with the returned source.
//
//nolint:ireturn // formatio.RawSource is the constructor type streamio's format registry stores.
func NewRawSource(cfg options.Config, src options.Source) (formatio.RawSource, error) {
	s, err := openShared(cfg, src)
	if err != nil {
		return nil, err
	}
	return &rawSource{s: s, name: src.Name}, nil
}

// Close is a no-op: rawSource does not own src.Reader.
func (r *rawSource) Close() error {
	return nil
}

// DecodeRaw claims row groups from the shared queue until none remain or ctx is cancelled, sending
// each one's standalone Parquet encoding to out. Safe for concurrent use.
func (r *rawSource) DecodeRaw(ctx context.Context, out chan<- [][]byte, stats *pool.DecodeStats) error {
	// Per-worker scratch, reused across every row group this worker claims so the buffer grows to
	// the largest row group once instead of per row group.
	var buf bytes.Buffer

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		idx := r.s.nextRowGroup.Add(1) - 1
		if idx >= int64(len(r.s.rowGroups)) {
			return nil
		}
		rg := r.s.rowGroups[idx]

		doc, err := r.extractRowGroup(ctx, rg, &buf, stats)
		if err != nil {
			return fmt.Errorf("parquet %s: row group %d: %w", r.name, idx, err)
		}

		// Count rows, not dispatched items: one item here is a whole row group, so the pool's
		// dispatch counter would otherwise report row groups as if they were documents.
		stats.Records.Add(rg.NumRows())

		select {
		case out <- [][]byte{doc}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// extractRowGroup re-frames rg as a standalone Parquet file, holding a reader slot for the
// duration so peak memory stays bounded by MaxOpenReaders row groups, same as the decoding path.
func (r *rawSource) extractRowGroup(
	ctx context.Context,
	rg parquetgo.RowGroup,
	buf *bytes.Buffer,
	stats *pool.DecodeStats,
) ([]byte, error) {
	select {
	case r.s.readerSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-r.s.readerSlots }()

	start := time.Now()
	defer func() { stats.ReadNs.Add(int64(time.Since(start))) }()

	return extractRawRowGroupBytes(rg, buf)
}

// extractRawRowGroupBytes writes rg as a complete, independently-openable Parquet file — magic
// bytes, one row group, footer — and returns a copy of those bytes.
//
// parquet-go's Writer.WriteRowGroup splices a file-backed row group's already-compressed column
// chunks straight through when the writer's configuration matches the source's, so nothing is
// decoded or re-encoded on this path; that verbatim copy is the entire point of raw passthrough.
// buf is caller-owned scratch, reset on entry and reused across calls; the result is copied out of
// it because the next call overwrites it while the sink may still hold this one.
func extractRawRowGroupBytes(rg parquetgo.RowGroup, buf *bytes.Buffer) ([]byte, error) {
	buf.Reset()

	w := parquetgo.NewGenericWriter[any](buf, rg.Schema())
	if _, err := w.WriteRowGroup(rg); err != nil {
		return nil, fmt.Errorf("writing row group: %w", err)
	}
	// Close writes the footer and magic trailer; without it the bytes are not a Parquet file.
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("closing row group writer: %w", err)
	}

	return cloneBuf(buf), nil
}
