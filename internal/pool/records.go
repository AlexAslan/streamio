package pool

import (
	"context"
	"errors"
	"io"
	"streamio/internal/formatio"
	"streamio/internal/options"
	"streamio/internal/record"
	"time"
)

// RunRecords runs the pool over the generic cross-format path: decode the input into canonical
// records and encode each batch in the requested output format, using the same fan-out/fan-in
// skeleton RunRaw uses.
//
// Decode workers come from dec (split across up to cfg.Run.Workers when it supports that, one
// otherwise), each paired with its own encoder from newEncoder since an encoder holds scratch and
// isn't safe for concurrent use. dec is not closed here — the caller that built it owns it.
func RunRecords(
	ctx context.Context,
	cfg options.Config,
	dec formatio.RecordDecoder,
	newEncoder func() (formatio.RecordEncoder, error),
	transformer options.Transformer,
	sink options.DocumentHandler,
) (options.Result, error) {
	// Built up front rather than inside each goroutine so a construction failure is reported
	// before any work starts, instead of racing the other workers to the errgroup.
	firstEnc, err := newEncoder()
	if err != nil {
		return options.Result{}, err
	}

	// A FinalizableRecordEncoder accumulates state across every batch in order (an open writer, a
	// derived schema), so it needs both a single decode worker and a single dispatch worker: run's
	// dispatch loop count comes from cfg.Workers directly, so without capping cfg.Workers itself,
	// several dispatch goroutines could still pull from the shared channel and call sink out of
	// order even with one decode worker producing batches in the right order to begin with.
	// Every other encoder has no such ordering requirement and is free to use cfg.Workers as given.
	if _, ok := firstEnc.(formatio.FinalizableRecordEncoder); ok {
		cfg.Run.Workers = 1
	}
	decoders := splitDecoders(dec, cfg.Run.Workers)

	workers := make([]*recordWorker, len(decoders))
	workers[0] = newRecordWorker(cfg, decoders[0], firstEnc, transformer)
	for i := 1; i < len(decoders); i++ {
		enc, encErr := newEncoder()
		if encErr != nil {
			return options.Result{}, encErr
		}
		workers[i] = newRecordWorker(cfg, decoders[i], enc, transformer)
	}

	return run(ctx, cfg, len(workers),
		func(ctx context.Context, worker int, out chan<- [][]byte, stats *DecodeStats) error {
			return workers[worker].decode(ctx, out, stats)
		}, sink)
}

// splitDecoders asks dec for up to limit decoders over the same input, falling back to driving dec
// alone (always correct, just serial) when it can't be split.
func splitDecoders(dec formatio.RecordDecoder, limit int) []formatio.RecordDecoder {
	splittable, ok := dec.(formatio.SplittableRecordDecoder)
	if !ok || limit <= 1 {
		return []formatio.RecordDecoder{dec}
	}

	decoders := splittable.Split(limit)
	if len(decoders) == 0 {
		return []formatio.RecordDecoder{dec}
	}
	return decoders
}

// recordWorker is one decode worker's decoder, encoder, and scratch — nothing here is shared with
// other workers.
type recordWorker struct {
	dec         formatio.RecordDecoder
	enc         formatio.RecordEncoder
	transformer options.Transformer
	onRowError  options.RowErrorPolicy

	// batch is the caller-owned batch the decoder refills, per formatio.RecordDecoder's contract.
	// Its length is also the most rows a single encoded document can cover, for an encoder that
	// renders a whole batch as one.
	batch []record.Record
}

// newRecordWorker pairs one decoder with one encoder and sizes their shared batch.
func newRecordWorker(
	cfg options.Config,
	dec formatio.RecordDecoder,
	enc formatio.RecordEncoder,
	transformer options.Transformer,
) *recordWorker {
	return &recordWorker{
		dec:         dec,
		enc:         enc,
		transformer: transformer,
		onRowError:  cfg.OnRowError,
		batch:       make([]record.Record, cfg.Run.BatchSize),
	}
}

// decode pulls and encodes batches until the decoder is exhausted or ctx is cancelled, sending
// each batch's documents to out.
func (w *recordWorker) decode(ctx context.Context, out chan<- [][]byte, stats *DecodeStats) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Decode and encode are timed as one span: ReadDuration means the time spent turning input
		// into documents, which on this path is both halves.
		readStart := time.Now()
		n, decErr := w.decodeBatch(ctx, stats)
		transformErr := w.transformBatch(n)
		var docs [][]byte
		var encErr error
		if transformErr == nil {
			docs, encErr = w.encodeBatch(n)
		}
		stats.ReadNs.Add(int64(time.Since(readStart)))

		if transformErr != nil {
			return transformErr
		}
		if encErr != nil {
			return encErr
		}

		// Count rows explicitly rather than letting the pool count dispatched items. The two agree
		// only when the encoder happens to return exactly one document per record; a whole-batch
		// encoder returns one item for n rows, and Stats.RowsRead means rows either way.
		stats.Records.Add(int64(n))

		if len(docs) > 0 {
			select {
			case out <- docs:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		switch {
		case errors.Is(decErr, io.EOF):
			return w.finalize(ctx, out)
		case decErr != nil:
			return decErr
		}
	}
}

// decodeBatch fills w.batch, retrying past a row-content error when w.onRowError.Mode is
// RowErrorSkip and the decoder wrapped it as a *formatio.RowError — the decoder's own contract for
// wrapping one guarantees it still leaves the decoder positioned to resume at the next row, so
// calling DecodeNext again picks up right after the dropped one. Any other error (io.EOF, a
// cancelled context, or a plain unwrapped error from a decoder that never distinguishes row
// content from I/O failures, e.g. Parquet) returns immediately, exactly as before this option
// existed.
func (w *recordWorker) decodeBatch(ctx context.Context, stats *DecodeStats) (int, error) {
	filled := 0
	for filled < len(w.batch) {
		n, err := w.dec.DecodeNext(ctx, w.batch[filled:])
		filled += n

		if err == nil {
			return filled, nil
		}
		if !w.skippable(err) {
			return filled, err
		}

		stats.RowsSkipped.Add(1)
		if w.onRowError.OnSkip != nil {
			w.onRowError.OnSkip(err)
		}
	}
	return filled, nil
}

// skippable reports whether err is a row error this worker should skip past rather than fail the
// run on: skipping is enabled and err wraps a *formatio.RowError.
func (w *recordWorker) skippable(err error) bool {
	if w.onRowError.Mode != options.RowErrorSkip {
		return false
	}
	var rowErr *formatio.RowError
	return errors.As(err, &rowErr)
}

// finalize dispatches the trailing bytes from the worker's encoder, if it implements
// FinalizableRecordEncoder; a no-op otherwise.
func (w *recordWorker) finalize(ctx context.Context, out chan<- [][]byte) error {
	fin, ok := w.enc.(formatio.FinalizableRecordEncoder)
	if !ok {
		return nil
	}

	doc, err := fin.Finalize()
	if err != nil {
		return err
	}
	if len(doc) == 0 {
		return nil
	}

	select {
	case out <- [][]byte{doc}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// transformBatch applies the configured record transform, when there is one, to the first n
// records the decoder filled.
func (w *recordWorker) transformBatch(n int) error {
	if n == 0 || w.transformer == nil {
		return nil
	}
	for i := range w.batch[:n] {
		rec, err := w.transformer.Transform(w.batch[i])
		if err != nil {
			return err
		}
		w.batch[i] = rec
	}
	return nil
}

// encodeBatch hands the first n records the decoder filled to the encoder, as however many
// documents it renders them as.
func (w *recordWorker) encodeBatch(n int) ([][]byte, error) {
	if n == 0 {
		return nil, nil
	}
	return w.enc.EncodeBatch(w.batch[:n])
}
