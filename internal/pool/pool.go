// Package pool runs the decode/dispatch worker pool shared by every streamio route: each format
// only supplies a formatio.RawSource (RunRaw) or a decoder/encoder pair (RunRecords).
package pool

import (
	"context"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
)

// DecodeStats is formatio.DecodeStats under the name the pool's own callers use.
type DecodeStats = formatio.DecodeStats

// RunRaw runs the pool over the raw-passthrough path: cfg.Run.Workers goroutines call
// src.DecodeRaw against as many dispatch goroutines calling sink, until done or an error stops
// every worker.
//
// src is not closed here — the caller that built it owns it.
func RunRaw(
	ctx context.Context,
	cfg options.Config,
	src formatio.RawSource,
	sink options.DocumentHandler,
) (options.Result, error) {
	fn := func(ctx context.Context, _ int, out chan<- [][]byte, stats *DecodeStats) error {
		return src.DecodeRaw(ctx, out, stats)
	}

	return run(ctx, cfg, cfg.Run.Workers, fn, sink)
}

// decodeFunc is one decode worker's whole job, shared by RunRaw and RunRecords. worker is that
// worker's index, for callers that pre-build per-worker state.
type decodeFunc func(ctx context.Context, worker int, out chan<- [][]byte, stats *DecodeStats) error

// run is the fan-out/fan-in skeleton behind RunRaw and RunRecords.
func run(
	ctx context.Context,
	cfg options.Config,
	decodeWorkers int,
	decode decodeFunc,
	sink options.DocumentHandler,
) (options.Result, error) {
	out := make(chan [][]byte, cfg.Run.Workers*options.DispatchQueueDepthPerWorker)

	var (
		stats      DecodeStats
		dispatched atomic.Int64
		dispatchNs atomic.Int64
	)

	// Decode and dispatch are cancelled by different things. Dispatch stops only when the caller
	// cancels or a sink fails, so a decode error never throws away batches already queued: the
	// documents a worker produced before failing are still delivered, which is what makes a
	// "rows before the bad one" outcome deterministic instead of a race between the error's
	// cancellation and the queued send. Decode workers stop on any error, including dispatch's.
	dispatchCtx, cancelDispatch := context.WithCancel(ctx)
	defer cancelDispatch()

	decodeGroup, decodeCtx := errgroup.WithContext(dispatchCtx)
	for worker := range decodeWorkers {
		decodeGroup.Go(func() error { return decode(decodeCtx, worker, out, &stats) })
	}

	// Closing out once every decode worker has returned is what lets dispatch workers' range
	// loops end instead of blocking forever.
	var decodeErr error
	decodeDone := make(chan struct{})
	go func() {
		defer close(decodeDone)
		decodeErr = decodeGroup.Wait()
		close(out)
	}()

	var dispatchGroup errgroup.Group
	for range cfg.Run.Workers {
		dispatchGroup.Go(func() error {
			err := dispatchLoop(dispatchCtx, out, sink, &dispatched, &dispatchNs)
			if err != nil {
				cancelDispatch()
			}
			return err
		})
	}

	// A dispatch failure is the root cause when both fail: it is what cancelled the decoders.
	err := dispatchGroup.Wait()
	<-decodeDone
	if err == nil {
		err = decodeErr
	}

	rowsRead := stats.Records.Load()
	if rowsRead == 0 {
		rowsRead = dispatched.Load()
	}

	return options.Result{
		Stats: options.Stats{
			RowsRead:            rowsRead,
			RowsSkipped:         stats.RowsSkipped.Load(),
			RowsFiltered:        stats.RowsFiltered.Load(),
			DocumentsDispatched: dispatched.Load(),
			ReadDuration:        time.Duration(stats.ReadNs.Load()),
			DispatchDuration:    time.Duration(dispatchNs.Load()),
		},
	}, err
}

// dispatchLoop calls sink for every batch sent until out closes or ctx is cancelled.
func dispatchLoop(
	ctx context.Context,
	out <-chan [][]byte,
	sink options.DocumentHandler,
	dispatched, dispatchNs *atomic.Int64,
) error {
	for {
		select {
		case batch, ok := <-out:
			if !ok {
				return nil
			}
			if err := dispatchBatch(ctx, batch, sink, dispatched, dispatchNs); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// dispatchBatch sends every document in batch to the sink, timing the whole batch as one span.
func dispatchBatch(
	ctx context.Context,
	batch [][]byte,
	sink options.DocumentHandler,
	dispatched, dispatchNs *atomic.Int64,
) error {
	start := time.Now()
	defer func() { dispatchNs.Add(int64(time.Since(start))) }()

	for _, doc := range batch {
		if err := sink(ctx, doc); err != nil {
			return err
		}
		dispatched.Add(1)
	}

	return nil
}
