package streamio

import (
	"context"
	"os"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/pool"
	"github.com/AlexAslan/streamio/record"
)

// ConsumeFile reads the file at path and hands its decoded records to consumers, without producing
// any output.
//
// newConsumer is called once per decode worker, so each consumer may aggregate without locking and
// the caller merges them afterwards. The input goes through the same detection, decompression,
// filters and transforms as ProcessFile; the batches a consumer sees are what an encoder would
// have been given.
func ConsumeFile(
	ctx context.Context,
	path string,
	newConsumer func() (RecordConsumer, error),
	opts ...Option,
) (Result, error) {
	f, err := os.Open(path)
	if err != nil {
		return Result{}, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return Result{}, err
	}
	return ConsumeReaderAt(ctx, Source{Reader: f, Size: stat.Size(), Name: path}, newConsumer, opts...)
}

// ConsumeReaderAt does what ConsumeFile does for any sized, randomly-addressable source.
//
// The records a consumer receives alias decoder buffers and are reused after Consume returns; see
// the record package for the lifetime rule.
func ConsumeReaderAt(
	ctx context.Context,
	src Source,
	newConsumer func() (RecordConsumer, error),
	opts ...Option,
) (Result, error) {
	cfg := options.New(opts...)
	if cfg.OptionErr != nil {
		return Result{}, cfg.OptionErr
	}

	// Resolves compression and format from the contents, exactly as ProcessReaderAt does.
	src, release, err := openInput(ctx, src, &cfg)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = release() }()

	support, ok := formatSupportFor(cfg.InputFormat)
	if !ok || support.newRecordDecoder == nil {
		return Result{}, errNoRecordDecoder(cfg.InputFormat)
	}
	dec, err := support.newRecordDecoder(cfg, src)
	if err != nil {
		return Result{}, err
	}
	defer dec.Close()

	newEncoder := func() (formatio.RecordEncoder, error) {
		c, cerr := newConsumer()
		if cerr != nil {
			return nil, cerr
		}
		return consumerEncoder{c: c}, nil
	}
	discard := func(context.Context, []byte) error { return nil }

	return pool.RunRecords(ctx, cfg, dec, newEncoder, cfg.Transformer, discard)
}

// consumerEncoder adapts a RecordConsumer to the pool's RecordEncoder seam: it renders no document,
// so nothing is dispatched.
type consumerEncoder struct {
	c RecordConsumer
}

func (e consumerEncoder) EncodeBatch(batch []record.Record) ([][]byte, error) {
	return nil, e.c.Consume(batch)
}
