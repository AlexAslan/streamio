package csvio

import (
	"errors"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

// errEncodeAfterFinalize is returned by EncodeBatch once Finalize has already run, mirroring
// parquetio's streamingEncoder guard against writing into a writer the caller has already closed
// out.
var errEncodeAfterFinalize = errors.New("csv: EncodeBatch called after Finalize")

// streamingEncoder renders every batch it is given as more rows of a single CSV/TSV document
// spanning the whole run, instead of one complete document per batch. It implements
// formatio.FinalizableRecordEncoder.
//
// The header is derived once, from the first batch; every later batch's records must match it.
type streamingEncoder struct {
	rw        *rowWriter
	header    []string
	rowBuf    rowScratch
	delimiter rune
	hasHeader bool
	finalized bool
}

// NewStreamingEncoder returns a formatio.FinalizableRecordEncoder producing one continuous CSV/TSV
// document across every batch in the run.
//
//nolint:ireturn // formatio.RecordEncoder is the constructor type streamio's format registry stores.
func NewStreamingEncoder(cfg options.Config) (formatio.RecordEncoder, error) {
	delimiter := delimiterFor(cfg, cfg.OutputFormat)
	return &streamingEncoder{
		delimiter: delimiter,
		hasHeader: cfg.CSV.HasHeader,
		rw:        newRowWriter(delimiter),
	}, nil
}

// EncodeBatch derives the header from the first batch if needed, writing it ahead of that batch's
// own rows, and renders every later batch as rows alone. An empty batch returns no document.
func (e *streamingEncoder) EncodeBatch(batch []record.Record) ([][]byte, error) {
	if e.finalized {
		return nil, errEncodeAfterFinalize
	}
	if len(batch) == 0 {
		return nil, nil
	}

	e.rw.reset()

	if e.header == nil {
		header, err := deriveHeader(batch[0])
		if err != nil {
			return nil, err
		}
		e.header = header
		if e.hasHeader {
			if err = e.rw.writeRow(headerFields(e.header)); err != nil {
				return nil, err
			}
		}
	}

	if err := writeRows(e.rw, e.header, batch, &e.rowBuf); err != nil {
		return nil, err
	}

	doc := e.rw.flush()
	if len(doc) == 0 {
		return nil, nil
	}
	return [][]byte{cloneBytes(doc)}, nil
}

// Finalize marks the run complete. A CSV/TSV document needs no trailing bytes beyond its rows.
func (e *streamingEncoder) Finalize() ([]byte, error) {
	e.finalized = true
	return nil, nil
}
