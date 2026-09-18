// Package arrowio supplies streamio's Apache Arrow IPC (file format) capabilities: decoding into
// canonical records and encoding records back into Arrow IPC documents. There is no raw-passthrough
// route — see NewRawSource's absence and encoder.go's doc for why.
package arrowio

import (
	"io"
	"streamio/internal/options"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// sharedState is read-only once built (aside from its atomic) and shared by every decode worker.
type sharedState struct {
	reader     *ipc.FileReader
	schema     *arrow.Schema
	nextBatch  atomic.Int64
	numRecords int
	batchSize  int
}

// openShared opens src as an Arrow IPC file and builds the state every decode worker shares.
// src.Reader must stay open for as long as the state is used: record batches are read straight
// from it on demand, not preloaded.
//
// ipc.NewFileReader needs Read+Seek+ReadAt (it locates the footer itself via Seek(0, io.SeekEnd)),
// which src.Reader (a bare io.ReaderAt) doesn't provide on its own; io.NewSectionReader bridges the
// two, since it implements exactly that trio over any io.ReaderAt and a known size.
func openShared(cfg options.Config, src options.Source) (*sharedState, error) {
	sr := io.NewSectionReader(src.Reader, 0, src.Size)

	fr, err := ipc.NewFileReader(sr)
	if err != nil {
		return nil, err
	}

	s := &sharedState{
		reader:     fr,
		schema:     fr.Schema(),
		numRecords: fr.NumRecords(),
		batchSize:  cfg.Run.BatchSize,
	}

	if cfg.Logger != nil {
		cfg.Logger.Printf("arrow file %s: %d record batches, size: %d bytes",
			src.Name, s.numRecords, src.Size)
	}

	return s, nil
}
