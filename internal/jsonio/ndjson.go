package jsonio

import (
	"io"
	"sync/atomic"

	"github.com/AlexAslan/streamio/internal/options"
)

// sharedState is read-only once built (aside from its atomics) and shared by every decode worker.
type sharedState struct {
	f         io.ReaderAt
	name      string
	size      int64
	nextChunk atomic.Int64

	readBufferSize int
	batchSize      int
	chunkSize      int
}

// openShared builds the state every decode worker shares, reading directly from src: every worker
// reads its chunk straight from src.Reader through an io.SectionReader.
func openShared(cfg options.Config, src options.Source) *sharedState {
	if cfg.Logger != nil {
		cfg.Logger.Printf("ndjson file %s, size: %d bytes", src.Name, src.Size)
	}

	return &sharedState{
		f:              src.Reader,
		name:           src.Name,
		size:           src.Size,
		readBufferSize: cfg.Run.ReadBufferSize,
		batchSize:      cfg.Run.BatchSize,
		chunkSize:      cfg.NDJSON.ChunkSize,
	}
}
