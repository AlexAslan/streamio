package jsonio

import (
	"os"
	"streamio/internal/options"
	"sync/atomic"
)

// sharedState is read-only once built (aside from its atomics) and shared by every decode worker.
type sharedState struct {
	f         *os.File
	path      string
	size      int64
	nextChunk atomic.Int64

	readBufferSize int
	batchSize      int
	chunkSize      int
}

// openShared opens the NDJSON file at path and builds the state every decode worker shares. The
// returned file is owned by the caller and must stay open for as long as the state is used: every
// worker reads its chunk straight from it through an io.SectionReader.
func openShared(cfg options.Config, path string) (*os.File, *sharedState, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	if cfg.Logger != nil {
		cfg.Logger.Printf("ndjson file %s, size: %d bytes", path, stat.Size())
	}

	return f, &sharedState{
		f:              f,
		path:           path,
		size:           stat.Size(),
		readBufferSize: cfg.Run.ReadBufferSize,
		batchSize:      cfg.Run.BatchSize,
		chunkSize:      cfg.NDJSON.ChunkSize,
	}, nil
}
