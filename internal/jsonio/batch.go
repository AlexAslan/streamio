package jsonio

import (
	"context"
	"streamio/internal/pool"
	"time"
)

// recordBatcher is one decode worker's private line-accumulation scratch, reused across every
// chunk it claims. Each worker has its own — there is no shared mutable state between workers.
type recordBatcher struct {
	readStart time.Time
	batch     [][]byte
}

func (s *sharedState) newRecordBatcher() *recordBatcher {
	return &recordBatcher{
		batch:     make([][]byte, 0, s.batchSize),
		readStart: time.Now(),
	}
}

// addLine appends line to rb's batch, flushing automatically once it reaches cfg.BatchSize.
func (s *sharedState) addLine(
	ctx context.Context,
	rb *recordBatcher,
	line []byte,
	out chan<- [][]byte,
	stats *pool.DecodeStats,
) error {
	rb.batch = append(rb.batch, line)
	if len(rb.batch) == s.batchSize {
		return s.flushLines(ctx, rb, out, stats)
	}
	return nil
}

// flushLines sends any lines accumulated in rb to out and resets its batch, recording elapsed
// decode time since the previous flush. A no-op when the batch is empty.
func (s *sharedState) flushLines(
	ctx context.Context,
	rb *recordBatcher,
	out chan<- [][]byte,
	stats *pool.DecodeStats,
) error {
	stats.ReadNs.Add(int64(time.Since(rb.readStart)))
	defer func() { rb.readStart = time.Now() }()

	if len(rb.batch) == 0 {
		return nil
	}
	sendBatch := rb.batch
	stats.Records.Add(int64(len(sendBatch)))
	rb.batch = make([][]byte, 0, s.batchSize)

	select {
	case out <- sendBatch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
