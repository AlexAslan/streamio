package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
)

// fileWriterBufSize is the buffered writer's size: large enough that a 20,000-row NDJSON fixture
// (~40,000 one- or two-chunk writes) needs only a handful of underlying syscalls instead of one
// per chunk — see fileWriter's own doc for why that syscall count otherwise dominates NDJSON
// output's per-row cost.
const fileWriterBufSize = 256 * 1024

// fileWriter is a streamio.DocumentHandler that appends every dispatched document straight
// to a single output file, under a mutex since decode workers may call the handler concurrently.
//
// It writes documents back to back with no separator, which is correct for both output shapes the
// CLI produces: NDJSON documents already carry their own trailing newline (added by newlineWriter,
// below), and a Parquet run in single-file mode dispatches its row groups and footer as one ordered
// sequence meant to be concatenated exactly this way.
//
// Writes go through a buffered writer rather than straight to the file: one call per document (two
// for NDJSON, its own bytes plus a newline) means one raw Write syscall per call without buffering,
// which measurably dominates NDJSON output's per-row cost at any real row count (see the CLI
// benchmark's README section) — buffering turns that into one syscall roughly every
// fileWriterBufSize bytes instead. close flushes the buffer before closing the file, so nothing
// written is lost if the caller forgets to check close's error (they shouldn't, but the buffer
// alone would otherwise silently drop unflushed bytes on Close).
type fileWriter struct {
	file *os.File
	buf  *bufio.Writer
	mu   sync.Mutex
}

// newFileWriter creates (or truncates) path and returns a fileWriter appending to it.
func newFileWriter(path string) (*fileWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", path, err)
	}
	return &fileWriter{file: f, buf: bufio.NewWriterSize(f, fileWriterBufSize)}, nil
}

// write is the streamio.DocumentHandler.
func (w *fileWriter) write(_ context.Context, doc []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeLocked(doc)
}

// close flushes any buffered bytes and closes the underlying file.
func (w *fileWriter) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		// Still attempt to close the file even if the flush failed, since a caller that only
		// checks close's error should still see this one rather than the file leaking open.
		_ = w.file.Close()
		return fmt.Errorf("flushing %s: %w", w.file.Name(), err)
	}
	return w.file.Close()
}

// writeLocked writes every chunk to the buffered writer in order, under the caller's held lock.
// Both chunks of a newlineWriter's document+separator pair go through one lock acquisition, not
// two, so a concurrent writer can never interleave a whole document between them.
func (w *fileWriter) writeLocked(chunks ...[]byte) error {
	for _, chunk := range chunks {
		if _, err := w.buf.Write(chunk); err != nil {
			return fmt.Errorf("writing to %s: %w", w.file.Name(), err)
		}
	}
	return nil
}

// newlineWriter wraps a fileWriter, appending a trailing newline after every document — the shape
// NDJSON output needs, since ProcessFile hands the handler one row's bytes at a time with no
// separator of its own.
type newlineWriter struct {
	*fileWriter
}

func (w newlineWriter) write(_ context.Context, doc []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeLocked(doc, []byte{'\n'})
}
