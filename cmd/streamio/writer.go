package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"

	"github.com/AlexAslan/streamio"
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
	// codec and fileBuf are set only for compressed output: buf feeds codec, which feeds fileBuf,
	// which feeds file.
	codec   io.Closer
	fileBuf *bufio.Writer
	mu      sync.Mutex
}

// outputCompression resolves --out-compression against path's extension: "" or "auto" follows the
// extension (.gz/.gzip, .zst/.zstd), anything else is taken literally.
func outputCompression(flag, path string) (streamio.Compression, error) {
	switch strings.ToLower(flag) {
	case "", "auto":
		lower := strings.ToLower(path)
		switch {
		case strings.HasSuffix(lower, ".gz"), strings.HasSuffix(lower, ".gzip"):
			return streamio.CompressionGzip, nil
		case strings.HasSuffix(lower, ".zst"), strings.HasSuffix(lower, ".zstd"):
			return streamio.CompressionZstd, nil
		default:
			return streamio.CompressionNone, nil
		}
	case "none":
		return streamio.CompressionNone, nil
	case "gzip":
		return streamio.CompressionGzip, nil
	case "zstd":
		return streamio.CompressionZstd, nil
	default:
		return "", fmt.Errorf("%w: %q", errUnknownCompression, flag)
	}
}

// newFileWriter creates (or truncates) path and returns a fileWriter appending to it, compressed
// per --out-compression (or path's extension when that is auto).
func newFileWriter(path, compressionFlag string) (*fileWriter, error) {
	comp, err := outputCompression(compressionFlag, path)
	if err != nil {
		return nil, err
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", path, err)
	}
	if comp == streamio.CompressionNone {
		return &fileWriter{file: f, buf: bufio.NewWriterSize(f, fileWriterBufSize)}, nil
	}

	fileBuf := bufio.NewWriterSize(f, fileWriterBufSize)
	var codec interface {
		io.Writer
		io.Closer
	}
	switch comp {
	case streamio.CompressionGzip:
		codec = gzip.NewWriter(fileBuf)
	case streamio.CompressionZstd:
		zw, zerr := zstd.NewWriter(fileBuf)
		if zerr != nil {
			_ = f.Close()
			return nil, fmt.Errorf("creating zstd writer for %s: %w", path, zerr)
		}
		codec = zw
	default:
		_ = f.Close()
		return nil, fmt.Errorf("%w: %q", errUnknownCompression, string(comp))
	}
	return &fileWriter{
		file:    f,
		buf:     bufio.NewWriterSize(codec, fileWriterBufSize),
		codec:   codec,
		fileBuf: fileBuf,
	}, nil
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
		if w.codec != nil {
			_ = w.codec.Close()
		}
		// Still attempt to close the file even if the flush failed, since a caller that only
		// checks close's error should still see this one rather than the file leaking open.
		_ = w.file.Close()
		return fmt.Errorf("flushing %s: %w", w.file.Name(), err)
	}
	if w.codec != nil {
		// The codec must write its trailer before the file buffer is flushed beneath it.
		if err := w.codec.Close(); err != nil {
			_ = w.file.Close()
			return fmt.Errorf("compressing %s: %w", w.file.Name(), err)
		}
		if err := w.fileBuf.Flush(); err != nil {
			_ = w.file.Close()
			return fmt.Errorf("flushing %s: %w", w.file.Name(), err)
		}
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
