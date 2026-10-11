// Package decompress turns a gzip- or zstd-compressed source into the sized, randomly-addressable
// options.Source every format's decoder needs.
package decompress

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"

	"github.com/AlexAslan/streamio/internal/options"
)

// DefaultMemoryLimit is how many decompressed bytes Open keeps in memory before spooling to disk.
const DefaultMemoryLimit = 64 << 20

// ErrTooLarge is returned when a compressed input expands past the configured size cap.
var ErrTooLarge = errors.New("streamio: decompressed input exceeds the configured size limit")

// ErrNotCompressed is returned when asked to decompress with CompressionNone or CompressionAuto.
var ErrNotCompressed = errors.New("streamio: no decompression codec selected")

// Limits bounds what Open may hold. A zero field means the default (MemoryLimit) or no cap
// (MaxSize).
type Limits struct {
	// MemoryLimit is the decompressed size above which Open spools to a temp file.
	MemoryLimit int64
	// MaxSize is the decompressed size above which Open fails with ErrTooLarge.
	MaxSize int64
}

// NewReader wraps r in a streaming decoder for c. The caller closes the returned reader.
func NewReader(r io.Reader, c options.Compression) (io.ReadCloser, error) {
	switch c {
	case options.CompressionGzip:
		zr, err := gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("opening gzip stream: %w", err)
		}
		return zr, nil
	case options.CompressionZstd:
		zr, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, fmt.Errorf("opening zstd stream: %w", err)
		}
		return zstdReadCloser{zr}, nil
	case options.CompressionAuto, options.CompressionNone:
		return nil, ErrNotCompressed
	default:
		return nil, fmt.Errorf("%w: %q", ErrNotCompressed, string(c))
	}
}

// zstdReadCloser adapts zstd.Decoder, whose Close returns nothing, to io.ReadCloser.
type zstdReadCloser struct{ *zstd.Decoder }

func (z zstdReadCloser) Close() error {
	z.Decoder.Close()
	return nil
}

// Open fully decompresses src and returns a Source over the result, plus a function releasing it.
//
// Small results stay in memory; larger ones spool to a temp file, which the returned function
// removes. Either way every decoder keeps its random access, so chunked parallel decoding is
// unaffected by the input having been compressed.
func Open(
	ctx context.Context,
	src options.Source,
	c options.Compression,
	lim Limits,
) (options.Source, func() error, error) {
	zr, err := NewReader(io.NewSectionReader(src.Reader, 0, src.Size), c)
	if err != nil {
		return options.Source{}, nil, fmt.Errorf("%s: %w", src.Name, err)
	}
	defer zr.Close()

	memLimit := lim.MemoryLimit
	if memLimit <= 0 {
		memLimit = DefaultMemoryLimit
	}

	var in io.Reader = &ctxReader{ctx: ctx, r: zr}
	if lim.MaxSize > 0 {
		in = &capReader{r: in, left: lim.MaxSize}
	}

	// One byte past the limit tells "fits in memory" from "needs spooling" without a second read.
	var mem bytes.Buffer
	if _, copyErr := io.Copy(&mem, io.LimitReader(in, memLimit+1)); copyErr != nil {
		return options.Source{}, nil, fmt.Errorf("decompressing %s: %w", src.Name, copyErr)
	}
	if int64(mem.Len()) <= memLimit {
		return options.Source{
			Reader: bytes.NewReader(mem.Bytes()),
			Size:   int64(mem.Len()),
			Name:   src.Name,
		}, func() error { return nil }, nil
	}

	return spool(&mem, in, src.Name)
}

// spool writes head followed by the rest of in to a temp file and returns it as a Source.
func spool(head *bytes.Buffer, in io.Reader, name string) (options.Source, func() error, error) {
	f, err := os.CreateTemp("", "streamio-decompressed-*")
	if err != nil {
		return options.Source{}, nil, fmt.Errorf("creating spool file for %s: %w", name, err)
	}
	cleanup := func() error {
		closeErr := f.Close()
		if rmErr := os.Remove(f.Name()); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			return rmErr
		}
		return closeErr
	}

	n, err := io.Copy(f, io.MultiReader(head, in))
	if err != nil {
		_ = cleanup()
		return options.Source{}, nil, fmt.Errorf("decompressing %s: %w", name, err)
	}
	return options.Source{Reader: f, Size: n, Name: name}, cleanup, nil
}

// ctxReader fails reads once ctx is cancelled, so a huge decompression stops promptly.
type ctxReader struct {
	ctx context.Context //nolint:containedctx // A reader has no other way to see cancellation.
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// capReader fails with ErrTooLarge once more than left bytes have been read.
type capReader struct {
	r    io.Reader
	left int64
}

func (c *capReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.left -= int64(n)
	if c.left < 0 {
		return n, ErrTooLarge
	}
	return n, err
}
