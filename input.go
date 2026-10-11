package streamio

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/AlexAslan/streamio/internal/decompress"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/sniff"
)

// Compression names the stream compression wrapped around an input file.
type Compression = options.Compression

const (
	// CompressionAuto detects the compression from the file's leading bytes.
	CompressionAuto = options.CompressionAuto
	// CompressionNone reads the file as-is.
	CompressionNone = options.CompressionNone
	// CompressionGzip reads the file as a gzip stream.
	CompressionGzip = options.CompressionGzip
	// CompressionZstd reads the file as a Zstandard stream.
	CompressionZstd = options.CompressionZstd
)

// ErrUnknownFormat is returned when an input's contents match no supported format and its
// extension offers no hint either.
var ErrUnknownFormat = sniff.ErrUnknownFormat

// ErrDecompressedTooLarge is returned when a compressed input expands past WithMaxDecompressedSize.
var ErrDecompressedTooLarge = decompress.ErrTooLarge

// DetectFormat returns src's format from its contents, seeing through gzip and zstd compression.
// The name's extension is only consulted when the contents leave the answer open.
func DetectFormat(src Source) (Format, error) {
	return sniffSource(src)
}

// sniffSource reads src's leading bytes (decompressing only that much) and reports its format.
func sniffSource(src Source) (Format, error) {
	head, err := readHead(src.Reader, src.Size)
	if err != nil {
		return "", err
	}

	comp := sniff.Compression(head)

	name := src.Name
	if comp != CompressionNone {
		zr, zerr := decompress.NewReader(io.NewSectionReader(src.Reader, 0, src.Size), comp)
		if zerr != nil {
			return "", fmt.Errorf("%s: %w", src.Name, zerr)
		}
		defer zr.Close()

		head = make([]byte, sniff.HeadSize)
		n, rerr := io.ReadFull(zr, head)
		if rerr != nil && !errors.Is(rerr, io.EOF) && !errors.Is(rerr, io.ErrUnexpectedEOF) {
			return "", fmt.Errorf("decompressing %s: %w", src.Name, rerr)
		}
		head = head[:n]
		name, _ = sniff.StripCompressionExt(name)
	}

	format, err := sniffFormat(head, name)
	if err != nil {
		return "", fmt.Errorf("%s: %w", src.Name, err)
	}
	return format, nil
}

// sniffFormat decides head's format, using name's extension only as a tiebreaker.
func sniffFormat(head []byte, name string) (Format, error) {
	hint, _ := sniff.FormatFromExt(name)
	return sniff.Format(head, hint)
}

// readHead reads up to sniff.HeadSize bytes from the start of r.
func readHead(r io.ReaderAt, size int64) ([]byte, error) {
	head := make([]byte, min(size, sniff.HeadSize))
	n, err := r.ReadAt(head, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("reading input header: %w", err)
	}
	return head[:n], nil
}

// openInput resolves src's compression and format into cfg, decompressing src when needed. The
// returned function releases whatever that took and is always safe to call.
func openInput(ctx context.Context, src Source, cfg *options.Config) (Source, func() error, error) {
	noop := func() error { return nil }

	head, err := readHead(src.Reader, src.Size)
	if err != nil {
		return Source{}, noop, err
	}
	comp := cfg.InputCompression
	if comp == CompressionAuto {
		comp = sniff.Compression(head)
	}

	release := noop
	name := src.Name
	if comp != CompressionNone {
		dsrc, rel, derr := decompress.Open(ctx, src, comp, decompress.Limits{MaxSize: cfg.MaxDecompressedSize})
		if derr != nil {
			return Source{}, noop, derr
		}
		name, _ = sniff.StripCompressionExt(src.Name)
		dsrc.Name = name
		src, release = dsrc, rel
	}

	if cfg.InputFormat != "" {
		return src, release, nil
	}

	head, err = readHead(src.Reader, src.Size)
	if err != nil {
		_ = release()
		return Source{}, noop, err
	}
	format, err := sniffFormat(head, name)
	if err != nil {
		_ = release()
		return Source{}, noop, fmt.Errorf("%s: %w", src.Name, err)
	}
	if hint, ok := sniff.FormatFromExt(name); ok && hint != format && cfg.Logger != nil {
		cfg.Logger.Printf("streamio: %s has a %s extension but %s contents; reading it as %s",
			src.Name, hint, format, format)
	}
	cfg.InputFormat = format
	return src, release, nil
}
