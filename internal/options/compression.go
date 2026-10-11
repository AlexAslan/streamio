package options

import (
	"errors"
	"fmt"
)

// ErrUnknownCompression is returned for a compression name streamio does not support.
var ErrUnknownCompression = errors.New("streamio: unknown input compression")

// Compression names the stream compression wrapped around an input file.
type Compression string

const (
	// CompressionAuto detects the compression from the file's leading bytes.
	CompressionAuto Compression = ""

	// CompressionNone reads the file as-is, skipping detection.
	CompressionNone Compression = "none"

	// CompressionGzip reads the file as a gzip stream (multi-member streams included).
	CompressionGzip Compression = "gzip"

	// CompressionZstd reads the file as a Zstandard stream.
	CompressionZstd Compression = "zstd"

	// CompressionBzip2 reads the file as a bzip2 stream. Read-only: there is no bzip2 writer.
	CompressionBzip2 Compression = "bzip2"

	// CompressionXZ reads the file as an xz stream.
	CompressionXZ Compression = "xz"

	// CompressionLZ4 reads the file as an LZ4 frame stream.
	CompressionLZ4 Compression = "lz4"

	// CompressionSnappy reads the file as a framed Snappy stream (the .sz / .snappy stream format).
	CompressionSnappy Compression = "snappy"

	// CompressionBrotli reads the file as a Brotli stream. Brotli has no magic number, so it is
	// never auto-detected from contents: only from a .br extension or by naming it explicitly.
	CompressionBrotli Compression = "brotli"
)

// Valid reports whether c is one of the named compressions or the auto-detect zero value.
func (c Compression) Valid() bool {
	switch c {
	case CompressionAuto, CompressionNone, CompressionGzip, CompressionZstd,
		CompressionBzip2, CompressionXZ, CompressionLZ4, CompressionSnappy, CompressionBrotli:
		return true
	default:
		return false
	}
}

// String returns the compression's name, or "auto" for the zero value.
func (c Compression) String() string {
	if c == CompressionAuto {
		return "auto"
	}
	return string(c)
}

// WithInputCompression sets the compression wrapped around the source file explicitly. Omitting
// it, or passing CompressionAuto, detects it from the file's leading bytes.
func WithInputCompression(c Compression) Option {
	return func(o *Config) {
		if o.OptionErr != nil {
			return
		}
		if !c.Valid() {
			o.OptionErr = fmt.Errorf("%w: %q", ErrUnknownCompression, string(c))
			return
		}
		o.InputCompression = c
	}
}

// WithMaxDecompressedSize caps how many bytes a compressed input may expand to, guarding against
// decompression bombs. n<=0 means no cap.
func WithMaxDecompressedSize(n int64) Option {
	return func(o *Config) { o.MaxDecompressedSize = n }
}
