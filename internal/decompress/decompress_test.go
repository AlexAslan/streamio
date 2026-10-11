package decompress_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio/internal/decompress"
	"github.com/AlexAslan/streamio/internal/options"
)

const payload = "id,name\n1,ann\n2,bob\n"

func gz(tb testing.TB, s string) []byte {
	tb.Helper()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, err := w.Write([]byte(s))
	require.NoError(tb, err)
	require.NoError(tb, w.Close())
	return b.Bytes()
}

func zst(tb testing.TB, s string) []byte {
	tb.Helper()
	var b bytes.Buffer
	w, err := zstd.NewWriter(&b)
	require.NoError(tb, err)
	_, err = w.Write([]byte(s))
	require.NoError(tb, err)
	require.NoError(tb, w.Close())
	return b.Bytes()
}

func source(data []byte) options.Source {
	return options.Source{Reader: bytes.NewReader(data), Size: int64(len(data)), Name: "in.csv.gz"}
}

func readAll(tb testing.TB, src options.Source) string {
	tb.Helper()
	got, err := io.ReadAll(io.NewSectionReader(src.Reader, 0, src.Size))
	require.NoError(tb, err)
	return string(got)
}

func TestOpen_InMemory(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		c    options.Compression
		data []byte
	}{
		"gzip": {options.CompressionGzip, gz(t, payload)},
		"zstd": {options.CompressionZstd, zst(t, payload)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			src, closeFn, err := decompress.Open(context.Background(), source(tc.data), tc.c, decompress.Limits{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, closeFn()) })

			assert.Equal(t, int64(len(payload)), src.Size)
			assert.Equal(t, payload, readAll(t, src))
			assert.Equal(t, "in.csv.gz", src.Name)
		})
	}
}

func TestOpen_SpoolsToDiskAndCleansUp(t *testing.T) {
	t.Parallel()

	big := strings.Repeat("abcdefghij\n", 1000)
	src, closeFn, err := decompress.Open(context.Background(), source(gz(t, big)),
		options.CompressionGzip, decompress.Limits{MemoryLimit: 100})
	require.NoError(t, err)

	f, ok := src.Reader.(*os.File)
	require.True(t, ok, "expected a spooled *os.File")
	name := f.Name()
	assert.Equal(t, big, readAll(t, src))

	require.NoError(t, closeFn())
	_, statErr := os.Stat(name)
	assert.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestOpen_GzipMultiMember(t *testing.T) {
	t.Parallel()

	data := append(gz(t, "a\n"), gz(t, "b\n")...)
	src, closeFn, err := decompress.Open(context.Background(), source(data), options.CompressionGzip, decompress.Limits{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })

	assert.Equal(t, "a\nb\n", readAll(t, src))
}

func TestOpen_MaxSize(t *testing.T) {
	t.Parallel()

	_, _, err := decompress.Open(context.Background(), source(gz(t, strings.Repeat("x", 10_000))),
		options.CompressionGzip, decompress.Limits{MaxSize: 100})
	require.ErrorIs(t, err, decompress.ErrTooLarge)
}

func TestOpen_Errors(t *testing.T) {
	t.Parallel()

	_, _, err := decompress.Open(context.Background(), source([]byte("not gzip")),
		options.CompressionGzip, decompress.Limits{})
	require.Error(t, err)

	_, _, err = decompress.Open(context.Background(), source([]byte(payload)),
		options.CompressionNone, decompress.Limits{})
	require.ErrorIs(t, err, decompress.ErrNotCompressed)
}

func TestOpen_ContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := decompress.Open(ctx, source(gz(t, payload)), options.CompressionGzip, decompress.Limits{})
	require.ErrorIs(t, err, context.Canceled)
}
