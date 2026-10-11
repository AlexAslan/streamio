package streamio_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio"
)

const inputNDJSON = "{\"id\":1,\"name\":\"ann\"}\n{\"id\":2,\"name\":\"bob\"}\n{\"id\":3,\"name\":\"cy\"}\n"

const inputCSV = "id,name\n1,ann\n2,bob\n3,cy\n"

func gzipBytes(tb testing.TB, s string) []byte {
	tb.Helper()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, err := w.Write([]byte(s))
	require.NoError(tb, err)
	require.NoError(tb, w.Close())
	return b.Bytes()
}

func zstdBytes(tb testing.TB, s string) []byte {
	tb.Helper()
	var b bytes.Buffer
	w, err := zstd.NewWriter(&b)
	require.NoError(tb, err)
	_, err = w.Write([]byte(s))
	require.NoError(tb, err)
	require.NoError(tb, w.Close())
	return b.Bytes()
}

func writeTemp(tb testing.TB, name string, data []byte) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), name)
	require.NoError(tb, os.WriteFile(path, data, 0o600))
	return path
}

// collect runs ProcessFile and returns every dispatched document, sorted for stable comparison.
func collect(tb testing.TB, path string, opts ...streamio.Option) []string {
	tb.Helper()

	var mu sync.Mutex
	var docs []string
	_, err := streamio.ProcessFile(context.Background(), path, func(_ context.Context, doc []byte) error {
		mu.Lock()
		defer mu.Unlock()
		docs = append(docs, string(doc))
		return nil
	}, opts...)
	require.NoError(tb, err)
	sort.Strings(docs)
	return docs
}

func TestProcessFile_CompressedInputMatchesPlain(t *testing.T) {
	t.Parallel()

	plain := collect(t, writeTemp(t, "in.ndjson", []byte(inputNDJSON)))
	require.Len(t, plain, 3)

	for name, data := range map[string][]byte{
		"in.ndjson.gz":  gzipBytes(t, inputNDJSON),
		"in.ndjson.zst": zstdBytes(t, inputNDJSON),
		"in.gz":         gzipBytes(t, inputNDJSON), // no inner extension: content decides
		"noext":         zstdBytes(t, inputNDJSON), // no extension at all
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, plain, collect(t, writeTemp(t, name, data)))
		})
	}
}

func TestProcessFile_CompressedCSVToJSON(t *testing.T) {
	t.Parallel()

	opts := []streamio.Option{streamio.WithCSVHasHeader(true), streamio.WithOutputFormat(streamio.FormatJSON)}
	plain := collect(t, writeTemp(t, "in.csv", []byte(inputCSV)), opts...)
	require.Len(t, plain, 3)

	got := collect(t, writeTemp(t, "in.csv.gz", gzipBytes(t, inputCSV)), opts...)
	assert.Equal(t, plain, got)
}

func TestProcessFile_MislabeledFileFollowsContent(t *testing.T) {
	t.Parallel()

	opts := []streamio.Option{streamio.WithCSVHasHeader(true)}
	want := collect(t, writeTemp(t, "in.csv", []byte(inputCSV)), opts...)

	// CSV bytes under an NDJSON name, and under no extension at all.
	assert.Equal(t, want, collect(t, writeTemp(t, "data.ndjson", []byte(inputCSV)), opts...))
	assert.Equal(t, want, collect(t, writeTemp(t, "data", []byte(inputCSV)), opts...))
}

func TestProcessFile_UnknownContentIsAnError(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "mystery", []byte("just some prose, no structure\n\x00\x01"))
	_, err := streamio.ProcessFile(context.Background(), path,
		func(context.Context, []byte) error { return nil })
	require.ErrorIs(t, err, streamio.ErrUnknownFormat)
}

func TestProcessFile_ExplicitInputCompressionAndFormat(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "in.bin", gzipBytes(t, inputNDJSON))
	got := collect(t, path,
		streamio.WithInputCompression(streamio.CompressionGzip),
		streamio.WithInputFormat(streamio.FormatJSON))
	assert.Len(t, got, 3)
}

func TestProcessFile_WrongExplicitCompressionFails(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "in.ndjson", []byte(inputNDJSON))
	_, err := streamio.ProcessFile(context.Background(), path,
		func(context.Context, []byte) error { return nil },
		streamio.WithInputCompression(streamio.CompressionGzip))
	require.Error(t, err)
}

func TestProcessFile_InvalidCompressionOption(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "in.ndjson", []byte(inputNDJSON))
	_, err := streamio.ProcessFile(context.Background(), path,
		func(context.Context, []byte) error { return nil },
		streamio.WithInputCompression("brotli"))
	require.Error(t, err)
}

func TestProcessFile_MaxDecompressedSize(t *testing.T) {
	t.Parallel()

	big := strings.Repeat("{\"a\":1}\n", 10_000)
	path := writeTemp(t, "big.ndjson.gz", gzipBytes(t, big))
	_, err := streamio.ProcessFile(context.Background(), path,
		func(context.Context, []byte) error { return nil },
		streamio.WithMaxDecompressedSize(1024))
	require.ErrorIs(t, err, streamio.ErrDecompressedTooLarge)
}

func TestDetectFormat(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		want streamio.Format
		data []byte
	}{
		"ndjson":        {streamio.FormatJSON, []byte(inputNDJSON)},
		"csv":           {streamio.FormatCSV, []byte(inputCSV)},
		"tsv":           {streamio.FormatTSV, []byte("a\tb\n1\t2\n")},
		"gzipped csv":   {streamio.FormatCSV, gzipBytes(t, inputCSV)},
		"zstd'd ndjson": {streamio.FormatJSON, zstdBytes(t, inputNDJSON)},
		"parquet":       {streamio.FormatParquet, []byte("PAR1\x00\x00PAR1")},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := streamio.DetectFormat(streamio.Source{
				Reader: bytes.NewReader(tc.data), Size: int64(len(tc.data)), Name: "x",
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDetectInputFormat_IgnoresCompressionExtension(t *testing.T) {
	t.Parallel()

	assert.Equal(t, streamio.FormatCSV, streamio.DetectInputFormat("a.csv.gz"))
	assert.Equal(t, streamio.FormatParquet, streamio.DetectInputFormat("a.parquet.zst"))
	assert.Equal(t, streamio.FormatJSON, streamio.DetectInputFormat("a.unknown"))
}
