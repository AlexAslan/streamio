package sniff_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/sniff"
)

func TestCompression(t *testing.T) {
	t.Parallel()

	assert.Equal(t, options.CompressionGzip, sniff.Compression([]byte{0x1f, 0x8b, 0x08}))
	assert.Equal(t, options.CompressionZstd, sniff.Compression([]byte{0x28, 0xb5, 0x2f, 0xfd, 0x00}))
	assert.Equal(t, options.CompressionXZ, sniff.Compression([]byte("\xfd7zXZ\x00\x00\x04")))
	assert.Equal(t, options.CompressionLZ4, sniff.Compression([]byte{0x04, 0x22, 0x4d, 0x18, 0x64}))
	assert.Equal(t, options.CompressionSnappy, sniff.Compression([]byte("\xff\x06\x00\x00sNaPpY\x01")))
	assert.Equal(t, options.CompressionBzip2, sniff.Compression([]byte("BZh91AY&SY\x00")))
	assert.Equal(t, options.CompressionNone, sniff.Compression([]byte(`{"a":1}`)))
	// Text that merely begins like the bzip2 magic is not bzip2.
	assert.Equal(t, options.CompressionNone, sniff.Compression([]byte("BZh, the header\n")))
	assert.Equal(t, options.CompressionNone, sniff.Compression([]byte("BZh01AY&SY")))
	assert.Equal(t, options.CompressionNone, sniff.Compression(nil))
}

func TestFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		head string
		hint options.OutputFormat
		want options.OutputFormat
	}{
		{"parquet magic", "PAR1\x00\x01", "", options.FormatParquet},
		{"parquet magic beats csv hint", "PAR1\x00\x01", options.FormatCSV, options.FormatParquet},
		{"arrow magic", "ARROW1\x00\x00", "", options.FormatArrow},
		{"ndjson", "{\"a\":1}\n{\"a\":2}\n", "", options.FormatJSON},
		{"ndjson with BOM and blank lead", "\xef\xbb\xbf\n  {\"a\":1,\"b\":2}\n", "", options.FormatJSON},
		{"ndjson beats csv hint", "{\"a\":1,\"b\":2}\n", options.FormatCSV, options.FormatJSON},
		{"csv", "id,name\n1,ann\n2,bob\n", "", options.FormatCSV},
		{"tsv", "id\tname\n1\tann\n2\tbob\n", "", options.FormatTSV},
		{"csv content beats tsv hint", "id,name\n1,ann\n", options.FormatTSV, options.FormatCSV},
		{"tsv with commas in text", "id\ttext\n1\ta, b, c\n2\td, e\n", "", options.FormatTSV},
		{"quoted delimiter ignored", "id\ttext\n1\t\"a,b\"\n", "", options.FormatTSV},
		{"single column csv uses hint", "id\n1\n2\n", options.FormatCSV, options.FormatCSV},
		{"single column tsv uses hint", "id\n1\n2\n", options.FormatTSV, options.FormatTSV},
		{"empty uses hint", "", options.FormatParquet, options.FormatParquet},
		{"empty without hint is json", "", "", options.FormatJSON},
		{"whitespace only is json", " \n\n", "", options.FormatJSON},
		{"garbage uses json hint", "not json\n", options.FormatJSON, options.FormatJSON},
		{"csv and tsv tie resolves by hint", "a,b\tc\nd,e\tf\n", options.FormatTSV, options.FormatTSV},
		{"csv and tsv tie defaults to csv", "a,b\tc\nd,e\tf\n", "", options.FormatCSV},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := sniff.Format([]byte(tt.head), tt.hint)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFormat_Errors(t *testing.T) {
	t.Parallel()

	_, err := sniff.Format([]byte("PARE\x00"), "")
	require.ErrorIs(t, err, sniff.ErrEncryptedParquet)

	_, err = sniff.Format([]byte("\x00\x01\x02binary"), options.FormatJSON)
	require.ErrorIs(t, err, sniff.ErrUnknownFormat)

	_, err = sniff.Format([]byte("just some prose\n"), "")
	require.ErrorIs(t, err, sniff.ErrUnknownFormat)
}

func TestFormat_TruncatedHeadDropsPartialLine(t *testing.T) {
	t.Parallel()

	// A head cut mid-line must not let the partial last line (one comma short) sink the vote.
	head := make([]byte, 0, sniff.HeadSize)
	for len(head)+len("a,b,c\n") <= sniff.HeadSize-2 {
		head = append(head, "a,b,c\n"...)
	}
	for len(head) < sniff.HeadSize {
		head = append(head, 'x')
	}

	got, err := sniff.Format(head, "")
	require.NoError(t, err)
	assert.Equal(t, options.FormatCSV, got)
}

func TestFormatFromExt(t *testing.T) {
	t.Parallel()

	for name, want := range map[string]options.OutputFormat{
		"a.parquet": options.FormatParquet, "a.CSV": options.FormatCSV, "a.tsv": options.FormatTSV,
		"a.arrow": options.FormatArrow, "a.ndjson": options.FormatJSON, "a.jsonl": options.FormatJSON,
		"a.json": options.FormatJSON,
	} {
		got, ok := sniff.FormatFromExt(name)
		assert.True(t, ok, name)
		assert.Equal(t, want, got, name)
	}

	_, ok := sniff.FormatFromExt("data.bin")
	assert.False(t, ok)
	_, ok = sniff.FormatFromExt("noext")
	assert.False(t, ok)
}

func TestStripCompressionExt(t *testing.T) {
	t.Parallel()

	name, c := sniff.StripCompressionExt("dir/a.csv.GZ")
	assert.Equal(t, "dir/a.csv", name)
	assert.Equal(t, options.CompressionGzip, c)

	name, c = sniff.StripCompressionExt("a.ndjson.zst")
	assert.Equal(t, "a.ndjson", name)
	assert.Equal(t, options.CompressionZstd, c)

	for ext, want := range map[string]options.Compression{
		".bz2": options.CompressionBzip2, ".xz": options.CompressionXZ, ".lz4": options.CompressionLZ4,
		".sz": options.CompressionSnappy, ".snappy": options.CompressionSnappy, ".br": options.CompressionBrotli,
	} {
		name, c = sniff.StripCompressionExt("dir/a.csv" + ext)
		assert.Equal(t, "dir/a.csv", name, ext)
		assert.Equal(t, want, c, ext)
	}

	name, c = sniff.StripCompressionExt("a.csv")
	assert.Equal(t, "a.csv", name)
	assert.Equal(t, options.CompressionNone, c)
}

// FuzzFormat checks Format never panics and only ever returns a supported format or an error.
func FuzzFormat(f *testing.F) {
	for _, seed := range []string{"", "{}", "a,b\n1,2", "a\tb\n1\t2", "PAR1", "ARROW1", "\x00", "\"a,b\n"} {
		f.Add([]byte(seed), "")
		f.Add([]byte(seed), string(options.FormatCSV))
	}

	f.Fuzz(func(t *testing.T, head []byte, hint string) {
		got, err := sniff.Format(head, options.OutputFormat(hint))
		if err != nil {
			return
		}
		switch got {
		case options.FormatJSON, options.FormatParquet, options.FormatCSV, options.FormatTSV, options.FormatArrow:
		default:
			// A hint is echoed back verbatim, so an arbitrary one may legitimately come out.
			require.Equal(t, options.OutputFormat(hint), got)
		}
	})
}

func TestDetect(t *testing.T) {
	t.Parallel()

	// Magic wins over a misleading name, and a name alone is trusted only for magic-less codecs.
	assert.Equal(t, options.CompressionGzip, sniff.Detect([]byte{0x1f, 0x8b}, "a.csv"))
	assert.Equal(t, options.CompressionNone, sniff.Detect([]byte("id,name\n"), "a.csv.gz"))
	assert.Equal(t, options.CompressionBrotli, sniff.Detect([]byte("\x1b\x0a\x00"), "a.csv.br"))
	assert.Equal(t, options.CompressionNone, sniff.Detect([]byte("id,name\n"), "a.csv"))
}
