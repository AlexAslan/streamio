package options

// OutputFormat names the representation a caller wants documents delivered in, and also a file's
// own native format.
type OutputFormat string

const (
	// FormatJSON delivers each document as a JSON object — the native format of an NDJSON input,
	// and the default for every other input.
	FormatJSON OutputFormat = "json"

	// FormatParquet delivers Parquet bytes. Against a Parquet input this is the raw-passthrough
	// case: each document is one standalone, self-contained single-row-group Parquet file.
	FormatParquet OutputFormat = "parquet"

	// FormatCSV delivers comma-separated rows.
	FormatCSV OutputFormat = "csv"

	// FormatTSV delivers tab-separated rows.
	FormatTSV OutputFormat = "tsv"
)

// String returns the format's name, or "unspecified" for the zero value. Config.New normalises the
// zero value to FormatJSON, so a Config in hand never reports "unspecified".
func (f OutputFormat) String() string {
	if f == "" {
		return "unspecified"
	}
	return string(f)
}
