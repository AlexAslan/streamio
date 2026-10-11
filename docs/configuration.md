# Configuration (`streamio.Option`)

| Option | Default | Effect |
|---|---|---|
| `WithInputFormat(f)` | content sniffing | Source file's native format. |
| `WithInputCompression(c)` | auto-detected | `CompressionNone`, `CompressionGzip`, `CompressionZstd`, `CompressionBzip2`, `CompressionXZ`, `CompressionLZ4`, `CompressionSnappy` or `CompressionBrotli`; pins the compression instead of sniffing it. Brotli can only be selected this way or by a `.br` extension. |
| `WithMaxDecompressedSize(n)` | no cap | Fails with `ErrDecompressedTooLarge` if a compressed input expands past `n` bytes. |
| `WithOutputFormat(f)` | `FormatJSON` | Format documents are delivered in. Matching the input's native format selects raw passthrough where available, unless a transform is configured. |
| `WithTransforms(rules...)` | none | Declarative rename/drop path transforms. Configuring any makes a same-format conversion decode and re-encode instead of streaming raw bytes, so the rules always apply. Dotted paths address nested map entries. |
| `WithParallelWorkers(n)` | `1` | Decode workers; also sizes the dispatch pool. |
| `WithChunkSize(n)` | 32 MiB | NDJSON/CSV/TSV byte-range chunk size. |
| `WithReadBufferSize(n)` | 32 MiB | NDJSON/CSV/TSV `bufio.Reader` size, capped to `ChunkSize`. |
| `WithBatchSize(n)` | 512 | Documents per dispatch send. For Parquet/CSV/TSV/Arrow output, rows per synthesized document. |
| `WithMaxOpenReaders(n)` | `Workers` | Caps simultaneously-open Parquet row-group readers. |
| `WithCSVDelimiter(r)` | `,` for CSV, `\t` for TSV | Field delimiter for CSV/TSV input and output. |
| `WithCSVHasHeader(b)` | `false` | Whether a CSV/TSV file's first row names its columns. |
| `WithSingleFileOutput(b)` | `false` | One continuous output document for the whole run instead of one per batch (Parquet, CSV/TSV, Arrow). |
| `WithSampling(spec)` | none | Keep a deterministic subset by hash of a key: a rate, a range, or per-stratum rates (see [sampling.md](sampling.md)). Forces the record path. |
| `WithRowFilter(f)` / `WithRecordTransform(f)` | none | Custom per-record stages built from per-worker factories (see [stages.md](stages.md)). Force the record path. |
| `WithLogger(l)` | none | Receives a one-line summary when `ProcessFile` starts. |
| `WithMaxRowErrors(n, onSkip)` | `0` | Rows allowed to fail and be skipped before the run fails (BigQuery-style `max_bad_records`). `0` fails on the first error. Applies to NDJSON and CSV/TSV field errors on the generic path only; CSV/TSV syntax errors and Parquet/Arrow decode errors are always fatal. `onSkip`, if set, is called per skipped row. |

## Profiling (`ProfileFile` / `ProfileReaderAt`)

These read the same input options as `ProcessFile` (`WithInputFormat`, `WithInputCompression`,
`WithCSVHasHeader`, `WithParallelWorkers`, `WithTransforms`, `WithMaxRowErrors`, ...) and ignore the
output options.

| Option | Default | Effect |
|---|---|---|
| `WithProfileTopN(n)` | `10` | Most frequent values kept per attribute. |
| `WithProfileMaxDistinct(n)` | `100000` | Distinct values each decode worker tracks exactly per attribute. Past it the attribute switches to approximate heavy-hitter counting: `DistinctExact` is `false`, `Distinct` is a lower bound, and top-value counts are estimates bounded by `ValueCount.MaxError`. Non-empty counts stay exact. |
| `WithProfileColumns(names...)` | all | Restrict the profile to these attributes and everything nested under them (`attributes` also selects `attributes.k`). |

What a profile counts:

- An attribute is *empty* when it is missing, null, a zero-length string, or an empty list.
  Whitespace is not empty, and neither is the text `NULL` in a CSV (CSV has no null).
- Nested map entries are named by dotted path. An attribute repeated within one record counts once,
  from its first occurrence, so non-empty never exceeds the record count.
- A value counts under the text it renders as, using the converters' own formatting: the number
  `1`, the float `1.0` and the string `"1"` are one value, a date renders as `2024-01-01`, and a
  list counts under the single value `[list]`.
- Columns appear in first-seen order. NDJSON whose keys differ between records can reorder
  between runs when more than one worker is used.
- At most 4096 attributes are reported; `ColumnsTruncated` says when more existed.

`n <= 0` means "use the default" for any numeric option. Zero `Format` means `FormatJSON`.

`ProcessFile` returns `streamio.Result{Stats: streamio.Stats{...}}`: `RowsRead`, `DocumentsDispatched`,
`RowsSkipped`, `ReadDuration`, `DispatchDuration` (the last two summed across workers).

## Reading from something other than a file path

`ProcessFile(ctx, path, handler, opts...)` wraps `ProcessReaderAt(ctx, streamio.Source{Reader, Name,
Size}, handler, opts...)`. `Source.Reader` only needs `io.ReaderAt` over the declared `Size`, so an
in-memory buffer (`bytes.NewReader`), a range-read-capable remote object, or an already-open
`*os.File` all work identically to `ProcessFile`.

There is no entry point for a non-seekable `io.Reader` (a pipe, stdin): the chunked-parallel design
requires random access, and Parquet/Arrow decode requires reading the footer before any row can be
decoded.
