# Configuration (`streamio.Option`)

| Option | Default | Effect |
|---|---|---|
| `WithInputFormat(f)` | extension detection | Source file's native format. |
| `WithOutputFormat(f)` | `FormatJSON` | Format documents are delivered in. Matching the input's native format selects raw passthrough where available. |
| `WithTransforms(rules...)` | none | Declarative rename/drop path transforms, generic path only. Dotted paths address nested map entries. |
| `WithParallelWorkers(n)` | `1` | Decode workers; also sizes the dispatch pool. |
| `WithChunkSize(n)` | 32 MiB | NDJSON/CSV/TSV byte-range chunk size. |
| `WithReadBufferSize(n)` | 32 MiB | NDJSON/CSV/TSV `bufio.Reader` size, capped to `ChunkSize`. |
| `WithBatchSize(n)` | 512 | Documents per dispatch send. For Parquet/CSV/TSV/Arrow output, rows per synthesized document. |
| `WithMaxOpenReaders(n)` | `Workers` | Caps simultaneously-open Parquet row-group readers. |
| `WithCSVDelimiter(r)` | `,` for CSV, `\t` for TSV | Field delimiter for CSV/TSV input and output. |
| `WithCSVHasHeader(b)` | `false` | Whether a CSV/TSV file's first row names its columns. |
| `WithSingleFileOutput(b)` | `false` | One continuous output document for the whole run instead of one per batch (Parquet, CSV/TSV, Arrow). |
| `WithLogger(l)` | none | Receives a one-line summary when `ProcessFile` starts. |
| `WithMaxRowErrors(n, onSkip)` | `0` | Rows allowed to fail and be skipped before the run fails (BigQuery-style `max_bad_records`). `0` fails on the first error. Applies to NDJSON and CSV/TSV field errors on the generic path only; CSV/TSV syntax errors and Parquet/Arrow decode errors are always fatal. `onSkip`, if set, is called per skipped row. |

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
