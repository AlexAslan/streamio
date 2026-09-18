# Configuration (`streamio.Option`)

| Option | Default | Effect |
|---|---|---|
| `WithInputFormat(f)` | extension detection | The source file's native format. Use this for extensionless files or callers that already know the format. |
| `WithOutputFormat(f)` | `FormatJSON` | The format documents are delivered in. Requesting the input's own native format selects raw passthrough. |
| `WithTransforms(rules...)` | none | Applies declarative rename/drop path transforms on cross-format routes. Dotted paths address nested map entries. Raw passthrough never decodes records, so it never transforms. A `DropPath` combined with a rule under it is rejected at construction time, since the drop would otherwise make that rule permanently dead. |
| `WithParallelWorkers(n)` | `1` | Decode workers for NDJSON and for Parquet raw passthrough; upper bound on the decoders a splittable `RecordDecoder` hands out on the generic path (see [architecture.md](architecture.md)). Also sizes the dispatch pool. |
| `WithChunkSize(n)` | 32 MiB | NDJSON's fixed byte-range chunk size. |
| `WithReadBufferSize(n)` | 32 MiB | NDJSON's `bufio.Reader` size; capped to `ChunkSize` so a small chunk doesn't pay for a full-size buffer per worker. |
| `WithBatchSize(n)` | 512 | Documents per send to a dispatch worker. On the generic path it also sizes the `ReadRows` fetch, and for Parquet *output* it is the number of rows per synthesized Parquet file; ignored by Parquet raw passthrough, which always sends one row group per item. |
| `WithMaxOpenReaders(n)` | `Workers` | Caps simultaneously-open Parquet row-group readers, for either read route (see [formats.md](formats.md)). Ignored by NDJSON. |
| `WithLogger(l)` | none | Receives a one-line summary (`row/line count, size`) when `ProcessFile` starts. |
| `WithMaxRowErrors(n, onSkip)` | `0` | Caps how many rows may fail to decode and be skipped before the run fails, mirroring BigQuery's load-job `max_bad_records`: `0` (the default) fails immediately on the first row error; a positive `n` skips up to `n` bad rows before failing on the `(n+1)`th. The limit is shared across every decode worker, not per-worker, so it means the same thing regardless of `--workers`. Only takes effect for NDJSON and CSV/TSV field errors on the generic decode/encode path — a CSV/TSV *syntax* error (e.g. an unterminated quoted field) and every Parquet decode error stay fail-fast regardless, since neither leaves the decoder at a well-defined "start of the next row" to resume from. `onSkip`, if non-nil, is called once per skipped row with the error that row raised. Exceeding the limit fails the run with a `*TooManyRowErrorsError` wrapping every row error collected up to and including the one that exceeded it. |

`n <= 0` for any numeric option means "use the default", identical to omitting it (for `WithMaxRowErrors` specifically, `n <= 0` means fail-fast, matching BigQuery's own default). Passing the zero `Format` (or omitting `WithOutputFormat` entirely) means `FormatJSON`.

`ProcessFile` returns `streamio.Result{Stats: streamio.Stats{...}}`. `RowsRead` counts logical input
rows; `DocumentsDispatched` counts successful handler calls. They differ on Parquet's raw path (one
document is a row group) and whenever an encoder renders a whole batch as one document. `RowsSkipped`
counts rows `WithMaxRowErrors` allowed to be skipped, always zero under the default. `Result` is
still returned alongside the error when the run fails from exceeding the limit, so `RowsSkipped` is
populated on that path too. `ReadDuration` and `DispatchDuration` are summed across every worker, so
they can exceed wall-clock time; that's expected for a concurrent run and is what makes
decode-vs-dispatch time attributable at all.

## Reading from something other than a file path

`ProcessFile(ctx, path, handler, opts...)` is a thin wrapper: it opens `path`, stats it, and calls
`streamio.ProcessReaderAt(ctx, streamio.Source{Reader, Name, Size}, handler, opts...)` — the actual
primitive every format's chunked-parallel decoder is built on. `Source.Reader` only needs to satisfy
`io.ReaderAt` over the declared `Size`, so anything with random-access reads (an in-memory buffer via
`bytes.NewReader`, a range-read-capable remote object, a `*os.File` opened by the caller instead of by
`ProcessFile`) can be decoded exactly like a real file, with identical throughput — every internal
decoder already only ever used `*os.File` through this same `io.ReaderAt` interface, so generalizing
the public entry point cost nothing measurable (verified by benchmark). `Source.Name` labels the
input in diagnostics and error text and need not be a real path.

There is no corresponding entry point for a non-seekable `io.Reader` (a pipe, stdin, a plain network
stream): the chunked-parallel design fundamentally requires random access to claim and read byte
ranges concurrently, and Parquet decode specifically requires reading the footer at the end of the
stream before any row can be decoded at all — neither is possible without buffering the whole input
first, which isn't something `ProcessReaderAt` does on a caller's behalf.
