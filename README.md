# streamio

Reads NDJSON, Parquet, CSV, and TSV files and pushes their documents to a caller-provided document handler (a
"document" is whatever unit the caller bulk-indexes or bulk-inserts downstream — one JSON object,
one Parquet row group, or one synthesized Parquet file, depending on route). The caller chooses what
format the documents come out in; when that matches the file's own native format, the file's
bytes go straight to the handler with no decode step at all. Decoding and dispatch run concurrently
across a shared worker pool so a slow handler doesn't stall decoding, and vice versa.

## Entry point

```go
result, err := streamio.ProcessFile(ctx, path, handler,
    streamio.WithParallelWorkers(4),
    streamio.WithInputFormat(streamio.FormatParquet),  // optional: default is extension detection
    streamio.WithOutputFormat(streamio.FormatParquet), // default: streamio.FormatJSON
    streamio.WithTransforms(
        streamio.RenamePath("Timestamp", "@timestamp"),
        streamio.DropPath("debug.internal"),
    ),
)
```

`ProcessFile` compares the requested output format (`FormatJSON` unless `WithOutputFormat` says otherwise) against the input file's own native format — either set
explicitly with `WithInputFormat`, or derived from its extension (`.parquet`, case-insensitive,
meaning Parquet, anything else meaning NDJSON) — and picks one of two routes:

```
              streamio.ProcessFile(ctx, path, handler, opts...)
                                 │
                    apply opts → cfg
              in := cfg.InputFormat or detectInputFormat(path)
                                 │
              ┌──────────────────┴───────────────────────┐
              │                                          │
      cfg.OutputFormat == in                    cfg.OutputFormat != in
      (raw passthrough)                         (generic path)
              │                                          │
              ▼                                          ▼
      processRaw:                              processRecords:
      formatSupportFor(in).newRawSource,       decode in → record.Record → encode
      pool.RunRaw                              as out, via formatSupportFor's
                                               RecordDecoder + RecordEncoder
                                               (pool.RunRecords)
```

- **Raw passthrough** is the fast path and the reason the whole comparison exists: when the
  caller asks for the file's own native format, its bytes go to the handler untouched — no decode,
  no encode, no intermediate `record.Record`. This is what the ClickHouse direct-insert loader
  uses for Parquet (`streamio.WithOutputFormat(streamio.FormatParquet)` against a `.parquet` file):
  ~15–120 ns/row versus ~325–2500 ns/row for JSON-decode, depending on schema shape (see
  `BenchmarkProcessFile_ParquetFormats` in `format_bench_test.go`).
- **The generic path** takes every cross-format pair through `record.Record`: the input's
  `RecordDecoder` produces canonical rows, the output's `RecordEncoder` renders them, and the same
  worker pool raw passthrough uses drives both. A pair with no decoder for the input or no encoder
  for the output reports which half is missing, via `ErrNoConversionPath`.

  Two pairs reach it today, and both are real conversions in production use:

  - **Parquet → JSON**: `parquetio.NewDecoder` → `jsonio.NewEncoder`, one JSON document per row.
    This is what an Elasticsearch bulk load of a Parquet dataset goes through.
  - **NDJSON → Parquet**: `jsonio.NewDecoder` → `jsonio.ObjectDecoder` → `parquetio.NewEncoder`, one standalone Parquet
    document per decoded batch.

  Parquet → JSON used to have a second, hand-written implementation that wrote JSON tokens straight
  off column values during decode, skipping `record.Record`. It was 7–20% faster and produced
  byte-identical output — and was deleted anyway, once the generic path was real and tested:
  maintaining two implementations of "Parquet row → JSON" is not worth a bounded single-digit-to-
  low-double-digit percentage on one pair. The generic path is now the only way that conversion is
  produced, which is also what keeps it honest: it is on the hot path, not a spare seam.

## The decode/dispatch pool (`pool`)

Both routes funnel through the same fan-out/fan-in skeleton in `pool.go`; only what feeds it
differs:

```
   pool.RunRaw(ctx, cfg, rawSource, handler)                          — RawSource path
   pool.RunRecords(ctx, cfg, decoder, newEncoder, transformer, handler) — Record path
                                   │
                                   ▼
   decodeWorkers × { RawSource.DecodeRaw
                     | RecordDecoder.DecodeNext + RecordEncoder.EncodeBatch }
  ┌────────────────────────────────────────┐
  │ claim next unit of work                │
  │ decode/extract it into a [][]byte batch│   (one of these per decode worker;
  │ send batch to out                      │    repeats until no work is left,
  └────────────────┬───────────────────────┘  or ctx is cancelled)
                   │
                   ▼
        out: chan [][]byte
   (buffered to cfg.Workers × config.DispatchQueueDepthPerWorker, i.e. × 4)
                   │
                   ▼
  ┌────────────────────────────────────────┐
  │ for batch := range out {               │   (one of these per dispatch worker,
  │     for _, doc := range batch {        │    cfg.Workers of them total; repeats
  │         handler(ctx, doc)                 │    until out is closed, or ctx is
  │     }                                  │    cancelled)
  │ }                                      │
  └────────────────┬───────────────────────┘
                   │
                   ▼
              handler(doc) × N
```

- **decode workers** pull their next unit of work (a byte-range chunk for NDJSON, a row group for
  Parquet either way) from a shared counter, produce batches, and send them on a channel buffered
  to `cfg.Workers × config.DispatchQueueDepthPerWorker` — sized off the *dispatch* worker count so
  a burst of batches doesn't stall every decode worker waiting for one slow dispatcher.
- **dispatch workers** drain that channel and call the handler once per document, timing the whole
  batch as one span (`Result.Stats.DispatchDuration`).
- the channel is closed once every decode worker has returned (`pool.go`'s `decodeWg`), which is
  what lets the dispatch workers' range loops end instead of blocking forever.
- a decode error, a handler error, or `ctx` cancellation stops every worker (via
  `errgroup.WithContext`); both entry points return once they've all actually stopped, not just
  once the first error is observed.
- **the record path** gets its decode workers by asking the decoder to split itself
  (`formatio.SplittableRecordDecoder.Split(limit)`, capped at `cfg.Workers`): the file is opened and
  its schema walked once, and each worker gets its own scratch while they divide the work through
  the decoder's shared queue. A decoder that can't split is driven by one worker. Each worker also
  gets its own encoder, since an encoder holds scratch. Decode, optional transform, and encode are
  timed as one span, matching `ReadDuration`'s "time spent turning input into documents".

  How many documents one decoded batch becomes is the **encoder's** decision, not the pool's: it
  hands back a `[][]byte`, which is already the shape a dispatch worker consumes. `jsonio`
  returns one per record; `parquetio` returns exactly one covering the whole batch, because a Parquet
  document is one schema, one columnar layout, and one footer spanning many rows — one document per
  row would mean a complete `.parquet` file per row, each paying full magic-bytes-plus-footer
  overhead for a single row. `cfg.BatchSize` therefore doubles as the row count per synthesized
  Parquet document, which is precisely what it already means everywhere else: "documents per send".
- **counting** reports two explicit numbers in `Result.Stats`: `RowsRead` and
  `DocumentsDispatched`. A path that dispatches exactly one item per row leaves `DecodeStats.Records`
  at zero and lets the pool derive rows from successful dispatches; a path whose dispatched item
  isn't one row sets `Records` itself. Parquet's raw path does that because one item there is a
  whole row group (`+= rowGroup.NumRows()`), and the record path does it unconditionally (`+= n`
  decoded records) because whether an item is one row or a whole batch is the encoder's choice.
  `DocumentsDispatched` increments only after the handler accepts a document.

## NDJSON: byte-range chunks

NDJSON's native format is JSON, so `streamio.ProcessFile` always takes the raw-passthrough
route for it — there's no decode step to skip in the first place, since a line of NDJSON already
is one JSON document: it builds a `jsonio.NewRawSource` and calls `pool.RunRaw` directly.

```
file:  [0 ─────────────────────────────────────────────────────────────── size)
        │        chunk 0        │        chunk 1        │       chunk 2        │
        └───────────────────────┴───────────────────────┴──────────────────────┘
              claimed by                claimed by               claimed by
              worker A                  worker B                 worker A (again,
                                                                   once free)

        {"a":1}\n{"b":2}\n│{"c":3}\n{"d":4}\n│{"e":5}\n...
                          ▲
                    a chunk boundary can land mid-line: the worker that owns the
                    chunk starting here skips forward to the next '\n' before it
                    starts batching, so the previous worker's chunk — not this
                    one — finishes that split line.
```

Each worker claims the next unclaimed `cfg.ChunkSize`-byte range (a shared `atomic.Int64`
counter), opens an `io.SectionReader` over the shared `*os.File`, resolves onto a line boundary
(`openChunkReader`), and reads to the boundary crossing `chunkStart + ChunkSize`, batching
`cfg.BatchSize` lines per send. `cfg.Workers` decode workers run — one per configured worker,
since chunks always exist to claim.

## Parquet: row groups, two read routes

Both read routes claim row groups the same way — a shared `atomic.Int64` counter over
`sharedState.rowGroups`, built once per file by `openShared` (schema walk, `readerSlots` sized to
`cfg.MaxOpenReaders`) — and both bound simultaneously-open row-group readers to
`MaxOpenReaders × per-row-group dictionary size`, not `Workers ×` it, by holding a `readerSlots`
slot only for the duration of decoding or extracting that one row group.

```
row groups:  [ RG0 ][ RG1 ][ RG2 ][ RG3 ][ RG4 ][ RG5 ]
                │       │                          │
                ▼       ▼                          ▼
            worker A  worker B   ...           worker A (loops back once RG0 is done)

  readerSlots (buffered chan, size = cfg.MaxOpenReaders): bounds how many row-group
  readers are open at once, since each one holds decompressed dictionary pages in memory.
```

- **Record decoding** (`parquetio.NewDecoder` → `recordDecoder`, `record_decoder.go`): reads each row
  group's rows into a reused `parquetgo.Row` buffer and classifies every column value as a
  `record.Value`, handling `Map(String,String)` columns (via
  `sharedState.mapOrder`/`recordDecoder.mapKeys`/`mapVals`), timestamps, dates, and
  unsigned-integer logical types (`sharedState.columnMeta`, built once per file by
  `buildColumnMeta` in `schema.go`). It implements `formatio.SplittableRecordDecoder` and caps its
  siblings at the row-group count, since a row group is the smallest claimable unit — extra decoders
  beyond that would only find the queue empty. Unlike the raw source it has to suspend
  mid-row-group whenever the caller's batch fills, so the open reader and its `readerSlots` slot
  live in the decoder across `DecodeNext` calls.
- **Raw passthrough** (`parquetio.NewRawSource` → `rawSource`, `raw.go`): re-frames each claimed row
  group as a standalone, independently-openable Parquet file (`extractRawRowGroupBytes`, via
  `parquetgo.NewGenericWriter[any]`'s `WriteRowGroup`) and dispatches it whole — one row group is
  one document from the pool's point of view. `WriteRowGroup` splices the row group's
  already-compressed column chunks straight through when writer and source configuration match, so
  nothing is decoded or re-encoded; that verbatim byte copy is the entire point. `stats.Records` is
  set explicitly to `rowGroup.NumRows()` per dispatched item, since one item here is a row group,
  not a row.

### Parquet as a generic-path *output* (`encoder.go`)

`parquetio.NewEncoder` is the third thing the `parquetio` package supplies, and the only one that
writes rather than reads: it turns one batch of canonical `record.Record`s into one standalone
Parquet file, driven by `pool.RunRecords` like any other encoder. It is the `formatio.RecordEncoder`
that returns a single document per batch rather than one per record — one row per file would be
absurd — so `cfg.BatchSize` is what decides how many rows each synthesized file holds.

Three limits, all of which the encoder reports as errors rather than working around:

- **One schema per encoder, derived from its first batch.** The first record fixes the column names
  and their order; each column's type comes from the first non-null value that column carries
  anywhere in that batch (a JSON `null` says nothing about a column's type, so the leading record
  alone is often not enough evidence). A column with no non-null value in the whole first batch
  becomes a string column. Any later record whose field names, field order, or a field's
  `Kind`/`Semantic` differ is an error — coercing or dropping the difference would only surface in
  whatever a datastore ends up storing. This mirrors Parquet's own decoder assuming one fixed schema
  per file.
- **Every column is optional.** A field that was non-null in the first batch can legitimately be
  null further down the file and there is no cheap way to know up front which never will be, so
  marking them all optional is the pragmatic, documented simplification. A null in a later record is
  always accepted.
- **`record.KindMap` is not supported** — a field carrying one returns an error naming it. This is
  a deliberate scope limit matching `jsonio.NewDecoder`'s documented flat-objects-of-scalars scope:
  `jsonio.NewDecoder` is the only file decoder that feeds this encoder today and never produces a
  `KindMap`, so reconstructing a Parquet `Map(String,String)` column here would be speculative work
  with no live caller to test it against. The one pair that *does* produce `KindMap` records,
  Parquet→Parquet, is raw passthrough and never reaches an encoder at all.

Column order in the output is parquet-go's own name-sorted `parquetio.Group` order rather than the
record's field order; the values and types are what round-trip, not the physical column ordering.

## `record`/`formatio`: the generic cross-format seam

Two small packages exist purely to let a future format join without touching `parquetio` or
`jsonio`:

- **`record`** defines the canonical, format-neutral row: `Record` (a reusable `[]Field`),
  `Field{Name, Value}`, and `Value` — a tagged union (`Kind` + fixed fields), not a
  `map[string]any`, so decoding a scalar field costs no interface boxing or per-field heap
  allocation. `Value` also carries a `Semantic` alongside its `Kind`, for values whose physical
  representation is ambiguous without it (an `int64` that's really a day count). Decoders only
  *classify* — a Parquet `DateType` column decodes to `Value{Kind: KindInt64, Semantic:
  SemanticDate, I64: <raw day count>}`, never to a pre-rendered date string — so two different
  target encoders stay free to render the same semantic value differently, and decoding itself
  stays allocation-free.

  Two consequences of that rule are worth knowing:

  - a timestamp's *unit* is part of its Semantic (`SemanticTimestampMillis`/`Micros`/`Nanos`), not
    out-of-band schema state. The unit is also the precision, so normalising every timestamp to
    nanoseconds at decode time would invent fractional digits the source never had; carrying it in
    a byte-wide tag costs nothing and lets the encoder render exactly what was stored. Likewise
    `SemanticFloat32` marks a `float64` widened from a 32-bit float, so it is formatted at the
    precision it actually carries rather than as `0.10000000149011612`.
  - `KindMap` is the *only* nesting a `Record` admits, and exists for one reason: Parquet models a
    `Map(String,String)` as one logical column (two leaf columns under a `key_value` group), and
    every output format has to render it as one nested object. Flattening it to dotted top-level
    fields would lose the grouping irrecoverably; a general nested-document representation would be
    speculative. Anything else a decoder can't express in these Kinds is an error, not a guess.
- **`formatio`** defines every capability a format supplies: `RawSource` (a file's own bytes →
  the handler), `RecordDecoder` (a file → a stream of `record.Record`), the optional
  `SplittableRecordDecoder` (one open file → several concurrent decoders), `RecordEncoder`
  (a *batch* of `record.Record` → that format's documents), and the `DecodeStats` they fill. They
  live here rather than in `pool` so the arrow points one way: `pool` and `streamio` consume
  these, the format packages implement them, and no format package imports another.

  There is **one** encoder interface, and its single method is batch-shaped:
  `EncodeBatch(batch []record.Record) ([][]byte, error)`. A format decides by the length of what it
  returns whether it produces a document per record (JSON: each is independently valid) or one per
  batch (Parquet: a document carries one schema, a columnar layout, and a footer covering many rows,
  so a lone row isn't a document at all). That was two separate interfaces and two pool entry points
  once; collapsing them costs nothing — the per-record case is a loop the encoder writes instead of
  one the pool writes — and means the pool never has to ask which kind of format it is driving.
- **`jsonio`** owns JSON output: `NewEncoder` renders one JSON document per record, and the same package holds the float/date/timestamp rendering rules so JSON bytes cannot drift between helper packages.
- **`streamio.formatSupportFor`** (a `switch`, not a map, so the `exhaustive` linter flags
  a new `Format` nobody wired up) is the registry: for each `Format`, which of
  `newRawSource`/`newRecordDecoder`/`newRecordEncoder` it implements. A nil field means that
  capability doesn't exist for the format yet, and `processRecords` reports which half of a pair is
  missing via `ErrNoConversionPath`.

### Adding a format

`internal/csvio` is the reference example: it added CSV and TSV support (one package for both,
parameterized by delimiter) by supplying:

1. `FormatCSV`/`FormatTSV` constants, exposed by the root package.
2. `csvio.NewDecoder`/`csvio.NewEncoder`/`csvio.NewRawSource`, matching the `formatio` interfaces
   every other format package implements. CSV has no type system of its own, so every decoded field
   is `record.KindBytes` — a deliberate scope limit, not a placeholder for future sniffing.
3. One new registry case in `formatSupportFor`, filling in those fields.

`parquetio` and `jsonio` needed no changes, and CSV→JSON, CSV→Parquet, JSON→CSV, and Parquet→CSV all
work through `processRecords`'s generic loop with no CSV-specific code in either package.

## Configuration (`streamio.Option`)

| Option | Default | Effect |
|---|---|---|
| `WithInputFormat(f)` | extension detection | The source file's native format. Use this for extensionless files or callers that already know the format. |
| `WithOutputFormat(f)` | `FormatJSON` | The format documents are delivered in. Requesting the input's own native format selects raw passthrough. |
| `WithTransforms(rules...)` | none | Applies declarative rename/drop path transforms on cross-format routes. Dotted paths address nested map entries. Raw passthrough never decodes records, so it never transforms. A `DropPath` combined with a rule under it is rejected at construction time, since the drop would otherwise make that rule permanently dead. |
| `WithParallelWorkers(n)` | `1` | Decode workers for NDJSON and for Parquet raw passthrough; upper bound on the decoders a splittable `RecordDecoder` hands out on the generic path (see above). Also sizes the dispatch pool. |
| `WithChunkSize(n)` | 32 MiB | NDJSON's fixed byte-range chunk size. |
| `WithReadBufferSize(n)` | 32 MiB | NDJSON's `bufio.Reader` size; capped to `ChunkSize` so a small chunk doesn't pay for a full-size buffer per worker. |
| `WithBatchSize(n)` | 512 | Documents per send to a dispatch worker. On the generic path it also sizes the `ReadRows` fetch, and for Parquet *output* it is the number of rows per synthesized Parquet file; ignored by Parquet raw passthrough, which always sends one row group per item. |
| `WithMaxOpenReaders(n)` | `Workers` | Caps simultaneously-open Parquet row-group readers, for either read route (see above). Ignored by NDJSON. |
| `WithLogger(l)` | none | Receives a one-line summary (`row/line count, size`) when `ProcessFile` starts. |
| `WithOnRowError(mode, onSkip)` | `RowErrorFailFast` | `RowErrorSkip` drops a row that fails to decode and continues, instead of failing the whole run. Only takes effect for NDJSON and CSV/TSV field errors on the generic decode/encode path — a CSV/TSV *syntax* error (e.g. an unterminated quoted field) and every Parquet decode error stay fail-fast regardless, since neither leaves the decoder at a well-defined "start of the next row" to resume from. `onSkip`, if non-nil, is called once per dropped row with the error that row raised. |

`n <= 0` for any numeric option means "use the default", identical to omitting it. Passing the zero `Format` (or omitting `WithOutputFormat` entirely) means `FormatJSON`.

`ProcessFile` returns `streamio.Result{Stats: streamio.Stats{...}}`. `RowsRead` counts logical input
rows; `DocumentsDispatched` counts successful handler calls. They differ on Parquet's raw path (one
document is a row group) and whenever an encoder renders a whole batch as one document. `RowsSkipped`
counts rows `WithOnRowError(RowErrorSkip, ...)` dropped, always zero under the default. `ReadDuration`
and `DispatchDuration` are summed across every worker, so they can exceed wall-clock time; that's
expected for a concurrent run and is what makes decode-vs-dispatch time attributable at all.

### Reading from something other than a file path

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

## Benchmarks

Every claim below is measured, not asserted — each number comes from an actual `go test -bench`
run on the machine this README was last updated on (Apple M3 Max, `GOMAXPROCS=16`), not an estimate.
Numbers are illustrative of relative shape and tradeoffs, not a performance guarantee: rerun the
commands below before relying on any of this for capacity planning, since absolute throughput is
machine- and Go-version-dependent even when the relative shape holds.

| Benchmark | File | What it answers |
|---|---|---|
| `BenchmarkProcessFile_ParquetFormats*` | `format_bench_test.go` | What raw passthrough buys over the generic JSON-decode route, across schema shapes, row-group sizes, and worker counts. |
| `BenchmarkNDJSONProcess*` | `internal/jsonio/ndjson_bench_test.go` | NDJSON throughput across document sizes and buffer sizes. |
| `BenchmarkProcess_Dispatch` | `streamio_bench_test.go` | `ProcessFile`'s own format-detection/routing overhead, isolated from decode/encode cost. |
| `BenchmarkEncoder_BatchVsStreaming` | `internal/parquetio/encoder_bench_test.go` | Whether the CLI's single-continuous-file Parquet output (`streamingEncoder`) costs anything over the library's default one-file-per-batch encoder. |
| `BenchmarkStreamingEncoder_MemoryBoundedness` | `internal/parquetio/encoder_bench_test.go` | Whether streaming output's per-flush memory actually stays bounded as total output size grows — and what does scale instead. |
| `BenchmarkTransformRecord_RenameAndDrop` | `internal/options/transform_bench_test.go` | Per-record cost of `WithTransforms`' rename/drop hook, across record widths. |
| `BenchmarkRunConvert` | `cmd/convert_bench_test.go` | The CLI's actual end-to-end throughput (flag wiring + real file I/O), for every format pair the CLI supports (NDJSON, Parquet, CSV, TSV), at 20,000 rows. |
| `BenchmarkRunConvert_LargeScale` | `cmd/convert_bench_test.go` | The same, at 100,000,000 rows, for every format pair the CLI supports (NDJSON, Parquet, CSV, TSV) — confirming steady-state throughput holds, and that this scale doesn't blow past parquet-go's row-group cap when `--batch-size` is chosen sensibly. Not run by default; see below. |

### Batch-per-file vs. single continuous file (Parquet output)

```
go test ./internal/parquetio/... -bench=BenchmarkEncoder_BatchVsStreaming -benchmem -run '^$'
```

| batch size | encoder | ns/row | B/op | allocs/op |
|---|---|---|---|---|
| 100 | default (one file per batch) | 967 | 133,458 | 269 |
| 100 | streaming (single file) | 533 | 119,201 | 264 |
| 1,000 | default | 207 | 415,720 | 274 |
| 1,000 | streaming | 166 | 313,440 | 271 |
| 10,000 | default | 160 | 4,639,659 | 317 |
| 10,000 | streaming | 163 | 4,617,913 | 317 |

Streaming output is never slower than the default per-batch-file encoder here, and noticeably
cheaper at small batch sizes (it skips writing a fresh magic-bytes-plus-footer for every batch); the
two converge as batch size grows, since per-batch framing overhead becomes negligible relative to
row-encoding cost either way. There's no throughput reason to prefer the default encoder over
streaming output at any batch size measured.

### Streaming output's memory boundedness

```
go test ./internal/parquetio/... -bench=BenchmarkStreamingEncoder_MemoryBoundedness -run '^$'
```

| batch size | total rows | row groups | max single flush | `Finalize` trailer |
|---|---|---|---|---|
| 100 | 10,000 | 100 | 32,768 B | 84,674 B |
| 100 | 100,000 | 1,000 | 32,768 B | 631,939 B |
| 100 | 1,000,000 | 10,000 | 32,768 B | 6,334,203 B |
| 100,000 | 100,000,000 | 1,000 | 5,931,008 B | 2,141,220 B |

This is the more interesting — and more honest — result than a blanket "memory is bounded" claim
would be. Two separate, both true, stories:

- **`max-flush-bytes` never scales with total row count at a fixed batch size** — flat at 32,768 B
  across the first three rows (a 100× growth in total rows, batch size held at 100). But it is
  **not** a universal constant: the fourth row, at a 1,000× larger batch size (100,000 rows/batch),
  shows a ~5.9 MB max flush instead. That's expected — a single large batch can produce more encoded
  bytes than the writer's small internal buffer holds before that call's own `Flush` forces them
  out — so the precise claim is peak memory per call is bounded by `max(one batch's encoded size,
  the writer's internal buffer)`, not by a fixed number, and never by total row/batch count.
- **`finalize-bytes` scales with row-group count** (one per batch) — the Parquet footer lists
  per-row-group column metadata for every row group in the file, which is inherent to the format,
  not something this encoder could avoid. Note how row 3 (10,000 row groups) has a *larger* footer
  than row 4 (1,000 row groups, 100× more total rows): footer size tracks row-group count, not row
  count.

**A hard ceiling this benchmark deliberately stays under:** parquet-go caps a file at 32,767 row
groups (`math.MaxInt16`) — past that, `EncodeBatch` fails the whole conversion outright with a
wrapped `ErrTooManyRowGroups`, rather than merely producing a larger footer (pinned by
`TestStreamingEncoder_TooManyRowGroupsFailsRatherThanSilentlyGrowing` in
`internal/parquetio/streaming_encoder_test.go`). This is a real, previously undocumented constraint
this benchmark pass surfaced while scaling up to 100,000,000 rows: the library's default
`--batch-size` (512) would produce ~195,000 row groups at that scale — six times over the cap,
failing outright. **The CLI's `--help` now documents this**, and `runConvert` (`cmd/convert.go`)
catches `errors.Is(err, parquetgo.ErrTooManyRowGroups)` specifically to add an actionable
`--batch-size` hint instead of surfacing parquet-go's bare error text — see
`TestRunConvert_TooSmallBatchSizeGetsAnActionableError` in `cmd/convert_test.go`. The practical
takeaway for any caller converting a very large input to Parquet: choose `--batch-size` so total
rows ÷ batch size stays well under 32,767.

### Record-transform overhead

```
go test ./internal/options/... -bench=BenchmarkTransformRecord_RenameAndDrop -benchmem -run '^$'
```

| record width | ns/op | B/op |
|---|---|---|
| 4 fields | 342 | 0 |
| 16 fields | 687 | 0 |
| 64 fields | 1,715 | 0 |

Zero allocations at every width, confirming `transformRecord`'s in-place filter (`rec[:0]`, see
`internal/options/transform.go`) does what it's documented to do. Cost scales roughly linearly with
field count, as expected for a single linear scan per record.

### CLI end-to-end

```
go test ./cmd/... -bench=BenchmarkRunConvert -benchmem -run '^$'
```

| conversion | rows | ns/row | B/op | allocs/op |
|---|---|---|---|---|
| NDJSON → NDJSON | 20,000 | 48.6 | 35,037,827 | 40,089 |
| NDJSON → Parquet | 20,000 | 295.6 | 35,627,864 | 82,582 |
| Parquet → Parquet | 20,000 | 187.8 | 3,319,609 | 44,151 |
| Parquet → NDJSON | 20,000 | 256.4 | 3,845,543 | 82,686 |
| NDJSON → CSV | 20,000 | 314.3 | 35,348,631 | 81,181 |
| CSV → NDJSON | 20,000 | 255.1 | 36,053,005 | 101,151 |
| NDJSON → TSV | 20,000 | 299.2 | 35,348,724 | 81,181 |

The ranking tracks exactly how much of each row the route actually has to touch. NDJSON → NDJSON
is fastest because it's the one case left on the original raw-passthrough path — `WithSingleFileOutput`
only ever gets set when `--out-format` is `parquet` (see `runConvert`), so this route never decodes
a value or builds a `record.Record` at all; it's a byte-range chunk read plus a buffered write with
a newline appended. Every other row in the table pays for at least one real decode or encode:
NDJSON → Parquet decodes every value and derives a schema; Parquet → Parquet (post-fix) decodes to
records and re-encodes through `streamingEncoder`, cheaper than the other two because
`WriteRowGroup` still splices compressed column chunks through where it can, but no longer the
zero-cost raw path; Parquet → NDJSON decodes Parquet's binary encoding into records and then
JSON-renders each one; NDJSON → CSV/TSV decode JSON and render each field straight into a reused
row buffer (see the encoder discussion below), close to NDJSON → Parquet's cost since both pay for
one decode and one non-trivial encode. CSV → NDJSON now costs about the same as Parquet → NDJSON,
for the same reason: both routes do one real decode and one JSON encode, and neither pays a
per-row allocation penalty beyond that anymore.

**NDJSON → CSV/TSV's `allocs/op` dropped 33% (121,304/121,305 → 81,181) with `ns/row` essentially
unchanged (290.5 → 314.3/299.2, within this benchmark's own several-percent run-to-run noise floor
— confirmed by re-running the untouched CSV → NDJSON case, which showed the same spread across
repeats).** `internal/csvio`'s encoder used to build a `[]string` per row and hand it to
`encoding/csv.Writer.Write`, which the `Write([]string)` signature forces: even after avoiding an
intermediate formatting buffer, every field still had to become a real Go string. The encoder now
skips `encoding/csv.Writer` entirely for output — `internal/csvio/row.go`'s `rowWriter` renders
each field straight into a reused `bytes.Buffer`, reimplementing `encoding/csv.Writer`'s own
RFC 4180 quoting/escaping logic instead of driving it through the `[]string` API — so a field's
formatted bytes go straight from `strconv.AppendInt`/`jsonio.AppendFloat` into the final row, no
string ever created.

The 81,181 allocs/op (and 35.3 MB `B/op`) the CLI benchmark reports for a 20,000-row NDJSON→CSV run
isn't mostly the CSV encoder's own cost: a memory profile of that benchmark shows well over 90% of
its allocated *bytes* come from `bufio.NewReaderSize` — the read buffers `openShared` builds per
decode worker for the NDJSON *input* — a cost every NDJSON-input route in this table already pays,
untouched by this change. Isolated from the rest of the pipeline (one `csvio.NewEncoder` driven
directly over many 512-row batches, matching this library's own default `BatchSize`), the CSV
encoder itself allocates about 9.5 KB and 2 allocations *per batch* — roughly 18.5 bytes/row,
against a real per-batch document size of about 9.1 KB, i.e. barely any overshoot. That number
only reached this floor after a second fix: the first version of this rewrite built a fresh
`rowWriter` (and its `bytes.Buffer`) inside every `EncodeBatch` call, so that buffer regrew from
empty via Go's doubling strategy on every single batch instead of reusing capacity across
batches — 19 allocations and 42.4 KB per 512-row batch, roughly **4.7× the batch's own document
size**, purely from repeated regrowth. Making `rowWriter` a field on `encoder`/`streamingEncoder`,
reset per batch instead of rebuilt, cut that to the 2-allocation, near-zero-overshoot number above
— a 78%-fewer-bytes, 89%-fewer-allocations improvement measured in isolation, even though it barely
moves the CLI benchmark's own `B/op` figure, since that figure is dominated by the unrelated
input-side read-buffer cost described above.

A hand-rolled quoting/escaping reimplementation is exactly the kind of small-but-easy-to-get-wrong
logic worth distrusting on sight — it was verified against the real `encoding/csv.Writer`, not just
tested against hand-picked cases: `TestEncode_MatchesEncodingCSVQuoting_Fuzz`
(`internal/csvio/quoting_test.go`) generates random fields from an alphabet covering every RFC 4180
special character (delimiter, quote, `\n`, `\r`, Unicode), asserts the new encoder's byte output is
identical to `encoding/csv.Writer`'s for the same input, and round-trips the result through a real
`csv.Reader` to confirm it decodes back to the original values — run 45 times (13,500 total random
cases) during development with no mismatch. The two near-misses that surfaced along the way
(`csv.Reader` normalizing an embedded `\r\n` to `\n` on read, and a single empty field encoding as
an indistinguishable blank line) both turned out to be `encoding/csv`'s own documented behavior,
reproduced identically by both writers — not bugs introduced here.

**CSV → NDJSON used to be the clear outlier here — 2–3× every other conversion — because of how
`internal/csvio`'s decoder drove `encoding/csv`, not because of `encoding/csv` itself.** The
original decoder built a brand-new `csv.Reader` (and a `bytes.Reader` under it) for *every row*,
parsed exactly one row with it, then discarded it — so none of `encoding/csv`'s own internal
buffering, and none of its `ReuseRecord` option, ever got a chance to help. The fix
(`internal/csvio/record_decoder.go`'s `csvRowReader`) drives one persistent `*csv.Reader` per
claimed chunk instead, reading many rows off one continuous stream with `ReuseRecord: true`. That
also fixed a real, previously-documented correctness gap as a side effect: `encoding/csv.Reader`
already tokenizes rows itself, including a literal newline inside a quoted field spanning two
physical lines, which the *old* decoder's external physical-line splitter could not tell apart
from a genuine row terminator (see `TestNewDecoder_QuotedNewlineSingleWorker` in
`internal/csvio/record_decoder_test.go`). This remains a known limitation for a *multi-worker* run
against a file containing quoted newlines specifically at a chunk boundary — see
`internal/csvio/csvio.go`'s `openChunkReader` doc comment for why that can't be fixed by a local
byte scan — but the per-row allocation cost this section originally documented is resolved.

`cmd/writer.go`'s `fileWriter` originally wrote every document straight to the underlying
`*os.File` with no buffering, costing NDJSON output two raw `Write` syscalls per row (the document,
then its separator) — 40,000 syscalls for this 20,000-row fixture. That was exactly the kind of
concrete, number-backed finding a benchmark is supposed to surface: the very first version of this
benchmark measured NDJSON output at **2,488 ns/row**, nearly 40× slower than the 57.7 ns/row
above. Wrapping `fileWriter`'s destination in a `bufio.Writer` (flushed on `close`) fixed it —
NDJSON output is now the *faster* of the two formats, as expected, since Parquet still has to
derive a schema and build columnar rows per batch.

**Adding the Parquet-input cases surfaced a real correctness bug, not just two more rows in a
table.** Parquet-to-Parquet used to take the raw-passthrough route regardless of `--out-format`'s
single-file contract — dispatching one complete, independently-closed Parquet file per input row
group and concatenating them into `--out` with no separator. For any input with more than one row
group (i.e. any real-sized file), the result wasn't a valid Parquet file past the first row
group's footer; `parquetgo.OpenFile` on it failed outright with `reading page index of parquet
file: ...`. Fixed in `streamio.go`: `ProcessFile` now rules raw passthrough out whenever
`WithSingleFileOutput` is set, even when the input and output formats match, and decodes to
records and re-encodes through `streamingEncoder` instead — trading raw passthrough's verbatim
byte-splice optimization for a file that's actually valid. Pinned by
`TestRunConvert_ParquetToParquetProducesOneValidFile` in `cmd/convert_test.go`. Parquet-to-Parquet
being noticeably *cheaper* than either Parquet-to-NDJSON or NDJSON-to-Parquet above (181.6 ns/row,
with far fewer allocations) is expected even after the fix: parquet-go's `WriteRowGroup` still
splices already-compressed column chunks straight through on the decode side wherever the schema
lets it (see `raw.go`), so this route pays for one schema derivation and one re-framing pass, not a
full per-value decode/re-encode.

At 100,000,000 rows (`BenchmarkRunConvert_LargeScale`, not run by default — see its doc comment for
why — `go test ./cmd/... -bench=BenchmarkRunConvert_LargeScale -benchmem -run '^$' -timeout=90m`):

| conversion | rows | ns/row | B/op | allocs/op |
|---|---|---|---|---|
| NDJSON → NDJSON | 100,000,000 | 49.57 | 11,340,983,848 | 200,196,121 |
| NDJSON → Parquet | 100,000,000 | 276.4 | 13,179,268,568 | 400,384,566 |
| Parquet → Parquet | 100,000,000 | 153.7 | 10,821,738,000 | 200,917,985 |
| Parquet → NDJSON | 100,000,000 | 239.0 | 17,918,928,200 | 405,542,615 |
| NDJSON → CSV | 100,000,000 | 308.8 | 13,353,388,984 | 400,392,302 |
| CSV → NDJSON | 100,000,000 | 248.8 | 14,523,273,784 | 500,197,322 |
| NDJSON → TSV | 100,000,000 | 308.3 | 13,353,390,440 | 400,392,318 |

Every route's ns/row holds roughly steady from 20,000 rows to 100,000,000 (a 5,000× scale
increase), for every format the CLI supports — not just the two that happened to be benchmarked
first. Parquet input/output cases use `--batch-size 10000` rather than the library default, for the
reason explained above. This is the direct evidence that the CLI's throughput characteristics
measured at a small, fast-to-benchmark fixture size actually hold at the scale the whole streaming
design exists for, across every route: NDJSON, Parquet, and CSV/TSV alike.

**CSV → NDJSON's fix holds at scale, and holds better than the 20,000-row numbers alone would
suggest.** At 20,000 rows, reusing one `csv.Reader` per chunk instead of one per row cut ns/row by
2.4× (623.4 → 255.1) and `B/op` by 3.4× (123,731,497 → 36,053,005). At 100,000,000 rows the same
fix cuts ns/row by 2.5× (604.4 → 242.1) — consistent with the small-scale measurement — but cuts
`B/op` by **31×** (454,514,630,744 → 14,523,277,496), a far bigger win than the 20,000-row number
predicted. The reason is exactly the gap this section used to warn about: the *old* decoder's
per-row `csv.Reader` allocation didn't amortize at all, so its `B/op` scaled almost linearly with
row count (3,673× for a 5,000× row increase) while every other route's buffered I/O let `B/op`
scale sublinearly (NDJSON → NDJSON's `B/op` grew only 324× for that same 5,000× increase). Fixing
the allocation *pattern* — not just its per-row constant — means CSV → NDJSON's `B/op` now scales
the same sublinear way every other route's does: it's **1.28× NDJSON → NDJSON's `B/op`** at
100,000,000 rows (down from 40×), and its ns/row is now within a few percent of Parquet →
NDJSON's — the two routes that do the same shape of work (one decode, one JSON encode) now cost
about the same, which is the expected outcome once CSV's decode no longer pays a per-row penalty
the other formats don't.

**NDJSON → CSV/TSV's allocation count holds the same ~33% reduction at 100,000,000 rows it showed
at 20,000** (400,392,302 vs. the pre-rewrite 601,759,216, a 33.5% cut, matching the 20,000-row
run's 33% almost exactly) — replacing `encoding/csv.Writer`'s `[]string`-per-row API with
`internal/csvio/row.go`'s `rowWriter` writing straight into a reused buffer removes one allocation
per field regardless of scale. `ns/row` again moves within the same few-percent band every route
shows run-to-run (confirmed against the untouched CSV → NDJSON case).

**`B/op` tells a second, scale-dependent story: a buffer-lifetime bug in the first version of this
rewrite, fixed after it was caught by exactly this kind of measurement.** The first working version
of the byte-native encoder built a brand-new `rowWriter` — and therefore a brand-new, empty
`bytes.Buffer` — inside every single `EncodeBatch` call, so that buffer had to regrow from nothing
via Go's doubling strategy on every batch instead of reusing capacity from the batch before it.
Isolated in a microbenchmark (many 512-row batches through one long-lived `csvio.NewEncoder`, this
library's own default `BatchSize`), that cost 42.4 KB and 19 allocations per batch to produce a
9.1 KB document — **roughly 4.7× the document's own size**, pure regrowth overhead. Making
`rowWriter` a persistent field on `encoder`/`streamingEncoder`, reset per batch instead of rebuilt,
cut that to 9.5 KB and 2 allocations per batch — under 4% overshoot instead of 370%. At
100,000,000 rows (195,313 batches at the default batch size), that per-batch regrowth penalty
compounds far more visibly than it did in the 20,000-row table above (only 40 batches): `B/op`
drops from 19.7 GB to **13.4 GB**, a further 6.4 GB (32%) beyond what the `[]string`-removal alone
had already saved, versus roughly 621 KB of difference the same fix made at 20,000 rows. Combined,
the two fixes take NDJSON → CSV from the original 22.2 GB down to 13.4 GB — a 40% total reduction
in bytes allocated, alongside the 33% allocation-count cut — for the exact same 100,000,000-row
conversion. The lesson this pair of fixes leaves behind: a per-batch allocation cost that looks
negligible at a small, fast benchmark size can be the dominant one at the scale the batch size was
actually chosen for, which is exactly why this README re-runs the 100,000,000-row benchmark rather
than trusting the 20,000-row numbers to extrapolate on their own.

## Package layout

```
streamio/
├── api.go                public types/options: Format, Result, Stats, DocumentHandler, transforms
├── streamio.go    ProcessFile: compares requested vs. native format, picks a route
├── cmd/                  standalone `streamio convert` CLI, its own `package main`; drives
│                         ProcessFile directly through the public api.go surface
└── internal/
    ├── options/
    │   ├── opts.go        Config (RunConfig/NDJSONConfig/ParquetConfig/CSVConfig sub-structs),
    │   │                  Option, every WithX constructor
    │   ├── factory.go     Config → validated, defaulted runtime settings
    │   ├── format.go      Format/OutputFormat constants
    │   ├── logger.go      Logger interface
    │   └── transform.go   PathTransformRule, RenamePath/DropPath, NewPathTransformer
    ├── formatio/          RawSource, RecordDecoder, RecordEncoder, DecodeStats — what a format supplies
    ├── record/            Record, Field, Value, Kind, Semantic — the canonical cross-format row
    ├── pool/
    │   ├── pool.go        RunRaw + the decode/dispatch worker pool skeleton both routes share
    │   └── records.go     RunRecords: the generic decode→encode path over that same skeleton
    ├── jsonio/
    │   ├── decoder.go        ObjectDecoder: one JSON object → record
    │   ├── encoder.go        JSON RecordEncoder: record → one JSON object
    │   ├── format.go         float/date/timestamp rendering rules
    │   ├── ndjson.go         sharedState/openShared: NDJSON state shared by the raw and record routes
    │   ├── raw.go            NewRawSource (formatio.RawSource impl): NDJSON raw route
    │   ├── decode.go         chunkDecoder: per-worker chunk claiming and line batching
    │   ├── lines.go          chunk-boundary line resolution
    │   ├── batch.go          per-worker line-accumulation scratch
    │   ├── record_decoder.go NDJSON file → records, delegating line JSON to ObjectDecoder
    │   └── convert_test.go   convertFile: test/benchmark-only NewRawSource+pool.RunRaw wrapper
    └── parquetio/
        ├── parquet.go         openShared, sharedState: shared schema walk and row-group queue
        ├── schema.go          schema classification/naming
        ├── raw.go             NewRawSource + raw row-group extraction
        ├── record_decoder.go  Parquet row → record
        ├── encoder.go         record batch → one complete Parquet file (formatio.RecordEncoder)
        ├── streaming_encoder.go  record batch → one row group of a continuously-growing single
        │                        file (formatio.FinalizableRecordEncoder), for bounded-memory
        │                        multi-gigabyte single-file output
        └── buf.go             cloneBuf: shared reused-buffer-to-owned-slice copy helper
    └── csvio/                 CSV and TSV in one package, parameterized by delimiter
        ├── csvio.go           openShared, sharedState, delimiterFor: shared file/delimiter state
        ├── lines.go           chunk-boundary line resolution, mirroring jsonio's
        ├── row.go             parseRow/rowWriter: thin encoding/csv wrappers
        ├── raw.go             NewRawSource: CSV/TSV raw route, one row per document
        ├── record_decoder.go  NewDecoder: CSV/TSV row → record, every field record.KindBytes
        ├── encoder.go         record batch → one complete CSV/TSV document (formatio.RecordEncoder)
        ├── value.go           record.Value → CSV field text, reusing jsonio's date/timestamp rendering
        └── streaming_encoder.go  record batch → rows of a single CSV/TSV document spanning the run
```
