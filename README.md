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
- **`record.KindMap` and `record.KindList` are not supported** — a field carrying either returns an
  error naming it. `jsonio.NewDecoder` decodes a nested JSON object or array into exactly these
  Kinds (see below), so this Parquet encoder is where that scope boundary actually lands: nothing
  stops a `KindMap`/`KindList` record from reaching `NewEncoder` on a JSON→Parquet run, and it
  rejects it outright rather than inventing a representation — reconstructing a Parquet
  `Map(String,String)` column, or a Parquet `LIST` column, from an arbitrary decoded value would be
  speculative work with no settled schema to target. The one pair that *does* produce `KindMap`
  records from the Parquet side, Parquet→Parquet, is raw passthrough and never reaches an encoder
  at all.

Column order in the output is parquet-go's own name-sorted `parquetio.Group` order rather than the
record's field order; the values and types are what round-trip, not the physical column ordering.

## Arrow IPC: record batches, one read route

Arrow IPC (`internal/arrowio`, backed by `github.com/apache/arrow-go/v18`) is columnar and
batch-oriented like Parquet, but has only the generic decode/re-encode route — there is no raw
passthrough:

```
record batches:  [ RB0 ][ RB1 ][ RB2 ][ RB3 ][ RB4 ][ RB5 ]
                    │       │                          │
                    ▼       ▼                          ▼
                worker A  worker B   ...           worker A (loops back once RB0 is done)
```

- **Record decoding** (`arrowio.NewDecoder` → `recordDecoder`, `record_decoder.go`): each decode
  worker claims the next unclaimed record batch via a shared `atomic.Int64` counter
  (`sharedState.nextBatch`, built once per file by `openShared`) and reads it via
  `ipc.FileReader.RecordBatchAt`, documented safe for concurrent use — the same claiming shape as
  Parquet's row groups, with Arrow's own indexed accessor standing in for
  `rowGroups []parquetgo.RowGroup`. It implements `formatio.SplittableRecordDecoder` and caps its
  siblings at the record-batch count, for the same reason Parquet caps at row-group count: a record
  batch is the smallest claimable unit, so extra decoders beyond that would only find the queue
  empty.
- **No raw passthrough.** Parquet's raw route works because `parquetgo.Writer.WriteRowGroup` can
  splice an already-compressed row group's column chunks straight through. Arrow's `ipc.FileWriter`
  has no equivalent — `Write` only accepts a live, already-decoded `arrow.RecordBatch` — so even an
  Arrow-to-Arrow conversion always takes the generic decode/re-encode path, the same as CSV/TSV
  (which never had an "already-encoded column chunk" to copy verbatim in the first place).
  `formatSupportFor`'s `newRawSource` is `nil` for `FormatArrow`, a state `formatSupport`'s own doc
  already calls out as legitimate.
- **`ipc.NewFileReader` needs `Seek`, not just `ReadAt`.** It locates its own footer via
  `Seek(0, io.SeekEnd)` rather than taking an explicit size parameter the way `parquetgo.OpenFile`
  does. `options.Source{Reader io.ReaderAt, Size int64}` doesn't satisfy that on its own, but
  `io.NewSectionReader(src.Reader, 0, src.Size)` does — `io.SectionReader` already implements
  `Read`+`ReadAt`+`Seek` over any `io.ReaderAt` and known size, so no custom adapter was needed
  (`arrow.go`'s `openShared`).
- **Values are copied out, not held open.** Arrow's Go records are reference-counted
  (`Release()`-requiring); rather than introduce a new held-open-across-batches resource lifetime
  nothing else in this codebase has, `recordDecoder.DecodeNext` copies every value out of the batch
  into a `record.Value` immediately and `Release()`s the Arrow record right away — matching every
  other decoder's existing contract that `Value.Str`/`Map`/`List` are valid only until the next
  decode call. This is honestly *not* a zero-copy path, unlike Parquet's `ByteArrayValue` aliasing
  the page buffer directly; it trades away some of Arrow's own zero-copy advantage for lifetime
  consistency with the rest of the codebase.
- **`record.KindMap` and `record.KindList` are supported, both ways.** Unlike the Parquet and CSV/TSV
  encoders (which reject both), Arrow's encoder (`encoder.go`) builds real nested columns —
  `record.KindMap` becomes an `arrow.StructType` column (named, heterogeneous fields, matching
  `record.Record`'s own shape exactly — not Arrow's `MapType`, which requires uniform key/value
  types across all entries and is a poor fit), `record.KindList` becomes an `arrow.ListType` column,
  via `array.NewStructBuilder`/`array.NewListBuilder`. Nesting can go arbitrarily deep (a `KindList`
  of `KindMap` of `KindList`, and so on) — both the schema derivation and the decode-side walk are
  recursive, not one-level-only.

### Arrow as a generic-path *output* (`encoder.go`)

Structurally the same three limits as the Parquet encoder: one schema derived from the first batch
(scanning the whole batch, not just the first record, for each column's first non-null value), every
column optional, and a schema mismatch in a later record is an error rather than a silent coercion.
`NewEncoder` produces one standalone Arrow IPC file per batch by default, or delegates to
`NewStreamingEncoder` under `WithSingleFileOutput(true)`.

`ipc.FileWriter` has no `Reset`/`Flush` — unlike `parquetgo`'s writer, which the streaming Parquet
encoder resets onto a fresh buffer per batch, Arrow's writer tracks a running byte offset
internally that must stay continuous for the whole file's block-offset table, written into the
footer only at `Close`. `streaming_encoder.go`'s `growingSink` (an `io.Writer` that appends to an
internal buffer and exposes `Drain()` to copy-out-and-truncate) works around this: `ipc.FileWriter`
writes to one continuous destination for the file's whole lifetime, while `EncodeBatch` still only
returns the bytes written since the last call, keeping peak memory bounded to one batch's encoded
size even though the writer itself is never reset.

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
  - `KindMap` and `KindList` are the two nested Kinds a `Record` admits. `KindMap` exists because
    Parquet models a `Map(String,String)` as one logical column (two leaf columns under a
    `key_value` group), and every output format has to render it as one nested object — flattening
    it to dotted top-level fields would lose the grouping irrecoverably. `KindList` exists for the
    equivalent case on the JSON side: `jsonio.NewDecoder` decodes a nested JSON array into an
    ordered `[]Value` rather than rejecting it, recursively — an array element can itself be a
    nested object or array, to whatever depth the document has. Anything else a decoder can't
    express in these Kinds is an error, not a guess. `jsonio` and `arrowio` both produce and consume
    `KindMap`/`KindList`; `csvio` and `parquetio` reject both, since neither CSV/TSV nor the Parquet
    encoder has anywhere to put an ordered nested sequence or a struct-shaped column.
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
| `BenchmarkRunConvert` | `cmd/convert_bench_test.go` | The CLI's actual end-to-end throughput (flag wiring + real file I/O), for every one of the 5×5 = 25 in→out pairs across every format the CLI supports (NDJSON, Parquet, CSV, TSV, Arrow), at 20,000 rows. |
| `BenchmarkRunConvert_LargeScale` | `cmd/convert_bench_test.go` | The same 25 pairs, at 10,000,000 rows — confirming steady-state throughput holds, and that this scale doesn't blow past parquet-go's row-group cap when `--batch-size` is chosen sensibly. Not run by default; see below. |

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

Covers all 5×5 = 25 in→out pairs across every format the CLI supports (`cmd/convert_bench_test.go`'s
`benchAllPairCases`), the same matrix `TestConvertMatrix_AllFormatPairs`
(`cmd/convert_matrix_test.go`) verifies for correctness.

**20,000 rows:**

| in → out | ns/row | B/op | allocs/op |
|---|---|---|---|
| json→json | 156.3 | 35,042,328 | 40,106 |
| json→parquet | 337.5 | 35,766,808 | 82,628 |
| json→csv | 323.0 | 35,384,360 | 81,194 |
| json→tsv | 332.8 | 35,386,360 | 81,203 |
| json→arrow | 294.9 | 36,781,248 | 83,422 |
| parquet→json | 281.4 | 3,982,648 | 83,317 |
| parquet→parquet | 221.9 | 3,486,944 | 44,200 |
| parquet→csv | 192.5 | 2,999,856 | 42,745 |
| parquet→tsv | 192.3 | 3,023,544 | 42,738 |
| parquet→arrow | 167.5 | 4,469,608 | 45,005 |
| csv→json | 329.2 | 36,086,368 | 101,165 |
| csv→parquet | 209.7 | 35,388,616 | 62,557 |
| csv→csv | 179.9 | 35,067,776 | 61,202 |
| csv→tsv | 177.9 | 35,067,264 | 61,198 |
| csv→arrow | 177.2 | 36,579,152 | 63,779 |
| tsv→json | 249.2 | 36,086,368 | 101,165 |
| tsv→parquet | 205.3 | 35,390,536 | 62,564 |
| tsv→csv | 178.2 | 35,069,776 | 61,212 |
| tsv→tsv | 175.5 | 35,069,776 | 61,212 |
| tsv→arrow | 160.3 | 36,579,728 | 63,783 |
| arrow→json | 227.9 | 2,381,560 | 62,249 |
| arrow→parquet | 176.1 | 1,902,080 | 23,710 |
| arrow→csv | 165.6 | 1,521,112 | 22,280 |
| arrow→tsv | 164.1 | 1,521,640 | 22,284 |
| arrow→arrow | 125.7 | 2,919,816 | 24,522 |

At 10,000,000 rows (`BenchmarkRunConvert_LargeScale`, not run by default —
`go test ./cmd/... -bench=BenchmarkRunConvert_LargeScale -benchmem -run '^$' -timeout=30m`):

| in → out | ns/row | B/op | allocs/op |
|---|---|---|---|
| json→json | 45.0 | 1,122,880,296 | 20,019,668 |
| json→parquet | 316.5 | 1,321,332,016 | 40,345,223 |
| json→csv | 331.6 | 1,286,628,248 | 40,040,242 |
| json→tsv | 332.5 | 1,286,627,608 | 40,040,236 |
| json→arrow | 279.6 | 2,026,530,088 | 40,622,222 |
| parquet→json | 253.2 | 1,926,175,632 | 41,033,419 |
| parquet→parquet | 187.4 | 1,257,725,800 | 20,876,320 |
| parquet→csv | 203.7 | 1,396,538,120 | 21,052,446 |
| parquet→tsv | 201.8 | 1,396,388,472 | 21,052,324 |
| parquet→arrow | 147.8 | 1,963,416,328 | 21,153,902 |
| csv→json | 241.7 | 1,428,549,464 | 50,020,717 |
| csv→parquet | 193.9 | 895,966,944 | 30,324,501 |
| csv→csv | 183.4 | 872,458,056 | 30,040,261 |
| csv→tsv | 183.7 | 872,458,008 | 30,040,260 |
| csv→arrow | 142.3 | 1,737,589,880 | 30,722,241 |
| tsv→json | 241.4 | 1,428,549,208 | 50,020,713 |
| tsv→parquet | 194.1 | 895,503,304 | 30,324,350 |
| tsv→csv | 185.0 | 872,458,712 | 30,040,260 |
| tsv→tsv | 183.3 | 872,457,736 | 30,040,258 |
| tsv→arrow | 144.3 | 1,737,590,072 | 30,722,244 |
| arrow→json | 234.3 | 1,176,534,880 | 30,290,728 |
| arrow→parquet | 161.2 | 663,929,072 | 10,617,681 |
| arrow→csv | 175.7 | 621,880,032 | 10,310,231 |
| arrow→tsv | 176.6 | 621,878,384 | 10,310,214 |
| arrow→arrow | 133.3 | 1,361,788,664 | 10,892,279 |

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
