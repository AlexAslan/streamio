# Formats

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
  Kinds (see [record.md](record.md)), so this Parquet encoder is where that scope boundary actually
  lands: nothing stops a `KindMap`/`KindList` record from reaching `NewEncoder` on a JSON→Parquet
  run, and it rejects it outright rather than inventing a representation — reconstructing a Parquet
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
