# Formats

## NDJSON: byte-range chunks

Native format is JSON, so `ProcessFile` always takes raw passthrough for it: a line of NDJSON is
already one JSON document.

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

Each worker claims the next unclaimed `cfg.ChunkSize`-byte range (a shared `atomic.Int64` counter),
opens an `io.SectionReader` over the shared file, resolves onto a line boundary, and reads to the
boundary crossing `chunkStart + ChunkSize`, batching `cfg.BatchSize` lines per send. CSV/TSV's raw
and record decoders use the same chunking shape.

## Parquet: row groups, two read routes

Both routes claim row groups via a shared `atomic.Int64` counter over `sharedState.rowGroups`, built
once per file (schema walk, `readerSlots` sized to `cfg.MaxOpenReaders`), bounding simultaneously-open
readers to `MaxOpenReaders × per-row-group dictionary size`, not `Workers ×` it.

```
row groups:  [ RG0 ][ RG1 ][ RG2 ][ RG3 ][ RG4 ][ RG5 ]
                │       │                          │
                ▼       ▼                          ▼
            worker A  worker B   ...           worker A (loops back once RG0 is done)
```

- **Record decoding** (`parquetio.NewDecoder`): reads each row group's rows into a reused
  `parquetgo.Row` buffer, classifying every column value — `Map(String,String)`, timestamps, dates,
  unsigned integers — via schema metadata built once per file. Implements
  `formatio.SplittableRecordDecoder`, capped at the row-group count.
- **Raw passthrough** (`parquetio.NewRawSource`): re-frames each claimed row group as a standalone
  Parquet file via `parquetgo.NewGenericWriter[any]`'s `WriteRowGroup`, which splices already-
  compressed column chunks through verbatim — no decode, no re-encode. One row group is one
  dispatched document.

### Parquet as a generic-path output

`parquetio.NewEncoder` turns one batch of `record.Record` into one standalone Parquet file, one
document per batch (`cfg.BatchSize` rows).

- Schema is derived from the first batch: field names/order from the first record, each column's
  type from its first non-null value anywhere in that batch. A later record with a different field
  name, order, or `Kind`/`Semantic` is an error.
- Every column is optional.
- `record.KindMap`/`record.KindList` are rejected outright — this encoder has no representation for
  a nested value.

Column order in the output is parquet-go's own name-sorted order, not the record's field order.

## Arrow IPC: record batches, one read route

Arrow IPC (`internal/arrowio`, `github.com/apache/arrow-go/v18`) is columnar and batch-oriented like
Parquet, but has no raw passthrough.

```
record batches:  [ RB0 ][ RB1 ][ RB2 ][ RB3 ][ RB4 ][ RB5 ]
                    │       │                          │
                    ▼       ▼                          ▼
                worker A  worker B   ...           worker A (loops back once RB0 is done)
```

- **Record decoding** (`arrowio.NewDecoder`): each worker claims the next record batch via a shared
  counter and reads it via `ipc.FileReader.RecordBatchAt`. Implements
  `formatio.SplittableRecordDecoder`, capped at the record-batch count.
- **No raw passthrough**: `ipc.FileWriter.Write` only accepts a live, already-decoded
  `arrow.RecordBatch` — no verbatim-bytes splice. Arrow-to-Arrow always takes the generic path.
- **`ipc.NewFileReader` needs `Seek`**, not just `ReadAt` — it locates its footer via
  `Seek(0, io.SeekEnd)`. `io.NewSectionReader(src.Reader, 0, src.Size)` satisfies this directly, no
  custom adapter needed.
- **Values are copied out, not held open.** Arrow's records are reference-counted
  (`Release()`-requiring); `recordDecoder.DecodeNext` copies every value into a `record.Value`
  immediately and releases the Arrow record, matching every other decoder's contract that
  `Value.Str`/`Map`/`List` are valid only until the next decode call. Not a zero-copy path.
- **`record.KindMap`/`record.KindList` are supported, both ways** — `record.KindMap` becomes an
  `arrow.StructType` column (not Arrow's `MapType`, which requires uniform key/value types),
  `record.KindList` becomes an `arrow.ListType` column, via `array.NewStructBuilder`/
  `array.NewListBuilder`. Nesting can go arbitrarily deep.

### Arrow as a generic-path output

Same three constraints as the Parquet encoder (schema from first batch, every column optional,
schema mismatch is an error), except nested Kinds are supported instead of rejected. `NewEncoder`
produces one Arrow IPC file per batch by default, or delegates to `NewStreamingEncoder` under
`WithSingleFileOutput(true)`.

`ipc.FileWriter` has no `Reset`/`Flush`; it tracks a running byte offset that must stay continuous
for the whole file. `streaming_encoder.go`'s `growingSink` (an `io.Writer` that appends to an
internal buffer and exposes `Drain()`) works around this: the writer sees one continuous destination
for the file's lifetime, while `EncodeBatch` still returns only the bytes written since the last
call, keeping peak memory bounded to one batch's encoded size.

## CSV/TSV: byte-range chunks, one raw route

`internal/csvio` implements CSV and TSV in one package, parameterized by delimiter. Chunking mirrors
NDJSON's. `csvio.NewRawSource` dispatches one row per document; `csvio.NewDecoder` decodes every
field as `record.KindBytes` — CSV has no type system of its own. `csvio.NewEncoder` renders one
document per batch, rejecting `record.KindMap`/`record.KindList` like the Parquet encoder.
