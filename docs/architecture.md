# Architecture

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
