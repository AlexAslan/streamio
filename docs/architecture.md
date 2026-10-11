# Architecture

## Pipeline stages

Between decode and encode the generic path runs, per worker, any configured row filters
(`WithRowFilter`), record transforms (`WithRecordTransform`) and the declarative rename/drop rules,
in that order. Each worker builds its own stage instances from factories, so stages hold scratch
without locking. Rejected records are compacted out of the batch by swapping, which keeps every
record's reusable buffers in place for the decoder's next fill. A transform's output lives in a
per-worker `record.Arena` reset at the start of each batch. `ConsumeFile` reuses the same pool with
a consumer in place of the encoder, and `ProfileFile` is built on it. See
[stages.md](stages.md) for the contract.

## Input detection

Before routing, `ProcessFile` resolves two things from the file's first 4 KiB:

1. **Compression**: gzip (`1f 8b`), zstd (`28 b5 2f fd`), bzip2 (`BZh` + block magic), xz
   (`fd 37 7a 58 5a 00`), LZ4 frame (`04 22 4d 18`) and framed Snappy magic. Brotli has no magic
   number, so it is picked up from a `.br` extension (or `WithInputCompression`) only; a plain-text
   file that merely *looks* compressed by name is still read as plain text. A compressed input is
   decompressed once, up front, into memory (up to 64 MiB) or a temp file removed when the run
   ends. This keeps the `io.ReaderAt` every decoder relies on, so NDJSON chunking and Parquet row
   groups still run in parallel. `WithMaxDecompressedSize` guards against decompression bombs.
   Decompression itself is a single sequential stream, done before decoding starts.
2. **Format**: Parquet (`PAR1`) and Arrow (`ARROW1`) by magic number, NDJSON by a leading `{`, and
   CSV vs TSV by which delimiter appears a consistent number of times per line outside quotes.

The extension is only a tiebreaker for what content can't settle (an empty file, a single-column
CSV or TSV, text with no delimiter at all). When content and extension disagree, content wins
and the `Logger` gets a one-line note. Content that matches nothing, with no extension hint, fails
with `ErrUnknownFormat` instead of being guessed as NDJSON.

## Entry point

```go
result, err := streamio.ProcessFile(ctx, path, handler,
    streamio.WithParallelWorkers(4),
    streamio.WithInputFormat(streamio.FormatParquet),  // optional: default is content sniffing
    streamio.WithOutputFormat(streamio.FormatParquet), // default: streamio.FormatJSON
    streamio.WithTransforms(
        streamio.RenamePath("Timestamp", "@timestamp"),
        streamio.DropPath("debug.internal"),
    ),
)
```

`ProcessFile` compares the requested output format (`FormatJSON` unless `WithOutputFormat` says
otherwise) against the input file's own native format — either set explicitly with
`WithInputFormat`, or sniffed from its contents (see below) — and picks one of two routes:

```
              streamio.ProcessFile(ctx, path, handler, opts...)
                                 │
                    apply opts → cfg
              in := cfg.InputFormat or sniffed from contents
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

- **Raw passthrough** is the fast path: when the caller asks for the file's own native format, its
  bytes go to the handler untouched — no decode, no encode, no intermediate `record.Record`. NDJSON,
  Parquet, and CSV/TSV each have a raw route (`formatSupportFor`'s `newRawSource`); Arrow IPC doesn't
  — `ipc.FileWriter` has no verbatim-bytes splice — so a same-format Arrow run always takes the
  generic path instead. Raw bytes never become records, so a same-format run that configures a
  transform (`WithTransforms`) also takes the generic path: the rules must see decoded rows.
- **The generic path** takes every cross-format pair through `record.Record`: the input's
  `RecordDecoder` produces canonical rows, the output's `RecordEncoder` renders them, and the same
  worker pool raw passthrough uses drives both. A pair with no decoder for the input or no encoder
  for the output reports which half is missing, via `ErrNoConversionPath`. Every format implements
  both `RecordDecoder` and `RecordEncoder`, so every in→out pair across NDJSON, Parquet, CSV, TSV,
  and Arrow IPC is reachable, either via raw passthrough or the generic path.

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

- **decode workers** pull their next unit of work (a byte-range chunk for NDJSON/CSV/TSV, a row
  group for Parquet, a record batch for Arrow) from a shared counter, produce batches, and send them
  on a channel buffered to `cfg.Workers × config.DispatchQueueDepthPerWorker` — sized off the
  *dispatch* worker count so a burst of batches doesn't stall every decode worker waiting for one
  slow dispatcher.
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
  hands back a `[][]byte`, which is already the shape a dispatch worker consumes. `jsonio` returns
  one document per record; `parquetio`, `csvio`, and `arrowio` each return one document per batch,
  since a Parquet file, an Arrow IPC file, and (for streaming output) a CSV/TSV document all carry
  framing that spans many rows — one document per row would mean re-paying that framing cost for
  every single row. `cfg.BatchSize` therefore doubles as the row count per synthesized document for
  those formats, which is precisely what it already means everywhere else: "documents per send".
- **counting** reports two explicit numbers in `Result.Stats`: `RowsRead` and
  `DocumentsDispatched`. A path that dispatches exactly one item per row leaves `DecodeStats.Records`
  at zero and lets the pool derive rows from successful dispatches; a path whose dispatched item
  isn't one row sets `Records` itself. Parquet's raw path does that because one item there is a
  whole row group (`+= rowGroup.NumRows()`), and the record path does it unconditionally (`+= n`
  decoded records) because whether an item is one row or a whole batch is the encoder's choice.
  `DocumentsDispatched` increments only after the handler accepts a document.

## Profiling

`ProfileFile` reuses the generic path rather than adding a per-format implementation: it opens the
input's `RecordDecoder` and hands `pool.RunRecords` a `RecordEncoder` that, instead of rendering
documents, folds each batch into counts and returns none.

The pool already builds one encoder per decode worker, so each worker aggregates into its own
`profile.Collector` with no locking on the hot path; the collectors are merged once after the run.
Row-error handling, transforms and worker splitting all come from the pool unchanged.

Counting avoids allocation once a value has been seen: lookups use `m[string(b)]`, which the
compiler does not allocate for, and each map entry is a pointer so a repeat sighting increments in
place. Numbers and dates render into a reused scratch buffer through `jsonio`'s append helpers.
Only a first-seen value copies its key, and flat records resolve each field's column from a
per-position cache instead of hashing its path.

Memory is bounded per attribute by `WithProfileMaxDistinct`. A column counts exactly until it holds
that many distinct values, then switches to Space-Saving: the least-counted value is evicted for
each newcomer, which inherits that count, so frequent values always survive and a tracked value is
never under-counted. Each worker's eviction floor is kept so `Merge` can report, per value, how far
an estimate may be off (`ValueCount.MaxError`). `Merge` adopts the first worker's maps instead of
copying them and selects the top values with a bounded heap, so it costs O(distinct · log topN).
