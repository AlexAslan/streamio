# Writing a pipeline stage

> **Experimental.** The `record` package and the stage options below are the first public commitment
> to streamio's record model. They may change in a v0 release if building real stages shows they
> need to.

A stage runs on every decoded record, between the decoder and the encoder, inside the parallel
worker pool. There are three kinds:

| Kind | Interface | Use it to |
|---|---|---|
| Row filter | `streamio.RowFilter` | drop records (sampling, predicates) |
| Record transform | `streamio.RecordTransform` | rewrite a record (masking, hashing) |
| Record consumer | `streamio.RecordConsumer` | read records and produce no output (profiling, scans) |

```go
res, err := streamio.ProcessFile(ctx, "in.ndjson", handler,
    streamio.WithRowFilter(newFilter),          // before any transform
    streamio.WithRecordTransform(newTransform), // then rewrite
    streamio.WithTransforms(streamio.DropPath("debug")), // then rename/drop rules
)
```

Order is fixed: decode → row filters → record transforms → `WithTransforms` rules → encode. Within a
kind, stages run in the order they were added. A row any filter rejects is counted in
`Stats.RowsFiltered` and never reaches a transform or the encoder; `Stats.RowsRead` still counts it.

## Factories, not instances

Every option takes a *factory*, called once per decode worker. Each worker gets its own instance, so
a stage can keep scratch buffers or an HMAC state without any locking. Never share mutable state
between instances without your own synchronization.

```go
streamio.WithRowFilter(func() (streamio.RowFilter, error) {
    return &myFilter{buf: make([]byte, 0, 256)}, nil // one per worker
})
```

A factory error fails the run before any record is processed.

## The lifetime rule

A `record.Record` and everything it references (`Value.Str`, `Value.Map`, `Value.List`, field
names) belongs to the decoder and is **reused for the next batch**. Within a call you may read it
freely. Anything you want to keep afterwards must be copied (`string(v.Str)`).

## Writing output: use the arena

A transform that replaces a value's bytes must put the new bytes where they outlive the call, but
not longer than the batch. A single reused buffer is a bug: the next record's value overwrites the
previous record's, and every record in the batch ends up pointing at the last one.

`RecordTransform.Apply` receives a `*record.Arena`, reset by the pool at the start of every batch:

```go
func (t *tokenize) Apply(rec record.Record, arena *record.Arena) (record.Record, error) {
    for i := range rec {
        if rec[i].Name != "email" {
            continue
        }
        text, ok := rec[i].Value.AppendText(t.scratch[:0]) // canonical text, no allocation
        if !ok {
            continue // null or empty
        }
        t.scratch = text
        token := t.hash(text)                              // your own []byte
        rec[i].Value = record.Bytes(arena.Copy(token))     // lives until the batch is encoded
    }
    return rec, nil
}
```

`Value.AppendText` is the canonical rendering every converter agrees on: floats formatted like
`encoding/json`, dates and timestamps as ISO-8601 by their `Semantic`, `-0` as `0`. Hash or compare
that text rather than your own formatting, so `1`, `1.0` and `"1"` are one value across formats.

## Consumers and multi-pass work

`streamio.ConsumeFile` / `ConsumeReaderAt` run the same decode pool and give each worker's batches
to its own `RecordConsumer`, with no output. Merge the per-worker consumers yourself afterwards.
This is how `ProfileFile` is built, and how a stage that needs a first pass (an exact-size sample,
a set of matching group keys) gets its statistics. Input is always a `ReaderAt` (compressed input is
spooled), so a second pass is cheap.

## Allocation discipline

The hot path is allocation-free once a stage is warm, and a stage should keep it that way: reuse
scratch, use `AppendText` and `Arena`, and avoid `fmt`. Assert it with `testing.AllocsPerRun`.

## Options that apply record-level work disable raw passthrough

A same-format conversion normally streams raw bytes. Any filter, transform, or `WithTransforms`
rule needs decoded records, so configuring one makes the run take the record path.
