# `record`/`formatio`: the generic cross-format seam

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
