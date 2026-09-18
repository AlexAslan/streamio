# `record`/`formatio`: the generic cross-format seam

Two small packages let a format join without touching any other format package.

- **`record`** defines the canonical, format-neutral row: `Record` (a reusable `[]Field`),
  `Field{Name, Value}`, and `Value` — a tagged union (`Kind` + fixed fields), not
  `map[string]any`. `Value` carries a `Semantic` alongside its `Kind` for values whose physical
  representation is ambiguous without it (an `int64` that's really a day count, or a timestamp's
  unit — `SemanticTimestampMillis`/`Micros`/`Nanos`). `SemanticFloat32` marks a `float64` widened
  from a 32-bit float, so it's rendered at the precision it actually carries.

  `KindMap` and `KindList` are the two nested Kinds a `Record` admits. `jsonio` and `arrowio` both
  produce and consume them; `csvio` and `parquetio` reject both.

- **`formatio`** defines every capability a format supplies: `RawSource` (a file's own bytes → the
  handler), `RecordDecoder` (a file → a stream of `record.Record`), the optional
  `SplittableRecordDecoder` (one open file → several concurrent decoders), `RecordEncoder` (a
  *batch* of `record.Record` → that format's documents), and the `DecodeStats` they fill. `pool` and
  `streamio` consume these; no format package imports another.

  The encoder interface has one batch-shaped method: `EncodeBatch(batch []record.Record) ([][]byte,
  error)`. A format decides by the length of what it returns whether it produces one document per
  record (`jsonio`) or one per batch (`parquetio`, `csvio`, `arrowio`).

- **`streamio.formatSupportFor`** (a `switch`, so the `exhaustive` linter flags an unwired
  `Format`) is the registry: for each `Format`, which of `newRawSource`/`newRecordDecoder`/
  `newRecordEncoder` it implements. A nil field means that capability doesn't exist for the format,
  and `processRecords` reports which half of a pair is missing via `ErrNoConversionPath`.
