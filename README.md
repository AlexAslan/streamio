# streamio

Reads NDJSON, Parquet, CSV, TSV, and Arrow IPC files and pushes their documents to a caller-provided
document handler (a "document" is whatever unit the caller bulk-indexes or bulk-inserts downstream —
one JSON object, one Parquet row group, or one synthesized Parquet file, depending on route). The
caller chooses what format the documents come out in; when that matches the file's own native
format, the file's bytes go straight to the handler with no decode step at all. Decoding and dispatch
run concurrently across a shared worker pool so a slow handler doesn't stall decoding, and vice versa.

## Docs

- [Architecture](docs/architecture.md) — entry point, routing, and the decode/dispatch pool.
- [Formats](docs/formats.md) — NDJSON, Parquet, and Arrow IPC read/write routes.
- [`record`/`formatio`](docs/record.md) — the generic cross-format seam every format package implements.
- [Configuration](docs/configuration.md) — `streamio.Option` reference and non-file-path input.
- [Benchmarks](docs/benchmarks.md) — measured throughput and memory across every format pair.
- [Package layout](docs/package-layout.md) — where everything lives.
