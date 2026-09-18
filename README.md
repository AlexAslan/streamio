# streamio

A Go library and CLI for converting between NDJSON, Parquet, CSV, TSV, and Arrow IPC. Any format can
be read and written, decoding and dispatch run concurrently on a shared worker pool, and when the
input and output formats match, bytes are streamed straight through with no decode step at all.

## Docs

- [Architecture](docs/architecture.md) — entry point, routing, and the decode/dispatch pool.
- [Formats](docs/formats.md) — NDJSON, Parquet, and Arrow IPC read/write routes.
- [`record`/`formatio`](docs/record.md) — the generic cross-format seam every format package implements.
- [Configuration](docs/configuration.md) — `streamio.Option` reference and non-file-path input.
- [Benchmarks](docs/benchmarks.md) — measured throughput and memory across every format pair.
- [Package layout](docs/package-layout.md) — where everything lives.
