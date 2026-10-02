# streamio

A Go library and CLI for converting between NDJSON, Parquet, CSV, TSV, and Arrow IPC at scale, built
for pipelines that move large datasets between storage and processing systems rather than one-off
scripting. Any format can be read and written, decoding and dispatch run concurrently on a shared
worker pool, and when the input and output formats match, bytes are streamed straight through with
no decode step at all — reliably processing files far larger than available memory, fast.

## Use cases

- Bulk-loading columnar files into a datastore.
- Converting exported NDJSON logs into columnar formats for analytics.
- Reshaping CSV/TSV exports into a format a downstream system expects.
- Any pipeline where the bottleneck is decode/encode throughput on multi-gigabyte files, not
  one-time convenience.

## Install

Prebuilt CLI binaries for Linux and macOS (amd64/arm64) and Windows (amd64) are on the
[releases page](https://github.com/AlexAslan/streamio/releases). Download the archive for your
platform, extract it, and put the `streamio` binary on your `PATH`.

To build from source instead:

```
go install github.com/AlexAslan/streamio/cmd@latest
```

## Getting started

Convert a file (output format defaults to `json`; input format is detected from the file
extension):

```
./streamio convert --in data.ndjson --out data.parquet --out-format parquet
```

`--out-format` accepts `json`, `parquet`, `csv`, `tsv`, or `arrow`. See `./streamio convert --help`
for every flag, including CSV/TSV delimiter and header options, batch/worker sizing, and
`--max-row-errors`.

As a library:

```
go get github.com/AlexAslan/streamio
```

```go
import "github.com/AlexAslan/streamio"

result, err := streamio.ProcessFile(ctx, "data.parquet", handler,
    streamio.WithOutputFormat(streamio.FormatJSON),
)
```

See [Configuration](docs/configuration.md) for the full `streamio.Option` reference.

## Docs

- [Architecture](docs/architecture.md) — entry point, routing, and the decode/dispatch pool.
- [Formats](docs/formats.md) — NDJSON, Parquet, and Arrow IPC read/write routes.
- [`record`/`formatio`](docs/record.md) — the generic cross-format seam every format package implements.
- [Configuration](docs/configuration.md) — `streamio.Option` reference and non-file-path input.
- [Benchmarks](docs/benchmarks.md) — measured throughput and memory across every format pair.
- [Package layout](docs/package-layout.md) — where everything lives.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues: [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE). Third-party dependency licenses are listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
